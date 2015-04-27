package com.qubole.hive.kinesis;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

import com.qubole.hive.kinesis.util.Constants;

public class HiveKinesisRecordReader implements RecordReader<Text, Text> {

    private int batchSize;
    private String streamName;
    private String shardId;
    private KinesisProxy kinesisProxy;
    private String shardIterator;
    private String endSeqNo = null;
    private String lastReadSeqNo = null;
    private List<Record> fetchedRecords = Collections.emptyList();
    private int positionToReadInFetchedRecords = 0;
    private boolean hasFinished = false;
    private boolean shardClosed = false;
    private Text currentKey = null;
    private Text currentValue = null;
    private DateTime iterationTimeout;
    private int retryMaxAttempts;
    private int retryInterval;
    private long nextCheckpointTimeMs = 0L;
    private boolean checkpointingEnabled;
    private KinesisShardCheckpointer checkpointer = null;

    private static final Log LOG = LogFactory.getLog(HiveKinesisRecordReader.class);

    public HiveKinesisRecordReader(KinesisProxy kinesisProxy,
            HiveKinesisInputSplit split, JobConf conf) {
        this(kinesisProxy, split, conf, (AmazonDynamoDBClient) null);
    }

    public HiveKinesisRecordReader(
            KinesisProxy kinesisProxy,
            HiveKinesisInputSplit split,
            JobConf conf,
            AmazonDynamoDBClient dynamoDBClient) {
        this(kinesisProxy,
                split,
                conf,
                dynamoDBClient,
                (KinesisShardCheckpointer) null);
    }

    //package access for testing
    HiveKinesisRecordReader(
            KinesisProxy kinesisProxy,
            HiveKinesisInputSplit split,
            JobConf conf,
            AmazonDynamoDBClient dynamoDBClient,
            KinesisShardCheckpointer checkpointer) {
        this.kinesisProxy = kinesisProxy;
        this.checkpointer = checkpointer;
        initialize(split, conf, dynamoDBClient);
    }

    private void initialize(HiveKinesisInputSplit split, JobConf conf,
            AmazonDynamoDBClient dynamoDBClient) {

        shardId = split.getShardId();
        streamName = split.getStreamName();

        //number of records to get at a time from the kinesis stream
        batchSize = conf.getInt(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE);
        
        //get iteration timeout (min) from conf
        //and set the timestamp at which timeout will occur
        int timeoutMin = conf.getInt(Constants.ITERATION_TIMEOUT_MIN,
                Constants.DEFAULT_ITERATION_TIMEOUT_MIN);
        DateTime cur = new DateTime();
        iterationTimeout = cur.plus(Duration.standardMinutes(timeoutMin));
        
        retryMaxAttempts = conf.getInt(Constants.RETRY_MAXATTEMPTS,
                Constants.DEFAULT_RETRY_MAXATTEMPTS);
        retryInterval = conf.getInt(Constants.RETRY_INTERVAL,
                Constants.DEFAULT_RETRY_INTERVAL);

        //get checkpointing info. from conf, and initialize checkpointer
        //if checkpointing is enabled
        checkpointingEnabled = conf.getBoolean(Constants.CHECKPOINTING_ENABLED, false);
        if (checkpointingEnabled) {
            if (checkpointer == null) {
                long checkpointIntervalMs = conf.getLong(Constants.CHECKPOINT_INTERVAL_MS,
                        Constants.DEFAULT_CHECKPOINT_INTERVAL_MS);
                int iterationNumber = conf.getInt(Constants.ITERATION_NUMBER, 0);
                String dynamoDBTable = conf.get(Constants.DYNAMO_TABLE_NAME);
                String logicalProcessName = conf.get(Constants.CHECKPOINT_LOGICAL_NAME);
                long readCapacity = conf.getLong(Constants.DYNAMO_READ_CAPACITY,
                        Constants.DEFAULT_DYNAMO_READ_CAPACITY);
                long writeCapacity = conf.getLong(Constants.DYNAMO_WRITE_CAPACITY,
                        Constants.DEFAULT_DYNAMO_WRITE_CAPACITY);
                checkpointer = new KinesisShardCheckpointer(
                        dynamoDBClient,
                        dynamoDBTable,
                        streamName,
                        shardId,
                        logicalProcessName,
                        iterationNumber,
                        checkpointIntervalMs,
                        readCapacity,
                        writeCapacity);
            }
            lastReadSeqNo = checkpointer.getLastReadSeqNo();
        }
    }

    private void getRecordsFromKinesis() throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("Trying to get next set of records from kinesis.");
        }
        try {
            if (lastReadSeqNo == null) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("This is the first set of records.");
                }
                shardIterator = kinesisProxy.getIterator(shardId, "TRIM_HORIZON", null);
            } else {
                shardIterator = kinesisProxy.getIterator(shardId,
                        "AFTER_SEQUENCE_NUMBER", lastReadSeqNo);
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        GetRecordsResult result;
        result = kinesisProxy.get(shardIterator, batchSize);
        fetchedRecords = result.getRecords();

        //If no records obtained, sleep and retry until we run out of attempts
        int retriesLeft = retryMaxAttempts;
        while (fetchedRecords.size() == 0 && retriesLeft > 0) {
            retriesLeft--;
            try {
                Thread.sleep(retryInterval);
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Interrupted state.");
                }
            }
            if (iterationTimeout.isBeforeNow()) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Timed out while retrying fetch");
                }
                return;
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("No records found in shard. Trying again.");
            }
            shardIterator = result.getNextShardIterator();
            if (shardIterator == null) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Found closed shard. Terminating iteration.");
                }
                shardClosed = true;
                return;
            }
            result = kinesisProxy.get(shardIterator, batchSize);
            fetchedRecords = result.getRecords();
        }
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {
        if (hasFinished) {
            return false;
        }
        if (fetchedRecords.size() == 0 ||
                positionToReadInFetchedRecords >= fetchedRecords.size()) {
            //records haven't been fetched yet, or fetched records have been exhausted
            //next batch of records is now fetched from the stream
            getRecordsFromKinesis();
            positionToReadInFetchedRecords = 0;
        }
        if (iterationTimeout.isBeforeNow()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Closing iteration due to timeout");
            }
            return false;
        }
        if (shardClosed || fetchedRecords.size() == 0) {
            return false;
        }
        Record rec = (Record) fetchedRecords.get(positionToReadInFetchedRecords++);
        key.set(rec.getPartitionKey());
        value.set(rec.getData().array());
        lastReadSeqNo = rec.getSequenceNumber();
        if (checkpointingEnabled) {
            checkpointer.checkpointIfTimeUp(lastReadSeqNo);
        }
        return true;
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public void close() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Iteration completed.");
        }
        if (checkpointingEnabled && lastReadSeqNo != null) {
            checkpointer.checkpoint(lastReadSeqNo);
        }
    }

    @Override
    public float getProgress() {
        return 0.0f;
    }

    @Override
    public long getPos() {
        return 0L;
    }
}
