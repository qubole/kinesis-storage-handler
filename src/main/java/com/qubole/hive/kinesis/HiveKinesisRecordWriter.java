package com.qubole.hive.kinesis;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

class HiveKinesisRecordWriter implements
        RecordWriter<Text, Text>,
        FileSinkOperator.RecordWriter {

    private static final Log LOG = LogFactory.getLog(HiveKinesisRecordWriter.class);

    private AmazonKinesis kinesis;
    private String streamName;

    public HiveKinesisRecordWriter(AmazonKinesis kinesis, String streamName) {
        if (kinesis == null) {
            throw new IllegalArgumentException("kinesis must not be null.");
        }
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("stream name must not be null or empty");
        }
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    private void sendKinesisRecord(ByteBuffer data) {
        boolean done = false;
        while (!done) {
            PutRecordRequest putReq = new PutRecordRequest();
            putReq.setStreamName(streamName);
            //random number is used as partition key
            //to ensure uniform partition between shards
            putReq.setPartitionKey(String.valueOf(Math.random())); 
            putReq.setSequenceNumberForOrdering(null);
            putReq.setData(data);
            try {
                kinesis.putRecord(putReq);
            } catch(ProvisionedThroughputExceededException ex) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Throughput exceeded. Sleeping for 10 ms and retrying");
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void write(Writable w) throws IOException {
        write((Text) null, (Text) w);
    }

    @Override
    public void write(Text key, Text value) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(value.getBytes());
        sendKinesisRecord(buffer);
    }

    @Override
    public void close(Reporter reporter) {
    }

    @Override
    public void close(boolean abort) {
    }
}
