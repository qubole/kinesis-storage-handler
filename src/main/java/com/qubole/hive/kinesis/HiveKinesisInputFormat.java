package com.qubole.hive.kinesis;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import com.qubole.hive.kinesis.util.Constants;
import com.qubole.hive.kinesis.util.HiveKinesisUtils;

public class HiveKinesisInputFormat implements InputFormat<Text, Text> {
   
    private static final Log LOG = LogFactory.getLog(HiveKinesisInputFormat.class);

    public HiveKinesisInputFormat() {
    }

    //kinesis proxy for each stream name
    private Map<String, KinesisProxy> proxies
            = new HashMap<String, KinesisProxy>();

    private AmazonDynamoDBClient dynamoDBClient = null;

    private void initializeKinesisProxy(JobConf conf) {
        String streamName = conf.get(Constants.STREAM_NAME);
        if (!proxies.containsKey(streamName)) {
            String endPointRegion = conf.get(Constants.ENDPOINT_REGION);
            if (endPointRegion == null) {
                endPointRegion = Constants.DEFAULT_ENDPOINT_REGION;
            }
            String endPoint = "kinesis." + endPointRegion + ".amazonaws.com";
            AWSCredentialsProvider credentialsProvider =
                    HiveKinesisUtils.getCredentialsProvider(conf);
            KinesisProxy proxy = new KinesisProxy(streamName,
                    credentialsProvider, endPoint);
            proxies.put(streamName, proxy);
        }
    }
        
    @Override
    public org.apache.hadoop.mapred.RecordReader<Text, Text>
            getRecordReader(org.apache.hadoop.mapred.InputSplit split,
                    JobConf job, Reporter reporter) {

        initializeKinesisProxy(job);
        KinesisProxy proxy = proxies.get(job.get(Constants.STREAM_NAME));
        HiveKinesisRecordReader recordReader;
        
        if (job.getBoolean(Constants.CHECKPOINTING_ENABLED, false)) {
            
            //if dynamoDBClient is null, initialize it
            if (dynamoDBClient == null) {
                dynamoDBClient = new AmazonDynamoDBClient(
                        HiveKinesisUtils.getCredentialsProvider(job));
            }
            recordReader =
                new HiveKinesisRecordReader(proxy, (HiveKinesisInputSplit) split,
                        job, dynamoDBClient);
        } else {
            recordReader =
                new HiveKinesisRecordReader(proxy, (HiveKinesisInputSplit) split, job);
        }
        return recordReader;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) 
            throws IOException {

        //numSplits is ignored. Each shard is a split.
        HiveKinesisUtils.validateConf(job);
        initializeKinesisProxy(job);
        String streamName = job.get(Constants.STREAM_NAME);
        KinesisProxy kinesisProxy = proxies.get(streamName);
        Set<String> shards = kinesisProxy.getAllShardIds();
        if (shards == null) {
            throw new IOException("Stream not in ACTIVE or UPDATING state");
        }
        InputSplit[] result = 
                new HiveKinesisInputSplit[shards.size()];
        int i = 0;
        for (String shard : shards) {
            result[i++] = new HiveKinesisInputSplit(streamName,
                    shard, job);
        }
        return result;
    }

    //package access for testing
    void setKinesisProxy(String streamName, KinesisProxy proxy) {
        proxies.put(streamName, proxy);
    }
}
