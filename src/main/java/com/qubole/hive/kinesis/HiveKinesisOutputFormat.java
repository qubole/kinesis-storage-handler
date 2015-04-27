package com.qubole.hive.kinesis;

import java.io.IOException;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

import com.qubole.hive.kinesis.util.Constants;
import com.qubole.hive.kinesis.util.HiveKinesisUtils;

public class HiveKinesisOutputFormat implements
        OutputFormat<Text, Text>,
        HiveOutputFormat<Text, Text> {

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job)
            throws IOException {
    }

    @Override
    public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
            JobConf job, String name, Progressable progress) {
       
        HiveKinesisUtils.validateConf(job);

        String streamName = job.get(Constants.STREAM_NAME);
        
        AWSCredentialsProvider credentialsProvider =
                HiveKinesisUtils.getCredentialsProvider(job);
        
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setUserAgent(ClientConfiguration.DEFAULT_USER_AGENT + name);
        AmazonKinesis kinesis =
            new AmazonKinesisClient(credentialsProvider, clientConfig);

        return new HiveKinesisRecordWriter(kinesis, streamName);
    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
            Path finalOutPath, final Class<? extends Writable> valueClass,
            boolean isCompressed, Properties tableProperties, Progressable progress)
            throws IOException {
        return (FileSinkOperator.RecordWriter) getRecordWriter(null, jc,
                "OutputFormat", progress);
    }
}
