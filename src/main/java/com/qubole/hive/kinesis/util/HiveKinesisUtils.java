package com.qubole.hive.kinesis.util;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.mapred.JobConf;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import com.google.common.base.Strings;

public class HiveKinesisUtils {

    public static void validateConf(JobConf conf) {
       Validate.notEmpty(conf.get(Constants.STREAM_NAME),
               Constants.STREAM_NAME + " has to be specified");
       int batchSize = conf.getInt(Constants.BATCH_SIZE,
               Constants.DEFAULT_BATCH_SIZE);
       Validate.isTrue(batchSize <= Constants.MAX_PERMITTED_BATCH_SIZE,
               "Batch size cannot exceed " + Constants.MAX_PERMITTED_BATCH_SIZE);
       //if checkpointing enabled, table name, logical name
       //have to be specified
       if (conf.getBoolean(Constants.CHECKPOINTING_ENABLED, false)) {
           Validate.notEmpty(conf.get(Constants.DYNAMO_TABLE_NAME),
                   "Dynamo DB table name has to be specified if checkpointing is enabled");
           Validate.notEmpty(conf.get(Constants.CHECKPOINT_LOGICAL_NAME),
                   "Logical name has to be specified if checkpointing is enabled");
       }
    }
    
    public static AWSCredentialsProvider getCredentialsProvider(JobConf conf) {
        final String awsAccessKey;
        final String awsSecretKey;
        
        String accessKeyFromConf = conf.get(Constants.ACCESS_KEY);
        String secretKeyFromConf = conf.get(Constants.SECRET_KEY);
        
        if (Strings.isNullOrEmpty(accessKeyFromConf) || Strings.isNullOrEmpty(secretKeyFromConf)) {
            awsAccessKey = conf.get(Constants.S3_ACCESS_KEY);
            awsSecretKey = conf.get(Constants.S3_SECRET_KEY);
        } else {
            awsAccessKey = accessKeyFromConf;
            awsSecretKey = secretKeyFromConf;
        }

        return new AWSCredentialsProvider() {
            @Override
            public void refresh() {
            }
            
            @Override
            public AWSCredentials getCredentials() {
                return new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            }
        };
    }
}
