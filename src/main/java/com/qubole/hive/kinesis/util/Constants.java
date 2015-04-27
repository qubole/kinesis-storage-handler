package com.qubole.hive.kinesis.util;

/**
 * Defines constants used by the storage handler.
 */
public class Constants {

    public static final String STREAM_NAME = "kinesis.stream.name";
    public static final String ENDPOINT_REGION = "kinesis.endpoint.region";
    public static final String DEFAULT_ENDPOINT_REGION = "us-east-1";
    public static final String ACCESS_KEY = "kinesis.accessKey";
    public static final String SECRET_KEY = "kinesis.secretKey";
    public static final String S3_ACCESS_KEY = "fs.s3.awsAccessKeyId";
    public static final String S3_SECRET_KEY = "fs.s3.awsSecretAccessKey";
    public static final String ITERATION_TIMEOUT_MIN = "kinesis.iteration.timeout";
    public static final int DEFAULT_ITERATION_TIMEOUT_MIN = 15;
    public static final String BATCH_SIZE = "kinesis.batch.size";
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final int MAX_PERMITTED_BATCH_SIZE = 10000;
    public static final String RETRY_MAXATTEMPTS = "kinesis.retry.maxattempts";
    public static final int DEFAULT_RETRY_MAXATTEMPTS = 3;
    public static final String RETRY_INTERVAL = "kinesis.retry.interval";
    public static final int DEFAULT_RETRY_INTERVAL = 1000; //milliseconds
    public static final String CHECKPOINTING_ENABLED = "kinesis.checkpoint.enabled";
    public static final String DYNAMO_TABLE_NAME = "kinesis.checkpoint.metastore.table.name";
    public static final String CHECKPOINT_LOGICAL_NAME = "kinesis.checkpoint.logical.name";
    public static final String DYNAMO_READ_CAPACITY = "kinesis.checkpoint.dynamo.read.capacity";
    public static final String DYNAMO_WRITE_CAPACITY = "kinesis.checkpoint.dynamo.write.capacity";
    public static final long DEFAULT_DYNAMO_READ_CAPACITY = 50L;
    public static final long DEFAULT_DYNAMO_WRITE_CAPACITY = 10L;
    public static final String CHECKPOINT_INTERVAL_MS = "kinesis.checkpoint.interval";
    public static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 60000; //checkpoint once every minute
    public static final String ITERATION_NUMBER = "kinesis.checkpoint.iteration.no";
}
