package com.qubole.hive.kinesis.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.mapred.JobConf;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

public class HiveKinesisUtilsTest {

    JobConf mockConf;

    private String ACCESS_KEY_1 = "ACCKEY1";
    private String SECRET_KEY_1 = "SECKEY1";
    private String MOCK_STR = "MOCK_STREAM";
    private String MOCK_TBL = "MOCK_TABLE";
    private String MOCK_LOGICAL = "MOCK_LOGICAL_NAME";

    @Before
    public void setUp() throws Exception {
        mockConf = createMock(JobConf.class);
    }

    @Test
    public void testGetCredentialsProvider() {
        expect(mockConf.get(Constants.ACCESS_KEY)).andReturn(ACCESS_KEY_1);
        expect(mockConf.get(Constants.SECRET_KEY)).andReturn(SECRET_KEY_1);
        replay(mockConf);

        AWSCredentials creds =
                HiveKinesisUtils.getCredentialsProvider(mockConf).getCredentials();

        assertEquals(ACCESS_KEY_1, creds.getAWSAccessKeyId());
        assertEquals(SECRET_KEY_1, creds.getAWSSecretKey());

        verify(mockConf);
    }

    @Test
    public void testGetCredentialsProviderWithEmptyUserProvidedCreds() {
        expect(mockConf.get(Constants.ACCESS_KEY)).andReturn(null);
        expect(mockConf.get(Constants.SECRET_KEY)).andReturn("");
        expect(mockConf.get(Constants.S3_ACCESS_KEY)).andReturn(ACCESS_KEY_1);
        expect(mockConf.get(Constants.S3_SECRET_KEY)).andReturn(SECRET_KEY_1);
        replay(mockConf);

        AWSCredentials creds =
                HiveKinesisUtils.getCredentialsProvider(mockConf).getCredentials();

        assertEquals(ACCESS_KEY_1, creds.getAWSAccessKeyId());
        assertEquals(SECRET_KEY_1, creds.getAWSSecretKey());

        verify(mockConf);
    }

    @Test
    public void testValidateConfWithValidConf() {
        expect(mockConf.get(Constants.STREAM_NAME)).andReturn(MOCK_STR);
        expect(mockConf.getInt(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE))
                .andReturn(Constants.DEFAULT_BATCH_SIZE);
        expect(mockConf.getBoolean(Constants.CHECKPOINTING_ENABLED, false))
                .andReturn(true);
        expect(mockConf.get(Constants.DYNAMO_TABLE_NAME)).andReturn(MOCK_TBL);
        expect(mockConf.get(Constants.CHECKPOINT_LOGICAL_NAME)).andReturn(MOCK_LOGICAL);
        replay(mockConf);

        HiveKinesisUtils.validateConf(mockConf);
        verify(mockConf);
    }

    @Test(expected = NullPointerException.class)
    public void testValidateConfWithNullStreamName() {
        expect(mockConf.get(Constants.STREAM_NAME)).andReturn(null);
        replay(mockConf);

        HiveKinesisUtils.validateConf(mockConf);
        verify(mockConf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfWithEmptyStreamName() {
        expect(mockConf.get(Constants.STREAM_NAME)).andReturn("");
        replay(mockConf);

        HiveKinesisUtils.validateConf(mockConf);
        verify(mockConf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfIllegalBatchSize() {
        expect(mockConf.get(Constants.STREAM_NAME)).andReturn(MOCK_STR);
        expect(mockConf.getInt(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE))
                .andReturn(12000);
        replay(mockConf);

        HiveKinesisUtils.validateConf(mockConf);
        verify(mockConf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfWithCheckpointingAndEmptyTableName() {
        expect(mockConf.get(Constants.STREAM_NAME)).andReturn("MOCK_STREAM");
        expect(mockConf.getInt(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE))
                .andReturn(Constants.DEFAULT_BATCH_SIZE);
        expect(mockConf.getBoolean(Constants.CHECKPOINTING_ENABLED, false)).andReturn(true);
        expect(mockConf.get(Constants.DYNAMO_TABLE_NAME)).andReturn("");
        replay(mockConf);

        HiveKinesisUtils.validateConf(mockConf);
        verify(mockConf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateConfWithCheckpointingAndEmptyLogicalName() {
        expect(mockConf.get(Constants.STREAM_NAME)).andReturn("MOCK_STREAM");
        expect(mockConf.getInt(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE))
                .andReturn(Constants.DEFAULT_BATCH_SIZE);
        expect(mockConf.getBoolean(Constants.CHECKPOINTING_ENABLED, false)).andReturn(true);
        expect(mockConf.get(Constants.DYNAMO_TABLE_NAME)).andReturn(MOCK_TBL);
        expect(mockConf.get(Constants.CHECKPOINT_LOGICAL_NAME)).andReturn("");
        replay(mockConf);

        HiveKinesisUtils.validateConf(mockConf);
        verify(mockConf);
    }
}
