package com.qubole.hive.kinesis;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.qubole.hive.kinesis.util.Constants;

public class HiveKinesisRecordReaderTest {

    private KinesisProxy mockKinesisProxy;
    private JobConf mockConf;
    private AmazonDynamoDBClient mockDynamoDBClient;
    private HiveKinesisInputSplit mockSplit;
    private KinesisShardCheckpointer mockCheckpointer;

    private String STR1 = "STREAM_NAME_1";
    private String SHARD1 = "SHARD_ID_1";
    private int BATCH_SIZE = 4;
    private String STARTING_SEQ_NO = "-1";
    private String ITERATOR1 = "ITERATOR_1";
    private String ITERATOR2 = "ITERATOR_2";
    private String ITERATOR3 = "ITERATOR_3";
    private String ITERATOR4 = "ITERATOR_4";
    private String ITERATOR5 = "ITERATOR_5";
    private Record[] records;

    private Record createKinesisRecord(String key, String data, String seqNo) {
        return new Record()
                .withPartitionKey(key)
                .withData(ByteBuffer.wrap(data.getBytes()))
                .withSequenceNumber(seqNo);
    }

    @Before
    public void setUp() {
        mockKinesisProxy = createStrictMock(KinesisProxy.class);
        mockConf = createMock(JobConf.class);
        mockDynamoDBClient = createMock(AmazonDynamoDBClient.class);
        mockSplit = createMock(HiveKinesisInputSplit.class);
        mockCheckpointer = createMock(KinesisShardCheckpointer.class);
        records = new Record[6];
        for (int i = 0; i < 6; i++) {
            records[i] = createKinesisRecord(
                    "KEY_" + i,
                    "DATA_" + i,
                    String.valueOf(i));
        }
    }

    private void expectForInitialize(boolean checkpointingEnabled) {
        expect(mockSplit.getShardId()).andReturn(SHARD1);
        expect(mockSplit.getStreamName()).andReturn(STR1);
        expect(mockConf.getInt(Constants.BATCH_SIZE,
                        Constants.DEFAULT_BATCH_SIZE))
                .andReturn(4);
        expect(mockConf.getInt(Constants.ITERATION_TIMEOUT_MIN,
                        Constants.DEFAULT_ITERATION_TIMEOUT_MIN))
                .andReturn(Constants.DEFAULT_ITERATION_TIMEOUT_MIN);
        expect(mockConf.getInt(Constants.RETRY_MAXATTEMPTS,
                        Constants.DEFAULT_RETRY_MAXATTEMPTS))
                .andReturn(2);
        expect(mockConf.getInt(Constants.RETRY_INTERVAL,
                        Constants.DEFAULT_RETRY_INTERVAL))
                .andReturn(5);
        expect(mockConf.getBoolean(Constants.CHECKPOINTING_ENABLED, false))
                .andReturn(checkpointingEnabled);
        
        if (checkpointingEnabled) {
            expect(mockCheckpointer.getLastReadSeqNo())
                .andReturn(STARTING_SEQ_NO);
        }
    }

    @Test
    public void testNextWithoutCheckpoint() throws IOException {
        expectForInitialize(false);

        //first getIterator call should be TRIM_HORIZON
        expect(mockKinesisProxy.getIterator(SHARD1, "TRIM_HORIZON", null))
                .andReturn(ITERATOR1);
        expect(mockKinesisProxy.get(ITERATOR1, 4))
                .andReturn(new GetRecordsResult().withRecords(records[0])
                            .withNextShardIterator(ITERATOR2));

        //next call would be after records[0], no more records returned
        expect(mockKinesisProxy.getIterator(SHARD1, "AFTER_SEQUENCE_NUMBER", "0"))
                .andReturn(ITERATOR2);
        expect(mockKinesisProxy.get(ITERATOR2, 4))
                .andReturn(new GetRecordsResult().withNextShardIterator(ITERATOR3));

        //closed shard returned after one retry
        expect(mockKinesisProxy.get(ITERATOR3, 4))
                .andReturn(new GetRecordsResult());
        
        replay(mockKinesisProxy, mockSplit, mockConf, mockDynamoDBClient,
                mockCheckpointer);

        HiveKinesisRecordReader recordReader = new HiveKinesisRecordReader(
                mockKinesisProxy,
                mockSplit,
                mockConf,
                mockDynamoDBClient,
                mockCheckpointer);

        Text key = recordReader.createKey();
        Text val = recordReader.createValue();
        
        assertTrue(recordReader.next(key, val));
        assertEquals(key.toString(), "KEY_0");
        assertEquals(val.toString(), "DATA_0");

        assertFalse(recordReader.next(key, val));

        verify(mockKinesisProxy, mockSplit, mockConf, mockDynamoDBClient,
                mockCheckpointer);
    }

    @Test
    public void testNextWithCheckpoint() throws IOException {
        expectForInitialize(true);

        //first getIterator call should be after starting_seq
        //given by checkpointer
        expect(mockKinesisProxy.getIterator(SHARD1, "AFTER_SEQUENCE_NUMBER", STARTING_SEQ_NO))
                .andReturn(ITERATOR1);
        expect(mockKinesisProxy.get(ITERATOR1, 4))
                .andReturn(new GetRecordsResult()
                        .withRecords(records[0], records[1], records[2], records[3])
                        .withNextShardIterator(ITERATOR2));

        //next getIterator call should be after records[3]
        expect(mockKinesisProxy.getIterator(SHARD1, "AFTER_SEQUENCE_NUMBER", "3"))
                .andReturn(ITERATOR2);
        expect(mockKinesisProxy.get(ITERATOR2, 4))
                .andReturn(new GetRecordsResult()
                        .withRecords(records[4], records[5])
                        .withNextShardIterator(ITERATOR3));

        //next getIterator call should be after records[5]
        expect(mockKinesisProxy.getIterator(SHARD1, "AFTER_SEQUENCE_NUMBER", "5"))
                .andReturn(ITERATOR3);
        //no records returned this time
        expect(mockKinesisProxy.get(ITERATOR3, 4))
                .andReturn(new GetRecordsResult()
                        .withNextShardIterator(ITERATOR4));

        //two retries using nextIterator
        expect(mockKinesisProxy.get(ITERATOR4, 4))
                .andReturn(new GetRecordsResult()
                        .withNextShardIterator(ITERATOR5));
        expect(mockKinesisProxy.get(ITERATOR5, 4))
                .andReturn(new GetRecordsResult()
                        .withNextShardIterator(ITERATOR5));

        //each next call should call checkpointIfTimeUp() with last seq no
        for (int i = 0; i < 6; i++) {
            mockCheckpointer.checkpointIfTimeUp(String.valueOf(i));
            expectLastCall();
        }

        replay(mockKinesisProxy, mockSplit, mockConf, mockDynamoDBClient,
                mockCheckpointer);

        HiveKinesisRecordReader recordReader = new HiveKinesisRecordReader(
                mockKinesisProxy,
                mockSplit,
                mockConf,
                mockDynamoDBClient,
                mockCheckpointer);

        //6 records are returned. The batch size is 4.
        Text key = recordReader.createKey();
        Text val = recordReader.createValue();
        for (int i = 0; i < 6; i++) {
            boolean gotRecord = recordReader.next(key, val);
            assertTrue("Record should have been obtained", gotRecord);
            assertEquals("KEY_" + i, key.toString());
            assertEquals("DATA_" + i, val.toString());
        }

        //record reader should return no more records
        boolean gotRecord = recordReader.next(key, val);
        assertFalse("Record should not have been obtained", gotRecord);

        verify(mockKinesisProxy, mockSplit, mockConf, mockDynamoDBClient,
                mockCheckpointer);
    }

    @Test
    public void testClose() {
        expectForInitialize(true);
        mockCheckpointer.checkpoint(STARTING_SEQ_NO);
        expectLastCall();
        replay(mockSplit, mockConf, mockCheckpointer);

        HiveKinesisRecordReader recordReader = new HiveKinesisRecordReader(
                mockKinesisProxy,
                mockSplit,
                mockConf,
                mockDynamoDBClient,
                mockCheckpointer);
        recordReader.close();
        
        verify(mockSplit, mockConf, mockCheckpointer);
    }
}
