package com.qubole.hive.kinesis;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;

import com.qubole.hive.kinesis.util.Constants;

public class HiveKinesisInputFormatTest {

    private KinesisProxy mockKinesisProxy;
    private JobConf mockJobConf;

    private String STR1 = "STREAM_NAME_1";
    private String SHARD1 = "SHARD_ID_1";
    private String SHARD2 = "SHARD_ID_2";
    private String SHARD3 = "SHARD_ID_3";

    @Before
    public void setUp() {
        mockKinesisProxy = createMock(KinesisProxy.class);
        mockJobConf = createNiceMock(JobConf.class);
    }

    private void expectConf() {
        expect(mockJobConf.get(Constants.STREAM_NAME))
                .andReturn(STR1).anyTimes();
        expect(mockJobConf.getInt(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE))
                .andReturn(5);
        expect(mockJobConf.get(Constants.ENDPOINT_REGION))
                .andReturn("kinesis-east-1").anyTimes();
        expect(mockJobConf.getBoolean(Constants.CHECKPOINTING_ENABLED, false))
                .andReturn(false).anyTimes();
        expect(mockJobConf.get(Constants.ACCESS_KEY))
                .andReturn("ACCESS_KEY").anyTimes();
        expect(mockJobConf.get(Constants.SECRET_KEY))
                .andReturn("SECRET_KEY").anyTimes();
        expect(mockJobConf.get("mapred.input.dir", ""))
                .andReturn("PATH").anyTimes();
    }

    @Test
    public void testGetSplits() throws IOException {
        expectConf();
        Set<String> shardIds = new HashSet<String>();
        shardIds.add(SHARD1);
        shardIds.add(SHARD2);
        shardIds.add(SHARD3);
        expect(mockKinesisProxy.getAllShardIds()).andReturn(shardIds);
        replay(mockKinesisProxy, mockJobConf);

        HiveKinesisInputFormat inputFormat = new HiveKinesisInputFormat();
        inputFormat.setKinesisProxy(STR1, mockKinesisProxy);
        InputSplit[] splits = inputFormat.getSplits(mockJobConf, 1000);
        assertEquals(3, splits.length);
        assertEquals(STR1, ((HiveKinesisInputSplit) splits[0]).getStreamName());
        assertEquals("PATH", ((HiveKinesisInputSplit) splits[0]).getPath().toString());

        List<String> shardIdsObtained = Arrays.asList(
                ((HiveKinesisInputSplit) splits[0]).getShardId(),
                ((HiveKinesisInputSplit) splits[1]).getShardId(),
                ((HiveKinesisInputSplit) splits[2]).getShardId());
        
        assertTrue(shardIdsObtained.contains(SHARD1));
        assertTrue(shardIdsObtained.contains(SHARD2));
        assertTrue(shardIdsObtained.contains(SHARD3));

        verify(mockKinesisProxy, mockJobConf);
    }
}
