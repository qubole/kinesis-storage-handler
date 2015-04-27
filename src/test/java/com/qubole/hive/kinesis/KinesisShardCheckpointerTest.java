package com.qubole.hive.kinesis;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;

public class KinesisShardCheckpointerTest {

    private String STR1 = "STREAM_NAME_1";
    private String SHARD1 = "SHARD_ID_1";
    private String LOGICAL1 = "LOGICAL_PROCESS_1";
    private String SEQ1 = "SEQ_NO_1";
    private String SEQ2 = "SEQ_NO_2";
    private long READ_CAP = 50L;
    private long WRITE_CAP = 10L;
    private long CHECKPOINT_INTERVAL = 10L;

    private KinesisShardCheckpointer checkpointer;
    private KinesisClientLeaseManager mockLeaseManager;

    @Before
    public void setUp() {
        mockLeaseManager = createMock(KinesisClientLeaseManager.class);
    }

    private void expectForInitialize() 
            throws DependencyException, ProvisionedThroughputException {
        expect(mockLeaseManager.createLeaseTableIfNotExists(READ_CAP, WRITE_CAP))
                .andReturn(true);
    }

    @Test
    public void testGetLastReadSeqNoZerothIteration()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        expectForInitialize();
        replay(mockLeaseManager);

        checkpointer = new KinesisShardCheckpointer(
                mockLeaseManager,
                STR1,
                SHARD1,
                LOGICAL1,
                0,
                CHECKPOINT_INTERVAL,
                READ_CAP,
                WRITE_CAP);

        String lastReadSeqNo =
                checkpointer.getLastReadSeqNo();

        //should be null, since iteration no. is 0
        assertNull(lastReadSeqNo);

        verify(mockLeaseManager);
    }

    @Test
    public void testGetLastReadSeqNoNonzeroIteration()
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        expectForInitialize();
        String expectedKey = new StringBuilder(LOGICAL1)
                .append("_").append(STR1)
                .append("_").append(SHARD1)
                .append("_").append(String.valueOf(2))
                .toString();
        KinesisClientLease expectedLease = new KinesisClientLease();
        expectedLease.setLeaseKey(expectedKey);
        expectedLease.setCheckpoint(SEQ1);
        expect(mockLeaseManager.getLease(expectedKey))
                .andReturn(expectedLease);
        replay(mockLeaseManager);

        checkpointer = new KinesisShardCheckpointer(
                mockLeaseManager,
                STR1,
                SHARD1,
                LOGICAL1,
                3,
                CHECKPOINT_INTERVAL,
                READ_CAP,
                WRITE_CAP);

        String lastReadSeqNo =
                checkpointer.getLastReadSeqNo();

        assertEquals(SEQ1, lastReadSeqNo);

        verify(mockLeaseManager);
    }

    @Test
    public void testCheckpoint() throws DependencyException, InvalidStateException,
            ProvisionedThroughputException {
        expectForInitialize();
        expect(mockLeaseManager.createLeaseIfNotExists(isA(KinesisClientLease.class)))
                .andReturn(true);
        expect(mockLeaseManager.updateLease(isA(KinesisClientLease.class)))
                .andReturn(true);
        replay(mockLeaseManager);

        checkpointer = new KinesisShardCheckpointer(
                mockLeaseManager,
                STR1, SHARD1, LOGICAL1, 3,
                CHECKPOINT_INTERVAL, READ_CAP, WRITE_CAP);

        //should not call checkpoint
        checkpointer.checkpointIfTimeUp("KEY");
        try {
            Thread.sleep(12);
        } catch (InterruptedException e) {}
        //should call checkpoint
        checkpointer.checkpointIfTimeUp("KEY");

        verify(mockLeaseManager);
    }
}
