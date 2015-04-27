package com.qubole.hive.kinesis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;

public class KinesisShardCheckpointer {
    
    private KinesisClientLeaseManager leaseManager;
    private KinesisClientLease lease;
    private long nextCheckpointTimeMs;
    private long checkpointIntervalMs;
    private String streamName;
    private String shardId;
    private String logicalProcessName;
    private int curIterationNumber;
    private long dynamoReadCapacity;
    private long dynamoWriteCapacity;

    private static final Log LOG = LogFactory.getLog(KinesisShardCheckpointer.class);

    private void reportExceptionToLog(Exception e) {
        if (LOG.isInfoEnabled()) {
            LOG.info(e.getMessage());
        }
    }

    public KinesisShardCheckpointer(
            AmazonDynamoDBClient dynamoDBClient,
            String dynamoDBTable,
            String streamName,
            String shardId,
            String logicalProcessName,
            int curIterationNumber,
            long checkpointIntervalMs,
            long dynamoReadCapacity,
            long dynamoWriteCapacity) {

        this(new KinesisClientLeaseManager(dynamoDBTable, dynamoDBClient),
                streamName,
                shardId,
                logicalProcessName,
                curIterationNumber,
                checkpointIntervalMs,
                dynamoReadCapacity,
                dynamoWriteCapacity);
    }

    //package access for testing
    KinesisShardCheckpointer(
            KinesisClientLeaseManager leaseManager,
            String streamName,
            String shardId,
            String logicalProcessName,
            int curIterationNumber,
            long checkpointIntervalMs,
            long dynamoReadCapacity,
            long dynamoWriteCapacity) {

        this.leaseManager = leaseManager;
        this.streamName = streamName;
        this.shardId = shardId;
        this.logicalProcessName = logicalProcessName;
        this.curIterationNumber = curIterationNumber;
        this.checkpointIntervalMs = checkpointIntervalMs;
        
        try {
            this.leaseManager.createLeaseTableIfNotExists(
                    dynamoReadCapacity, dynamoWriteCapacity);
        } catch (DependencyException e) {
            reportExceptionToLog(e);
        } catch (ProvisionedThroughputException e) {
            reportExceptionToLog(e);
        }

        lease = new KinesisClientLease();
        lease.setLeaseKey(createCheckpointKey(curIterationNumber));

        resetNextCheckpointTime();
    }

    private void resetNextCheckpointTime() {
        nextCheckpointTimeMs = System.currentTimeMillis() + checkpointIntervalMs;
    }

    public void initialize() {
    }

    private String createCheckpointKey(int iterationNo) {
        return new StringBuilder(logicalProcessName)
                .append("_")
                .append(streamName)
                .append("_")
                .append(shardId)
                .append("_")
                .append(String.valueOf(iterationNo))
                .toString();
    }

    public String getLastReadSeqNo() {
        //return checkpoint of previous iteration if found

        String lastReadSeqNo = null;
        KinesisClientLease oldLease = null;
        if (curIterationNumber > 0) {
            try {
                oldLease = leaseManager.getLease(createCheckpointKey(curIterationNumber - 1));
            } catch (DependencyException e) {
                reportExceptionToLog(e);
            } catch (InvalidStateException e) {
                reportExceptionToLog(e);
            } catch (ProvisionedThroughputException e) {
                reportExceptionToLog(e);
            }
            if (oldLease != null) {
                lastReadSeqNo = oldLease.getCheckpoint();
            }
        }
        if (LOG.isInfoEnabled()) {
            if (lastReadSeqNo == null) {
                LOG.info("Previous checkpoint not found. Starting from beginning of shard");
            } else {
                LOG.info("Resuming from " + lastReadSeqNo);
            }
        }
        return lastReadSeqNo;
    }

    public void checkpoint(String lastReadSeqNo) {
        LOG.info(new StringBuilder("Trying to checkpoint at").append(lastReadSeqNo).toString());
        try {
            lease.setCheckpoint(lastReadSeqNo);
            leaseManager.createLeaseIfNotExists(lease);
            if (!leaseManager.updateLease(lease)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Checkpointing unsuccessful");
                }
            }
        } catch (DependencyException e) {
            reportExceptionToLog(e);
        } catch (InvalidStateException e) {
            reportExceptionToLog(e);
        } catch (ProvisionedThroughputException e) {
            reportExceptionToLog(e);
        }
        resetNextCheckpointTime();
    }

    public void checkpointIfTimeUp(String lastReadSeqNo) {
        if (System.currentTimeMillis() >= nextCheckpointTimeMs) {
            checkpoint(lastReadSeqNo);
        }
    }
}
