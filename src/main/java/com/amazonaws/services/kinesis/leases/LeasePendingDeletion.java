package com.amazonaws.services.kinesis.leases;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * Helper class for cleaning up leases.
 */
@Accessors(fluent = true)
@Value
@EqualsAndHashCode(exclude = {"queueEntryTime"})
public class LeasePendingDeletion {
    private final KinesisClientLease lease;
    private final ShardInfo shardInfo;
    private final long queueEntryTime = System.currentTimeMillis();

    public boolean pendingForLongerThan(long threshold) {
        return System.currentTimeMillis() - queueEntryTime > threshold;
    }
}

