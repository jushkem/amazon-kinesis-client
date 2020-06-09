package com.amazonaws.services.kinesis.leases.impl;

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.LeasePendingDeletion;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.CollectionUtils;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Helper class to cleanup of any expired/closed shard leases. It will cleanup leases periodically as defined by
 * {@link KinesisClientLibConfiguration#leaseCleanupIntervalMillis()} upon worker shutdown, following a re-shard event or
 * a shard expiring from the service.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class LeaseCleanupManager {
    @NonNull
    private IKinesisProxy kinesisProxy;
    @NonNull
    private final ILeaseManager<KinesisClientLease> leaseManager;
    @NonNull
    private final ScheduledExecutorService deletionThreadPool;
    @NonNull
    private final IMetricsFactory metricsFactory;
    private final boolean cleanupLeasesUponShardCompletion;
    private final long leaseCleanupIntervalMillis;
    private final long completedLeaseCleanupThresholdMillis;
    private final long garbageLeaseCleanupThresholdMillis;

    private static final int MAX_RECORDS = 1;
    private static final long INITIAL_DELAY = 0L;
    private static final Log LOG = LogFactory.getLog(LeaseCleanupManager.class);

    private final Queue<LeasePendingDeletion> deletionQueue = new ConcurrentLinkedQueue<>();

    @Getter
    private volatile boolean isRunning = false;


    /**
     * Starts the lease cleanup thread, which is scheduled periodically as specified by
     * {@link LeaseCleanupManager#leaseCleanupIntervalMillis}
     */
    public void start() {
        LOG.debug("Starting lease cleanup thread.");
        deletionThreadPool.scheduleAtFixedRate(new LeaseCleanupThread(), INITIAL_DELAY, leaseCleanupIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Enqueues a lease for deletion.
     * @param leasePendingDeletion
     */
    public void enqueueForDeletion(LeasePendingDeletion leasePendingDeletion) {
        final KinesisClientLease lease = leasePendingDeletion.lease();
        if (lease == null) {
            LOG.warn("Cannot enqueue lease " + lease.getLeaseKey() + " for deferred deletion - instance doesn't hold " +
                    "the lease for that shard.");
        } else {
            if (!deletionQueue.contains(leasePendingDeletion)) {
                LOG.debug("Enqueuing lease " + lease.getLeaseKey() + " for deferred deletion.");
                deletionQueue.add(leasePendingDeletion);
            } else {
                LOG.warn("Lease " + lease.getLeaseKey() + " is already pending deletion, not enqueueing for deletion.");
            }
        }
    }

    /**
     * Returns how many leases are currently waiting in the queue pending deletion.
     * @return number of leases pending deletion.
     */
    public int leasesPendingDeletion() {
        return deletionQueue.size();
    }

    private boolean cleanupLease(LeasePendingDeletion leasePendingDeletion) throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final KinesisClientLease lease = leasePendingDeletion.lease();
        final ShardInfo shardInfo = leasePendingDeletion.shardInfo();

        boolean cleanedUpLease = false;
        boolean alreadyCheckedForGarbageCollection = false;

        if (leasePendingDeletion.pendingForLongerThan(completedLeaseCleanupThresholdMillis)) {
            if (cleanupLeasesUponShardCompletion) {
                Set<String> childShardIds = leaseManager.getLease(lease.getLeaseKey()).getChildShardIds();
                if (CollectionUtils.isNullOrEmpty(childShardIds)) {
                    try {
                        childShardIds = getChildShards(shardInfo);
                        updateLeaseWithChildShards(leasePendingDeletion, childShardIds);
                    } catch (ResourceNotFoundException e) {
                        return cleanupLeaseForGarbageShard(lease);
                    } finally {
                        alreadyCheckedForGarbageCollection = true;
                    }
                }
                cleanedUpLease = cleanupLeaseForCompletedShard(lease, shardInfo, childShardIds);
            }
        }

        if (!alreadyCheckedForGarbageCollection && leasePendingDeletion.pendingForLongerThan(garbageLeaseCleanupThresholdMillis)) {
            try {
                getChildShards(shardInfo);
            } catch (ResourceNotFoundException e) {
                cleanedUpLease = cleanupLeaseForGarbageShard(lease);
            }
        }

        return cleanedUpLease;
    }

    private Set<String> getChildShards(ShardInfo shardInfo) {
        final String shardIterator = kinesisProxy.getIterator(shardInfo.getShardId(), ShardIteratorType.LATEST.toString(), null);
        final GetRecordsResult getRecordsResult = kinesisProxy.get(shardIterator, MAX_RECORDS);

        return getRecordsResult.getChildShards().stream().map(c -> c.getShardId()).collect(Collectors.toSet());
    }


    // A lease that ended with SHARD_END from ResourceNotFoundException is safe to delete if it no longer exists in the
    // stream (known explicitly from ResourceNotFound being thrown when processing this shard),
    private boolean cleanupLeaseForGarbageShard(KinesisClientLease lease) throws DependencyException,
            ProvisionedThroughputException, InvalidStateException {
        LOG.info("Deleting lease for " + lease.getLeaseKey() + " as it is not present in the stream.");
        leaseManager.deleteLease(lease);
        return true;
    }

    private boolean allParentShardLeasesDeleted(KinesisClientLease lease)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        for (String parentShard : lease.getParentShardIds()) {
            final Lease parentLease = leaseManager.getLease(parentShard);

            if (parentLease != null) {
                LOG.warn("Lease for " + lease.getLeaseKey() + " has a parent lease " + parentLease.getLeaseKey() +
                        " which is still present in the lease table, skipping deletion for this lease.");
                return false;
            }
        }
        return true;
    }

    // We should only be deleting the current shard's lease if
    // 1. All of its children are currently being processed, i.e their checkpoint is not TRIM_HORIZON or AT_TIMESTAMP.
    // 2. Its parent shard lease(s) have already been deleted.
    private boolean cleanupLeaseForCompletedShard(KinesisClientLease lease, ShardInfo shardInfo, Set<String> childShardKeys)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException, IllegalStateException {
        final Set<String> processedChildShardLeases = new HashSet<>();

        for (String childShardKey : childShardKeys) {
            final KinesisClientLease childShardLease = leaseManager.getLease(childShardKey);

            if (childShardLease == null) {
                throw new IllegalStateException("Child lease " + childShardKey + " for completed shard not found in " +
                        "lease table - not cleaning up lease " + lease.getLeaseKey());
            }

            if (!childShardLease.getCheckpoint().equals(ExtendedSequenceNumber.TRIM_HORIZON)
                    && !childShardLease.getCheckpoint().equals(ExtendedSequenceNumber.AT_TIMESTAMP)) {
                processedChildShardLeases.add(childShardLease.getLeaseKey());
            }
        }

        if (!allParentShardLeasesDeleted(lease) || !Objects.equals(childShardKeys, processedChildShardLeases)) {
            return false;
        }

        LOG.info("Deleting lease " + lease.getLeaseKey() + " as it has been completely processed and processing " +
                "of child shard(s) has begun.");
        leaseManager.deleteLease(lease);

        return true;
    }

    private void updateLeaseWithChildShards(LeasePendingDeletion leasePendingDeletion, Set<String> childShardKeys)
            throws DependencyException, ProvisionedThroughputException, InvalidStateException {
        final ShardInfo shardInfo = leasePendingDeletion.shardInfo();
        final KinesisClientLease updatedLease = leasePendingDeletion.lease().copy();
        updatedLease.setChildShardIds(childShardKeys);

        final boolean updated = leaseManager.updateLease(updatedLease);
        if (!updated) {
            throw new InvalidStateException("Failed to update parent lease with child shard information for shard " + shardInfo.getShardId());
        }
    }

    private void cleanupLeases() {
        if (deletionQueue.isEmpty()) {
            LOG.debug("No leases pending deletion.");
        } else {
            final Queue<LeasePendingDeletion> failedDeletions = new ConcurrentLinkedQueue<>();
            LOG.debug("Attempting to clean up " + deletionQueue.size() + " lease(s).");

            while (!deletionQueue.isEmpty()) {
                final LeasePendingDeletion garbageLease = deletionQueue.poll();
                final String leaseKey = garbageLease.lease().getLeaseKey();
                boolean deletionFailed = true;
                try {
                    if (cleanupLease(garbageLease)) {
                        LOG.debug("Successfully cleaned up lease " + leaseKey + ".");
                        deletionFailed = false;
                    }
                } catch (Exception e) {
                    LOG.error("Failed to cleanup lease " + leaseKey + ".");
                }

                if (deletionFailed) {
                    LOG.debug("Did not cleanup lease " + leaseKey + ". Re-enqueueing for deletion.");
                    failedDeletions.add(garbageLease);
                }
            }

            deletionQueue.addAll(failedDeletions);
        }
    }

    private class LeaseCleanupThread implements Runnable {
        @Override
        public void run() {
            cleanupLeases();
        }
    }
}
