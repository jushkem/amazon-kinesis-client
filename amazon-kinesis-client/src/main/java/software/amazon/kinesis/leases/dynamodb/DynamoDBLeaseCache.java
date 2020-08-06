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

package software.amazon.kinesis.leases.dynamodb;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory cache for leases, to avoid frequent calls to scan the lease table.
 */
@RequiredArgsConstructor
public class DynamoDBLeaseCache {
    @NonNull
    private volatile Map<String, List<Lease>> cachedLeaseTable;
    private volatile Instant lastCacheUpdateTime = Instant.now();

    public final String SENTINEL_STREAM_NAME = "SENTINEL_STREAM_NAME";
    private final Duration MAX_LEASE_CACHE_STALENESS_DURATION = Duration.ofSeconds(10);

    /**
     * Fetches leases for a given stream identifier from the cache.
     * @param streamIdentifier stream name to search under.
     * @return list of leases, empty if one is not found.
     */
    public List<Lease> listLeasesForStream(StreamIdentifier streamIdentifier) {
        final String streamName = Optional.ofNullable(streamIdentifier).map(s -> s.serialize()).orElse(SENTINEL_STREAM_NAME);
        return Optional.ofNullable(cachedLeaseTable.get(streamName)).orElse(new ArrayList<>());
    }

    /**
     * Checks if leases for a given stream are present in the cache.
     *
     * @param streamIdentifier can be null.
     * @return true if cache contains a lease for the stream, false otherwise.
     */
    public boolean hasLeaseForStream(StreamIdentifier streamIdentifier) {
        final String streamName = Optional.ofNullable(streamIdentifier).map(s -> s.serialize()).orElse(SENTINEL_STREAM_NAME);
        return cachedLeaseTable.containsKey(streamName);
    }

    /**
     * Checks if the cached lease table is still within the max allowed staleness period.
     * @return true if cache is not stale, false otherwise.
     */
    public boolean cacheIsStillFresh() {
        final Duration secondsSinceLastCacheUpdate = Duration.between(lastCacheUpdateTime, Instant.now());
        return secondsSinceLastCacheUpdate.compareTo(MAX_LEASE_CACHE_STALENESS_DURATION) < 0;
    }

    /**
     * Update cached lease table with new leases pulled from DDB.
     * @param newLeases fresh leases to put into the cache.
     * @throws InvalidStateException
     */
    public void updateCachedLeaseTable(List<Lease> newLeases) throws InvalidStateException {
        cachedLeaseTable.clear();
        for (Lease l : newLeases) {

            final String streamName;
            if (l instanceof MultiStreamLease) {
                streamName = ((MultiStreamLease) l).streamIdentifier();
            } else if (l instanceof Lease) {
                streamName = SENTINEL_STREAM_NAME;
            } else {
                throw new InvalidStateException("Retrieved lease which was not a valid lease - " + l.toString());
            }

            final List<Lease> leasesForStream = cachedLeaseTable.getOrDefault(streamName, new ArrayList<>());
            leasesForStream.add(l);
        }
        lastCacheUpdateTime = Instant.now();
    }
}