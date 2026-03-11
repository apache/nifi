/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.logging.ComponentLog;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base for Kinesis consumer clients. Provides per-shard result queues and
 * shard-claim infrastructure used by both polling (shared-throughput) and EFO
 * (enhanced-fan-out) implementations.
 *
 * <p>Results are stored in per-shard FIFO queues rather than a single shared queue.
 * This guarantees that results for the same shard are always consumed in enqueue order,
 * preventing out-of-order delivery when concurrent tasks cannot claim the same shard.
 */
abstract class KinesisConsumerClient {

    private static final long DIAGNOSTIC_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    protected final KinesisClient kinesisClient;
    protected final ComponentLog logger;
    private final Map<String, Queue<ShardFetchResult>> shardQueues = new ConcurrentHashMap<>();
    private final Map<String, Object> shardLocks = new ConcurrentHashMap<>();
    private final Semaphore resultNotification = new Semaphore(0);
    protected final Set<String> shardsInFlight = ConcurrentHashMap.newKeySet();

    private volatile long lastDiagnosticLogNanos;
    private volatile Instant timestampForInitialPosition;

    KinesisConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger) {
        this.kinesisClient = kinesisClient;
        this.logger = logger;
    }

    void setTimestampForInitialPosition(final Instant timestamp) {
        this.timestampForInitialPosition = timestamp;
    }

    Instant getTimestampForInitialPosition() {
        return timestampForInitialPosition;
    }

    void initialize(final KinesisAsyncClient asyncClient, final String streamName, final String consumerName) {
    }

    abstract void startFetches(List<Shard> shards, String streamName, int batchSize,
            String initialStreamPosition, KinesisShardManager shardManager);

    abstract boolean hasPendingFetches();

    abstract void acknowledgeResults(List<ShardFetchResult> results);

    abstract void rollbackResults(List<ShardFetchResult> results);

    abstract void removeUnownedShards(Set<String> ownedShards);

    abstract void logDiagnostics(int ownedCount, int cachedShardCount);

    Object getShardLock(final String shardId) {
        return shardLocks.computeIfAbsent(shardId, k -> new Object());
    }

    void close() {
        shardQueues.clear();
        shardLocks.clear();
        resultNotification.drainPermits();
        shardsInFlight.clear();
    }

    void enqueueResult(final ShardFetchResult result) {
        shardQueues.computeIfAbsent(result.shardId(), k -> new ConcurrentLinkedQueue<>()).add(result);
        resultNotification.release();
    }

    ShardFetchResult pollShardResult(final String shardId) {
        final Queue<ShardFetchResult> queue = shardQueues.get(shardId);
        final ShardFetchResult result = queue == null ? null : queue.poll();
        if (result != null) {
            onResultPolled();
        }
        return result;
    }

    protected void onResultPolled() {
    }

    int drainShardQueue(final String shardId) {
        final Queue<ShardFetchResult> queue = shardQueues.get(shardId);
        if (queue == null) {
            return 0;
        }
        int drained = 0;
        while (queue.poll() != null) {
            drained++;
        }
        return drained;
    }

    ShardFetchResult pollAnyResult(final long timeout, final TimeUnit unit) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadlineNanos) {
            for (final Queue<ShardFetchResult> queue : shardQueues.values()) {
                final ShardFetchResult result = queue.poll();
                if (result != null) {
                    onResultPolled();
                    return result;
                }
            }
            final long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            if (remainingMs <= 0) {
                break;
            }
            resultNotification.drainPermits();
            resultNotification.tryAcquire(Math.min(remainingMs, 100), TimeUnit.MILLISECONDS);
        }
        return null;
    }

    List<String> getShardIdsWithResults() {
        final List<String> ids = new ArrayList<>();
        for (final Map.Entry<String, Queue<ShardFetchResult>> entry : shardQueues.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                ids.add(entry.getKey());
            }
        }
        return ids;
    }

    boolean awaitResults(final long timeout, final TimeUnit unit) throws InterruptedException {
        resultNotification.drainPermits();
        return resultNotification.tryAcquire(timeout, unit);
    }

    int totalQueuedResults() {
        int total = 0;
        for (final Queue<ShardFetchResult> queue : shardQueues.values()) {
            total += queue.size();
        }
        return total;
    }

    boolean hasQueuedResults() {
        for (final Queue<ShardFetchResult> queue : shardQueues.values()) {
            if (!queue.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    boolean claimShard(final String shardId) {
        return shardsInFlight.add(shardId);
    }

    void releaseShards(final Collection<String> shardIds) {
        shardsInFlight.removeAll(shardIds);
    }

    protected static ShardFetchResult createFetchResult(final String shardId, final List<Record> records, final long millisBehindLatest) {
        return new ShardFetchResult(shardId, ProducerLibraryDeaggregator.deaggregate(shardId, records), millisBehindLatest);
    }

    long drainDeduplicatedEventCount() {
        return 0;
    }

    protected boolean shouldLogDiagnostics() {
        final long now = System.nanoTime();
        if (now < DIAGNOSTIC_INTERVAL_NANOS + lastDiagnosticLogNanos) {
            return false;
        }
        lastDiagnosticLogNanos = now;
        return true;
    }
}
