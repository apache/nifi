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
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Shared-throughput Kinesis consumer that runs a continuous background fetch loop per shard.
 * Each owned shard gets its own virtual thread that repeatedly calls GetRecords and enqueues
 * results for the processor, mirroring the producer-consumer architecture of the KCL Scheduler.
 * This keeps data flowing between onTrigger invocations rather than fetching on-demand.
 *
 * <p>Concurrency is bounded by a semaphore with {@value #MAX_CONCURRENT_FETCHES} permits so
 * that at most that many GetRecords HTTP calls are in flight at any moment, preventing
 * connection-pool exhaustion. Additionally, when the shared result queue exceeds
 * {@value #MAX_QUEUED_RESULTS} entries the fetch loop sleeps until the processor drains results.
 */
final class PollingKinesisClient extends KinesisConsumerClient {

    private static final long DEFAULT_EMPTY_SHARD_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long DEFAULT_ERROR_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(2);
    static final int MAX_QUEUED_RESULTS = 500;
    static final int MAX_CONCURRENT_FETCHES = 50;

    private final ExecutorService fetchExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, PollingShardState> pollingShardStates = new ConcurrentHashMap<>();
    private final Semaphore fetchPermits = new Semaphore(MAX_CONCURRENT_FETCHES, true);
    private final long emptyShardBackoffNanos;
    private final long errorBackoffNanos;
    private volatile Instant timestampForInitialPosition;

    PollingKinesisClient(final KinesisClient kinesisClient, final ComponentLog logger) {
        this(kinesisClient, logger, DEFAULT_EMPTY_SHARD_BACKOFF_NANOS, DEFAULT_ERROR_BACKOFF_NANOS);
    }

    PollingKinesisClient(final KinesisClient kinesisClient, final ComponentLog logger,
            final long emptyShardBackoffNanos, final long errorBackoffNanos) {
        super(kinesisClient, logger);
        this.emptyShardBackoffNanos = emptyShardBackoffNanos;
        this.errorBackoffNanos = errorBackoffNanos;
    }

    void setTimestampForInitialPosition(final Instant timestamp) {
        this.timestampForInitialPosition = timestamp;
    }

    @Override
    void startFetches(final List<Shard> shards, final String streamName, final int batchSize,
            final String initialStreamPosition, final KinesisShardManager shardManager) {
        if (fetchExecutor.isShutdown()) {
            return;
        }

        for (final Shard shard : shards) {
            final String shardId = shard.shardId();
            final PollingShardState existing = pollingShardStates.get(shardId);
            if (existing == null) {
                final PollingShardState state = new PollingShardState();
                if (pollingShardStates.putIfAbsent(shardId, state) == null && state.tryStartLoop()) {
                    launchFetchLoop(state, shardId, streamName, batchSize, initialStreamPosition, shardManager);
                }
            } else if (!existing.isExhausted() && !existing.isStopped() && !existing.isLoopRunning()
                    && existing.tryStartLoop()) {
                logger.warn("Restarting dead fetch loop for shard {}", shardId);
                launchFetchLoop(existing, shardId, streamName, batchSize, initialStreamPosition, shardManager);
            }
        }
    }

    @Override
    boolean hasPendingFetches() {
        if (hasQueuedResults()) {
            return true;
        }
        for (final PollingShardState state : pollingShardStates.values()) {
            if (!state.isExhausted() && !state.isStopped()) {
                return true;
            }
        }
        return false;
    }

    @Override
    void acknowledgeResults(final List<ShardFetchResult> results) {
    }

    @Override
    void rollbackResults(final List<ShardFetchResult> results) {
        for (final ShardFetchResult result : results) {
            final PollingShardState state = pollingShardStates.get(result.shardId());
            if (state != null) {
                state.requestReset();
            }
        }
    }

    @Override
    void removeUnownedShards(final Set<String> ownedShards) {
        pollingShardStates.entrySet().removeIf(entry -> {
            if (!ownedShards.contains(entry.getKey())) {
                entry.getValue().stop();
                return true;
            }
            return false;
        });
    }

    @Override
    void logDiagnostics(final int ownedCount, final int cachedShardCount) {
        if (!shouldLogDiagnostics()) {
            return;
        }

        int active = 0;
        int exhausted = 0;
        int stopped = 0;
        int dead = 0;
        for (final PollingShardState state : pollingShardStates.values()) {
            if (state.isExhausted()) {
                exhausted++;
            } else if (state.isStopped()) {
                stopped++;
            } else if (!state.isLoopRunning()) {
                dead++;
            } else {
                active++;
            }
        }

        logger.debug("Kinesis polling diagnostics: discoveredShards={}, ownedShards={}, queueDepth={}, "
                + "fetchLoops={}, active={}, exhausted={}, stopped={}, dead={}, concurrentFetches={}",
                cachedShardCount, ownedCount, totalQueuedResults(), pollingShardStates.size(),
                active, exhausted, stopped, dead, MAX_CONCURRENT_FETCHES - fetchPermits.availablePermits());
    }

    @Override
    void close() {
        for (final PollingShardState state : pollingShardStates.values()) {
            state.stop();
        }
        pollingShardStates.clear();
        fetchExecutor.shutdownNow();
        super.close();
    }

    private void launchFetchLoop(final PollingShardState state, final String shardId,
            final String streamName, final int batchSize, final String initialStreamPosition,
            final KinesisShardManager shardManager) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            fetchExecutor.submit(() -> {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
                try {
                    runFetchLoop(state, shardId, streamName, batchSize, initialStreamPosition, shardManager);
                } catch (final Throwable t) {
                    if (!state.isStopped()) {
                        logger.error("Fetch loop for shard {} terminated unexpectedly", shardId, t);
                    }
                } finally {
                    state.markLoopStopped();
                }
            });
        } catch (final RejectedExecutionException e) {
            state.markLoopStopped();
            logger.debug("Executor shut down; cannot start fetch loop for shard {}", shardId);
        }
    }

    private void runFetchLoop(final PollingShardState state, final String shardId,
            final String streamName, final int batchSize, final String initialStreamPosition,
            final KinesisShardManager shardManager) {

        state.setIterator(getShardIterator(state, streamName, shardId, initialStreamPosition, shardManager));

        while (!Thread.currentThread().isInterrupted() && !state.isStopped()) {
            try {
                if (state.isExhausted()) {
                    return;
                }

                if (state.isResetRequested()) {
                    state.clearReset();
                    state.setIterator(getShardIterator(state, streamName, shardId, initialStreamPosition, shardManager));
                }

                if (state.getIterator() == null) {
                    state.setIterator(getShardIterator(state, streamName, shardId, initialStreamPosition, shardManager));
                    if (state.getIterator() == null) {
                        sleepNanos(errorBackoffNanos);
                        continue;
                    }
                }

                if (totalQueuedResults() >= MAX_QUEUED_RESULTS) {
                    sleepNanos(emptyShardBackoffNanos);
                    continue;
                }

                try {
                    fetchPermits.acquire();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                final GetRecordsResponse response;
                try {
                    response = fetchRecords(shardId, state, batchSize);
                } finally {
                    fetchPermits.release();
                }
                if (response == null) {
                    continue;
                }

                final List<software.amazon.awssdk.services.kinesis.model.Record> records = response.records();
                if (!records.isEmpty()) {
                    final long millisBehind = response.millisBehindLatest() != null ? response.millisBehindLatest() : -1;
                    enqueueResult(createFetchResult(shardId, records, millisBehind));
                }

                state.setIterator(response.nextShardIterator());
                if (state.getIterator() == null) {
                    state.markExhausted();
                    return;
                }

                if (records.isEmpty()) {
                    sleepNanos(emptyShardBackoffNanos);
                }
            } catch (final Exception e) {
                if (!state.isStopped()) {
                    logger.error("Unexpected error in fetch loop for shard {}; will retry", shardId, e);
                    state.setIterator(null);
                    sleepNanos(errorBackoffNanos);
                }
            }
        }
    }

    private GetRecordsResponse fetchRecords(final String shardId, final PollingShardState state, final int batchSize) {
        final GetRecordsRequest request = GetRecordsRequest.builder()
                .shardIterator(state.getIterator())
                .limit(batchSize)
                .build();

        try {
            return kinesisClient.getRecords(request);
        } catch (final software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
                       | software.amazon.awssdk.services.kinesis.model.LimitExceededException e) {
            logger.debug("GetRecords throttled for shard {}; will retry after backoff", shardId);
            sleepNanos(errorBackoffNanos);
            return null;
        } catch (final software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException e) {
            logger.info("Shard iterator expired for shard {}; will re-acquire", shardId);
            state.setIterator(null);
            sleepNanos(errorBackoffNanos);
            return null;
        } catch (final SdkClientException e) {
            if (!state.isStopped()) {
                logger.warn("GetRecords timed out for shard {}; will retry with existing iterator", shardId);
                sleepNanos(errorBackoffNanos);
            }
            return null;
        } catch (final Exception e) {
            if (!state.isStopped()) {
                logger.error("GetRecords failed for shard {}", shardId, e);
                state.setIterator(null);
                sleepNanos(errorBackoffNanos);
            }
            return null;
        }
    }

    private static void sleepNanos(final long nanos) {
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String getShardIterator(final PollingShardState state, final String streamName,
            final String shardId, final String initialStreamPosition, final KinesisShardManager shardManager) {
        final String lastSequenceNumber;
        try {
            lastSequenceNumber = shardManager.readCheckpoint(shardId);
        } catch (final Exception e) {
            if (!state.isStopped()) {
                logger.warn("Failed to read checkpoint for shard {}; will retry", shardId, e);
            }
            return null;
        }

        final ShardIteratorType iteratorType;
        final String startingSequenceNumber;
        final Instant timestamp;
        if (lastSequenceNumber != null) {
            iteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER;
            startingSequenceNumber = lastSequenceNumber;
            timestamp = null;
        } else {
            iteratorType = ShardIteratorType.fromValue(initialStreamPosition);
            startingSequenceNumber = null;
            timestamp = (iteratorType == ShardIteratorType.AT_TIMESTAMP) ? timestampForInitialPosition : null;
        }

        logger.debug("Getting shard iterator for shard {} with type={}, startingSeq={}, timestamp={}",
                shardId, iteratorType, startingSequenceNumber, timestamp);

        final GetShardIteratorRequest.Builder iteratorRequestBuilder = GetShardIteratorRequest.builder()
            .streamName(streamName)
            .shardId(shardId)
            .shardIteratorType(iteratorType);

        if (startingSequenceNumber != null) {
            iteratorRequestBuilder.startingSequenceNumber(startingSequenceNumber);
        }
        if (timestamp != null) {
            iteratorRequestBuilder.timestamp(timestamp);
        }

        try {
            fetchPermits.acquire();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }

        try {
            return kinesisClient.getShardIterator(iteratorRequestBuilder.build()).shardIterator();
        } catch (final Exception e) {
            if (!state.isStopped()) {
                logger.error("Failed to get shard iterator for shard {} (type={}, seq={})",
                        shardId, iteratorType, startingSequenceNumber, e);
            }
            return null;
        } finally {
            fetchPermits.release();
        }
    }

    static final class PollingShardState {
        private volatile String currentIterator;
        private volatile boolean shardExhausted;
        private volatile boolean stopped;
        private volatile boolean resetRequested;
        private final AtomicBoolean loopRunning = new AtomicBoolean();

        String getIterator() {
            return currentIterator;
        }

        void setIterator(final String iterator) {
            currentIterator = iterator;
        }

        boolean isExhausted() {
            return shardExhausted;
        }

        void markExhausted() {
            shardExhausted = true;
        }

        boolean isStopped() {
            return stopped;
        }

        void stop() {
            stopped = true;
        }

        boolean isResetRequested() {
            return resetRequested;
        }

        void requestReset() {
            resetRequested = true;
        }

        void clearReset() {
            resetRequested = false;
        }

        boolean tryStartLoop() {
            return loopRunning.compareAndSet(false, true);
        }

        void markLoopStopped() {
            loopRunning.set(false);
        }

        boolean isLoopRunning() {
            return loopRunning.get();
        }
    }
}
