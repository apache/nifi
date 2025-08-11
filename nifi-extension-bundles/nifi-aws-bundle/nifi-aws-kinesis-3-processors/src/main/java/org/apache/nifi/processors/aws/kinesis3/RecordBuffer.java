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
package org.apache.nifi.processors.aws.kinesis3;

import jakarta.annotation.Nullable;
import org.apache.nifi.logging.ComponentLog;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.emptyList;

/**
 * RecordBuffer keeps track of all created Shard buffers, including exclusive read access via leasing,
 * and memory consumption tracking. It acts as the main interface between KCL callbacks and
 * the {@link ConsumeKinesis3Stream} processor, routing events to appropriate ShardBuffers and ensuring
 * thread-safe operations.
 */
final class RecordBuffer {

    private final ComponentLog logger;

    private final BlockingMemoryTracker memoryTracker;

    private final AtomicInteger bufferIdCounter = new AtomicInteger(0);
    private final ConcurrentMap<ShardBufferId, ShardBuffer> shardBuffers = new ConcurrentHashMap<>();

    /**
     * A queue with shard buffers that are available for consumption.
     * Note: when a buffer is invalidated it is NOT removed from the queue.
     */
    private final BlockingQueue<ShardBufferId> buffersWithData = new LinkedBlockingQueue<>();

    RecordBuffer(final ComponentLog logger, final long maxMemoryBytes) {
        this.logger = logger;
        this.memoryTracker = new BlockingMemoryTracker(logger, maxMemoryBytes);
    }

    // ========== Methods called from Kinesis Client Library ==========

    ShardBufferId createBuffer(final String shardId) {
        final ShardBufferId id = new ShardBufferId(shardId, bufferIdCounter.getAndIncrement());

        logger.info("Creating new buffer for shard {} with id {}", shardId, id);

        shardBuffers.put(id, new ShardBuffer(logger));
        return id;
    }

    void addRecords(final ShardBufferId bufferId, final List<KinesisClientRecord> records, final RecordProcessorCheckpointer checkpointer) {
        if (records.isEmpty()) {
            return;
        }

        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot add records with sequence and subsequence numbers: {}.{} - {}.{}",
                    bufferId,
                    records.getFirst().sequenceNumber(),
                    records.getFirst().subSequenceNumber(),
                    records.getLast().sequenceNumber(),
                    records.getLast().subSequenceNumber());
            return;
        }

        memoryTracker.addRecordsMemory(records);
        final ShardBufferOfferResult result = buffer.offer(records, checkpointer);
        switch (result) {
            case ShardBufferOfferResult.Added r -> {
                logger.debug("Successfully added records with sequence and subsequence numbers: {}.{} - {}.{} to buffer {}",
                        records.getFirst().sequenceNumber(),
                        records.getFirst().subSequenceNumber(),
                        records.getLast().sequenceNumber(),
                        records.getLast().subSequenceNumber(),
                        buffer);

                if (r.bufferWasEmpty()) {
                    logger.debug("Making the buffer {} available for lease, as it has been added new records", bufferId);
                    buffersWithData.add(bufferId);
                }
            }
            case ShardBufferOfferResult.BufferInvalidated __ -> {
                // If the buffer was already invalidated, we should free memory for these records.
                logger.debug("Buffer with id {} was invalidated. Cannot add records with sequence and subsequence numbers: {}.{} - {}.{}",
                        bufferId,
                        records.getFirst().sequenceNumber(),
                        records.getFirst().subSequenceNumber(),
                        records.getLast().sequenceNumber(),
                        records.getLast().subSequenceNumber());
                memoryTracker.freeRecordsMemory(records);
            }
        }
    }

    /**
     * Called when a shard ends - marks the buffer as ended but keeps it for final processing.
     */
    void finishConsumption(final ShardBufferId bufferId, final RecordProcessorCheckpointer checkpointer) {
        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot checkpoint last record", bufferId);
            return;
        }

        logger.info("Finishing consumption for buffer {}. Checkpointing last record", bufferId);
        buffer.finishRecordConsumption(checkpointer);

        logger.debug("Removing buffer with id {} after successful last record checkpoint", bufferId);
        shardBuffers.remove(bufferId);
    }

    /**
     * Called when lease is lost - immediately invalidates the buffer to prevent further access.
     */
    void consumerLeaseLost(final ShardBufferId bufferId) {
        final ShardBuffer buffer = shardBuffers.remove(bufferId);

        logger.info("Lease lost for buffer {}. Invalidating it", bufferId);

        if (buffer != null) {
            final Collection<KinesisClientRecord> records = buffer.invalidate();
            memoryTracker.freeRecordsMemory(records);
        }
    }

    // ========== Methods called from the processor ==========

    /**
     * Acquires a lease for a buffer that has data available for consumption.
     * If no buffers are available, returns an empty Optional.
     */
    Optional<ShardBufferLease> acquireBufferLease() {
        while (true) {
            final ShardBufferId bufferId = buffersWithData.poll();
            if (bufferId == null) {
                // The queue is empty. Nothing to consume.
                return Optional.empty();
            }

            // By the time the bufferId is polled, it might have been invalidated already.
            if (shardBuffers.containsKey(bufferId)) {
                logger.debug("Acquired lease for buffer {}", bufferId);
                return Optional.of(new ShardBufferLease(bufferId));
            }
        }
    }

    /**
     * Consumes records from the buffer associated with the given lease.
     * The records have to be committed or rolled back later.
     */
    List<KinesisClientRecord> consumeRecords(final ShardBufferLease lease) {
        if (lease.returnedToPool.get()) {
            logger.warn("Attempting to consume records from a buffer that was already returned to the pool. Ignoring.");
            return emptyList();
        }

        final ShardBufferId bufferId = lease.bufferId;

        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot consume records", bufferId);
            return emptyList();
        }

        return buffer.consumeRecords();
    }

    void commitConsumedRecords(final ShardBufferLease lease) {
        if (lease.returnedToPool.get()) {
            logger.warn("Attempting to commit records from a buffer that was already returned to the pool. Ignoring.");
            return;
        }

        final ShardBufferId bufferId = lease.bufferId;

        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot commit consumed records", bufferId);
            return;
        }

        final List<KinesisClientRecord> consumedRecords = buffer.commitConsumedRecords();
        memoryTracker.freeRecordsMemory(consumedRecords);
    }

    void rollbackConsumedRecords(final ShardBufferLease lease) {
        if (lease.returnedToPool.get()) {
            logger.warn("Attempting to rollback records from a buffer that was already returned to the pool. Ignoring.");
            return;
        }

        final ShardBufferId bufferId = lease.bufferId;
        final ShardBuffer buffer = shardBuffers.get(bufferId);

        if (buffer != null) {
            buffer.rollbackConsumedRecords();
        }
    }

    /**
     * Returns the lease for a buffer back to the pool. Making it available for consumption again.
     */
    void returnBufferLease(final ShardBufferLease lease) {
        if (lease.returnedToPool.getAndSet(true)) {
            logger.warn("Attempting to return a buffer that was already returned to the pool. Ignoring.");
            return;
        }

        final ShardBufferId bufferId = lease.bufferId;
        final ShardBuffer buffer = shardBuffers.get(bufferId);

        // If a buffer has any pending records we want to make it available for consumption immediately.
        if (buffer != null && buffer.hasUnprocessedRecords()) {
            logger.debug("Making the buffer {} available for lease, as it has more unprocessed records", bufferId);
            buffersWithData.add(bufferId);
        }
    }

    record ShardBufferId(String shardId, int bufferId) {
    }

    static class ShardBufferLease {

        private final ShardBufferId bufferId;
        private final AtomicBoolean returnedToPool = new AtomicBoolean(false);

        ShardBufferLease(final ShardBufferId bufferId) {
            this.bufferId = bufferId;
        }

        // Interface for the processor to access the shardId.
        String shardId() {
            return bufferId.shardId();
        }
    }

    /**
     * A memory tracker which blocks a thread when the memory usage exceeds the allowed maximum.
     */
    private static class BlockingMemoryTracker {

        private static final long AWAIT_MILLIS = 100;

        private final ComponentLog logger;

        private final long maxMemoryBytes;

        private long consumedMemoryBytes = 0;
        private final Lock memoryGaugeLock = new ReentrantLock();
        private final Condition decreasedConsumedMemoryCondition = memoryGaugeLock.newCondition();

        BlockingMemoryTracker(ComponentLog logger, final long maxMemoryBytes) {
            this.logger = logger;
            this.maxMemoryBytes = maxMemoryBytes;
        }

        void addRecordsMemory(final Collection<KinesisClientRecord> newRecords) {
            if (newRecords.isEmpty()) {
                return;
            }

            final long consumedBytes = calculateMemoryUsage(newRecords);
            if (consumedBytes > maxMemoryBytes) {
                throw new IllegalArgumentException(("Attempting to consume more memory than allowed. " +
                        "A record batch takes up %d bytes, while the max buffer size is %d bytes").formatted(consumedBytes, maxMemoryBytes));
            }

            try {
                memoryGaugeLock.lock();
                while (consumedMemoryBytes + consumedBytes > maxMemoryBytes) {
                    try {
                        decreasedConsumedMemoryCondition.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted while waiting for available memory in RecordBuffer", e);
                    }
                }

                consumedMemoryBytes += consumedBytes;
                logger.debug("Reserved {} bytes for {} records. Total consumed memory: {} bytes",
                        consumedBytes, newRecords.size(), consumedMemoryBytes);
            } finally {
                memoryGaugeLock.unlock();
            }
        }

        void freeRecordsMemory(final Collection<KinesisClientRecord> consumedRecords) {
            if (consumedRecords.isEmpty()) {
                return;
            }

            final long freedBytes = calculateMemoryUsage(consumedRecords);

            try {
                memoryGaugeLock.lock();
                if (consumedMemoryBytes < freedBytes) {
                    throw new IllegalStateException("Attempting to free more memory than currently used");
                }

                consumedMemoryBytes -= freedBytes;
                logger.debug("Freed {} bytes for {} records. Total consumed memory: {} bytes",
                        freedBytes, consumedRecords.size(), consumedMemoryBytes);
                decreasedConsumedMemoryCondition.signalAll();
            } finally {
                memoryGaugeLock.unlock();
            }
        }

        private long calculateMemoryUsage(final Collection<KinesisClientRecord> records) {
            return records.stream()
                    .map(KinesisClientRecord::data)
                    .filter(Objects::nonNull)
                    .mapToLong(ByteBuffer::capacity)
                    .sum();
        }
    }

    private sealed interface ShardBufferOfferResult permits
            ShardBufferOfferResult.Added, ShardBufferOfferResult.BufferInvalidated {

        record Added(boolean bufferWasEmpty) implements ShardBufferOfferResult {
        }

        record BufferInvalidated() implements ShardBufferOfferResult {
        }

        static ShardBufferOfferResult invalidated() {
            return new BufferInvalidated();
        }

        static ShardBufferOfferResult added(final boolean bufferWasEmpty) {
            return new Added(bufferWasEmpty);
        }
    }

    /**
     * ShardBuffer stores all records for a single shard in two queues:
     * - IN_PROGRESS: records that have been consumed but not yet checkpointed.
     * - PENDING: records that have been added but not yet consumed.
     * <p>
     * When consuming records all PENDING record are moved to IN_PROGRESS.
     * After a successful checkpoint all IN_PROGRESS records are cleared.
     * After a rollback all IN_PROGRESS records are kept, allowing to retry consumption.
     */
    private static class ShardBuffer {

        private static final long AWAIT_MILLIS = 100;

        // Retry configuration.
        private static final int MAX_RETRY_ATTEMPTS = 5;
        private static final long BASE_RETRY_DELAY_MILLIS = 100;
        private static final long MAX_RETRY_DELAY_MILLIS = 10_000;
        private static final Random RANDOM = new Random();

        private final ComponentLog logger;

        /**
         * Checkpointer for the latest record in the inProgressRecords.
         */
        private @Nullable RecordProcessorCheckpointer latestInProgressCheckpointer;
        private final List<KinesisClientRecord> inProgressRecords = new ArrayList<>();

        /**
         * Checkpoint for the latest record in the pendingRecords.
         */
        private @Nullable RecordProcessorCheckpointer latestPendingCheckpointer;
        private final List<KinesisClientRecord> pendingRecords = new ArrayList<>();

        private final Lock recordsLock = new ReentrantLock();
        private final Condition emptyBufferCondition = recordsLock.newCondition();

        private final AtomicBoolean invalidated = new AtomicBoolean(false);

        ShardBuffer(final ComponentLog logger) {
            this.logger = logger;
        }

        ShardBufferOfferResult offer(final List<KinesisClientRecord> records, final RecordProcessorCheckpointer recordsCheckpointer) {
            if (invalidated.get()) {
                return ShardBufferOfferResult.invalidated();
            }

            recordsLock.lock();
            try {
                if (invalidated.get()) {
                    return ShardBufferOfferResult.invalidated();
                }

                final boolean wasEmpty = inProgressRecords.isEmpty() && pendingRecords.isEmpty();

                pendingRecords.addAll(records);
                latestPendingCheckpointer = recordsCheckpointer;

                return ShardBufferOfferResult.added(wasEmpty);

            } finally {
                recordsLock.unlock();
            }
        }

        List<KinesisClientRecord> consumeRecords() {
            if (invalidated.get()) {
                return emptyList();
            }

            recordsLock.lock();
            try {
                if (invalidated.get()) {
                    return emptyList();
                }

                inProgressRecords.addAll(pendingRecords);
                latestInProgressCheckpointer = latestPendingCheckpointer;

                pendingRecords.clear();
                latestPendingCheckpointer = null;

                return List.copyOf(inProgressRecords);
            } finally {
                recordsLock.unlock();
            }
        }

        List<KinesisClientRecord> commitConsumedRecords() {
            if (invalidated.get()) {
                return emptyList();
            }

            recordsLock.lock();
            try {
                if (invalidated.get()) {
                    return emptyList();
                }

                checkpointSafely(latestInProgressCheckpointer);

                final List<KinesisClientRecord> checkpointedRecords = List.copyOf(inProgressRecords);
                inProgressRecords.clear();

                if (pendingRecords.isEmpty()) {
                    emptyBufferCondition.signalAll();
                }

                return checkpointedRecords;

            } finally {
                recordsLock.unlock();
            }
        }

        void rollbackConsumedRecords() {
            if (invalidated.get()) {
                return;
            }

            recordsLock.lock();
            try {
                if (invalidated.get()) {
                    return;
                }

                inProgressRecords.forEach(record -> record.data().rewind());

            } finally {
                recordsLock.unlock();
            }
        }

        void finishRecordConsumption(final RecordProcessorCheckpointer completionCheckpointer) {
            if (invalidated.get()) {
                return;
            }

            recordsLock.lock();
            try {
                // Record consumption can be finished only when all accepted records were committed.
                while (!inProgressRecords.isEmpty() || !pendingRecords.isEmpty()) {
                    if (invalidated.get()) {
                        return;
                    }

                    try {
                        emptyBufferCondition.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted while waiting for records to be consumed", e);
                    }
                }

                checkpointSafely(completionCheckpointer);
            } finally {
                recordsLock.unlock();
            }
        }

        Collection<KinesisClientRecord> invalidate() {
            if (invalidated.getAndSet(true)) {
                return emptyList();
            }

            recordsLock.lock();
            try {
                final List<KinesisClientRecord> bufferedRecords = new ArrayList<>(inProgressRecords.size() + pendingRecords.size());
                bufferedRecords.addAll(inProgressRecords);
                bufferedRecords.addAll(pendingRecords);

                inProgressRecords.clear();
                latestInProgressCheckpointer = null;

                pendingRecords.clear();
                latestPendingCheckpointer = null;

                return bufferedRecords;
            } finally {
                recordsLock.unlock();
            }
        }

        boolean hasUnprocessedRecords() {
            if (invalidated.get()) {
                return false;
            }

            recordsLock.lock();
            try {
                return !invalidated.get()
                        && (!inProgressRecords.isEmpty() || !pendingRecords.isEmpty());
            } finally {
                recordsLock.unlock();
            }
        }

        /**
         * Performs checkpointing using exponential backoff and jitter, if needed.
         *
         * @param checkpointer the checkpointer to use.
         */
        private void checkpointSafely(final @Nullable RecordProcessorCheckpointer checkpointer) {
            if (checkpointer == null) {
                logger.warn("Attempting to checkpoint records with a null checkpointer. Ignoring checkpoint");
                return;
            }

            for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
                try {
                    checkpointer.checkpoint();
                    if (attempt > 1) {
                        logger.debug("Checkpoint succeeded on attempt {}", attempt);
                    }
                    return; // Success, exit retry loop.
                } catch (final ThrottlingException | InvalidStateException | KinesisClientLibDependencyException e) {
                    if (attempt == MAX_RETRY_ATTEMPTS) {
                        logger.error("Failed to checkpoint after {} attempts, giving up. Last error: {}",
                                MAX_RETRY_ATTEMPTS, e.getMessage(), e);
                        return; // Max attempts reached, give up.
                    }

                    final long delayMillis = calculateRetryDelay(attempt);

                    logger.debug("Checkpoint failed on attempt {} with {}, retrying in {} ms",
                            attempt, e.getMessage(), delayMillis);

                    try {
                        Thread.sleep(delayMillis);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted during checkpoint retry backoff", ie);
                    }
                } catch (final ShutdownException e) {
                    logger.warn("Failed to checkpoint records due to shutdown: {}. Ignoring checkpoint", e.getMessage(), e);
                    return; // Don't retry on shutdown.
                }
            }
        }

        private long calculateRetryDelay(final int attempt) {
            final long desiredBaseDelayMillis = BASE_RETRY_DELAY_MILLIS * (1L << (attempt - 1));
            final long baseDelayMillis = Math.min(desiredBaseDelayMillis, MAX_RETRY_DELAY_MILLIS);
            final long jitterMillis = RANDOM.nextLong(baseDelayMillis / 4); // Up to 25% jitter.
            return baseDelayMillis + jitterMillis;
        }
    }
}
