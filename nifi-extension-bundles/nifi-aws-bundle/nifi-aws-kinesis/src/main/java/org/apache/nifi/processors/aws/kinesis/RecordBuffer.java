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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * RecordBuffer keeps track of all created Shard buffers, including exclusive read access via leasing,
 * and memory consumption tracking. It acts as the main interface between KCL callbacks and
 * the {@link ConsumeKinesis} processor, routing events to appropriate ShardBuffers and ensuring
 * thread-safe operations.
 */
final class RecordBuffer {

    private final ComponentLog logger;

    private final BlockingMemoryTracker memoryTracker;

    private final AtomicLong bufferIdCounter = new AtomicLong(0);
    private final ConcurrentMap<ShardBufferId, ShardBuffer> shardBuffers = new ConcurrentHashMap<>();

    /**
     * A queue with ids shard buffers available for leasing.
     * <p>
     * Note: when a buffer is invalidated its id is NOT removed from the queue immediately.
     */
    private final ConcurrentLinkedQueue<ShardBufferId> buffersToLease = new ConcurrentLinkedQueue<>();

    RecordBuffer(final ComponentLog logger, final long maxMemoryBytes) {
        this.logger = logger;
        this.memoryTracker = new BlockingMemoryTracker(logger, maxMemoryBytes);
    }

    // ========== Methods called from Kinesis Client Library ==========

    ShardBufferId createBuffer(final String shardId) {
        final ShardBufferId id = new ShardBufferId(shardId, bufferIdCounter.getAndIncrement());

        logger.info("Creating new buffer for shard {} with id {}", shardId, id);

        shardBuffers.put(id, new ShardBuffer(logger));
        buffersToLease.add(id);
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

        memoryTracker.reserveMemory(records);
        final boolean addedRecords = buffer.offer(records, checkpointer);

        if (addedRecords) {
            logger.debug("Successfully added records with sequence and subsequence numbers: {}.{} - {}.{} to buffer with id {}",
                    records.getFirst().sequenceNumber(),
                    records.getFirst().subSequenceNumber(),
                    records.getLast().sequenceNumber(),
                    records.getLast().subSequenceNumber(),
                    bufferId);
        } else {
            logger.debug("Buffer with id {} was invalidated. Cannot add records with sequence and subsequence numbers: {}.{} - {}.{}",
                    bufferId,
                    records.getFirst().sequenceNumber(),
                    records.getFirst().subSequenceNumber(),
                    records.getLast().sequenceNumber(),
                    records.getLast().subSequenceNumber());
            // If the buffer was invalidated, we should free memory reserved for these records.
            memoryTracker.freeMemory(records);
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
            memoryTracker.freeMemory(records);
        }
    }

    // ========== Methods called from the processor ==========

    /**
     * Acquires a lease for a buffer that has data available for consumption.
     * If no data is available in the buffers, returns an empty Optional.
     * <p>
     * After acquiring a lease, the processor can consume records from the buffer.
     * After consuming the records the processor must always {@link #returnBufferLease(ShardBufferLease)}.
     */
    Optional<ShardBufferLease> acquireBufferLease() {
        final Set<ShardBufferId> seenBuffers = new HashSet<>();

        while (true) {
            final ShardBufferId bufferId = buffersToLease.poll();
            if (bufferId == null) {
                // The queue is empty or all buffers were seen already. Nothing to consume.
                return Optional.empty();
            }

            if (seenBuffers.contains(bufferId)) {
                // If the same buffer is seen again, there is a high chance we iterated through most of the buffers and didn't find any that isn't empty.
                // To avoid burning CPU we return empty here, even if some buffer received records in the meantime. It will be picked up in the next iteration.
                buffersToLease.add(bufferId);
                return Optional.empty();
            }

            final ShardBuffer buffer = shardBuffers.get(bufferId);

            if (buffer == null) {
                // By the time the bufferId is polled, it might have been invalidated. No need to return it to the queue.
                logger.debug("Buffer with id {} was removed while polling for lease. Continuing to poll.", bufferId);
            } else if (!buffer.hasUnprocessedRecords()) {
                seenBuffers.add(bufferId);
                buffersToLease.add(bufferId);
                logger.debug("Buffer with id {} is empty. Continuing to poll.", bufferId);
            } else {
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
        memoryTracker.freeMemory(consumedRecords);
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
        buffersToLease.add(bufferId);

        logger.debug("The buffer {} is available for lease again", bufferId);
    }

    record ShardBufferId(String shardId, long bufferId) {
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

        private final AtomicLong consumedMemoryBytes = new AtomicLong(0);
        /**
         * Whenever memory is freed a latch opens. Then replaced with a new one.
         */
        private volatile CountDownLatch memoryAvailableLatch = new CountDownLatch(1);

        BlockingMemoryTracker(final ComponentLog logger, final long maxMemoryBytes) {
            this.logger = logger;
            this.maxMemoryBytes = maxMemoryBytes;
        }

        void reserveMemory(final Collection<KinesisClientRecord> newRecords) {
            if (newRecords.isEmpty()) {
                return;
            }

            final long consumedBytes = calculateMemoryUsage(newRecords);
            if (consumedBytes > maxMemoryBytes) {
                throw new IllegalArgumentException(("Attempting to consume more memory than allowed. " +
                        "A record batch takes up %d bytes, while the max buffer size is %d bytes").formatted(consumedBytes, maxMemoryBytes));
            }

            while (true) {
                final long currentlyConsumedBytes = consumedMemoryBytes.get();
                final long newConsumedBytes = currentlyConsumedBytes + consumedBytes;

                if (newConsumedBytes <= maxMemoryBytes) {
                    if (consumedMemoryBytes.compareAndSet(currentlyConsumedBytes, newConsumedBytes)) {
                        logger.debug("Reserved {} bytes for {} records. Total consumed memory: {} bytes",
                                consumedBytes, newRecords.size(), newConsumedBytes);
                        break;
                    }
                    // If we're here, the compare and set operation failed, as another thread has modified the gauge in meantime.
                    // Retrying the operation.

                } else {
                    // Not enough memory available, need to wait.
                    try {
                        memoryAvailableLatch.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted while waiting for available memory in RecordBuffer", e);
                    }
                }
            }
        }

        void freeMemory(final Collection<KinesisClientRecord> consumedRecords) {
            if (consumedRecords.isEmpty()) {
                return;
            }

            final long freedBytes = calculateMemoryUsage(consumedRecords);

            while (true) {
                final long currentlyConsumedBytes = consumedMemoryBytes.get();
                if (currentlyConsumedBytes < freedBytes) {
                    throw new IllegalStateException("Attempting to free more memory than currently used");
                }

                final long newTotal = currentlyConsumedBytes - freedBytes;
                if (consumedMemoryBytes.compareAndSet(currentlyConsumedBytes, newTotal)) {
                    logger.debug("Freed {} bytes for {} records. Total consumed memory: {} bytes",
                            freedBytes, consumedRecords.size(), newTotal);

                    final CountDownLatch oldLatch = memoryAvailableLatch;
                    memoryAvailableLatch = new CountDownLatch(1); // New latch for future waiters.
                    oldLatch.countDown(); // Release any waiting threads for free memory.
                    break;
                }
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

    private record RecordBatch(List<KinesisClientRecord> records,
                               @Nullable RecordProcessorCheckpointer checkpointer) {
    }

    /**
     * ShardBuffer stores all record batches for a single shard in two queues:
     * - IN_PROGRESS: record batches that have been consumed but not yet checkpointed.
     * - PENDING: record batches that have been added but not yet consumed.
     * <p>
     * When consuming records all PENDING batches are moved to IN_PROGRESS.
     * After a successful checkpoint all IN_PROGRESS batches are cleared.
     * After a rollback all IN_PROGRESS batches are kept, allowing to retry consumption.
     * <p>
     * Each batch preserves the original grouping of records as provided by Kinesis
     * along with their associated checkpointer, ensuring atomicity.
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
         * Lock-free queues for managing record batches with their checkpointers in different states.
         */
        private final LinkedBlockingDeque<RecordBatch> inProgressBatches = new LinkedBlockingDeque<>();
        private final LinkedBlockingQueue<RecordBatch> pendingBatches = new LinkedBlockingQueue<>();
        /**
         * Counter for tracking the number of records in the buffer. Can be larger than the number of records in the queues.
         */
        private final AtomicInteger recordsCount = new AtomicInteger(0);

        /**
         * A countdown latch that is used to signal when the buffer becomes empty. Used when ShardBuffer should be closed.
         */
        private volatile @Nullable CountDownLatch emptyBufferLatch = null;
        private final AtomicBoolean invalidated = new AtomicBoolean(false);

        ShardBuffer(final ComponentLog logger) {
            this.logger = logger;
        }

        /**
         * @param records             kinesis records to add.
         * @param recordsCheckpointer checkpointer for the records being added.
         * @return true if the records were added successfully, false if a buffer was invalidated.
         */
        boolean offer(final List<KinesisClientRecord> records, final RecordProcessorCheckpointer recordsCheckpointer) {
            if (invalidated.get()) {
                // If buffer was invalidated after adding, the records will be cleaned up by invalidate()
                // but we should return false to indicate the operation didn't succeed.
                return false;
            }

            // Records count must be always equal to or larger than the number of records in the queues.
            // Thus, the ordering of the operations.
            recordsCount.addAndGet(records.size());
            pendingBatches.offer(new RecordBatch(records, recordsCheckpointer));

            return true;
        }

        List<KinesisClientRecord> consumeRecords() {
            if (invalidated.get()) {
                return emptyList();
            }

            pendingBatches.drainTo(inProgressBatches);

            return inProgressBatches.stream()
                    .map(RecordBatch::records)
                    .flatMap(List::stream)
                    .toList();
        }

        List<KinesisClientRecord> commitConsumedRecords() {
            if (invalidated.get()) {
                return emptyList();
            }

            final RecordBatch lastBatch = inProgressBatches.peekLast();
            if (lastBatch == null) {
                // The buffer could be invalidated in the meantime, or no records were consumed.
                return emptyList();
            }

            checkpointSafely(lastBatch.checkpointer());

            final List<RecordBatch> checkpointedBatches = new ArrayList<>(inProgressBatches.size());
            inProgressBatches.drainTo(checkpointedBatches);

            final List<KinesisClientRecord> checkpointedRecords = checkpointedBatches.stream()
                    .map(RecordBatch::records)
                    .flatMap(List::stream)
                    .toList();
            // Records count must be always equal to or larger than the number of records in the queues.
            // Thus, the ordering of the operations.
            recordsCount.addAndGet(-checkpointedRecords.size());

            final CountDownLatch localEmptyBufferLatch = this.emptyBufferLatch;
            if (localEmptyBufferLatch != null && !hasUnprocessedRecords()) {
                // If the latch is not null, it means we are waiting for the buffer to become empty.
                localEmptyBufferLatch.countDown();
            }

            return checkpointedRecords;
        }

        void rollbackConsumedRecords() {
            if (invalidated.get()) {
                return;
            }

            for (final RecordBatch recordBatch : inProgressBatches) {
                recordBatch.records().forEach(record -> record.data().rewind());
            }
        }

        void finishRecordConsumption(final RecordProcessorCheckpointer completionCheckpointer) {
            while (true) {
                if (hasUnprocessedRecords()) {
                    // Wait for buffer to become empty.
                    try {
                        final CountDownLatch localEmptyBufferLatch = new CountDownLatch(1);
                        emptyBufferLatch = localEmptyBufferLatch;
                        localEmptyBufferLatch.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted while waiting for records to be consumed", e);
                    }
                } else {
                    // Buffer is empty, perform final checkpoint.
                    checkpointSafely(completionCheckpointer);
                    return;
                }
            }
        }

        Collection<KinesisClientRecord> invalidate() {
            if (invalidated.getAndSet(true)) {
                return emptyList();
            }

            final List<RecordBatch> inProgressBatchesCopy = new ArrayList<>(inProgressBatches.size());
            inProgressBatches.drainTo(inProgressBatchesCopy);

            final List<RecordBatch> pendingBatchesCopy = new ArrayList<>(pendingBatches.size());
            pendingBatches.drainTo(pendingBatchesCopy);

            // No need to adjust recordsCount after invalidation.

            return Stream.of(inProgressBatchesCopy, pendingBatchesCopy)
                    .flatMap(List::stream)
                    .map(RecordBatch::records)
                    .flatMap(List::stream)
                    .toList();
        }

        /**
         * Checks if the buffer has any records. <b>Can produce false positives.</b>
         * @return whether there are any records in the buffer.
         */
        boolean hasUnprocessedRecords() {
            if (invalidated.get()) {
                return false;
            }

            return recordsCount.get() > 0;
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
