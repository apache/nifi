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
import org.apache.nifi.processors.aws.kinesis.RecordBuffer.ShardBufferId;
import org.apache.nifi.processors.aws.kinesis.RecordBuffer.ShardBufferLease;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;

/**
 * A record buffer which limits the maximum memory usage across all shard buffers.
 * If the memory limit is reached, adding new records will block until enough memory is freed.
 */
final class MemoryBoundRecordBuffer implements RecordBuffer.ForKinesisClientLibrary, RecordBuffer.ForProcessor {

    private final ComponentLog logger;

    private final long checkpointIntervalMillis;
    private final BlockingMemoryTracker memoryTracker;

    private final AtomicLong bufferIdCounter = new AtomicLong(0);
    private final ConcurrentMap<ShardBufferId, ShardBuffer> shardBuffers = new ConcurrentHashMap<>();

    /**
     * A queue with ids shard buffers available for leasing.
     * <p>
     * Note: when a buffer is invalidated its id is NOT removed from the queue immediately.
     */
    private final Queue<ShardBufferId> buffersToLease = new ConcurrentLinkedQueue<>();

    MemoryBoundRecordBuffer(final ComponentLog logger, final long maxMemoryBytes, final Duration checkpointInterval) {
        this.logger = logger;
        this.memoryTracker = new BlockingMemoryTracker(logger, maxMemoryBytes);
        this.checkpointIntervalMillis = checkpointInterval.toMillis();
    }

    @Override
    public ShardBufferId createBuffer(final String shardId) {
        final ShardBufferId id = new ShardBufferId(shardId, bufferIdCounter.getAndIncrement());

        logger.debug("Creating new buffer for shard {} with id {}", shardId, id);

        shardBuffers.put(id, new ShardBuffer(id, logger, checkpointIntervalMillis));
        buffersToLease.add(id);
        return id;
    }

    @Override
    public void addRecords(final ShardBufferId bufferId, final List<KinesisClientRecord> records, final RecordProcessorCheckpointer checkpointer) {
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

        final RecordBatch recordBatch = new RecordBatch(records, checkpointer, calculateMemoryUsage(records));
        memoryTracker.reserveMemory(recordBatch);
        final boolean addedRecords = buffer.offer(recordBatch);

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
            memoryTracker.freeMemory(List.of(recordBatch));
        }
    }

    @Override
    public void checkpointEndedShard(final ShardBufferId bufferId, final RecordProcessorCheckpointer checkpointer) {
        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot checkpoint the ended shard", bufferId);
            return;
        }

        logger.debug("Finishing consumption for buffer {}. Checkpointing the ended shard", bufferId);
        buffer.checkpointEndedShard(checkpointer);

        logger.debug("Removing buffer with id {} after successful ended shard checkpoint", bufferId);
        shardBuffers.remove(bufferId);
    }

    @Override
    public void shutdownShardConsumption(final ShardBufferId bufferId, final RecordProcessorCheckpointer checkpointer) {
        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot shutdown shard consumption", bufferId);
            return;
        }

        logger.debug("Shutting down the buffer {}. Checkpointing last consumed record", bufferId);
        buffer.shutdownBuffer(checkpointer);

        logger.debug("Removing buffer with id {} after successful last consumed record checkpoint", bufferId);
        shardBuffers.remove(bufferId);
    }

    @Override
    public void consumerLeaseLost(final ShardBufferId bufferId) {
        final ShardBuffer buffer = shardBuffers.remove(bufferId);

        logger.debug("Lease lost for buffer {}: Invalidating", bufferId);

        if (buffer != null) {
            final Collection<RecordBatch> invalidatedBatches = buffer.invalidate();
            memoryTracker.freeMemory(invalidatedBatches);
        }
    }

    @Override
    public Optional<ShardBufferLease> acquireBufferLease() {
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
            } else if (buffer.isEmpty()) {
                seenBuffers.add(bufferId);
                buffersToLease.add(bufferId);
                logger.debug("Buffer with id {} is empty. Continuing to poll.", bufferId);
            } else {
                logger.debug("Acquired lease for buffer {}", bufferId);
                return Optional.of(new StandardShardBufferLease(bufferId));
            }
        }
    }

    @Override
    public List<KinesisClientRecord> consumeRecords(final ShardBufferLease lease) {
        if (!(lease instanceof StandardShardBufferLease standardLease)) {
            throw new IllegalArgumentException("Unexpected lease type: " + lease.getClass().getName());
        }

        if (standardLease.returnedToPool.get()) {
            logger.warn("Attempting to consume records from a buffer that was already returned to the pool. Ignoring.");
            return emptyList();
        }

        final ShardBufferId bufferId = standardLease.bufferId;

        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot consume records", bufferId);
            return emptyList();
        }

        return buffer.consumeRecords();
    }

    @Override
    public void commitConsumedRecords(final ShardBufferLease lease) {
        if (!(lease instanceof StandardShardBufferLease standardLease)) {
            throw new IllegalArgumentException("Unexpected lease type: " + lease.getClass().getName());
        }

        if (standardLease.returnedToPool.get()) {
            logger.warn("Attempting to commit records from a buffer that was already returned to the pool. Ignoring.");
            return;
        }

        final ShardBufferId bufferId = standardLease.bufferId;

        final ShardBuffer buffer = shardBuffers.get(bufferId);
        if (buffer == null) {
            logger.debug("Buffer with id {} not found. Cannot commit consumed records", bufferId);
            return;
        }

        final List<RecordBatch> consumedBatches = buffer.commitConsumedRecords();
        memoryTracker.freeMemory(consumedBatches);
    }

    @Override
    public void rollbackConsumedRecords(final ShardBufferLease lease) {
        if (!(lease instanceof StandardShardBufferLease standardLease)) {
            throw new IllegalArgumentException("Unexpected lease type: " + lease.getClass().getName());
        }

        if (standardLease.returnedToPool.get()) {
            logger.warn("Attempting to rollback records from a buffer that was already returned to the pool. Ignoring.");
            return;
        }

        final ShardBufferId bufferId = standardLease.bufferId;
        final ShardBuffer buffer = shardBuffers.get(bufferId);

        if (buffer != null) {
            buffer.rollbackConsumedRecords();
        }
    }

    @Override
    public void returnBufferLease(final ShardBufferLease lease) {
        if (!(lease instanceof StandardShardBufferLease standardLease)) {
            throw new IllegalArgumentException("Unexpected lease type: " + lease.getClass().getName());
        }

        if (standardLease.returnedToPool.getAndSet(true)) {
            logger.warn("Attempting to return a buffer that was already returned to the pool. Ignoring.");
            return;
        }

        final ShardBufferId bufferId = standardLease.bufferId;
        buffersToLease.add(bufferId);

        logger.debug("The buffer {} is available for lease again", bufferId);
    }

    private static class StandardShardBufferLease implements ShardBufferLease {

        private final ShardBufferId bufferId;
        private final AtomicBoolean returnedToPool = new AtomicBoolean(false);

        StandardShardBufferLease(final ShardBufferId bufferId) {
            this.bufferId = bufferId;
        }

        @Override
        public String shardId() {
            return bufferId.shardId();
        }
    }

    /**
     * A memory tracker which blocks a thread when the memory usage exceeds the allowed maximum.
     * <p>
     * In order to make progress, the memory consumption may exceed the limit, but any new records will not be accepted.
     * This is done to support the case when a single record batch is larger than the allowed memory limit.
     */
    private static class BlockingMemoryTracker {

        private static final long AWAIT_MILLIS = 100;

        private final ComponentLog logger;

        private final long maxMemoryBytes;

        private final AtomicLong consumedMemoryBytes = new AtomicLong(0);
        /**
         * Whenever memory is freed a latch opens. Then replaced with a new one.
         */
        private final AtomicReference<CountDownLatch> memoryAvailableLatch = new AtomicReference<>(new CountDownLatch(1));

        BlockingMemoryTracker(final ComponentLog logger, final long maxMemoryBytes) {
            this.logger = logger;
            this.maxMemoryBytes = maxMemoryBytes;
        }

        void reserveMemory(final RecordBatch recordBatch) {
            final long consumedBytes = recordBatch.batchSizeBytes();

            if (consumedBytes == 0) {
                return;
            }

            while (true) {
                final long currentlyConsumedBytes = consumedMemoryBytes.get();

                if (currentlyConsumedBytes >= maxMemoryBytes) {
                    // Not enough memory available, need to wait.
                    try {
                        memoryAvailableLatch.get().await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted while waiting for available memory in RecordBuffer", e);
                    }
                } else {
                    final long newConsumedBytes = currentlyConsumedBytes + consumedBytes;
                    if (consumedMemoryBytes.compareAndSet(currentlyConsumedBytes, newConsumedBytes)) {
                        logger.debug("Reserved {} bytes for {} records. Total consumed memory: {} bytes",
                                consumedBytes, recordBatch.size(), newConsumedBytes);
                        break;
                    }
                    // If we're here, the compare and set operation failed, as another thread has modified the gauge in meantime.
                    // Retrying the operation.
                }
            }
        }

        void freeMemory(final Collection<RecordBatch> consumedBatches) {
            if (consumedBatches.isEmpty()) {
                return;
            }

            long freedBytes = 0;
            for (final RecordBatch batch : consumedBatches) {
                freedBytes += batch.batchSizeBytes();
            }

            while (true) {
                final long currentlyConsumedBytes = consumedMemoryBytes.get();
                if (currentlyConsumedBytes < freedBytes) {
                    throw new IllegalStateException("Attempting to free more memory than currently used");
                }

                final long newTotal = currentlyConsumedBytes - freedBytes;
                if (consumedMemoryBytes.compareAndSet(currentlyConsumedBytes, newTotal)) {
                    logger.debug("Freed {} bytes for {} batches. Total consumed memory: {} bytes",
                            freedBytes, consumedBatches.size(), newTotal);

                    final CountDownLatch oldLatch = memoryAvailableLatch.getAndSet(new CountDownLatch(1));
                    oldLatch.countDown(); // Release any waiting threads for free memory.
                    break;
                }
                // If we're here, the compare and set operation failed, as another thread has modified the gauge in meantime.
                // Retrying the operation.
            }
        }
    }

    private record RecordBatch(List<KinesisClientRecord> records,
                               @Nullable RecordProcessorCheckpointer checkpointer,
                               long batchSizeBytes) {
        int size() {
            return records.size();
        }
    }

    private long calculateMemoryUsage(final Collection<KinesisClientRecord> records) {
        long totalBytes = 0;
        for (final KinesisClientRecord record : records) {
            final ByteBuffer data = record.data();
            if (data != null) {
                totalBytes += data.capacity();
            }
        }
        return totalBytes;
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

        private final ShardBufferId bufferId;
        private final ComponentLog logger;

        private final long checkpointIntervalMillis;
        private volatile long nextCheckpointTimeMillis;
        /**
         * A last records checkpointer that was ignored due to the checkpoint interval.
         * If null, the last checkpoint was successful or no checkpoint was attempted yet.
         */
        private volatile @Nullable RecordProcessorCheckpointer lastIgnoredCheckpointer = null;

        /**
         * Queues for managing record batches with their checkpointers in different states.
         */
        private final Queue<RecordBatch> inProgressBatches = new ConcurrentLinkedQueue<>();
        private final Queue<RecordBatch> pendingBatches = new ConcurrentLinkedQueue<>();
        /**
         * Counter for tracking the number of batches in the buffer. Can be larger than the number of batches in the queues.
         */
        private final AtomicInteger batchesCount = new AtomicInteger(0);

        /**
         * A countdown latch that is used to signal when the buffer becomes empty. Used when ShardBuffer should be closed.
         */
        private volatile @Nullable CountDownLatch emptyBufferLatch = null;
        private final AtomicBoolean invalidated = new AtomicBoolean(false);

        ShardBuffer(final ShardBufferId bufferId, final ComponentLog logger, final long checkpointIntervalMillis) {
            this.bufferId = bufferId;
            this.logger = logger;
            this.checkpointIntervalMillis = checkpointIntervalMillis;
            this.nextCheckpointTimeMillis = System.currentTimeMillis() + checkpointIntervalMillis;
        }

        /**
         * @param recordBatch record batch with records to add.
         * @return true if the records were added successfully, false if a buffer was invalidated.
         */
        boolean offer(final RecordBatch recordBatch) {
            if (invalidated.get()) {
                return false;
            }

            // Batches count must be always equal to or larger than the number of batches in the queues.
            // Thus, the ordering of the operations.
            batchesCount.incrementAndGet();
            pendingBatches.offer(recordBatch);

            return true;
        }

        List<KinesisClientRecord> consumeRecords() {
            if (invalidated.get()) {
                return emptyList();
            }

            RecordBatch pendingBatch;
            while ((pendingBatch = pendingBatches.poll()) != null) {
                inProgressBatches.offer(pendingBatch);
            }

            final List<KinesisClientRecord> recordsToConsume = new ArrayList<>();
            for (final RecordBatch batch : inProgressBatches) {
                recordsToConsume.addAll(batch.records());
            }

            return recordsToConsume;
        }

        List<RecordBatch> commitConsumedRecords() {
            if (invalidated.get()) {
                return emptyList();
            }

            final List<RecordBatch> checkpointedBatches = new ArrayList<>();
            RecordBatch batch;
            while ((batch = inProgressBatches.poll()) != null) {
                checkpointedBatches.add(batch);
            }

            if (checkpointedBatches.isEmpty()) {
                // The buffer could be invalidated in the meantime, or no records were consumed.
                return emptyList();
            }

            // Batches count must always be equal to or larger than the number of batches in the queues.
            // To achieve so, the count is decreased only after the queue has been emptied.
            batchesCount.addAndGet(-checkpointedBatches.size());

            final RecordProcessorCheckpointer lastBatchCheckpointer = checkpointedBatches.getLast().checkpointer();
            if (System.currentTimeMillis() >= nextCheckpointTimeMillis) {
                checkpointSafely(lastBatchCheckpointer);
                nextCheckpointTimeMillis = System.currentTimeMillis() + checkpointIntervalMillis;
                lastIgnoredCheckpointer = null;
            } else {
                // Saving the checkpointer for later, in case shutdown happens before the next checkpoint.
                lastIgnoredCheckpointer = lastBatchCheckpointer;
            }

            final CountDownLatch localEmptyBufferLatch = this.emptyBufferLatch;
            if (localEmptyBufferLatch != null && isEmpty()) {
                // If the latch is not null, it means we are waiting for the buffer to become empty.
                localEmptyBufferLatch.countDown();
            }

            return checkpointedBatches;
        }

        void rollbackConsumedRecords() {
            if (invalidated.get()) {
                return;
            }

            for (final RecordBatch recordBatch : inProgressBatches) {
                for (final KinesisClientRecord record : recordBatch.records()) {
                    record.data().rewind();
                }
            }
        }

        void checkpointEndedShard(final RecordProcessorCheckpointer checkpointer) {
            while (true) {
                if (invalidated.get()) {
                    return;
                }

                if (batchesCount.get() == 0) {
                    // Buffer is empty, perform final checkpoint.
                    checkpointSafely(checkpointer);
                    return;
                }

                // Wait for the records to be consumed first.
                try {
                    final CountDownLatch localEmptyBufferLatch = new CountDownLatch(1);
                    this.emptyBufferLatch = localEmptyBufferLatch;
                    localEmptyBufferLatch.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Thread interrupted while waiting for records to be consumed", e);
                }
            }
        }

        void shutdownBuffer(final RecordProcessorCheckpointer checkpointer) {
            if (invalidated.get()) {
                return;
            }

            if (batchesCount.get() == 0) {
                checkpointSafely(checkpointer);
            } else {
                // If there are still records in the buffer, checkpointing with the latest provided checkpointer is not safe.
                // But, if the records were committed without checkpointing in the past, we can checkpoint them now.
                final RecordProcessorCheckpointer lastCheckpointer = this.lastIgnoredCheckpointer;
                if (lastCheckpointer != null) {
                    checkpointSafely(lastCheckpointer);
                }
            }
        }

        Collection<RecordBatch> invalidate() {
            if (invalidated.getAndSet(true)) {
                return emptyList();
            }

            final List<RecordBatch> batches = new ArrayList<>();
            RecordBatch batch;
            // If both consumeRecords and invalidate are called concurrently, invalidation must always consume all batches.
            // Since consumeRecords moves batches from pending to in_progress, during invalidation pending batches should be drained first.
            while ((batch = pendingBatches.poll()) != null) {
                batches.add(batch);
            }
            while ((batch = inProgressBatches.poll()) != null) {
                batches.add(batch);
            }

            // No need to adjust batchesCount after invalidation.

            return batches;
        }

        /**
         * Checks if the buffer has any records. <b>Can produce false negatives.</b>
         *
         * @return whether there are any records in the buffer.
         */
        boolean isEmpty() {
            return invalidated.get() || batchesCount.get() == 0;
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

            logger.debug("Performing checkpoint for buffer with id {}", bufferId);

            for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
                try {
                    checkpointer.checkpoint();
                    if (attempt > 1) {
                        logger.debug("Checkpoint succeeded on attempt {}", attempt);
                    }
                    return; // Success, exit retry loop.
                } catch (final ThrottlingException | InvalidStateException | KinesisClientLibDependencyException e) {
                    if (attempt == MAX_RETRY_ATTEMPTS) {
                        logger.error("Failed to checkpoint after {} attempts, giving up", MAX_RETRY_ATTEMPTS, e);
                        return; // Max attempts reached, give up.
                    }

                    final long delayMillis = calculateRetryDelay(attempt);

                    logger.debug("Checkpoint failed on attempt {} with {}, retrying in {} ms",
                            attempt, e.getMessage(), delayMillis);

                    try {
                        Thread.sleep(delayMillis);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.warn("Thread interrupted while waiting to retry checkpoint. Exiting retry loop", ie);
                        return;
                    }
                } catch (final ShutdownException e) {
                    logger.warn("Failed to checkpoint records due to shutdown. Ignoring checkpoint", e);
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
