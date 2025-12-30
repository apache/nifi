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

import org.apache.nifi.documentation.init.NopComponentLog;
import org.apache.nifi.processors.aws.kinesis.MemoryBoundRecordBuffer.Lease;
import org.apache.nifi.processors.aws.kinesis.RecordBuffer.ShardBufferId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.KinesisClientLibDependencyException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.processor.Checkpointer;
import software.amazon.kinesis.processor.PreparedCheckpointer;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryBoundRecordBufferTest {

    private static final long MAX_MEMORY_BYTES = 1024L;
    private static final String SHARD_ID_1 = "shard-1";
    private static final String SHARD_ID_2 = "shard-2";
    private static final Duration CHECKPOINT_INTERVAL = Duration.ZERO;

    private MemoryBoundRecordBuffer recordBuffer;
    private TestCheckpointer checkpointer1;
    private TestCheckpointer checkpointer2;

    @BeforeEach
    void setUp() {
        recordBuffer = new MemoryBoundRecordBuffer(new NopComponentLog(), MAX_MEMORY_BYTES, CHECKPOINT_INTERVAL);
        checkpointer1 = new TestCheckpointer();
        checkpointer2 = new TestCheckpointer();
    }

    @Test
    void testCreateBuffer() {
        final ShardBufferId bufferId1 = recordBuffer.createBuffer(SHARD_ID_1);
        assertEquals(SHARD_ID_1, bufferId1.shardId());

        final ShardBufferId bufferId2 = recordBuffer.createBuffer(SHARD_ID_2);
        assertEquals(SHARD_ID_2, bufferId2.shardId());

        final ShardBufferId newBufferId1 = recordBuffer.createBuffer(SHARD_ID_1);
        assertEquals(SHARD_ID_1, newBufferId1.shardId());

        assertNotEquals(bufferId1, bufferId2);
        assertNotEquals(bufferId1, newBufferId1);
    }

    @Test
    void testAddRecordsToBuffer() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        // Buffer without records is not available for leasing.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());

        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        // Should be able to get buffer ID from pool since buffer has records.
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease.shardId());
    }

    @Test
    void testAddEmptyRecordsList() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> emptyRecords = Collections.emptyList();

        recordBuffer.addRecords(bufferId, emptyRecords, checkpointer1);

        // Should not be able to get buffer ID from pool since no records were added.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());
    }

    @Test
    void testConsumeRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(3);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertEquals(records, consumedRecords);
        // Just consuming record should not checkpoint them.
        assertEquals(TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER, checkpointer1.latestCheckpointedSequenceNumber());
    }

    @Test
    void testCommitConsumedRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);
        recordBuffer.commitConsumedRecords(lease);

        assertEquals(records.getLast().sequenceNumber(), checkpointer1.latestCheckpointedSequenceNumber());
    }

    @Test
    void testCommitConsumedRecords_withRecordsAddedBeforeCommit() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> originalRecords = createTestRecords(2);

        recordBuffer.addRecords(bufferId, originalRecords, checkpointer1);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);

        // Simulating new records added in parallel, before a commit.
        final List<KinesisClientRecord> newRecords = createTestRecords(5);
        recordBuffer.addRecords(bufferId, newRecords, checkpointer1);

        recordBuffer.commitConsumedRecords(lease);

        // Only originalRecords, which were consumed, are checkpointed.
        assertEquals(originalRecords.getLast().sequenceNumber(), checkpointer1.latestCheckpointedSequenceNumber());
    }

    @Test
    void testRollbackConsumedRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(3);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        final List<String> messages = consumedRecords.stream()
                .map(this::readContent)
                .toList();

        recordBuffer.rollbackConsumedRecords(lease);

        // Checkpointer should not be called during rollback.
        assertEquals(TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER, checkpointer1.latestCheckpointedSequenceNumber());

        final List<String> rolledBackMessages = recordBuffer.consumeRecords(lease).stream()
                .map(this::readContent)
                .toList();
        assertEquals(messages, rolledBackMessages);
    }

    private String readContent(final KinesisClientRecord record) {
        final ByteBuffer data = record.data();
        final byte[] buffer = new byte[data.remaining()];
        data.get(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
    }

    @Test
    void testReturnBufferIdToPool() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        final Lease lease1 = recordBuffer.acquireBufferLease().orElseThrow();

        // Consume some records but don't return buffer id to the pool.
        recordBuffer.consumeRecords(lease1);

        recordBuffer.addRecords(bufferId, createTestRecords(1), checkpointer2);

        // The buffer is still unavailable.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());

        // After returning id to the pool it's possible to get the buffer from pool again.
        recordBuffer.returnBufferLease(lease1);
        final Lease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease2.shardId());
    }

    @Test
    void testReturnBufferIdToPool_multipleReturns() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        final Lease lease1 = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.returnBufferLease(lease1);
        recordBuffer.returnBufferLease(lease1);

        // Can retrieve the id only once.
        final Lease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease2.shardId());
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());
    }

    @Test
    void testReturnBufferIdToPool_withUncommittedRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        final Lease lease1 = recordBuffer.acquireBufferLease().orElseThrow();

        // Consume some records, but don't commit them.
        final List<KinesisClientRecord> lease1Records = recordBuffer.consumeRecords(lease1);
        recordBuffer.returnBufferLease(lease1);

        final Lease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease2.shardId());
        final List<KinesisClientRecord> lease2Records = recordBuffer.consumeRecords(lease2);

        // Until committed, the records stay in the buffer.
        assertEquals(lease1Records, lease2Records);
    }

    @Test
    void testReturnBufferIdToPoolWithNoPendingRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        // Get buffer from pool and consume all records.
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);
        recordBuffer.commitConsumedRecords(lease);

        // Return buffer ID to pool - should not make buffer available since no pending records.
        recordBuffer.returnBufferLease(lease);

        // Should not be able to get buffer from pool.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());
    }

    @Test
    void testConsumerLeaseLost() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        // Before lease lost buffer should be available in the pool.
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease.shardId());

        // Simulate lease lost.
        recordBuffer.consumerLeaseLost(bufferId);

        // Should not be able to consume records from invalidated buffer.
        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertTrue(consumedRecords.isEmpty());

        // Should not be able to commit records for invalidated buffer.
        recordBuffer.commitConsumedRecords(lease);
        assertEquals(TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER, checkpointer1.latestCheckpointedSequenceNumber());

        // Buffer should not be available in pool.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());
    }

    @Test
    void testCheckpointEndedShard() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);
        recordBuffer.commitConsumedRecords(lease);

        recordBuffer.checkpointEndedShard(bufferId, checkpointer2);
        assertEquals(TestCheckpointer.LATEST_SEQUENCE_NUMBER, checkpointer2.latestCheckpointedSequenceNumber());

        // Buffer should be removed and not available for operations.
        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertTrue(consumedRecords.isEmpty());
    }

    @Test
    void testShutdownShardConsumption_forEmptyBuffer() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);
        recordBuffer.commitConsumedRecords(lease);

        recordBuffer.shutdownShardConsumption(bufferId, checkpointer2);
        assertEquals(TestCheckpointer.LATEST_SEQUENCE_NUMBER, checkpointer2.latestCheckpointedSequenceNumber());

        // Buffer should be removed and not available for operations.
        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertTrue(consumedRecords.isEmpty());
    }

    @Test
    void testShutdownShardConsumption_forBufferWithRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        recordBuffer.shutdownShardConsumption(bufferId, checkpointer2);
        assertEquals(TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER, checkpointer1.latestCheckpointedSequenceNumber());
        assertEquals(TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER, checkpointer2.latestCheckpointedSequenceNumber());

        assertTrue(recordBuffer.acquireBufferLease().isEmpty(), "Buffer should not be available after shutdown");
    }

    @Test
    void testShutdownShardConsumption_whileOtherShardIsValid() {
        final int bufferSize = 100;

        // Create buffer with small memory limit.
        final MemoryBoundRecordBuffer recordBuffer = new MemoryBoundRecordBuffer(new NopComponentLog(), bufferSize, CHECKPOINT_INTERVAL);
        final ShardBufferId bufferId1 = recordBuffer.createBuffer(SHARD_ID_1);
        final ShardBufferId bufferId2 = recordBuffer.createBuffer(SHARD_ID_2);

        final List<KinesisClientRecord> records1 = List.of(createRecordWithSize(bufferSize));
        recordBuffer.addRecords(bufferId1, records1, checkpointer1);

        // Shutting down a buffer with a record.
        recordBuffer.shutdownShardConsumption(bufferId1, checkpointer1);

        // Adding records to another buffer.
        final List<KinesisClientRecord> records2 = List.of(createRecordWithSize(bufferSize));
        assertTimeoutPreemptively(
                Duration.ofSeconds(1),
                () -> recordBuffer.addRecords(bufferId2, records2, checkpointer2),
                "Records should be added to a buffer without memory backpressure");

        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_2, lease.shardId(), "Expected to acquire a lease for " + SHARD_ID_2);
        assertEquals(records2, recordBuffer.consumeRecords(lease));
    }

    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testMemoryBackpressure() throws InterruptedException {
        // Create buffer with small memory limit.
        final MemoryBoundRecordBuffer recordBuffer = new MemoryBoundRecordBuffer(new NopComponentLog(), 100L, CHECKPOINT_INTERVAL);
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);

        // Still fits into the buffer.
        final List<KinesisClientRecord> initialRecords = List.of(createRecordWithSize(80), createRecordWithSize(20));
        recordBuffer.addRecords(bufferId, initialRecords, checkpointer1);

        final CountDownLatch startLatch = new CountDownLatch(1);

        final List<KinesisClientRecord> notFittingRecords = List.of(createRecordWithSize(50));
        // Thread that tries to add records (should block due to memory limit).
        final Thread addRecordsThread = new Thread(() -> {
            startLatch.countDown();
            // Doesn't fit into the buffer.
            recordBuffer.addRecords(bufferId, notFittingRecords, checkpointer1);
        });

        addRecordsThread.start();
        startLatch.await();

        // Wait for thread to try to add records and get blocked.
        Thread.sleep(200);
        assertTrue(addRecordsThread.isAlive(), "Thread should be blocked waiting for memory");

        // Commit records in the buffer to free memory.
        final Lease lease1 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(initialRecords, recordBuffer.consumeRecords(lease1));
        recordBuffer.commitConsumedRecords(lease1);
        recordBuffer.returnBufferLease(lease1);

        // Thread should get unblocked and add the message.
        addRecordsThread.join();
        final Lease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(notFittingRecords, recordBuffer.consumeRecords(lease2));
    }

    @Test
    void testMemoryBackpressure_forRecordsLargerThanMaxBuffer() {
        // Create buffer with small memory limit.
        final int bufferSize = 10;
        final MemoryBoundRecordBuffer recordBuffer = new MemoryBoundRecordBuffer(new NopComponentLog(), bufferSize, CHECKPOINT_INTERVAL);
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);

        // A single batch with size double the buffer size.
        final List<KinesisClientRecord> reallyLargeBatch = List.of(createRecordWithSize(bufferSize), createRecordWithSize(bufferSize));

        // It's possible to insert a batch that exceeds the buffer size.
        assertDoesNotThrow(() -> recordBuffer.addRecords(bufferId, reallyLargeBatch, checkpointer1));
    }

    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testConcurrentBufferAccess() throws InterruptedException {
        final int numberOfShards = 10;
        final int numberOfRecordConsumers = 10;
        final int totalThreads = numberOfShards + numberOfRecordConsumers;

        final int recordsPerShard = 5;

        final ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        // All threads start at the same moment.
        final CountDownLatch startLatch = new CountDownLatch(totalThreads);
        final CountDownLatch finishLatch = new CountDownLatch(totalThreads);

        final List<Integer> shardIds = IntStream.range(0, numberOfShards).boxed().toList();
        final List<ShardBufferId> bufferIds = shardIds.stream()
                .map(shardId -> recordBuffer.createBuffer("shard-" + shardId))
                .toList();

        // Every producer returns a checkpointer for the records it produced.
        final List<TestCheckpointer> checkpointers = shardIds.stream()
                .map(shardId -> {
                    final ShardBufferId bufferId = bufferIds.get(shardId);
                    final List<KinesisClientRecord> records = createTestRecords(recordsPerShard);
                    final TestCheckpointer threadCheckpointer = new TestCheckpointer();

                    executor.submit(() -> {
                        try {
                            startLatch.countDown();
                            startLatch.await();

                            recordBuffer.addRecords(bufferId, records, threadCheckpointer);
                        } catch (final InterruptedException e) {
                            throw new RuntimeException(e);
                        } finally {
                            finishLatch.countDown();
                        }
                    });

                    return threadCheckpointer;
                })
                .toList();

        // Every consumer returns a list of shardId:sequenceNumber strings for the records it consumed.
        final List<Future<List<String>>> processedRecordsFutures = IntStream.range(0, numberOfRecordConsumers)
                .mapToObj(__ -> executor.submit(() -> {
                    try {
                        startLatch.countDown();
                        startLatch.await();

                        Optional<Lease> maybeLease = Optional.empty();
                        while (maybeLease.isEmpty()) {
                            maybeLease = recordBuffer.acquireBufferLease();
                            Thread.sleep(100); // Wait for records to be added by producers.
                        }

                        final Lease lease = maybeLease.orElseThrow();
                        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);

                        recordBuffer.commitConsumedRecords(lease);

                        return consumedRecords.stream()
                                .map(record -> lease.shardId() + ":" + record.sequenceNumber())
                                .toList();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        finishLatch.countDown();
                    }
                }))
                .toList();

        finishLatch.await();
        executor.shutdown();

        assertAll(
                checkpointers.stream().map(it -> () ->
                        assertNotEquals(
                                TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER,
                                it.latestCheckpointedSequenceNumber(),
                                "Every checkpointer should have been called"))
        );

        final long uniqueRecordsProcessed = processedRecordsFutures.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(Collection::stream)
                .distinct()
                .count();
        assertEquals(numberOfShards * recordsPerShard, uniqueRecordsProcessed);
    }

    @Test
    void testGetMultipleBuffersFromPool() {
        final ShardBufferId bufferId1 = recordBuffer.createBuffer(SHARD_ID_1);
        final ShardBufferId bufferId2 = recordBuffer.createBuffer(SHARD_ID_2);

        // Add records to both buffers.
        recordBuffer.addRecords(bufferId1, createTestRecords(2), checkpointer1);
        recordBuffer.addRecords(bufferId2, createTestRecords(3), checkpointer2);

        // Should be able to get both buffer IDs from pool.
        final Lease lease1 = recordBuffer.acquireBufferLease().orElseThrow();
        final Lease lease2 = recordBuffer.acquireBufferLease().orElseThrow();

        assertEquals(SHARD_ID_1, lease1.shardId());
        assertEquals(SHARD_ID_2, lease2.shardId());

        // Should get different buffer IDs.
        assertNotEquals(lease1.shardId(), lease2.shardId());
    }

    @Test
    void testCommitRecordsWhileNewRecordsArrive() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);

        // Add records to pending queue.
        final List<KinesisClientRecord> batch1 = createTestRecords(2);
        recordBuffer.addRecords(bufferId, batch1, checkpointer1);

        // Consume batch1 records (moves from pending to in-progress).
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();

        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertEquals(batch1, consumedRecords);

        // Add more records while others are in-progress.
        final List<KinesisClientRecord> batch2 = createTestRecords(1);
        recordBuffer.addRecords(bufferId, batch2, checkpointer2);

        // Commit in-progress records.
        recordBuffer.commitConsumedRecords(lease);

        // Consume batch2 records.
        final List<KinesisClientRecord> remainingRecords = recordBuffer.consumeRecords(lease);
        assertEquals(batch2, remainingRecords);
    }

    @ParameterizedTest
    @ValueSource(classes = {
            KinesisClientLibDependencyException.class,
            InvalidStateException.class,
            ThrottlingException.class,
    })
    void testRetriableCheckpointExceptions(final Class<? extends Exception> exceptionClass) throws Exception {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        final Exception exception = exceptionClass.getDeclaredConstructor(String.class).newInstance("Thrown from test");
        final TestCheckpointer failingCheckpointer = new TestCheckpointer(exception, 2);

        recordBuffer.addRecords(bufferId, records, failingCheckpointer);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();
        recordBuffer.consumeRecords(lease);

        // Should handle all exception types gracefully.
        recordBuffer.commitConsumedRecords(lease);

        assertEquals(records.getLast().sequenceNumber(), failingCheckpointer.latestCheckpointedSequenceNumber());
    }

    @Test
    void testShutdownDuringCheckpoint() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        final ShutdownException exception = new ShutdownException("Test shutdown exception");
        final TestCheckpointer failingCheckpointer = new TestCheckpointer(exception, 1);

        recordBuffer.addRecords(bufferId, records, failingCheckpointer);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();
        recordBuffer.consumeRecords(lease);

        recordBuffer.commitConsumedRecords(lease);
        assertEquals(TestCheckpointer.NO_CHECKPOINT_SEQUENCE_NUMBER, failingCheckpointer.latestCheckpointedSequenceNumber());
    }

    @Test
    @Timeout(value = 3, unit = SECONDS)
    void testCheckpointEndedShardWaitsForPendingRecords() throws InterruptedException {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final Lease lease = recordBuffer.acquireBufferLease().orElseThrow();
        recordBuffer.consumeRecords(lease);

        final CountDownLatch finishStarted = new CountDownLatch(1);

        // Start finishConsumption in another thread.
        final Thread finishThread = new Thread(() -> {
            finishStarted.countDown();
            recordBuffer.checkpointEndedShard(bufferId, checkpointer2);
        });

        finishThread.start();
        finishStarted.await();

        // Give finishThread time to get blocked.
        Thread.sleep(200);
        assertTrue(finishThread.isAlive(), "finishConsumption should block until records are committed");

        // Commit records to unblock finishConsumption.
        recordBuffer.commitConsumedRecords(lease);
        assertEquals(records.getLast().sequenceNumber(), checkpointer1.latestCheckpointedSequenceNumber());

        finishThread.join();
        assertEquals(
                TestCheckpointer.LATEST_SEQUENCE_NUMBER,
                checkpointer2.latestCheckpointedSequenceNumber(),
                "Checkpointer should be called after finishConsumption unblocks");
    }

    private List<KinesisClientRecord> createTestRecords(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    final String data = "test-record-" + i;
                    return KinesisClientRecord.builder()
                            .data(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer())
                            .partitionKey("partition-" + i)
                            .sequenceNumber(String.valueOf(i))
                            .build();
                })
                .toList();
    }

    private KinesisClientRecord createRecordWithSize(int sizeBytes) {
        final byte[] data = new byte[sizeBytes];
        Arrays.fill(data, (byte) 'X');

        return KinesisClientRecord.builder()
                .data(ByteBuffer.wrap(data).asReadOnlyBuffer())
                .partitionKey("partition")
                .sequenceNumber("1")
                .build();
    }

    /**
     * Test implementation of RecordProcessorCheckpointer that tracks checkpoint calls
     * and can simulate various exception scenarios.
     */
    private static class TestCheckpointer implements RecordProcessorCheckpointer {

        static final String NO_CHECKPOINT_SEQUENCE_NUMBER = "NONE";
        static final String LATEST_SEQUENCE_NUMBER = "LATEST";

        private final Exception exceptionToThrow;
        private final AtomicInteger throwsLeft;

        private volatile String latestCheckpointedSequenceNumber = NO_CHECKPOINT_SEQUENCE_NUMBER;

        TestCheckpointer() {
            this.exceptionToThrow = null;
            this.throwsLeft = new AtomicInteger(0);
        }

        TestCheckpointer(final Exception exceptionToThrow, final int maxThrows) {
            this.exceptionToThrow = exceptionToThrow;
            this.throwsLeft = new AtomicInteger(maxThrows);
        }

        @Override
        public void checkpoint() throws KinesisClientLibDependencyException, InvalidStateException, ThrottlingException, ShutdownException {
            doCheckpoint(LATEST_SEQUENCE_NUMBER);
        }

        @Override
        public void checkpoint(Record record) {
            throw notImplemented();
        }

        @Override
        public void checkpoint(String sequenceNumber) {
            throw notImplemented();
        }

        @Override
        public void checkpoint(String sequenceNumber, long subSequenceNumber) throws ShutdownException, InvalidStateException {
            doCheckpoint(sequenceNumber);
        }

        private void doCheckpoint(final String sequenceNumber) throws InvalidStateException, ShutdownException {
            if (exceptionToThrow != null && throwsLeft.decrementAndGet() == 0) {
                switch (exceptionToThrow) {
                    case KinesisClientLibDependencyException e -> throw e;
                    case InvalidStateException e -> throw e;
                    case ThrottlingException e -> throw e;
                    case ShutdownException e -> throw e;
                    default -> throw new RuntimeException(exceptionToThrow);
                }
            }

            latestCheckpointedSequenceNumber = sequenceNumber;
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint() {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(byte[] applicationState) {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(Record record) {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(Record record, byte[] applicationState) {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber) {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber, byte[] applicationState) {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber) {
            throw notImplemented();
        }

        @Override
        public PreparedCheckpointer prepareCheckpoint(String sequenceNumber, long subSequenceNumber, byte[] applicationState) {
            throw notImplemented();
        }

        @Override
        public Checkpointer checkpointer() {
            throw notImplemented();
        }

        String latestCheckpointedSequenceNumber() {
            return latestCheckpointedSequenceNumber;
        }

        private static RuntimeException notImplemented() {
            return new UnsupportedOperationException("Not implemented for test");
        }
    }
}
