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
import org.apache.nifi.processors.aws.kinesis.RecordBuffer.ShardBufferId;
import org.apache.nifi.processors.aws.kinesis.RecordBuffer.ShardBufferLease;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordBufferTest {

    private static final long MAX_MEMORY_BYTES = 1024L;
    private static final String SHARD_ID_1 = "shard-1";
    private static final String SHARD_ID_2 = "shard-2";

    private RecordBuffer recordBuffer;
    private TestCheckpointer checkpointer1;
    private TestCheckpointer checkpointer2;

    @BeforeEach
    void setUp() {
        recordBuffer = new RecordBuffer(new NopComponentLog(), MAX_MEMORY_BYTES);
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
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();
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

        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();

        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertEquals(records, consumedRecords);
        // Just consuming record should not checkpoint them.
        assertFalse(checkpointer1.isCheckpointed());
    }

    @Test
    void testCommitConsumedRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);
        recordBuffer.commitConsumedRecords(lease);

        assertTrue(checkpointer1.isCheckpointed());
    }

    @Test
    void testRollbackConsumedRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(3);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();

        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        final List<String> messages = consumedRecords.stream()
                .map(this::readContent)
                .toList();

        recordBuffer.rollbackConsumedRecords(lease);

        // Checkpointer should not be called during rollback.
        assertFalse(checkpointer1.isCheckpointed());

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

        final ShardBufferLease lease1 = recordBuffer.acquireBufferLease().orElseThrow();

        // Consume some records but don't return buffer id to the pool.
        recordBuffer.consumeRecords(lease1);

        recordBuffer.addRecords(bufferId, createTestRecords(1), checkpointer2);

        // The buffer is still unavailable.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());

        // After returning id to the pool it's possible to get the buffer from pool again.
        recordBuffer.returnBufferLease(lease1);
        final ShardBufferLease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease2.shardId());
    }

    @Test
    void testReturnBufferIdToPool_multipleReturns() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        final ShardBufferLease lease1 = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.returnBufferLease(lease1);
        recordBuffer.returnBufferLease(lease1);

        // Can retrieve the id only once.
        final ShardBufferLease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease2.shardId());
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());
    }

    @Test
    void testReturnBufferIdToPool_withUncommittedRecords() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(2);

        recordBuffer.addRecords(bufferId, records, checkpointer1);

        final ShardBufferLease lease1 = recordBuffer.acquireBufferLease().orElseThrow();

        // Consume some records, but don't commit them.
        final List<KinesisClientRecord> lease1Records = recordBuffer.consumeRecords(lease1);
        recordBuffer.returnBufferLease(lease1);

        final ShardBufferLease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
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
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();

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
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(SHARD_ID_1, lease.shardId());

        // Simulate lease lost.
        recordBuffer.consumerLeaseLost(bufferId);

        // Should not be able to consume records from invalidated buffer.
        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertTrue(consumedRecords.isEmpty());

        // Should not be able to commit records for invalidated buffer.
        recordBuffer.commitConsumedRecords(lease);
        assertFalse(checkpointer1.isCheckpointed());

        // Buffer should not be available in pool.
        assertTrue(recordBuffer.acquireBufferLease().isEmpty());
    }

    @Test
    void testFinishConsumption() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();

        recordBuffer.consumeRecords(lease);
        recordBuffer.commitConsumedRecords(lease);

        // Finish consumption (shard ended).
        recordBuffer.finishConsumption(bufferId, checkpointer2);
        assertTrue(checkpointer2.isCheckpointed());

        // Buffer should be removed and not available for operations.
        final List<KinesisClientRecord> consumedRecords = recordBuffer.consumeRecords(lease);
        assertTrue(consumedRecords.isEmpty());
    }

    @Test
    @Timeout(value = 5, unit = SECONDS)
    void testMemoryBackpressure() throws InterruptedException {
        // Create buffer with small memory limit.
        final RecordBuffer recordBuffer = new RecordBuffer(new NopComponentLog(), 100L);
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);

        // Still fits into the buffer.
        final List<KinesisClientRecord> initialRecords = List.of(createRecordWithSize(80));
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
        final ShardBufferLease lease1 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(initialRecords, recordBuffer.consumeRecords(lease1));
        recordBuffer.commitConsumedRecords(lease1);

        // Thread should get unblocked and add the message.
        addRecordsThread.join();
        final ShardBufferLease lease2 = recordBuffer.acquireBufferLease().orElseThrow();
        assertEquals(notFittingRecords, recordBuffer.consumeRecords(lease2));
    }

    @Test
    void testMemoryBackpressure_forRecordsLargerThanMaxBuffer() {
        // Create buffer with small memory limit.
        final int bufferSize = 10;
        final RecordBuffer recordBuffer = new RecordBuffer(new NopComponentLog(), bufferSize);
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);

        final KinesisClientRecord reallyLargeRecord = createRecordWithSize(bufferSize + 1);

        final IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> recordBuffer.addRecords(bufferId, List.of(reallyLargeRecord), checkpointer1));
        assertTrue(e.getMessage().startsWith("Attempting to consume more memory than allowed."));
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

                        Optional<ShardBufferLease> maybeLease = Optional.empty();
                        while (maybeLease.isEmpty()) {
                            maybeLease = recordBuffer.acquireBufferLease();
                            Thread.sleep(100); // Wait for records to be added by producers.
                        }

                        final ShardBufferLease lease = maybeLease.orElseThrow();
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
                checkpointers.stream()
                        .map(it -> () -> assertTrue(it.isCheckpointed(), "Every checkpointer should have been called"))
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
        final ShardBufferLease lease1 = recordBuffer.acquireBufferLease().orElseThrow();
        final ShardBufferLease lease2 = recordBuffer.acquireBufferLease().orElseThrow();

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
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();

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
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();
        recordBuffer.consumeRecords(lease);

        // Should handle all exception types gracefully.
        recordBuffer.commitConsumedRecords(lease);

        assertTrue(failingCheckpointer.isCheckpointed());
    }

    @Test
    void testShutdownDuringCheckpoint() {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        final ShutdownException exception = new ShutdownException("Test shutdown exception");
        final TestCheckpointer failingCheckpointer = new TestCheckpointer(exception, 1);

        recordBuffer.addRecords(bufferId, records, failingCheckpointer);
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();
        recordBuffer.consumeRecords(lease);

        recordBuffer.commitConsumedRecords(lease);
        assertFalse(failingCheckpointer.isCheckpointed());
    }

    @Test
    @Timeout(value = 3, unit = SECONDS)
    void testFinishConsumptionWaitsForPendingRecords() throws InterruptedException {
        final ShardBufferId bufferId = recordBuffer.createBuffer(SHARD_ID_1);
        final List<KinesisClientRecord> records = createTestRecords(1);

        recordBuffer.addRecords(bufferId, records, checkpointer1);
        final ShardBufferLease lease = recordBuffer.acquireBufferLease().orElseThrow();
        recordBuffer.consumeRecords(lease);

        final CountDownLatch finishStarted = new CountDownLatch(1);

        // Start finishConsumption in another thread.
        final Thread finishThread = new Thread(() -> {
            finishStarted.countDown();
            recordBuffer.finishConsumption(bufferId, checkpointer2);
        });

        finishThread.start();
        finishStarted.await();

        // Give finishThread time to get blocked.
        Thread.sleep(200);
        assertTrue(finishThread.isAlive(), "finishConsumption should block until records are committed");

        // Commit records to unblock finishConsumption.
        recordBuffer.commitConsumedRecords(lease);
        assertTrue(checkpointer1.isCheckpointed());

        finishThread.join();
        assertTrue(checkpointer2.isCheckpointed(), "Checkpointer should be called after finishConsumption unblocks");
    }

    private List<KinesisClientRecord> createTestRecords(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    final String data = "test-record-" + i;
                    final KinesisClientRecord record = KinesisClientRecord.builder()
                            .data(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer())
                            .partitionKey("partition-" + i)
                            .sequenceNumber(String.valueOf(i))
                            .build();
                    return record;
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

        private final AtomicBoolean checkpointed = new AtomicBoolean(false);
        private final Exception exceptionToThrow;
        private final AtomicInteger throwsLeft;

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
            if (exceptionToThrow != null && throwsLeft.decrementAndGet() == 0) {
                switch (exceptionToThrow) {
                    case KinesisClientLibDependencyException e -> throw e;
                    case InvalidStateException e -> throw e;
                    case ThrottlingException e -> throw e;
                    case ShutdownException e -> throw e;
                    default -> throw new RuntimeException(exceptionToThrow);
                }
            }

            if (checkpointed.getAndSet(true)) {
                throw new IllegalStateException("TestCheckpointer has already been checkpointed");
            }
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
        public void checkpoint(String sequenceNumber, long subSequenceNumber) {
            throw notImplemented();
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

        boolean isCheckpointed() {
            return checkpointed.get();
        }

        private static RuntimeException notImplemented() {
            return new UnsupportedOperationException("Not implemented for test");
        }
    }
}