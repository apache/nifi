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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PollingKinesisClientTest {

    private static final GetShardIteratorResponse ITERATOR_RESPONSE =
            GetShardIteratorResponse.builder().shardIterator("iter-1").build();

    private KinesisClient mockKinesisClient;
    private KinesisShardManager mockShardManager;
    private PollingKinesisClient consumer;

    @BeforeEach
    void setUp() {
        mockKinesisClient = mock(KinesisClient.class);
        mockShardManager = mock(KinesisShardManager.class);
        consumer = new PollingKinesisClient(mockKinesisClient, mock(ComponentLog.class), 1L, 1L);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * When a shard reaches exhaustion (nextShardIterator is null), the records from the
     * final GetRecords response must be queued before the loop exits. A regression would
     * silently drop the last batch of every exhausted shard.
     */
    @Test
    void testExhaustedShardDeliversAllRecords() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "A"), record("200", "B"), record("300", "C"))
                .nextShardIterator(null).millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult result = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(result, "Last batch of an exhausted shard must not be dropped");
        assertEquals(3, result.records().size());
        assertEquals(new BigInteger("100"), result.firstSequenceNumber());
        assertEquals(new BigInteger("300"), result.lastSequenceNumber());

        assertEventuallyNoPendingFetches();
    }

    /**
     * When a fetch loop thread dies from an uncaught Throwable, the next startFetches call
     * must detect the dead loop and restart it so records continue flowing. An Error thrown
     * from readCheckpoint escapes the Exception catch in getShardIterator, propagates through
     * runFetchLoop, and is caught by the Throwable guard in launchFetchLoop.
     */
    @Test
    void testDeadLoopRecoveryRestoresDataFlow() throws Exception {
        when(mockShardManager.readCheckpoint(anyString()))
                .thenThrow(new AssertionError("simulated loop death"))
                .thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "data")).nextShardIterator(null).millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        final List<Shard> shards = shards("shard-1");
        consumer.startFetches(shards, "test-stream", 1000, "TRIM_HORIZON", mockShardManager);
        Thread.sleep(100);

        consumer.startFetches(shards, "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult result = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(result, "Dead loop must be restarted and produce records");
        assertEquals(new BigInteger("100"), result.firstSequenceNumber());
    }

    /**
     * Validates that after a rollback (session commit failure), the fetch loop re-acquires
     * the shard iterator from the checkpoint position, not from where it left off. This
     * prevents data loss when in-flight records need to be re-consumed.
     */
    @Test
    void testRollbackCausesIteratorReAcquisitionFromCheckpoint() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null).thenReturn("500");
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("600", "data"), record("700", "data2"))
                .nextShardIterator("iter-next").millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult firstResult = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstResult);

        consumer.rollbackResults(List.of(firstResult));

        final ArgumentCaptor<GetShardIteratorRequest> captor = ArgumentCaptor.forClass(GetShardIteratorRequest.class);
        verify(mockKinesisClient, timeout(5000).atLeast(2)).getShardIterator(captor.capture());

        final GetShardIteratorRequest reAcquisition = captor.getAllValues().get(captor.getAllValues().size() - 1);
        assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, reAcquisition.shardIteratorType(),
                "After rollback, iterator must resume from checkpoint, not from where it left off");
        assertEquals("500", reAcquisition.startingSequenceNumber());
    }

    /**
     * Validates that an expired iterator is transparently replaced by re-acquiring from
     * the checkpoint. Records must still arrive after the transient error.
     */
    @Test
    void testExpiredIteratorRecovery() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final ExpiredIteratorException expired = ExpiredIteratorException.builder().message("expired").build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "data")).nextShardIterator(null).millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(expired).thenReturn(response);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult result = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(result, "Records must arrive after expired iterator recovery");

        verify(mockKinesisClient, timeout(5000).atLeast(2)).getShardIterator(any(GetShardIteratorRequest.class));
    }

    /**
     * Verifies that iterator recovery does not reorder results within a shard when newer data
     * is already queued ahead of replay from the persisted checkpoint.
     */
    @Test
    void testExpiredIteratorRecoveryDoesNotDeliverSameShardOutOfOrder() throws Exception {
        final AtomicInteger getRecordsCallCount = new AtomicInteger();
        final AtomicInteger getShardIteratorCallCount = new AtomicInteger();
        final CountDownLatch firstResultPolled = new CountDownLatch(1);

        when(mockShardManager.readCheckpoint(anyString())).thenReturn("100");
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenAnswer(invocation -> {
            final int callNumber = getShardIteratorCallCount.incrementAndGet();
            return GetShardIteratorResponse.builder().shardIterator("iter-" + callNumber).build();
        });

        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenAnswer(invocation -> {
            getRecordsCallCount.incrementAndGet();
            final GetRecordsRequest request = invocation.getArgument(0);

            return switch (request.shardIterator()) {
                case "iter-1" -> GetRecordsResponse.builder()
                        .records(record("200", "A"))
                        .nextShardIterator("iter-1a")
                        .millisBehindLatest(0L)
                        .build();
                case "iter-1a" -> {
                    firstResultPolled.await(10, TimeUnit.SECONDS);
                    yield GetRecordsResponse.builder()
                            .records(record("300", "B"))
                            .nextShardIterator("iter-1b")
                            .millisBehindLatest(0L)
                            .build();
                }
                case "iter-1b" -> throw ExpiredIteratorException.builder().message("expired").build();
                case "iter-2" -> GetRecordsResponse.builder()
                        .records(record("200", "A-replay"))
                        .nextShardIterator("iter-2a")
                        .millisBehindLatest(0L)
                        .build();
                default -> GetRecordsResponse.builder()
                        .records(List.<Record>of())
                        .nextShardIterator(request.shardIterator())
                        .millisBehindLatest(0L)
                        .build();
            };
        });

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult firstResult = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstResult, "Initial result must be available");
        assertEquals(new BigInteger("200"), firstResult.firstSequenceNumber());

        firstResultPolled.countDown();

        final long replayQueuedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < replayQueuedDeadline && (getShardIteratorCallCount.get() < 2 || getRecordsCallCount.get() < 4)) {
            Thread.sleep(20);
        }

        final ShardFetchResult firstAfterRecovery = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstAfterRecovery, "Queue must contain results after iterator recovery");
        assertEquals(new BigInteger("200"), firstAfterRecovery.firstSequenceNumber(),
                "Replayed checkpoint data must be delivered before any stale newer batch from the same shard");
    }

    /**
     * Validates that throttling (ProvisionedThroughputExceededException) is transient:
     * the loop retries and records eventually arrive without data loss.
     */
    @Test
    void testThrottledFetchRetriesWithoutDataLoss() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final ProvisionedThroughputExceededException throttled =
                ProvisionedThroughputExceededException.builder().message("throttled").build();
        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "data")).nextShardIterator(null).millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(throttled).thenReturn(response);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult result = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(result, "Throttled fetch must retry and eventually deliver records");
    }

    /**
     * Validates that a RuntimeException from readCheckpoint (e.g., DynamoDB throttle on
     * startup) does not permanently kill the fetch loop. The getShardIterator method
     * catches the exception and returns null, the loop backs off and retries. Records
     * must eventually arrive.
     */
    @Test
    void testCheckpointReadFailureRetriesWithoutKillingLoop() throws Exception {
        when(mockShardManager.readCheckpoint(anyString()))
                .thenThrow(new RuntimeException("DynamoDB throttle"))
                .thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "data")).nextShardIterator(null).millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult result = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(result, "Checkpoint read failure must not permanently kill the fetch loop");
    }

    /**
     * Validates that close() terminates all fetch loops and that the consumer reports
     * no pending work, preventing the processor from spin-waiting after shutdown.
     */
    @Test
    void testCloseTerminatesAllFetchingAndDrainsQueue() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "data")).nextShardIterator("iter-next").millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        consumer.startFetches(shards("shard-1", "shard-2"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        verify(mockKinesisClient, timeout(5000).atLeast(1)).getRecords(any(GetRecordsRequest.class));

        consumer.close();

        assertFalse(consumer.hasPendingFetches(), "After close, hasPendingFetches must return false");

        consumer = null;
    }

    /**
     * Validates that once all shards are exhausted and the queue is drained,
     * hasPendingFetches returns false. This ensures the processor's poll loop
     * exits promptly rather than spin-waiting indefinitely.
     */
    @Test
    void testHasPendingFetchesFalseWhenAllShardsExhausted() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final GetRecordsResponse response = GetRecordsResponse.builder()
                .records(record("100", "data")).nextShardIterator(null).millisBehindLatest(0L).build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        consumer.startFetches(shards("shard-1", "shard-2"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        drainAllResults();
        assertEventuallyNoPendingFetches();
    }

    /**
     * Reproduces out-of-order delivery caused by stale results remaining in the per-shard queue after a rollback.
     * The scenario for a single shard:
     *
     * <ol>
     *   <li>Fetch loop enqueues result 1 (sequence 100-200) and result 2 (sequence 300-400).</li>
     *   <li>Consumer polls result 1 only; result 2 remains in the queue.</li>
     *   <li>Consumer calls rollbackResults on result 1, which drains the queue synchronously and sets the reset flag.
     *       The fetch loop detects the flag, drains any stragglers, resets the shard iterator, and re-fetches.</li>
     *   <li>After the reset, the first result polled must come from the re-fetched sequence (sequence 500),
     *       not the stale result 2 (sequence 300).</li>
     * </ol>
     */
    @Test
    void testRollbackDrainsStaleResultsFromQueue() throws Exception {
        final AtomicInteger getRecordsCallCount = new AtomicInteger();
        final AtomicInteger getShardIteratorCallCount = new AtomicInteger();

        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenAnswer(invocation -> {
            final int callNumber = getShardIteratorCallCount.incrementAndGet();
            return GetShardIteratorResponse.builder().shardIterator("iter-" + callNumber).build();
        });

        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenAnswer(invocation -> {
            getRecordsCallCount.incrementAndGet();
            final GetRecordsRequest request = invocation.getArgument(0);

            if (request.shardIterator().equals("iter-1")) {
                return GetRecordsResponse.builder()
                        .records(record("100", "A"), record("200", "B"))
                        .nextShardIterator("iter-1a").millisBehindLatest(0L).build();
            }
            if (request.shardIterator().equals("iter-1a")) {
                return GetRecordsResponse.builder()
                        .records(record("300", "C"), record("400", "D"))
                        .nextShardIterator("iter-1b").millisBehindLatest(0L).build();
            }
            if (request.shardIterator().startsWith("iter-1")) {
                return GetRecordsResponse.builder()
                        .records(List.<Record>of())
                        .nextShardIterator(request.shardIterator()).millisBehindLatest(0L).build();
            }
            return GetRecordsResponse.builder()
                    .records(record("500", "E"), record("600", "F"))
                    .nextShardIterator("iter-post-reset-next").millisBehindLatest(0L).build();
        });

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult firstResult = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstResult, "First result must be available");
        assertEquals(new BigInteger("100"), firstResult.firstSequenceNumber());

        final long enqueueDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < enqueueDeadline && getRecordsCallCount.get() < 2) {
            Thread.sleep(20);
        }
        Thread.sleep(50);

        consumer.rollbackResults(List.of(firstResult));

        final long resetDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (getShardIteratorCallCount.get() < 2 && System.nanoTime() < resetDeadline) {
            Thread.sleep(20);
        }

        final ShardFetchResult firstAfterRollback = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstAfterRollback, "Queue must contain results after rollback and re-fetch");
        assertEquals(new BigInteger("500"), firstAfterRollback.firstSequenceNumber(),
                "First result after rollback must be re-fetched data, not a stale pre-rollback result");
    }

    /**
     * Verifies that rollbackResults drains the queue synchronously so that a concurrent consumer cannot poll stale
     * pre-rollback results. The fetch loop is blocked inside a GetRecords call while rollback happens, proving the
     * drain must occur in rollbackResults itself rather than being deferred to the fetch loop thread.
     */
    @Test
    void testRollbackDrainsSynchronouslyPreventingConcurrentStaleRead() throws Exception {
        final CountDownLatch fetchLoopBlocked = new CountDownLatch(1);
        final CountDownLatch unblockFetchLoop = new CountDownLatch(1);
        final AtomicInteger getRecordsCallCount = new AtomicInteger();

        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenReturn(GetShardIteratorResponse.builder().shardIterator("iter-1").build());

        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenAnswer(invocation -> {
            final int callNumber = getRecordsCallCount.incrementAndGet();
            final GetRecordsRequest request = invocation.getArgument(0);

            if (callNumber == 1) {
                return GetRecordsResponse.builder()
                        .records(record("100", "A"), record("200", "B"))
                        .nextShardIterator("iter-1a").millisBehindLatest(0L).build();
            }
            if (callNumber == 2) {
                return GetRecordsResponse.builder()
                        .records(record("300", "C"), record("400", "D"))
                        .nextShardIterator("iter-1b").millisBehindLatest(0L).build();
            }
            if (callNumber == 3) {
                fetchLoopBlocked.countDown();
                unblockFetchLoop.await(10, TimeUnit.SECONDS);
                return GetRecordsResponse.builder()
                        .records(List.<Record>of())
                        .nextShardIterator("iter-1c").millisBehindLatest(0L).build();
            }
            return GetRecordsResponse.builder()
                    .records(List.<Record>of())
                    .nextShardIterator(request.shardIterator()).millisBehindLatest(0L).build();
        });

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult firstResult = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstResult, "First result must be available");
        assertEquals(new BigInteger("100"), firstResult.firstSequenceNumber());

        fetchLoopBlocked.await(5, TimeUnit.SECONDS);

        consumer.rollbackResults(List.of(firstResult));

        final ShardFetchResult staleResult = consumer.pollShardResult("shard-1");

        unblockFetchLoop.countDown();

        assertNull(staleResult, "After rollback, stale results must not be pollable — rollbackResults must drain synchronously");
    }

    /**
     * Reproduces the race condition where the fetch loop returns records from GetRecords AFTER rollbackResults has
     * already drained the queue. Without the per-shard lock, the fetch loop would enqueue the stale result after the
     * drain, making it visible to the next consumer poll. With the lock, the enqueue is rejected because the fetch
     * loop sees isResetRequested inside the synchronized block.
     *
     * <p>Timeline:
     * <ol>
     *   <li>Fetch loop enqueues result 1 (sequence 100-200) and result 2 (sequence 300-400).</li>
     *   <li>Consumer polls result 1.</li>
     *   <li>GetRecords call 3 blocks, holding a response with records (sequence 500-600).</li>
     *   <li>Consumer rolls back result 1 — this drains result 2 and sets the reset flag.</li>
     *   <li>GetRecords call 3 unblocks — fetch loop receives the stale response.</li>
     *   <li>Fetch loop calls enqueueIfActive under the shard lock, sees isResetRequested, and discards the result.</li>
     *   <li>Fetch loop processes the reset and re-fetches from the checkpoint (sequence 800).</li>
     *   <li>The first result available after rollback must be the re-fetched data (sequence 800), not the stale data (sequence 500).</li>
     * </ol>
     */
    @Test
    void testLockPreventsStaleEnqueueDuringConcurrentRollback() throws Exception {
        final CountDownLatch fetchLoopBlockedInsideGetRecords = new CountDownLatch(1);
        final CountDownLatch unblockGetRecords = new CountDownLatch(1);
        final AtomicInteger getRecordsCallCount = new AtomicInteger();
        final AtomicInteger getShardIteratorCallCount = new AtomicInteger();

        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenAnswer(invocation -> {
            final int callNumber = getShardIteratorCallCount.incrementAndGet();
            return GetShardIteratorResponse.builder().shardIterator("iter-" + callNumber).build();
        });

        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenAnswer(invocation -> {
            final int callNumber = getRecordsCallCount.incrementAndGet();
            final GetRecordsRequest request = invocation.getArgument(0);

            if (callNumber == 1) {
                return GetRecordsResponse.builder()
                        .records(record("100", "A"), record("200", "B"))
                        .nextShardIterator("iter-1a").millisBehindLatest(0L).build();
            }
            if (callNumber == 2) {
                return GetRecordsResponse.builder()
                        .records(record("300", "C"), record("400", "D"))
                        .nextShardIterator("iter-1b").millisBehindLatest(0L).build();
            }
            if (callNumber == 3) {
                fetchLoopBlockedInsideGetRecords.countDown();
                unblockGetRecords.await(10, TimeUnit.SECONDS);
                return GetRecordsResponse.builder()
                        .records(record("500", "E"), record("600", "F"))
                        .nextShardIterator("iter-1c").millisBehindLatest(0L).build();
            }
            if (request.shardIterator().startsWith("iter-2")) {
                return GetRecordsResponse.builder()
                        .records(record("800", "G"), record("900", "H"))
                        .nextShardIterator("iter-2a").millisBehindLatest(0L).build();
            }
            return GetRecordsResponse.builder()
                    .records(List.<Record>of())
                    .nextShardIterator(request.shardIterator()).millisBehindLatest(0L).build();
        });

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        final ShardFetchResult firstResult = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstResult, "First result must be available");
        assertEquals(new BigInteger("100"), firstResult.firstSequenceNumber());

        fetchLoopBlockedInsideGetRecords.await(5, TimeUnit.SECONDS);

        consumer.rollbackResults(List.of(firstResult));

        unblockGetRecords.countDown();

        final long resetDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (getShardIteratorCallCount.get() < 2 && System.nanoTime() < resetDeadline) {
            Thread.sleep(20);
        }

        final ShardFetchResult firstAfterRollback = consumer.pollAnyResult(5, TimeUnit.SECONDS);
        assertNotNull(firstAfterRollback, "Queue must contain results after rollback and re-fetch");
        assertEquals(new BigInteger("800"), firstAfterRollback.firstSequenceNumber(),
                "First result after rollback must be re-fetched data (800), not the stale data (500) that was returned from GetRecords during the rollback");
    }

    /**
     * Verifies that when GetShardIterator throws {@link ResourceNotFoundException} (shard has
     * been deleted via split, merge, or stream resharding), the fetch loop marks the shard as
     * exhausted and exits cleanly rather than retrying indefinitely.
     */
    @Test
    void testResourceNotFoundOnGetShardIteratorMarksShardExhausted() throws Exception {
        final ResourceNotFoundException notFound = ResourceNotFoundException.builder()
                .message("Shard shard-1 does not exist")
                .build();
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenThrow(notFound);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        assertEventuallyNoPendingFetches();

        final ShardFetchResult result = consumer.pollAnyResult(500, TimeUnit.MILLISECONDS);
        assertNull(result);

        verify(mockKinesisClient, times(1)).getShardIterator(any(GetShardIteratorRequest.class));
        verify(mockKinesisClient, never()).getRecords(any(GetRecordsRequest.class));
    }

    /**
     * Verifies that when GetRecords throws {@link ResourceNotFoundException} (shard deleted
     * between obtaining an iterator and fetching records), the fetch loop marks the shard as
     * exhausted and exits cleanly.
     */
    @Test
    void testResourceNotFoundOnGetRecordsMarksShardExhausted() throws Exception {
        when(mockShardManager.readCheckpoint(anyString())).thenReturn(null);
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(ITERATOR_RESPONSE);

        final ResourceNotFoundException notFound = ResourceNotFoundException.builder()
                .message("Shard shard-1 does not exist")
                .build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenThrow(notFound);

        consumer.startFetches(shards("shard-1"), "test-stream", 1000, "TRIM_HORIZON", mockShardManager);

        assertEventuallyNoPendingFetches();

        final ShardFetchResult result = consumer.pollAnyResult(500, TimeUnit.MILLISECONDS);
        assertNull(result);

        // Records were not polled after receiving ResourceNotFoundException.
        verify(mockKinesisClient, times(1)).getRecords(any(GetRecordsRequest.class));
    }

    private void drainAllResults() throws InterruptedException {
        ShardFetchResult discarded;
        do {
            discarded = consumer.pollAnyResult(500, TimeUnit.MILLISECONDS);
        } while (discarded != null);
    }

    private void assertEventuallyNoPendingFetches() throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (!consumer.hasPendingFetches()) {
                return;
            }
            Thread.sleep(50);
        }
        assertFalse(consumer.hasPendingFetches(), "Expected hasPendingFetches to become false");
    }

    private static List<Shard> shards(final String... shardIds) {
        return Arrays.stream(shardIds).map(id -> Shard.builder().shardId(id).build()).toList();
    }

    private static Record record(final String sequenceNumber, final String data) {
        return Record.builder()
                .sequenceNumber(sequenceNumber).partitionKey("pk-" + sequenceNumber)
                .approximateArrivalTimestamp(Instant.now())
                .data(SdkBytes.fromString(data, StandardCharsets.UTF_8)).build();
    }
}
