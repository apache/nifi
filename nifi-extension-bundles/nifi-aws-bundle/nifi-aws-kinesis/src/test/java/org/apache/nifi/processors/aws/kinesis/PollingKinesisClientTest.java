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
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
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
        assertEquals("100", result.firstSequenceNumber());
        assertEquals("300", result.lastSequenceNumber());

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
        assertEquals("100", result.firstSequenceNumber());
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
