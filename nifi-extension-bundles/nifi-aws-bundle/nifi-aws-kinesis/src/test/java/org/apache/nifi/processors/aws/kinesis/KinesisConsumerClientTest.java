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
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KinesisConsumerClientTest {

    /**
     * Verifies that when an EFO subscription expires and is renewed, the renewal uses the
     * maximum of lastAcknowledged, lastQueued, and DynamoDB checkpoint. Here the acknowledged
     * sequence (99999) exceeds the checkpoint (11111), so the renewal should use 99999.
     */
    @Test
    void testSubscriptionRenewalUsesLastAcknowledgedSequenceNumber() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn("11111");

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);

        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(1, capturedRequests.size());
        assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, capturedRequests.get(0).startingPosition().type());
        assertEquals("11111", capturedRequests.get(0).startingPosition().sequenceNumber(),
                "Initial subscription should use the DynamoDB checkpoint");

        final EnhancedFanOutClient.ShardConsumer consumer = client.getShardConsumer("shardId-000000000001");
        consumer.setLastQueuedSequenceNumber(new BigInteger("99999"));
        consumer.resetForRenewal();

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(2, capturedRequests.size());
        assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, capturedRequests.get(1).startingPosition().type());
        assertEquals("99999", capturedRequests.get(1).startingPosition().sequenceNumber(),
                "Renewal should use max(lastQueued, checkpoint) = lastQueued");

        verify(mockShardManager, times(2)).readCheckpoint("shardId-000000000001");
    }

    /**
     * Verifies that when a ShardConsumer has no acknowledged data, the renewal falls back to
     * the DynamoDB checkpoint.
     */
    @Test
    void testSubscriptionRenewalFallsBackToCheckpointWhenNoQueuedData() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn("55555");

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);

        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        final EnhancedFanOutClient.ShardConsumer consumer = client.getShardConsumer("shardId-000000000001");
        consumer.resetForRenewal();

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(2, capturedRequests.size());
        assertEquals("55555", capturedRequests.get(0).startingPosition().sequenceNumber());
        assertEquals("55555", capturedRequests.get(1).startingPosition().sequenceNumber(),
                "Renewal with no acknowledged data should fall back to DynamoDB checkpoint");

        verify(mockShardManager, times(2)).readCheckpoint("shardId-000000000001");
    }

    /**
     * Verifies that renewal uses the lastQueuedSequenceNumber when it exceeds the checkpoint.
     */
    @Test
    void testSubscriptionRenewalUsesLastQueuedSequence() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn("10000");

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);
        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        final EnhancedFanOutClient.ShardConsumer consumer = client.getShardConsumer("shardId-000000000001");
        consumer.setLastQueuedSequenceNumber(new BigInteger("20000"));
        consumer.resetForRenewal();

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(2, capturedRequests.size());
        assertEquals("20000", capturedRequests.get(1).startingPosition().sequenceNumber(),
                "Renewal should use max(lastQueued=20000, checkpoint=10000) = lastQueued");

        verify(mockShardManager, times(2)).readCheckpoint("shardId-000000000001");
    }

    /**
     * Verifies that renewal always uses the maximum of lastQueued and the DynamoDB checkpoint.
     * This prevents one-event replay duplicates caused by races between onNext counter updates
     * and concurrent handler onError callbacks.
     */
    @Test
    void testSubscriptionRenewalAlwaysUsesMaxSequence() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn("50000");

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);
        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        simulateExpiredSubscriptionWithState(client, "shardId-000000000001", "90000");
        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(2, capturedRequests.size());
        assertEquals("90000", capturedRequests.get(1).startingPosition().sequenceNumber(),
                "Renewal should use max(lastQueued=90000, checkpoint=50000) = 90000");

        simulateExpiredSubscriptionWithState(client, "shardId-000000000001", "95000");
        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(3, capturedRequests.size());
        assertEquals("95000", capturedRequests.get(2).startingPosition().sequenceNumber(),
                "Renewal should use max(lastQueued=95000, checkpoint=50000) = 95000");

        verify(mockShardManager, times(3)).readCheckpoint("shardId-000000000001");
    }

    /**
     * Verifies that polling a queued result does not affect the renewal position. The renewal
     * uses max(lastQueued, checkpoint) regardless of whether results have been polled.
     */
    @Test
    void testSubscriptionRenewalAfterPollBeforeAcknowledgeUsesMaxSequence() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn("50000");

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);
        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);
        simulateExpiredSubscriptionWithState(client, "shardId-000000000001", "90000");
        client.enqueueResult(shardFetchResult("shardId-000000000001", "90000"));

        final ShardFetchResult polled = client.pollShardResult("shardId-000000000001");
        assertNotNull(polled, "Expected queued result to be polled");

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(2, capturedRequests.size());
        assertEquals("90000", capturedRequests.get(1).startingPosition().sequenceNumber(),
                "Renewal should use max(lastQueued=90000, checkpoint=50000) = 90000");

        verify(mockShardManager, times(2)).readCheckpoint("shardId-000000000001");
    }

    /**
     * Verifies that acknowledging multiple fetched results from the same shard requests only one
     * additional EFO event for that shard.
     */
    @Test
    void testAcknowledgeResultsRequestsNextOncePerShard() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn("50000");

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);
        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());
        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        final EnhancedFanOutClient.ShardConsumer consumer = client.getShardConsumer("shardId-000000000001");
        final Subscription subscription = mock(Subscription.class);
        consumer.setSubscription(subscription);
        consumer.pause();

        client.acknowledgeResults(List.of(
                shardFetchResult("shardId-000000000001", "60000"),
                shardFetchResult("shardId-000000000001", "61000")));

        verify(subscription, times(1)).request(1);
    }

    /**
     * Verifies that concurrent startup calls do not create duplicate initial subscriptions
     * for the same shard.
     */
    @Test
    void testConcurrentStartFetchesCreatesSingleInitialSubscriptionPerShard() throws Exception {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn(null);

        final KinesisAsyncClient mockAsyncClient = mock(KinesisAsyncClient.class);
        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();

        when(mockAsyncClient.subscribeToShard(any(SubscribeToShardRequest.class), any(SubscribeToShardResponseHandler.class)))
                .thenAnswer(invocation -> {
                    capturedRequests.add(invocation.getArgument(0));
                    Thread.sleep(50L);
                    return new CompletableFuture<>();
                });

        final EnhancedFanOutClient client = new EnhancedFanOutClient(mock(KinesisClient.class), mock(ComponentLog.class));
        client.initializeForTest(mockAsyncClient, "arn:aws:kinesis:us-east-1:123456789:stream/test/consumer/test:1");

        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());
        final CountDownLatch startLatch = new CountDownLatch(1);

        final Thread t1 = new Thread(() -> runStartFetches(client, shards, mockShardManager, startLatch));
        final Thread t2 = new Thread(() -> runStartFetches(client, shards, mockShardManager, startLatch));
        t1.start();
        t2.start();
        startLatch.countDown();
        t1.join(5_000L);
        t2.join(5_000L);

        assertEquals(1, capturedRequests.size(),
                "Concurrent startup should create only one initial SubscribeToShard request per shard");
    }

    private static EnhancedFanOutClient createEfoClient(final List<SubscribeToShardRequest> capturedRequests) {
        final KinesisAsyncClient mockAsyncClient = mock(KinesisAsyncClient.class);

        when(mockAsyncClient.subscribeToShard(any(SubscribeToShardRequest.class), any(SubscribeToShardResponseHandler.class)))
                .thenAnswer(invocation -> {
                    capturedRequests.add(invocation.getArgument(0));
                    return CompletableFuture.completedFuture(null);
                });

        final EnhancedFanOutClient client = new EnhancedFanOutClient(mock(KinesisClient.class), mock(ComponentLog.class));
        client.initializeForTest(mockAsyncClient, "arn:aws:kinesis:us-east-1:123456789:stream/test/consumer/test:1");
        return client;
    }

    private static void simulateExpiredSubscriptionWithState(
            final EnhancedFanOutClient client,
            final String shardId,
            final String lastQueuedSeq) {
        final EnhancedFanOutClient.ShardConsumer consumer = client.getShardConsumer(shardId);
        consumer.resetForRenewal();
        consumer.setLastQueuedSequenceNumber(new BigInteger(lastQueuedSeq));
    }

    /**
     * Verifies that a stale error callback from an old subscription does not corrupt the state
     * of a newer subscription. This tests the generation counter mechanism that prevents a race
     * between the response handler's onError and the subscriber's onError when they fire on
     * different threads for the same error, with a new subscription created in between.
     *
     * <p>The race without the generation counter:
     * <ol>
     *   <li>Response handler onError fires on Netty thread: subscribing = false</li>
     *   <li>NiFi thread creates new subscription B: subscribing = true</li>
     *   <li>Old subscriber onError fires: nulls B's subscription, subscribing = false</li>
     *   <li>B's state is corrupted, leading to duplicate subscriptions</li>
     * </ol>
     */
    @Test
    void testStaleErrorCallbackDoesNotCorruptNewSubscription() throws Exception {
        final ComponentLog mockLogger = mock(ComponentLog.class);
        final KinesisAsyncClient mockAsyncClient = mock(KinesisAsyncClient.class);

        when(mockAsyncClient.subscribeToShard(any(SubscribeToShardRequest.class), any(SubscribeToShardResponseHandler.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        final EnhancedFanOutClient.ShardConsumer consumer =
                new EnhancedFanOutClient.ShardConsumer("shardId-000000000001", result -> { }, new ConcurrentLinkedQueue<>(), mockLogger);

        final StartingPosition pos = StartingPosition.builder()
                .type(ShardIteratorType.TRIM_HORIZON)
                .build();

        consumer.subscribe(mockAsyncClient, "test-arn", pos);
        final int gen1 = consumer.getSubscriptionGeneration();
        assertEquals(1, gen1);

        consumer.setSubscription(mock(Subscription.class));

        assertNotNull(consumer.getSubscription(), "Subscription should be set after onSubscribe");

        consumer.endSubscriptionIfCurrent(gen1);
        assertFalse(consumer.isSubscribing(), "subscribing should be false after endSubscription");

        consumer.subscribe(mockAsyncClient, "test-arn", pos);
        final int gen2 = consumer.getSubscriptionGeneration();
        assertEquals(2, gen2);

        consumer.setSubscription(mock(Subscription.class));

        assertNotNull(consumer.getSubscription(), "New subscription should be set");

        consumer.endSubscriptionIfCurrent(gen1);

        assertNotNull(consumer.getSubscription(),
                "Stale callback (gen1) must NOT null out gen2's subscription");
        assertTrue(consumer.isSubscribing(),
                "Stale callback (gen1) must NOT reset gen2's subscribing flag");

        consumer.endSubscriptionIfCurrent(gen2);

        assertFalse(consumer.isSubscribing(),
                "Current-generation callback should clean up normally");
    }

    /**
     * Simulates the error path that fires when a SubscribeToShard call receives a
     * {@link ResourceNotFoundException}. The onError callback delegates to processError,
     * which classifies the exception, sets the shardNotFound flag, and ends the subscription.
     * This test exercises that path by passing a real ResourceNotFoundException through
     * processError and verifying the resulting state.
     */
    @Test
    void testResourceNotFoundExceptionEndsSubscriptionCleanly() {
        final ComponentLog mockLogger = mock(ComponentLog.class);
        final KinesisAsyncClient mockAsyncClient = mock(KinesisAsyncClient.class);

        final ResourceNotFoundException exception = ResourceNotFoundException.builder()
                .message("Shard shardId-000000000001 does not exist")
                .build();

        when(mockAsyncClient.subscribeToShard(any(SubscribeToShardRequest.class), any(SubscribeToShardResponseHandler.class)))
                .then(inv -> {
                    final SubscribeToShardResponseHandler handler = inv.getArgument(1);
                    handler.exceptionOccurred(exception);
                    return CompletableFuture.completedFuture(null);
                });

        final EnhancedFanOutClient.ShardConsumer consumer =
                new EnhancedFanOutClient.ShardConsumer("shardId-000000000001", result -> { }, new ConcurrentLinkedQueue<>(), mockLogger);

        final StartingPosition pos = StartingPosition.builder()
                .type(ShardIteratorType.TRIM_HORIZON)
                .build();

        consumer.subscribe(mockAsyncClient, "test-arn", pos);

        assertFalse(consumer.isSubscribing());
        assertNull(consumer.getSubscription());
        assertTrue(consumer.isShardNotFound());
    }

    /**
     * Verifies the startFetches cleanup path for a shard marked as not-found. When a
     * ShardConsumer has its shardNotFound flag set (as would happen after a
     * ResourceNotFoundException in the onError callback), the next startFetches invocation
     * should remove the consumer from the map instead of attempting a new subscription.
     */
    @Test
    void testStartFetchesRemovesConsumerWithNotFoundShard() {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        when(mockShardManager.readCheckpoint("shardId-000000000001")).thenReturn(null);

        final List<SubscribeToShardRequest> capturedRequests = new ArrayList<>();
        final EnhancedFanOutClient client = createEfoClient(capturedRequests);

        final List<Shard> shards = List.of(Shard.builder().shardId("shardId-000000000001").build());
        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertEquals(1, capturedRequests.size());
        final EnhancedFanOutClient.ShardConsumer consumer = client.getShardConsumer("shardId-000000000001");
        assertNotNull(consumer);

        consumer.markShardNotFound();

        client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", mockShardManager);

        assertNull(client.getShardConsumer("shardId-000000000001"),
                "ShardConsumer marked as shard-not-found must be removed by startFetches");
        assertEquals(1, capturedRequests.size(),
                "No new subscription should be created for a shard-not-found consumer");
    }

    private static ShardFetchResult shardFetchResult(final String shardId, final String sequenceNumber) {
        final UserRecord record = new UserRecord(shardId, sequenceNumber, 0, "pk", "{}".getBytes(), null);
        return new ShardFetchResult(shardId, List.of(record), 0L);
    }

    /**
     * Verifies the per-shard exclusive processing lock: claimShard returns false when a shard is
     * already claimed, and releaseShards makes it claimable again. Different shards are independent.
     */
    @Test
    void testShardClaimExclusivity() {
        final PollingKinesisClient client = new PollingKinesisClient(mock(KinesisClient.class), mock(ComponentLog.class));

        assertTrue(client.claimShard("shard-1"), "First claim should succeed");
        assertFalse(client.claimShard("shard-1"), "Duplicate claim should fail");
        assertTrue(client.claimShard("shard-2"), "Different shard claim should succeed independently");

        client.releaseShards(List.of("shard-1"));
        assertTrue(client.claimShard("shard-1"), "Claim after release should succeed");
        assertFalse(client.claimShard("shard-2"), "shard-2 was not released and should still be claimed");
    }

    /**
     * Verifies that enqueueResult stores results in per-shard queues and that
     * pollShardResult retrieves them in FIFO order.
     */
    @Test
    void testEnqueueResultIsPolledFromShardQueue() {
        final PollingKinesisClient client = new PollingKinesisClient(mock(KinesisClient.class), mock(ComponentLog.class));
        final ShardFetchResult result = shardFetchResult("shard-1", "12345");

        client.enqueueResult(result);

        final ShardFetchResult polled = client.pollShardResult("shard-1");
        assertNotNull(polled, "Enqueued result should be available for polling from its shard queue");
        assertEquals("shard-1", polled.shardId());
    }

    /**
     * Verifies that per-shard queues preserve FIFO order. When multiple results for the
     * same shard are enqueued (potentially interleaved with other shards), polling that
     * shard's queue must return them in enqueue order. This prevents the out-of-order
     * delivery that occurred with the old flat-queue requeue-to-back design.
     */
    @Test
    void testPerShardOrderingPreservedAcrossEnqueues() {
        final PollingKinesisClient client = new PollingKinesisClient(mock(KinesisClient.class), mock(ComponentLog.class));

        client.enqueueResult(shardFetchResult("shard-5", "100"));
        client.enqueueResult(shardFetchResult("shard-3", "500"));
        client.enqueueResult(shardFetchResult("shard-5", "200"));
        client.enqueueResult(shardFetchResult("shard-3", "600"));
        client.enqueueResult(shardFetchResult("shard-5", "300"));

        assertEquals(new BigInteger("100"), client.pollShardResult("shard-5").firstSequenceNumber());
        assertEquals(new BigInteger("200"), client.pollShardResult("shard-5").firstSequenceNumber());
        assertEquals(new BigInteger("300"), client.pollShardResult("shard-5").firstSequenceNumber());
        assertNull(client.pollShardResult("shard-5"), "Queue should be empty after draining");

        assertEquals(new BigInteger("500"), client.pollShardResult("shard-3").firstSequenceNumber());
        assertEquals(new BigInteger("600"), client.pollShardResult("shard-3").firstSequenceNumber());
        assertNull(client.pollShardResult("shard-3"), "Queue should be empty after draining");
    }

    /**
     * Verifies that getShardIdsWithResults returns only shards with non-empty queues,
     * and that totalQueuedResults reflects the actual count across all shard queues.
     */
    @Test
    void testQueueIntrospectionMethods() {
        final PollingKinesisClient client = new PollingKinesisClient(mock(KinesisClient.class), mock(ComponentLog.class));

        assertFalse(client.hasQueuedResults());
        assertEquals(0, client.totalQueuedResults());
        assertTrue(client.getShardIdsWithResults().isEmpty());

        client.enqueueResult(shardFetchResult("shard-1", "10"));
        client.enqueueResult(shardFetchResult("shard-2", "20"));
        client.enqueueResult(shardFetchResult("shard-1", "30"));

        assertTrue(client.hasQueuedResults());
        assertEquals(3, client.totalQueuedResults());
        assertEquals(Set.of("shard-1", "shard-2"), new HashSet<>(client.getShardIdsWithResults()));

        client.pollShardResult("shard-1");
        client.pollShardResult("shard-1");

        assertEquals(1, client.totalQueuedResults());
        assertEquals(List.of("shard-2"), client.getShardIdsWithResults());
    }

    /**
     * Reproduces the scenario from the original out-of-order delivery bug. With a single
     * flat queue and requeue-to-back, result B would end up behind C after being requeued,
     * causing C to be delivered first. Per-shard queues eliminate this: when Task-2 cannot
     * claim shard-1, it simply skips the shard queue. B and C remain in FIFO order for the
     * next consumer.
     */
    @Test
    void testPerShardQueuesPreventOutOfOrderDeliveryAcrossInvocations() {
        final PollingKinesisClient client = new PollingKinesisClient(mock(KinesisClient.class), mock(ComponentLog.class));

        final ShardFetchResult resultA = shardFetchResult("shard-1", "100");
        final ShardFetchResult resultB = shardFetchResult("shard-1", "200");
        final ShardFetchResult resultC = shardFetchResult("shard-1", "300");
        final ShardFetchResult otherShardResult = shardFetchResult("shard-2", "999");

        client.enqueueResult(resultA);
        client.enqueueResult(resultB);
        client.enqueueResult(otherShardResult);
        client.enqueueResult(resultC);

        // Task-1: claims shard-1, polls A(100) from its per-shard queue
        assertTrue(client.claimShard("shard-1"), "Task-1 should claim shard-1");
        final ShardFetchResult polledA = client.pollShardResult("shard-1");
        assertEquals(new BigInteger("100"), polledA.firstSequenceNumber());

        // Task-2: cannot claim shard-1 (held by Task-1), so it skips shard-1 entirely.
        // B and C remain at the head of shard-1's queue, undisturbed.
        assertFalse(client.claimShard("shard-1"), "Task-2 cannot claim shard-1 (held by Task-1)");

        // Task-2: claims shard-2 and polls its result
        assertTrue(client.claimShard("shard-2"));
        final ShardFetchResult polledOther = client.pollShardResult("shard-2");
        assertEquals(new BigInteger("999"), polledOther.firstSequenceNumber());

        // Task-1 commits A and releases shard-1
        client.releaseShards(List.of("shard-1"));

        // Next invocation: shard-1's queue still has B(200) then C(300) in correct order
        final ShardFetchResult firstPoll = client.pollShardResult("shard-1");
        final ShardFetchResult secondPoll = client.pollShardResult("shard-1");

        assertNotNull(firstPoll, "Expected shard-1 queue to have B");
        assertNotNull(secondPoll, "Expected shard-1 queue to have C");
        assertEquals(new BigInteger("200"), firstPoll.firstSequenceNumber(), "First poll must be B(200), not C(300)");
        assertEquals(new BigInteger("300"), secondPoll.firstSequenceNumber(), "Second poll must be C(300)");
        assertNull(client.pollShardResult("shard-1"), "shard-1 queue should be empty");
    }

    private static void runStartFetches(final KinesisConsumerClient client, final List<Shard> shards,
            final KinesisShardManager shardManager, final CountDownLatch startLatch) {
        try {
            startLatch.await();
            client.startFetches(shards, "test-stream", 100, "TRIM_HORIZON", shardManager);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
