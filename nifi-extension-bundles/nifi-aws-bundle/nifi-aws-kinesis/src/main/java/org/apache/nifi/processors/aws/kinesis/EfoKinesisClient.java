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
import org.apache.nifi.processor.exception.ProcessException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Enhanced Fan-Out Kinesis consumer that uses SubscribeToShard with dedicated throughput
 * per shard via HTTP/2. Uses Reactive Streams demand-driven backpressure to control the
 * rate of event delivery.
 */
final class EfoKinesisClient extends KinesisConsumerClient {

    private static final long SUBSCRIBE_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long CONSUMER_REGISTRATION_POLL_MILLIS = 1_000;
    private static final int CONSUMER_REGISTRATION_MAX_ATTEMPTS = 60;
    private static final int MAX_QUEUED_RESULTS = 200;

    private final Map<String, ShardConsumer> shardConsumers = new ConcurrentHashMap<>();
    private final Queue<ShardConsumer> pausedConsumers = new ConcurrentLinkedQueue<>();
    private volatile KinesisAsyncClient kinesisAsyncClient;
    private volatile String consumerArn;
    private volatile Instant timestampForInitialPosition;

    EfoKinesisClient(final KinesisClient kinesisClient, final ComponentLog logger) {
        super(kinesisClient, logger);
    }

    void setTimestampForInitialPosition(final Instant timestamp) {
        this.timestampForInitialPosition = timestamp;
    }

    @Override
    void initialize(final KinesisAsyncClient asyncClient, final String streamName, final String consumerName) {
        this.kinesisAsyncClient = asyncClient;
        registerEfoConsumer(streamName, consumerName);
    }

    void initializeForTest(final KinesisAsyncClient asyncClient, final String theConsumerArn) {
        this.kinesisAsyncClient = asyncClient;
        this.consumerArn = theConsumerArn;
    }

    ShardConsumer getShardConsumer(final String shardId) {
        return shardConsumers.get(shardId);
    }

    @Override
    void startFetches(final List<Shard> shards, final String streamName, final int batchSize, final String initialStreamPosition, final KinesisShardManager shardManager) {
        final long now = System.nanoTime();

        for (final Shard shard : shards) {
            final String shardId = shard.shardId();
            final ShardConsumer existing = shardConsumers.get(shardId);

            if (existing == null) {
                final String checkpoint = shardManager.readCheckpoint(shardId);
                final BigInteger lastSeq = checkpoint != null ? new BigInteger(checkpoint) : null;
                final StartingPosition startingPosition = buildStartingPosition(lastSeq, initialStreamPosition);
                logger.info("Creating EFO subscription for shard {} with type={}, seq={}", shardId, startingPosition.type(), lastSeq);
                final ShardConsumer shardConsumer = new ShardConsumer(shardId, result -> enqueueIfActiveConsumer(shardId, result), pausedConsumers, logger);
                final ShardConsumer prior = shardConsumers.putIfAbsent(shardId, shardConsumer);
                if (prior == null) {
                    try {
                        shardConsumer.subscribe(kinesisAsyncClient, consumerArn, startingPosition);
                    } catch (final Exception e) {
                        shardConsumers.remove(shardId, shardConsumer);
                        throw e;
                    }
                }
            } else if (existing.isSubscriptionExpired()) {
                final long lastAttempt = existing.getLastSubscribeAttemptNanos();
                if (lastAttempt > 0 && now < lastAttempt + SUBSCRIBE_BACKOFF_NANOS) {
                    continue;
                }

                final String checkpoint = shardManager.readCheckpoint(shardId);
                final BigInteger checkpointSeq = checkpoint == null ?  null : new BigInteger(checkpoint);
                final BigInteger lastQueued = existing.getLastQueuedSequenceNumber();
                final BigInteger resumeSeq = maxSequenceNumber(lastQueued, checkpointSeq);
                final StartingPosition startingPosition = buildStartingPosition(resumeSeq, initialStreamPosition);
                logger.debug("Renewing expired EFO subscription for shard {} with type={}, seq={}", shardId, startingPosition.type(), resumeSeq);
                existing.subscribe(kinesisAsyncClient, consumerArn, startingPosition);
            }
        }

        resumePausedConsumers();
    }

    @Override
    boolean hasPendingFetches() {
        return !shardConsumers.isEmpty();
    }

    @Override
    long drainDeduplicatedEventCount() {
        long total = 0;
        for (final ShardConsumer sc : shardConsumers.values()) {
            total += sc.drainDeduplicatedEventCount();
        }
        return total;
    }

    @Override
    void acknowledgeResults(final List<ShardFetchResult> results) {
        resumePausedConsumers();
    }

    private void resumePausedConsumers() {
        int available = MAX_QUEUED_RESULTS - totalQueuedResults();
        int resumed = 0;
        ShardConsumer consumer;
        while (resumed < available && (consumer = pausedConsumers.poll()) != null) {
            if (consumer.requestNextIfReady()) {
                resumed++;
            }
        }
    }

    private void enqueueIfActiveConsumer(final String shardId, final ShardFetchResult result) {
        synchronized (getShardLock(shardId)) {
            if (shardConsumers.containsKey(shardId)) {
                enqueueResult(result);
            }
        }
    }

    private ShardConsumer drainAndRemoveConsumer(final String shardId) {
        synchronized (getShardLock(shardId)) {
            drainShardQueue(shardId);
            return shardConsumers.remove(shardId);
        }
    }

    @Override
    void rollbackResults(final List<ShardFetchResult> results) {
        for (final ShardFetchResult result : results) {
            final ShardConsumer shardConsumer = drainAndRemoveConsumer(result.shardId());
            if (shardConsumer != null) {
                shardConsumer.cancel();
            }
        }
    }

    @Override
    void removeUnownedShards(final Set<String> ownedShards) {
        shardConsumers.entrySet().removeIf(entry -> {
            if (!ownedShards.contains(entry.getKey())) {
                entry.getValue().cancel();
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

        final long now = System.nanoTime();
        int activeSubscriptions = 0;
        int expiredSubscriptions = 0;
        int backedOff = 0;
        for (final ShardConsumer sc : shardConsumers.values()) {
            if (sc.isSubscriptionExpired()) {
                expiredSubscriptions++;
                final long lastAttempt = sc.getLastSubscribeAttemptNanos();
                if (lastAttempt > 0 && now < lastAttempt + SUBSCRIBE_BACKOFF_NANOS) {
                    backedOff++;
                }
            } else {
                activeSubscriptions++;
            }
        }

        final int queueDepth = totalQueuedResults();
        logger.debug("Kinesis EFO diagnostics: discoveredShards={}, ownedShards={}, queueDepth={}/{}, shardConsumers={}, activeSubscriptions={}, expiredSubscriptions={}, backedOff={}",
                cachedShardCount, ownedCount, queueDepth, MAX_QUEUED_RESULTS, shardConsumers.size(), activeSubscriptions, expiredSubscriptions, backedOff);
    }

    @Override
    void close() {
        for (final ShardConsumer sc : shardConsumers.values()) {
            sc.cancel();
        }
        shardConsumers.clear();

        if (kinesisAsyncClient != null) {
            kinesisAsyncClient.close();
            kinesisAsyncClient = null;
        }

        super.close();
    }

    String getConsumerArn() {
        return consumerArn;
    }

    private void registerEfoConsumer(final String streamName, final String consumerName) {
        final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder().streamName(streamName).build();
        final DescribeStreamResponse describeResponse = kinesisClient.describeStream(describeStreamRequest);
        final String arn = describeResponse.streamDescription().streamARN();

        try {
            final DescribeStreamConsumerRequest describeConsumerReq = DescribeStreamConsumerRequest.builder()
                    .streamARN(arn)
                    .consumerName(consumerName)
                    .build();
            final ConsumerStatus status = kinesisClient.describeStreamConsumer(describeConsumerReq).consumerDescription().consumerStatus();
            if (status == ConsumerStatus.ACTIVE) {
                consumerArn = kinesisClient.describeStreamConsumer(describeConsumerReq).consumerDescription().consumerARN();
                logger.info("EFO consumer [{}] already registered and ACTIVE", consumerName);
                return;
            }
        } catch (final ResourceNotFoundException ignored) {
        }

        try {
            final RegisterStreamConsumerRequest registerRequest = RegisterStreamConsumerRequest.builder()
                    .streamARN(arn)
                    .consumerName(consumerName)
                    .build();
            final RegisterStreamConsumerResponse registerResponse = kinesisClient.registerStreamConsumer(registerRequest);
            consumerArn = registerResponse.consumer().consumerARN();
            logger.info("Registered EFO consumer [{}], waiting for ACTIVE status", consumerName);
        } catch (final ResourceInUseException e) {
            final DescribeStreamConsumerRequest fallbackRequest = DescribeStreamConsumerRequest.builder()
                    .streamARN(arn)
                    .consumerName(consumerName)
                    .build();
            consumerArn = kinesisClient.describeStreamConsumer(fallbackRequest).consumerDescription().consumerARN();
            logger.info("EFO consumer [{}] already being registered", consumerName);
        }

        waitForConsumerActive(arn, consumerName);
    }

    private void waitForConsumerActive(final String theStreamArn, final String consumerName) {
        final DescribeStreamConsumerRequest describeConsumerRequest = DescribeStreamConsumerRequest.builder()
                .streamARN(theStreamArn)
                .consumerName(consumerName)
                .build();

        for (int i = 0; i < CONSUMER_REGISTRATION_MAX_ATTEMPTS; i++) {
            final ConsumerStatus status = kinesisClient.describeStreamConsumer(describeConsumerRequest).consumerDescription().consumerStatus();
            if (status == ConsumerStatus.ACTIVE) {
                logger.info("EFO consumer [{}] is now ACTIVE", consumerName);
                return;
            }

            try {
                Thread.sleep(CONSUMER_REGISTRATION_POLL_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted while waiting for EFO consumer registration", e);
            }
        }

        throw new ProcessException("EFO consumer [%s] did not become ACTIVE within %d seconds".formatted(consumerName, CONSUMER_REGISTRATION_MAX_ATTEMPTS));
    }

    private static BigInteger maxSequenceNumber(final BigInteger a, final BigInteger b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    private StartingPosition buildStartingPosition(final BigInteger sequenceNumber, final String initialStreamPosition) {
        if (sequenceNumber != null) {
            return StartingPosition.builder()
                    .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .sequenceNumber(sequenceNumber.toString())
                    .build();
        }
        final ShardIteratorType iteratorType = ShardIteratorType.fromValue(initialStreamPosition);
        final StartingPosition.Builder builder = StartingPosition.builder().type(iteratorType);
        if (iteratorType == ShardIteratorType.AT_TIMESTAMP && timestampForInitialPosition != null) {
            builder.timestamp(timestampForInitialPosition);
        }
        return builder.build();
    }

    static final class ShardConsumer {
        private final String shardId;
        private final Consumer<ShardFetchResult> resultSink;
        private final Queue<ShardConsumer> pausedConsumers;
        private final ComponentLog consumerLogger;
        private final AtomicBoolean subscribing = new AtomicBoolean(false);
        private final AtomicBoolean paused = new AtomicBoolean(false);
        private final AtomicInteger subscriptionGeneration = new AtomicInteger();
        private final AtomicLong deduplicatedEvents = new AtomicLong();
        private volatile Subscription subscription;
        private volatile CompletableFuture<Void> subscriptionFuture;
        private volatile long lastSubscribeAttemptNanos;
        private volatile BigInteger lastQueuedSequenceNumber;
        ShardConsumer(final String shardId, final Consumer<ShardFetchResult> resultSink,
                      final Queue<ShardConsumer> pausedConsumers, final ComponentLog consumerLogger) {
            this.shardId = shardId;
            this.resultSink = resultSink;
            this.pausedConsumers = pausedConsumers;
            this.consumerLogger = consumerLogger;
        }

        void subscribe(final KinesisAsyncClient asyncClient, final String theConsumerArn, final StartingPosition startingPosition) {
            if (!subscribing.compareAndSet(false, true)) {
                return;
            }

            final int generation = subscriptionGeneration.incrementAndGet();

            try {
                final SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                        .consumerARN(theConsumerArn)
                        .shardId(shardId)
                        .startingPosition(startingPosition)
                        .build();

                final SubscribeToShardResponseHandler handler = SubscribeToShardResponseHandler.builder()
                        .subscriber(() -> new DemandDrivenSubscriber(generation))
                        .onError(t -> {
                            logSubscriptionError(t);
                            endSubscriptionIfCurrent(generation);
                        })
                        .build();

                lastSubscribeAttemptNanos = System.nanoTime();
                subscriptionFuture = asyncClient.subscribeToShard(request, handler);
            } catch (final Exception e) {
                subscribing.set(false);
                throw e;
            }
        }

        void requestNext() {
            final Subscription sub = subscription;
            if (sub != null) {
                sub.request(1);
            }
        }

        boolean requestNextIfReady() {
            if (paused.compareAndSet(true, false)) {
                final Subscription sub = subscription;
                if (sub != null) {
                    sub.request(1);
                    return true;
                }
            }
            return false;
        }

        boolean isSubscriptionExpired() {
            final CompletableFuture<Void> future = subscriptionFuture;
            return future == null || future.isDone();
        }

        long getLastSubscribeAttemptNanos() {
            return lastSubscribeAttemptNanos;
        }

        BigInteger getLastQueuedSequenceNumber() {
            return lastQueuedSequenceNumber;
        }

        long drainDeduplicatedEventCount() {
            return deduplicatedEvents.getAndSet(0);
        }

        void cancel() {
            final CompletableFuture<Void> future = subscriptionFuture;
            if (future != null) {
                future.cancel(true);
            }
            subscription = null;
        }

        void resetForRenewal() {
            subscribing.set(false);
            lastSubscribeAttemptNanos = 0;
        }

        void setLastQueuedSequenceNumber(final BigInteger seq) {
            lastQueuedSequenceNumber = seq;
        }

        void setSubscription(final Subscription sub) {
            subscription = sub;
        }

        Subscription getSubscription() {
            return subscription;
        }

        int getSubscriptionGeneration() {
            return subscriptionGeneration.get();
        }

        boolean isSubscribing() {
            return subscribing.get();
        }

        void pause() {
            if (paused.compareAndSet(false, true)) {
                pausedConsumers.add(this);
            }
        }

        private void logSubscriptionError(final Throwable t) {
            if (isCancellation(t)) {
                consumerLogger.debug("EFO subscription cancelled for shard {}", shardId);
            } else if (isRetryableSubscriptionError(t)) {
                consumerLogger.info("EFO subscription temporarily rejected for shard {}; will retry after backoff", shardId);
            } else if (isRetryableStreamDisconnect(t)) {
                consumerLogger.info("EFO subscription disconnected for shard {}; will retry", shardId);
            } else {
                consumerLogger.error("EFO subscription error for shard {}", shardId, t);
            }
        }

        private static boolean isCancellation(final Throwable t) {
            if (t instanceof CancellationException) {
                return true;
            }
            if (t instanceof IOException && "Request cancelled".equals(t.getMessage())) {
                return true;
            }
            final Throwable cause = t.getCause();
            return cause != null && cause != t && isCancellation(cause);
        }

        private static boolean isRetryableSubscriptionError(final Throwable t) {
            if (t instanceof LimitExceededException || t instanceof ResourceInUseException) {
                return true;
            }
            final Throwable cause = t.getCause();
            return cause != null && cause != t && isRetryableSubscriptionError(cause);
        }

        private static boolean isRetryableStreamDisconnect(final Throwable t) {
            if (t instanceof IOException || t instanceof SdkClientException) {
                return true;
            }
            if (t instanceof AwsServiceException ase && ase.statusCode() >= 500) {
                return true;
            }
            final String className = t.getClass().getName();
            if (className.startsWith("io.netty.")) {
                return true;
            }
            final Throwable cause = t.getCause();
            return cause != null && cause != t && isRetryableStreamDisconnect(cause);
        }

        void endSubscriptionIfCurrent(final int generation) {
            if (subscriptionGeneration.get() == generation) {
                subscription = null;
                subscribing.set(false);
            }
        }

        private List<Record> deduplicateRecords(final List<Record> records) {
            final BigInteger threshold = lastQueuedSequenceNumber;
            if (threshold == null) {
                return records;
            }
            int firstNewIndex = records.size();
            for (int i = 0; i < records.size(); i++) {
                if (new BigInteger(records.get(i).sequenceNumber()).compareTo(threshold) > 0) {
                    firstNewIndex = i;
                    break;
                }
            }

            if (firstNewIndex == 0) {
                return records;
            }

            final int kept = records.size() - firstNewIndex;
            deduplicatedEvents.incrementAndGet();
            if (kept == 0) {
                consumerLogger.debug("Skipped re-delivered EFO event for shard {} ({} records already seen)", shardId, records.size());
            } else {
                consumerLogger.debug("Filtered {} duplicate record(s) from EFO event for shard {} (kept {})", firstNewIndex, shardId, kept);
            }

            return records.subList(firstNewIndex, records.size());
        }

        private class DemandDrivenSubscriber implements Subscriber<SubscribeToShardEventStream> {
            private final int generation;

            DemandDrivenSubscriber(final int generation) {
                this.generation = generation;
            }

            @Override
            public void onSubscribe(final Subscription sub) {
                subscription = sub;
                paused.set(false);
                sub.request(1);
            }

            @Override
            public void onNext(final SubscribeToShardEventStream eventStream) {
                if (!(eventStream instanceof SubscribeToShardEvent event)) {
                    requestNext();
                    return;
                }

                if (event.records().isEmpty()) {
                    requestNext();
                    return;
                }

                final long millisBehind = event.millisBehindLatest() != null ? event.millisBehindLatest() : -1;
                final List<Record> records = deduplicateRecords(event.records());
                if (records.isEmpty()) {
                    requestNext();
                    return;
                }

                final ShardFetchResult result = createFetchResult(shardId, records, millisBehind);
                lastQueuedSequenceNumber = result.lastSequenceNumber();
                resultSink.accept(result);
                if (paused.compareAndSet(false, true)) {
                    pausedConsumers.add(ShardConsumer.this);
                }
            }

            @Override
            public void onError(final Throwable t) {
                logSubscriptionError(t);
                endSubscriptionIfCurrent(generation);
            }

            @Override
            public void onComplete() {
                consumerLogger.debug("EFO subscription completed normally for shard {}", shardId);
                endSubscriptionIfCurrent(generation);
            }
        }
    }
}
