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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates shard ownership and checkpoints across clustered processor instances using
 * a single DynamoDB table.
 *
 * <p>The table stores two record types under the same stream hash key:
 * <ul>
 *   <li>Shard rows: key {@code (streamName, shardId)} with lease/checkpoint fields</li>
 *   <li>Node heartbeat rows: key {@code (streamName, "__node__#<nodeId>")}</li>
 * </ul>
 *
 * <p>Lease lifecycle used during refresh:
 * <ol>
 *   <li>Discover active shard leases and identify currently available shards</li>
 *   <li>Compute fair-share target from active node heartbeats</li>
 *   <li>If this node is over target, mark excess shards for graceful relinquish</li>
 *   <li>Continue renewing leases for owned shards and draining relinquishing shards</li>
 *   <li>After drain deadline, explicitly release relinquishing shards</li>
 *   <li>Acquire available shards until fair-share target is reached</li>
 * </ol>
 *
 * <p>Graceful relinquish is designed to reduce duplicate replay at rebalance boundaries by:
 * (a) stopping new fetches immediately (shard removed from {@code ownedShards}),
 * (b) briefly retaining lease ownership to allow in-flight work to finish,
 * then (c) explicitly releasing the lease for fast handoff.
 *
 * <p><strong>Shard split/merge:</strong> this implementation does not enforce parent-before-child
 * ordering. When a shard is split or shards are merged, child shards become eligible for
 * consumption immediately alongside any still-active parent shards. Callers that require strict
 * ordering across split/merge boundaries would need to defer child shard assignment until the
 * parent shard's {@code SHARD_END} has been reached and checkpointed.
 */
final class KinesisShardManager {

    private static final long DEFAULT_SHARD_CACHE_MILLIS = 60_000;
    private static final long DEFAULT_LEASE_DURATION_MILLIS = 30_000;
    private static final long DEFAULT_LEASE_REFRESH_INTERVAL_MILLIS = 10_000;
    private static final long DEFAULT_NODE_HEARTBEAT_EXPIRATION_MILLIS =
            DEFAULT_LEASE_DURATION_MILLIS + DEFAULT_LEASE_REFRESH_INTERVAL_MILLIS;

    private final KinesisClient kinesisClient;
    private final DynamoDbClient dynamoDbClient;
    private final ComponentLog logger;
    private final String nodeId;
    private final String checkpointTableName;
    private final String streamName;
    private final long shardCacheMillis;
    private final long leaseDurationMillis;
    private final long leaseRefreshIntervalMillis;
    private final long nodeHeartbeatExpirationMillis;
    private final long relinquishDrainMillis;

    private volatile ShardCache shardCache = new ShardCache(List.of(), Instant.EPOCH);
    private final Set<String> ownedShards = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> pendingRelinquishDeadlines = new ConcurrentHashMap<>();
    private final AtomicBoolean leaseRefreshInProgress = new AtomicBoolean(false);
    private final Map<String, ShardCheckpoint> highestWrittenCheckpoints = new ConcurrentHashMap<>();
    private volatile Instant lastLeaseRefresh = Instant.EPOCH;
    private volatile String activeCheckpointTableName;

    KinesisShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient, final ComponentLog logger,
            final String checkpointTableName, final String streamName) {
        this(kinesisClient, dynamoDbClient, logger, checkpointTableName, streamName,
                DEFAULT_SHARD_CACHE_MILLIS, DEFAULT_LEASE_DURATION_MILLIS,
                DEFAULT_LEASE_REFRESH_INTERVAL_MILLIS, DEFAULT_NODE_HEARTBEAT_EXPIRATION_MILLIS);
    }

    KinesisShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient, final ComponentLog logger,
            final String checkpointTableName, final String streamName, final long shardCacheMillis,
            final long leaseDurationMillis, final long leaseRefreshIntervalMillis, final long nodeHeartbeatExpirationMillis) {
        this.kinesisClient = kinesisClient;
        this.dynamoDbClient = dynamoDbClient;
        this.logger = logger;
        this.nodeId = UUID.randomUUID().toString();
        this.checkpointTableName = checkpointTableName;
        this.streamName = streamName;
        this.shardCacheMillis = shardCacheMillis;
        this.leaseDurationMillis = leaseDurationMillis;
        this.leaseRefreshIntervalMillis = leaseRefreshIntervalMillis;
        this.nodeHeartbeatExpirationMillis = nodeHeartbeatExpirationMillis;
        this.relinquishDrainMillis = Math.max(2_000L, leaseRefreshIntervalMillis);
        this.activeCheckpointTableName = checkpointTableName;
    }

    void ensureCheckpointTableExists() {
        final CheckpointTableUtils.TableSchema currentSchema =
                CheckpointTableUtils.getTableSchema(dynamoDbClient, checkpointTableName);
        logger.debug("Checkpoint table [{}] detected as {}", checkpointTableName, currentSchema);

        final LegacyCheckpointMigrator migrator =
                new LegacyCheckpointMigrator(dynamoDbClient, checkpointTableName, streamName, nodeId, logger);

        switch (currentSchema) {
            case NOT_FOUND -> {
                final String orphanedMigration = migrator.findMigrationTable();
                if (orphanedMigration == null) {
                    CheckpointTableUtils.createNewSchemaTable(dynamoDbClient, logger, checkpointTableName);
                    CheckpointTableUtils.waitForTableActive(dynamoDbClient, logger, checkpointTableName);
                } else {
                    logger.info("Found orphaned migration table [{}]; renaming to [{}]",
                            orphanedMigration, checkpointTableName);
                    migrator.renameMigrationTable(orphanedMigration);
                }
            }
            case NEW -> {
                CheckpointTableUtils.waitForTableActive(dynamoDbClient, logger, checkpointTableName);
                migrator.cleanupLingeringMigration();
            }
            case LEGACY -> {
                migrator.migrateAndRename();
            }
            default -> throw new ProcessException(
                    "Unsupported DynamoDB schema for checkpoint table [%s]".formatted(checkpointTableName));
        }

        activeCheckpointTableName = checkpointTableName;
        logger.info("Using checkpoint table [{}] for stream [{}]", activeCheckpointTableName, streamName);
    }

    List<Shard> getShards() {
        final ShardCache current = shardCache;
        if (!current.shards().isEmpty() && Instant.now().toEpochMilli() < current.refreshTime().toEpochMilli() + shardCacheMillis) {
            return current.shards();
        }
        return refreshShards();
    }

    int getCachedShardCount() {
        return shardCache.shards().size();
    }

    Set<String> getOwnedShardIds() {
        return Collections.unmodifiableSet(ownedShards);
    }

    List<Shard> getOwnedShards() {
        final List<Shard> result = new ArrayList<>();
        for (final Shard shard : getShards()) {
            if (ownedShards.contains(shard.shardId())) {
                result.add(shard);
            }
        }
        return result;
    }

    boolean shouldProcessFetchedResult(final String shardId) {
        return ownedShards.contains(shardId) || pendingRelinquishDeadlines.containsKey(shardId);
    }

    void refreshLeasesIfNecessary(final int clusterMemberCount) {
        if (Instant.now().toEpochMilli() < leaseRefreshIntervalMillis + lastLeaseRefresh.toEpochMilli()) {
            return;
        }

        if (!leaseRefreshInProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            final List<Shard> allShards = getShards();
            final Set<String> currentShardIds = new HashSet<>();
            for (final Shard shard : allShards) {
                currentShardIds.add(shard.shardId());
            }

            final long now = Instant.now().toEpochMilli();
            updateNodeHeartbeat(now);
            final Map<String, List<String>> ownerToShards = new HashMap<>();
            final List<String> availableShardIds = new ArrayList<>();

            final Map<String, Map<String, AttributeValue>> leaseItemsByShardId = queryAllLeaseItems();
            for (final String shardId : currentShardIds) {
                final Map<String, AttributeValue> item = leaseItemsByShardId.get(shardId);
                if (item != null && item.containsKey("leaseOwner")) {
                    final String owner = item.get("leaseOwner").s();
                    final AttributeValue expiryAttr = item.get("leaseExpiry");
                    final long expiry = expiryAttr != null ? Long.parseLong(expiryAttr.n()) : 0;
                    if (expiry >= now) {
                        ownerToShards.computeIfAbsent(owner, k -> new ArrayList<>()).add(shardId);
                    } else {
                        availableShardIds.add(shardId);
                    }
                } else {
                    availableShardIds.add(shardId);
                }
            }

            final Set<String> stillOwned = new HashSet<>(ownerToShards.getOrDefault(nodeId, List.of()));
            ownedShards.retainAll(stillOwned);
            pendingRelinquishDeadlines.keySet().removeIf(shardId -> !currentShardIds.contains(shardId) || !stillOwned.contains(shardId));

            final int heartbeatNodes = countActiveNodes(now);
            final int totalOwners = Math.max(heartbeatNodes, Math.max(1, clusterMemberCount));
            final int targetCount = (allShards.size() + totalOwners - 1) / totalOwners;

            final List<String> currentlyOwned = new ArrayList<>(ownerToShards.getOrDefault(nodeId, List.of()));
            final int excessCount = Math.max(0, currentlyOwned.size() - targetCount);
            if (excessCount > 0) {
                for (int index = 0; index < excessCount && index < currentlyOwned.size(); index++) {
                    final String shardToRelinquish = currentlyOwned.get(index);
                    if (!pendingRelinquishDeadlines.containsKey(shardToRelinquish)) {
                        pendingRelinquishDeadlines.put(shardToRelinquish, now + relinquishDrainMillis);
                        ownedShards.remove(shardToRelinquish);
                        logger.info("Starting graceful relinquish for shard {}", shardToRelinquish);
                    }
                }
            }

            for (final String shardId : ownedShards) {
                tryAcquireLease(shardId);
            }

            for (final Map.Entry<String, Long> pendingRelinquish : new ArrayList<>(pendingRelinquishDeadlines.entrySet())) {
                final String shardId = pendingRelinquish.getKey();
                final long relinquishDeadline = pendingRelinquish.getValue();
                if (!stillOwned.contains(shardId)) {
                    pendingRelinquishDeadlines.remove(shardId);
                    continue;
                }

                if (now < relinquishDeadline) {
                    tryAcquireLease(shardId);
                } else {
                    try {
                        releaseLease(shardId);
                        pendingRelinquishDeadlines.remove(shardId);
                        logger.info("Completed graceful relinquish for shard {}", shardId);
                    } catch (final Exception e) {
                        logger.warn("Failed to complete graceful relinquish for shard {}", shardId, e);
                    }
                }
            }

            for (final String shardId : availableShardIds) {
                if (ownedShards.size() >= targetCount) {
                    break;
                }
                if (pendingRelinquishDeadlines.containsKey(shardId)) {
                    continue;
                }
                if (tryAcquireLease(shardId)) {
                    ownedShards.add(shardId);
                }
            }

            ownedShards.removeIf(id -> !currentShardIds.contains(id));
            lastLeaseRefresh = Instant.now();
        } finally {
            leaseRefreshInProgress.set(false);
        }
    }

    String readCheckpoint(final String shardId) {
        final GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(activeCheckpointTableName)
                .key(checkpointKey(shardId))
                .consistentRead(true)
                .build();
        final GetItemResponse response = dynamoDbClient.getItem(getItemRequest);

        if (response.hasItem() && response.item().containsKey("sequenceNumber")) {
            final String value = response.item().get("sequenceNumber").s();
            if (isValidSequenceNumber(value)) {
                logger.debug("Read checkpoint for shard {}: {}", shardId, value);
                return value;
            }
            logger.warn("Ignoring non-numeric checkpoint [{}] for shard {} in table [{}]", value, shardId, activeCheckpointTableName);
        } else {
            logger.debug("No checkpoint found for shard {} in table [{}]", shardId, activeCheckpointTableName);
        }
        return null;
    }

    void writeCheckpoints(final Map<String, ShardCheckpoint> checkpoints) {
        for (final Map.Entry<String, ShardCheckpoint> entry : checkpoints.entrySet()) {
            writeCheckpoint(entry.getKey(), entry.getValue());
        }
    }

    void releaseAllLeases() {
        final Set<String> shardsToRelease = new HashSet<>(ownedShards);
        shardsToRelease.addAll(pendingRelinquishDeadlines.keySet());
        for (final String shardId : shardsToRelease) {
            try {
                releaseLease(shardId);
            } catch (final Exception e) {
                logger.warn("Failed to release lease for shard {}", shardId, e);
            }
        }

        try {
            final UpdateItemRequest heartbeatReleaseRequest = UpdateItemRequest.builder()
                    .tableName(activeCheckpointTableName)
                    .key(nodeHeartbeatKey())
                    .updateExpression("REMOVE nodeHeartbeat, lastUpdateTimestamp")
                    .build();
            dynamoDbClient.updateItem(heartbeatReleaseRequest);
        } catch (final Exception e) {
            logger.debug("Failed to clear node heartbeat record for node {}", nodeId, e);
        }
    }

    void close() {
        ownedShards.clear();
        pendingRelinquishDeadlines.clear();
        highestWrittenCheckpoints.clear();
        leaseRefreshInProgress.set(false);
        lastLeaseRefresh = Instant.EPOCH;
        shardCache = new ShardCache(List.of(), Instant.EPOCH);
    }

    private synchronized List<Shard> refreshShards() {
        final ShardCache current = shardCache;
        if (!current.shards().isEmpty() && Instant.now().toEpochMilli() < current.refreshTime().toEpochMilli() + shardCacheMillis) {
            return current.shards();
        }

        final List<Shard> allShards = new ArrayList<>();
        String nextToken = null;
        do {
            final ListShardsRequest request = nextToken != null
                    ? ListShardsRequest.builder().nextToken(nextToken).build()
                    : ListShardsRequest.builder().streamName(streamName).build();

            final ListShardsResponse response = kinesisClient.listShards(request);
            allShards.addAll(response.shards());
            nextToken = response.nextToken();
        } while (nextToken != null);

        logger.debug("ListShards returned {} shards for stream {}", allShards.size(), streamName);
        shardCache = new ShardCache(allShards, Instant.now());
        return allShards;
    }

    private static boolean isValidSequenceNumber(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int idx = 0; idx < value.length(); idx++) {
            if (!Character.isDigit(value.charAt(idx))) {
                return false;
            }
        }
        return true;
    }

    private boolean tryAcquireLease(final String shardId) {
        final long now = Instant.now().toEpochMilli();
        final long expiry = now + leaseDurationMillis;
        final AttributeValue ownerVal = AttributeValue.builder().s(nodeId).build();
        final AttributeValue expiryVal = AttributeValue.builder().n(String.valueOf(expiry)).build();
        final AttributeValue nowVal = AttributeValue.builder().n(String.valueOf(now)).build();

        try {
            final UpdateItemRequest leaseRequest = UpdateItemRequest.builder()
                    .tableName(activeCheckpointTableName)
                    .key(checkpointKey(shardId))
                    .updateExpression("SET leaseOwner = :owner, leaseExpiry = :exp, lastUpdateTimestamp = :ts")
                    .conditionExpression("attribute_not_exists(leaseOwner) OR leaseOwner = :owner OR leaseExpiry < :now")
                    .expressionAttributeValues(Map.of(
                            ":owner", ownerVal,
                            ":exp", expiryVal,
                            ":now", nowVal,
                            ":ts", nowVal))
                    .build();
            dynamoDbClient.updateItem(leaseRequest);
            return true;
        } catch (final ConditionalCheckFailedException e) {
            return false;
        } catch (final Exception e) {
            logger.warn("Failed to acquire lease for shard {}", shardId, e);
            return false;
        }
    }

    private void updateNodeHeartbeat(final long now) {
        final UpdateItemRequest heartbeatRequest = UpdateItemRequest.builder()
                .tableName(activeCheckpointTableName)
                .key(nodeHeartbeatKey())
                .updateExpression("SET nodeHeartbeat = :heartbeat, lastUpdateTimestamp = :ts")
                .expressionAttributeValues(Map.of(
                    ":heartbeat", AttributeValue.builder().n(String.valueOf(now)).build(),
                    ":ts", AttributeValue.builder().n(String.valueOf(now)).build()))
                .build();
        dynamoDbClient.updateItem(heartbeatRequest);
    }

    private Map<String, Map<String, AttributeValue>> queryAllLeaseItems() {
        final Map<String, Map<String, AttributeValue>> itemsByShardId = new HashMap<>();
        final QueryRequest.Builder queryBuilder = QueryRequest.builder()
                .tableName(activeCheckpointTableName)
                .consistentRead(true)
                .keyConditionExpression("streamName = :streamName")
                .expressionAttributeValues(Map.of(
                    ":streamName", AttributeValue.builder().s(streamName).build()));

        Map<String, AttributeValue> exclusiveStartKey = null;
        do {
            final QueryRequest queryRequest = exclusiveStartKey == null
                    ? queryBuilder.build()
                    : queryBuilder.exclusiveStartKey(exclusiveStartKey).build();
            final QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
            for (final Map<String, AttributeValue> item : queryResponse.items()) {
                final AttributeValue shardIdAttr = item.get("shardId");
                if (shardIdAttr == null) {
                    continue;
                }
                final String shardId = shardIdAttr.s();
                if (shardId.startsWith(CheckpointTableUtils.NODE_HEARTBEAT_PREFIX)
                        || CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID.equals(shardId)) {
                    continue;
                }
                itemsByShardId.put(shardId, item);
            }

            exclusiveStartKey = queryResponse.lastEvaluatedKey();
        } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());

        return itemsByShardId;
    }

    private int countActiveNodes(final long now) {
        final QueryRequest.Builder queryRequestBuilder = QueryRequest.builder()
                .tableName(activeCheckpointTableName)
                .consistentRead(true)
                .keyConditionExpression("streamName = :streamName AND begins_with(shardId, :nodePrefix)")
                .expressionAttributeValues(Map.of(
                    ":streamName", AttributeValue.builder().s(streamName).build(),
                    ":nodePrefix", AttributeValue.builder().s(CheckpointTableUtils.NODE_HEARTBEAT_PREFIX).build()));

        Map<String, AttributeValue> exclusiveStartKey = null;
        int activeNodes = 0;
        do {
            final QueryRequest queryRequest = exclusiveStartKey == null
                    ? queryRequestBuilder.build()
                    : queryRequestBuilder.exclusiveStartKey(exclusiveStartKey).build();
            final QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
            for (final Map<String, AttributeValue> item : queryResponse.items()) {
                final AttributeValue heartbeatValue = item.get("nodeHeartbeat");
                if (heartbeatValue == null) {
                    continue;
                }

                final long heartbeatMillis = Long.parseLong(heartbeatValue.n());
                if (now <= heartbeatMillis + nodeHeartbeatExpirationMillis) {
                    activeNodes++;
                }
            }

            exclusiveStartKey = queryResponse.lastEvaluatedKey();
        } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());

        return Math.max(1, activeNodes);
    }

    private void writeCheckpoint(final String shardId, final ShardCheckpoint checkpoint) {
        final BigInteger incomingSeq = new BigInteger(checkpoint.sequenceNumber());

        final ShardCheckpoint written = highestWrittenCheckpoints.compute(shardId, (key, existing) -> {
            if (existing != null && checkpoint.max(existing) == existing) {
                return existing;
            }

            try {
                final long now = Instant.now().toEpochMilli();
                final UpdateItemRequest checkpointRequest = UpdateItemRequest.builder()
                        .tableName(activeCheckpointTableName)
                        .key(checkpointKey(shardId))
                        .updateExpression("SET sequenceNumber = :seq, subSequenceNumber = :subSeq,"
                                + " lastUpdateTimestamp = :ts, leaseExpiry = :exp")
                        .conditionExpression("leaseOwner = :owner")
                        .expressionAttributeValues(Map.of(
                                ":seq", AttributeValue.builder().s(checkpoint.sequenceNumber()).build(),
                                ":subSeq", AttributeValue.builder().n(String.valueOf(checkpoint.subSequenceNumber())).build(),
                                ":ts", AttributeValue.builder().n(String.valueOf(now)).build(),
                                ":exp", AttributeValue.builder().n(String.valueOf(now + leaseDurationMillis)).build(),
                                ":owner", AttributeValue.builder().s(nodeId).build()))
                        .build();
                dynamoDbClient.updateItem(checkpointRequest);
                logger.debug("Checkpointed shard {} at sequence {}/{}", shardId, checkpoint.sequenceNumber(), checkpoint.subSequenceNumber());
                return checkpoint;
            } catch (final ConditionalCheckFailedException e) {
                logger.warn("Lost lease on shard {} during checkpoint; another node may have taken it", shardId);
            } catch (final Exception e) {
                logger.error("Failed to write checkpoint for shard {}", shardId, e);
            }
            return existing;
        });

        if (written != null && incomingSeq.compareTo(new BigInteger(written.sequenceNumber())) < 0) {
            logger.debug("Skipped checkpoint regression for shard {} (highest: {}, attempted: {})", shardId, written.sequenceNumber(), checkpoint.sequenceNumber());
        }
    }

    private Map<String, AttributeValue> checkpointKey(final String shardId) {
        return Map.of(
            "streamName", AttributeValue.builder().s(streamName).build(),
            "shardId", AttributeValue.builder().s(shardId).build());
    }

    private void releaseLease(final String shardId) {
        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(activeCheckpointTableName)
                .key(checkpointKey(shardId))
                .updateExpression("REMOVE leaseOwner, leaseExpiry")
                .conditionExpression("leaseOwner = :owner")
                .expressionAttributeValues(Map.of(":owner", AttributeValue.builder().s(nodeId).build()))
                .build();
        dynamoDbClient.updateItem(request);
    }

    private Map<String, AttributeValue> nodeHeartbeatKey() {
        return checkpointKey(CheckpointTableUtils.NODE_HEARTBEAT_PREFIX + nodeId);
    }

    private record ShardCache(List<Shard> shards, Instant refreshTime) { }
}
