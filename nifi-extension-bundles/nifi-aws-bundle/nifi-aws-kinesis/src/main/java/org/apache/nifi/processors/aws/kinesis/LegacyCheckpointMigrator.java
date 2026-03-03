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
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.Map;

/**
 * Handles one-time migration of checkpoint data from legacy KCL-format DynamoDB tables
 * to the new composite-key schema used by {@link KinesisShardManager}, including the
 * table rename lifecycle. Uses distributed locks in the target table to coordinate
 * migration and rename operations across clustered nodes.
 */
final class LegacyCheckpointMigrator {

    private static final String MIGRATION_TABLE_SUFFIX = "_migration";
    private static final String LEGACY_LEASE_KEY_ATTRIBUTE = "leaseKey";
    private static final String LEGACY_CHECKPOINT_ATTRIBUTE = "checkpoint";
    private static final String MIGRATION_STATUS_ATTRIBUTE = "migrationStatus";
    private static final String MIGRATION_STATUS_IN_PROGRESS = "IN_PROGRESS";
    private static final String MIGRATION_STATUS_COMPLETE = "COMPLETE";
    private static final long MIGRATION_LOCK_STALE_MILLIS = 600_000;
    private static final long MIGRATION_WAIT_MILLIS = 2_000;
    private static final int MIGRATION_WAIT_MAX_ATTEMPTS = 180;
    private static final long RENAME_LOCK_STALE_MILLIS = 120_000;
    private static final long RENAME_POLL_MILLIS = 1_000;
    private static final int RENAME_POLL_MAX_ATTEMPTS = 60;

    private final DynamoDbClient dynamoDbClient;
    private final String checkpointTableName;
    private final String streamName;
    private final String nodeId;
    private final ComponentLog logger;

    LegacyCheckpointMigrator(final DynamoDbClient dynamoDbClient, final String checkpointTableName,
            final String streamName, final String nodeId, final ComponentLog logger) {
        this.dynamoDbClient = dynamoDbClient;
        this.checkpointTableName = checkpointTableName;
        this.streamName = streamName;
        this.nodeId = nodeId;
        this.logger = logger;
    }

    String findMigrationTable() {
        final String migrationTableName = checkpointTableName + MIGRATION_TABLE_SUFFIX;
        if (CheckpointTableUtils.getTableSchema(dynamoDbClient, migrationTableName)
                == CheckpointTableUtils.TableSchema.NEW) {
            return migrationTableName;
        }
        return null;
    }

    void cleanupLingeringMigration() {
        final String lingeringMigration = findMigrationTable();
        if (lingeringMigration == null) {
            return;
        }
        logger.info("Cleaning up lingering migration table [{}]", lingeringMigration);
        CheckpointTableUtils.copyCheckpointItems(dynamoDbClient, logger, lingeringMigration, checkpointTableName);
        CheckpointTableUtils.deleteTable(dynamoDbClient, logger, lingeringMigration);
    }

    void migrateAndRename() {
        final String existingMigration = findMigrationTable();
        final String migrationTableName;

        if (existingMigration == null) {
            migrationTableName = checkpointTableName + MIGRATION_TABLE_SUFFIX;
            logger.info("Legacy checkpoint table detected; migrating via [{}]", migrationTableName);
            CheckpointTableUtils.createNewSchemaTable(dynamoDbClient, logger, migrationTableName);
            CheckpointTableUtils.waitForTableActive(dynamoDbClient, logger, migrationTableName);
            migrateCheckpoints(checkpointTableName, migrationTableName);
        } else {
            migrationTableName = existingMigration;
            logger.info("Found existing migration table [{}]; completing rename to [{}]",
                    migrationTableName, checkpointTableName);
        }

        renameMigrationTable(migrationTableName);
    }

    void renameMigrationTable(final String migrationTableName) {
        if (acquireRenameLock(migrationTableName)) {
            CheckpointTableUtils.deleteTable(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.waitForTableDeleted(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.createNewSchemaTable(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.waitForTableActive(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.copyCheckpointItems(dynamoDbClient, logger, migrationTableName, checkpointTableName);
            CheckpointTableUtils.deleteTable(dynamoDbClient, logger, migrationTableName);
        } else {
            waitForTableRenamed(migrationTableName);
        }
    }

    private boolean acquireRenameLock(final String migrationTableName) {
        try {
            final long now = Instant.now().toEpochMilli();
            final Map<String, AttributeValue> key = migrationMarkerKey();
            final Map<String, AttributeValue> values = Map.of(
                    ":owner", AttributeValue.builder().s(nodeId).build(),
                    ":now", AttributeValue.builder().n(String.valueOf(now)).build());
            final UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(migrationTableName)
                    .key(key)
                    .updateExpression("SET renameOwner = :owner, renameStartedAt = :now")
                    .conditionExpression("attribute_not_exists(renameOwner)")
                    .expressionAttributeValues(values)
                    .build();
            dynamoDbClient.updateItem(request);
            logger.info("Acquired rename lock for migration table [{}]", migrationTableName);
            return true;
        } catch (final ConditionalCheckFailedException e) {
            logger.debug("Rename lock already held for migration table [{}]", migrationTableName);
            return false;
        } catch (final ResourceNotFoundException e) {
            logger.debug("Migration table [{}] already deleted; rename must be complete", migrationTableName);
            return false;
        }
    }

    private boolean forceAcquireStaleRenameLock(final String migrationTableName) {
        try {
            final long now = Instant.now().toEpochMilli();
            final long staleThreshold = now - RENAME_LOCK_STALE_MILLIS;
            final Map<String, AttributeValue> key = migrationMarkerKey();
            final Map<String, AttributeValue> values = Map.of(
                    ":owner", AttributeValue.builder().s(nodeId).build(),
                    ":now", AttributeValue.builder().n(String.valueOf(now)).build(),
                    ":staleThreshold", AttributeValue.builder().n(String.valueOf(staleThreshold)).build());
            final UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(migrationTableName)
                    .key(key)
                    .updateExpression("SET renameOwner = :owner, renameStartedAt = :now")
                    .conditionExpression("attribute_exists(renameOwner) AND renameStartedAt < :staleThreshold")
                    .expressionAttributeValues(values)
                    .build();
            dynamoDbClient.updateItem(request);
            logger.info("Force-acquired stale rename lock for migration table [{}]", migrationTableName);
            return true;
        } catch (final ConditionalCheckFailedException | ResourceNotFoundException e) {
            return false;
        }
    }

    private Map<String, AttributeValue> migrationMarkerKey() {
        return Map.of(
                "streamName", AttributeValue.builder().s(streamName).build(),
                "shardId", AttributeValue.builder().s(CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID).build());
    }

    private void waitForTableRenamed(final String migrationTableName) {
        for (int i = 0; i < RENAME_POLL_MAX_ATTEMPTS; i++) {
            if (CheckpointTableUtils.getTableSchema(dynamoDbClient, checkpointTableName)
                    == CheckpointTableUtils.TableSchema.NEW) {
                logger.info("Migration table rename complete; table [{}] is now available", checkpointTableName);
                return;
            }

            try {
                Thread.sleep(RENAME_POLL_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted while waiting for migration table rename to complete", e);
            }
        }

        logger.warn("Timed out waiting for migration table rename; attempting stale lock takeover");
        if (forceAcquireStaleRenameLock(migrationTableName)) {
            CheckpointTableUtils.deleteTable(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.waitForTableDeleted(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.createNewSchemaTable(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.waitForTableActive(dynamoDbClient, logger, checkpointTableName);
            CheckpointTableUtils.copyCheckpointItems(dynamoDbClient, logger, migrationTableName, checkpointTableName);
            CheckpointTableUtils.deleteTable(dynamoDbClient, logger, migrationTableName);
        } else if (CheckpointTableUtils.getTableSchema(dynamoDbClient, checkpointTableName)
                == CheckpointTableUtils.TableSchema.NEW) {
            logger.info("Migration table rename completed during takeover attempt");
        } else {
            throw new ProcessException(
                    "Unable to complete migration table rename for [%s]".formatted(checkpointTableName));
        }
    }

    private void migrateCheckpoints(final String sourceTableName, final String targetTableName) {
        if (isMigrationComplete(targetTableName)) {
            logger.debug("Legacy checkpoint migration already complete for stream [{}]", streamName);
            return;
        }

        if (acquireMigrationLock(targetTableName)) {
            performMigration(sourceTableName, targetTableName);
        } else {
            waitForMigrationComplete(targetTableName);
            if (!isMigrationComplete(targetTableName)) {
                logger.warn("Migration wait timed out with stale lock; attempting takeover for stream [{}]", streamName);
                if (forceAcquireStaleMigrationLock(targetTableName)) {
                    performMigration(sourceTableName, targetTableName);
                } else {
                    throw new ProcessException(
                            "Unable to acquire migration lock for stream [%s]".formatted(streamName));
                }
            }
        }
    }

    private void performMigration(final String sourceTableName, final String targetTableName) {
        try {
            migrateLegacyCheckpoints(sourceTableName, targetTableName);
            markMigrationComplete(targetTableName);
        } catch (final Exception e) {
            clearMigrationLock(targetTableName);
            throw new ProcessException("Failed to migrate legacy checkpoints to [%s]".formatted(targetTableName), e);
        }
    }

    private boolean acquireMigrationLock(final String targetTableName) {
        try {
            final long now = Instant.now().toEpochMilli();
            final PutItemRequest request = PutItemRequest.builder()
                    .tableName(targetTableName)
                    .item(Map.of(
                        "streamName", AttributeValue.builder().s(streamName).build(),
                        "shardId", AttributeValue.builder().s(CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID).build(),
                        MIGRATION_STATUS_ATTRIBUTE, AttributeValue.builder().s(MIGRATION_STATUS_IN_PROGRESS).build(),
                        "migrationOwner", AttributeValue.builder().s(nodeId).build(),
                        "migrationStartedAt", AttributeValue.builder().n(String.valueOf(now)).build(),
                        "lastUpdateTimestamp", AttributeValue.builder().n(String.valueOf(now)).build()))
                    .conditionExpression("attribute_not_exists(streamName)")
                    .build();
            dynamoDbClient.putItem(request);
            logger.info("Acquired checkpoint migration lock for stream [{}]", streamName);
            return true;
        } catch (final ConditionalCheckFailedException e) {
            return false;
        }
    }

    private boolean forceAcquireStaleMigrationLock(final String targetTableName) {
        try {
            final long now = Instant.now().toEpochMilli();
            final long staleThreshold = now - MIGRATION_LOCK_STALE_MILLIS;
            final UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(targetTableName)
                    .key(Map.of(
                        "streamName", AttributeValue.builder().s(streamName).build(),
                        "shardId", AttributeValue.builder().s(CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID).build()))
                    .updateExpression("SET migrationOwner = :owner, migrationStartedAt = :now, lastUpdateTimestamp = :now")
                    .conditionExpression("migrationStatus = :inProgress AND migrationStartedAt < :staleThreshold")
                    .expressionAttributeValues(Map.of(
                        ":owner", AttributeValue.builder().s(nodeId).build(),
                        ":now", AttributeValue.builder().n(String.valueOf(now)).build(),
                        ":inProgress", AttributeValue.builder().s(MIGRATION_STATUS_IN_PROGRESS).build(),
                        ":staleThreshold", AttributeValue.builder().n(String.valueOf(staleThreshold)).build()))
                    .build();
            dynamoDbClient.updateItem(request);
            logger.info("Force-acquired stale migration lock for stream [{}]", streamName);
            return true;
        } catch (final ConditionalCheckFailedException e) {
            return false;
        }
    }

    private void migrateLegacyCheckpoints(final String sourceTableName, final String targetTableName) {
        logger.info("Starting legacy checkpoint migration from [{}] to [{}] for stream [{}]",
                sourceTableName, targetTableName, streamName);

        Map<String, AttributeValue> exclusiveStartKey = null;
        int scanned = 0;
        int migrated = 0;
        int skippedMissingAttr = 0;
        int skippedNonNumeric = 0;
        do {
            final ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(sourceTableName)
                    .exclusiveStartKey(exclusiveStartKey)
                    .build();
            final ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

            for (final Map<String, AttributeValue> item : scanResponse.items()) {
                scanned++;
                final AttributeValue leaseKeyAttr = item.get(LEGACY_LEASE_KEY_ATTRIBUTE);
                final AttributeValue checkpointAttr = item.get(LEGACY_CHECKPOINT_ATTRIBUTE);
                if (leaseKeyAttr == null || checkpointAttr == null) {
                    skippedMissingAttr++;
                    logger.warn("Skipping legacy item missing leaseKey or checkpoint: keys={}", item.keySet());
                    continue;
                }

                final String shardId = extractShardId(leaseKeyAttr.s());
                if (shardId == null || shardId.isEmpty()) {
                    skippedMissingAttr++;
                    continue;
                }

                final String checkpoint = checkpointAttr.s();
                if (!isValidSequenceNumber(checkpoint)) {
                    skippedNonNumeric++;
                    logger.warn("Skipping non-numeric legacy checkpoint [{}] for shard {}", checkpoint, shardId);
                    continue;
                }

                final long now = Instant.now().toEpochMilli();
                final UpdateItemRequest request = UpdateItemRequest.builder()
                        .tableName(targetTableName)
                        .key(Map.of(
                            "streamName", AttributeValue.builder().s(streamName).build(),
                            "shardId", AttributeValue.builder().s(shardId).build()))
                        .updateExpression("SET sequenceNumber = :seq, lastUpdateTimestamp = :ts")
                        .expressionAttributeValues(Map.of(
                            ":seq", AttributeValue.builder().s(checkpoint).build(),
                            ":ts", AttributeValue.builder().n(String.valueOf(now)).build()))
                        .build();
                dynamoDbClient.updateItem(request);
                migrated++;
            }

            exclusiveStartKey = scanResponse.lastEvaluatedKey();
        } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());

        logger.info("Legacy checkpoint migration complete for stream [{}]: scanned={}, migrated={}, skippedNonNumeric={}, skippedMissingAttr={}",
                streamName, scanned, migrated, skippedNonNumeric, skippedMissingAttr);
    }

    private static String extractShardId(final String legacyLeaseKey) {
        if (legacyLeaseKey == null || legacyLeaseKey.isEmpty()) {
            return null;
        }
        final int separatorIndex = legacyLeaseKey.lastIndexOf(':');
        if (separatorIndex >= 0 && separatorIndex + 1 < legacyLeaseKey.length()) {
            return legacyLeaseKey.substring(separatorIndex + 1);
        }
        return legacyLeaseKey;
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

    private void markMigrationComplete(final String targetTableName) {
        final long now = Instant.now().toEpochMilli();
        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(targetTableName)
                .key(Map.of(
                    "streamName", AttributeValue.builder().s(streamName).build(),
                    "shardId", AttributeValue.builder().s(CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID).build()))
                .updateExpression("SET migrationStatus = :status, migrationCompletedAt = :doneAt, lastUpdateTimestamp = :ts REMOVE migrationOwner")
                .expressionAttributeValues(Map.of(
                    ":status", AttributeValue.builder().s(MIGRATION_STATUS_COMPLETE).build(),
                    ":doneAt", AttributeValue.builder().n(String.valueOf(now)).build(),
                    ":ts", AttributeValue.builder().n(String.valueOf(now)).build()))
                .build();
        dynamoDbClient.updateItem(request);
    }

    private void clearMigrationLock(final String targetTableName) {
        final UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(targetTableName)
                .key(Map.of(
                    "streamName", AttributeValue.builder().s(streamName).build(),
                    "shardId", AttributeValue.builder().s(CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID).build()))
                .updateExpression("REMOVE migrationStatus, migrationOwner, migrationStartedAt")
                .build();
        dynamoDbClient.updateItem(request);
    }

    private boolean isMigrationComplete(final String targetTableName) {
        final GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(targetTableName)
                .key(Map.of(
                    "streamName", AttributeValue.builder().s(streamName).build(),
                    "shardId", AttributeValue.builder().s(CheckpointTableUtils.MIGRATION_MARKER_SHARD_ID).build()))
                .build());
        if (!response.hasItem()) {
            return false;
        }
        final AttributeValue status = response.item().get(MIGRATION_STATUS_ATTRIBUTE);
        return status != null && MIGRATION_STATUS_COMPLETE.equals(status.s());
    }

    private void waitForMigrationComplete(final String targetTableName) {
        for (int attempt = 0; attempt < MIGRATION_WAIT_MAX_ATTEMPTS; attempt++) {
            if (isMigrationComplete(targetTableName)) {
                logger.info("Observed completed checkpoint migration for stream [{}]", streamName);
                return;
            }

            try {
                Thread.sleep(MIGRATION_WAIT_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted while waiting for checkpoint migration to complete", e);
            }
        }

        logger.warn("Timed out waiting for checkpoint migration to complete for stream [{}]; will check for stale lock",
                streamName);
    }
}
