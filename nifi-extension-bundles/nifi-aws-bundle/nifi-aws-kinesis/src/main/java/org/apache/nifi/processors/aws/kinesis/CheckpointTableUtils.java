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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.util.List;
import java.util.Map;

/**
 * Shared DynamoDB table lifecycle operations for checkpoint tables. Used by both
 * {@link KinesisShardManager} for runtime table management and
 * {@link LegacyCheckpointMigrator} for migration and rename operations.
 */
final class CheckpointTableUtils {

    static final String NODE_HEARTBEAT_PREFIX = "__node__#";
    static final String MIGRATION_MARKER_SHARD_ID = "__migration__";

    private static final long TABLE_POLL_MILLIS = 1_000;
    private static final int TABLE_POLL_MAX_ATTEMPTS = 60;

    private CheckpointTableUtils() { }

    enum TableSchema {
        NEW,
        LEGACY,
        UNKNOWN,
        NOT_FOUND
    }

    static TableSchema getTableSchema(final DynamoDbClient client, final String tableName) {
        try {
            final DescribeTableResponse describe = client.describeTable(
                    DescribeTableRequest.builder().tableName(tableName).build());
            final List<KeySchemaElement> keySchema = describe.table().keySchema();
            if (keySchema.size() == 2
                    && hasKey(keySchema, "streamName", KeyType.HASH)
                    && hasKey(keySchema, "shardId", KeyType.RANGE)) {
                return TableSchema.NEW;
            }
            if (keySchema.size() == 1 && hasKey(keySchema, "leaseKey", KeyType.HASH)) {
                return TableSchema.LEGACY;
            }
            return TableSchema.UNKNOWN;
        } catch (final ResourceNotFoundException notFound) {
            return TableSchema.NOT_FOUND;
        }
    }

    static void createNewSchemaTable(final DynamoDbClient client, final ComponentLog logger, final String tableName) {
        final TableSchema tableSchema = getTableSchema(client, tableName);
        if (tableSchema == TableSchema.NEW) {
            logger.info("DynamoDB checkpoint table [{}] already exists", tableName);
            return;
        }
        if (tableSchema == TableSchema.LEGACY || tableSchema == TableSchema.UNKNOWN) {
            throw new ProcessException(
                    "Checkpoint table [%s] exists but does not match expected schema".formatted(tableName));
        }

        logger.info("Creating DynamoDB checkpoint table [{}]", tableName);
        try {
            final CreateTableRequest request = CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(
                            KeySchemaElement.builder().attributeName("streamName").keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName("shardId").keyType(KeyType.RANGE).build())
                    .attributeDefinitions(
                            AttributeDefinition.builder().attributeName("streamName").attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName("shardId").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build();
            client.createTable(request);
        } catch (final ResourceInUseException alreadyCreating) {
            logger.info("DynamoDB checkpoint table [{}] is already being created by another node", tableName);
        }
    }

    static void waitForTableActive(final DynamoDbClient client, final ComponentLog logger, final String tableName) {
        final DescribeTableRequest request = DescribeTableRequest.builder().tableName(tableName).build();
        for (int i = 0; i < TABLE_POLL_MAX_ATTEMPTS; i++) {
            final TableStatus status = client.describeTable(request).table().tableStatus();
            if (status == TableStatus.ACTIVE) {
                logger.info("DynamoDB checkpoint table [{}] is now ACTIVE", tableName);
                return;
            }

            try {
                Thread.sleep(TABLE_POLL_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted while waiting for DynamoDB table to become ACTIVE", e);
            }
        }
        throw new ProcessException("DynamoDB checkpoint table [%s] did not become ACTIVE within %d seconds"
                .formatted(tableName, TABLE_POLL_MAX_ATTEMPTS));
    }

    static void deleteTable(final DynamoDbClient client, final ComponentLog logger, final String tableName) {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
            logger.info("Initiated deletion of DynamoDB table [{}]", tableName);
        } catch (final ResourceNotFoundException e) {
            logger.debug("Table [{}] already deleted", tableName);
        }
    }

    static void waitForTableDeleted(final DynamoDbClient client, final ComponentLog logger, final String tableName) {
        final DescribeTableRequest request = DescribeTableRequest.builder().tableName(tableName).build();
        for (int i = 0; i < TABLE_POLL_MAX_ATTEMPTS; i++) {
            try {
                client.describeTable(request);
            } catch (final ResourceNotFoundException e) {
                logger.info("DynamoDB table [{}] has been deleted", tableName);
                return;
            }

            try {
                Thread.sleep(TABLE_POLL_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted while waiting for DynamoDB table deletion", e);
            }
        }
        throw new ProcessException(
                "DynamoDB table [%s] was not deleted within %d seconds".formatted(tableName, TABLE_POLL_MAX_ATTEMPTS));
    }

    static void copyCheckpointItems(final DynamoDbClient client, final ComponentLog logger,
            final String sourceTableName, final String destTableName) {
        logger.info("Copying checkpoint items from [{}] to [{}]", sourceTableName, destTableName);
        final TableSchema destinationSchema = getTableSchema(client, destTableName);
        if (destinationSchema == TableSchema.NOT_FOUND || destinationSchema == TableSchema.UNKNOWN) {
            throw new ProcessException("Cannot copy checkpoint items to [%s]: destination schema is %s"
                    .formatted(destTableName, destinationSchema));
        }

        Map<String, AttributeValue> exclusiveStartKey = null;
        int copied = 0;
        do {
            final ScanRequest scanRequest = exclusiveStartKey == null
                    ? ScanRequest.builder().tableName(sourceTableName).build()
                    : ScanRequest.builder().tableName(sourceTableName).exclusiveStartKey(exclusiveStartKey).build();
            final ScanResponse scanResponse = client.scan(scanRequest);

            for (final Map<String, AttributeValue> item : scanResponse.items()) {
                final AttributeValue shardIdAttr = item.get("shardId");
                if (shardIdAttr != null) {
                    final String shardId = shardIdAttr.s();
                    if (shardId.startsWith(NODE_HEARTBEAT_PREFIX)
                            || MIGRATION_MARKER_SHARD_ID.equals(shardId)) {
                        continue;
                    }
                }

                final Map<String, AttributeValue> destinationItem = convertItemForDestinationSchema(item, destinationSchema);
                if (destinationItem == null) {
                    logger.debug("Skipping checkpoint item during copy because it cannot be converted for {} schema: keys={}",
                            destinationSchema, item.keySet());
                    continue;
                }

                client.putItem(PutItemRequest.builder()
                        .tableName(destTableName)
                        .item(destinationItem)
                        .build());
                copied++;
            }

            exclusiveStartKey = scanResponse.lastEvaluatedKey();
        } while (exclusiveStartKey != null && !exclusiveStartKey.isEmpty());

        logger.info("Copied {} checkpoint item(s) from [{}] to [{}]", copied, sourceTableName, destTableName);
    }

    private static Map<String, AttributeValue> convertItemForDestinationSchema(final Map<String, AttributeValue> item,
            final TableSchema destinationSchema) {
        return switch (destinationSchema) {
            case NEW -> item;
            case LEGACY -> convertToLegacyItem(item);
            case NOT_FOUND, UNKNOWN -> null;
        };
    }

    private static Map<String, AttributeValue> convertToLegacyItem(final Map<String, AttributeValue> item) {
        if (item.containsKey("leaseKey")) {
            return item;
        }

        final AttributeValue streamName = item.get("streamName");
        final AttributeValue shardId = item.get("shardId");
        if (streamName == null || shardId == null) {
            return null;
        }

        final String shardIdValue = shardId.s();
        if (shardIdValue == null || shardIdValue.isEmpty()
                || shardIdValue.startsWith(NODE_HEARTBEAT_PREFIX)
                || MIGRATION_MARKER_SHARD_ID.equals(shardIdValue)) {
            return null;
        }

        final AttributeValue sequenceNumber = item.get("sequenceNumber");
        final String leaseKey = streamName.s() + ":" + shardIdValue;
        if (sequenceNumber != null && sequenceNumber.s() != null) {
            return Map.of(
                    "leaseKey", AttributeValue.builder().s(leaseKey).build(),
                    "checkpoint", AttributeValue.builder().s(sequenceNumber.s()).build());
        }

        return Map.of("leaseKey", AttributeValue.builder().s(leaseKey).build());
    }

    private static boolean hasKey(final List<KeySchemaElement> keySchema, final String keyName, final KeyType keyType) {
        for (final KeySchemaElement element : keySchema) {
            if (keyName.equals(element.attributeName()) && keyType == element.keyType()) {
                return true;
            }
        }
        return false;
    }
}
