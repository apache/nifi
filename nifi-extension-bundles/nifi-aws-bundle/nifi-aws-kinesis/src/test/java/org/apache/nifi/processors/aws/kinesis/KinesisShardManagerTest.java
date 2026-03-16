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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KinesisShardManagerTest {

    private DynamoDbClient dynamoDb;
    private KinesisShardManager manager;

    @BeforeEach
    void setUp() {
        dynamoDb = mock(DynamoDbClient.class);
        manager = new KinesisShardManager(mock(KinesisClient.class), dynamoDb, mock(ComponentLog.class), "test-table", "test-stream");
    }

    /**
     * Verifies that writeCheckpoints enforces monotonic checkpoint advancement.
     * A later call with a lower sequence number must not overwrite a higher one.
     */
    @Test
    void testCheckpointMonotonicity() {
        final UpdateItemResponse emptyResponse = UpdateItemResponse.builder().build();
        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenReturn(emptyResponse);

        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("50000")));
        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("30000")));
        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("70000")));

        final ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDb, times(2)).updateItem(captor.capture());

        final List<UpdateItemRequest> requests = captor.getAllValues();
        assertEquals("50000", requests.get(0).expressionAttributeValues().get(":seq").s());
        assertEquals("70000", requests.get(1).expressionAttributeValues().get(":seq").s(),
                "Only increasing checkpoints should be written to DynamoDB");
    }

    /**
     * Verifies that checkpoints for different shards are tracked independently.
     * A lower checkpoint on shard-1 must not affect shard-2.
     */
    @Test
    void testCheckpointMonotonicityPerShard() {
        final UpdateItemResponse emptyResponse = UpdateItemResponse.builder().build();
        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenReturn(emptyResponse);

        manager.writeCheckpoints(Map.of(
                "shard-1", new BigInteger("50000"),
                "shard-2", new BigInteger("20000")));
        manager.writeCheckpoints(Map.of(
                "shard-1", new BigInteger("30000"),
                "shard-2", new BigInteger("40000")));

        final ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDb, times(3)).updateItem(captor.capture());

        long shard1Writes = 0;
        long shard2Writes = 0;
        for (final UpdateItemRequest request : captor.getAllValues()) {
            if ("shard-1".equals(request.key().get("shardId").s())) {
                shard1Writes++;
            } else if ("shard-2".equals(request.key().get("shardId").s())) {
                shard2Writes++;
            }
        }

        assertEquals(1, shard1Writes, "shard-1 regression (30000 < 50000) should be skipped");
        assertEquals(2, shard2Writes, "shard-2 advance (40000 > 20000) should be written");
    }

    /**
     * Verifies that close() resets the monotonic checkpoint guard, allowing a fresh start.
     */
    @Test
    void testCloseResetsCheckpointGuard() {
        final UpdateItemResponse emptyResponse = UpdateItemResponse.builder().build();
        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenReturn(emptyResponse);

        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("50000")));
        manager.close();
        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("30000")));

        verify(dynamoDb, times(2)).updateItem(any(UpdateItemRequest.class));
    }

    /**
     * Verifies that readCheckpoint returns null when no checkpoint item exists in DynamoDB.
     * A wrong return value here would cause the processor to either skip data (if it returned
     * a stale sequence) or re-process from TRIM_HORIZON unnecessarily.
     */
    @Test
    void testReadCheckpointReturnsNullForMissingCheckpoint() {
        final GetItemResponse emptyResponse = GetItemResponse.builder().item(Map.of()).build();
        when(dynamoDb.getItem(any(GetItemRequest.class))).thenReturn(emptyResponse);

        assertNull(manager.readCheckpoint("shard-1"),
                "Missing checkpoint must return null so the processor starts from the initial stream position");
    }

    /**
     * Verifies that readCheckpoint ignores a non-numeric sequence number stored in DynamoDB.
     * Passing a corrupt value to GetShardIterator would cause an API error and potentially
     * force a full re-read from TRIM_HORIZON, creating massive duplication.
     */
    @Test
    void testReadCheckpointIgnoresInvalidSequenceNumber() {
        final GetItemResponse response = GetItemResponse.builder()
                .item(Map.of(
                        "streamName", AttributeValue.builder().s("test-stream").build(),
                        "shardId", AttributeValue.builder().s("shard-1").build(),
                        "sequenceNumber", AttributeValue.builder().s("NOT_A_NUMBER").build()))
                .build();
        when(dynamoDb.getItem(any(GetItemRequest.class))).thenReturn(response);

        assertNull(manager.readCheckpoint("shard-1"),
                "Non-numeric sequence number must be treated as missing to avoid API errors");
    }

    /**
     * Verifies that a ConditionalCheckFailedException during checkpoint write (indicating
     * lease loss) does not crash the processor and does not corrupt the in-memory monotonic
     * guard. A subsequent higher-valued checkpoint must still be attempted.
     */
    @Test
    void testWriteCheckpointHandlesLostLeaseGracefully() {
        final ConditionalCheckFailedException lostLease = ConditionalCheckFailedException.builder().message("lost lease").build();
        final UpdateItemResponse emptyResponse = UpdateItemResponse.builder().build();
        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenThrow(lostLease).thenReturn(emptyResponse);

        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("50000")));
        manager.writeCheckpoints(Map.of("shard-1", new BigInteger("70000")));

        final ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDb, times(2)).updateItem(captor.capture());

        assertEquals("50000", captor.getAllValues().get(0).expressionAttributeValues().get(":seq").s());
        assertEquals("70000", captor.getAllValues().get(1).expressionAttributeValues().get(":seq").s(),
                "After a lost-lease failure, the next higher checkpoint must still be attempted");
    }

    /**
     * Verifies that when no checkpoint table exists and no orphaned migration table exists,
     * a fresh table is created with the configured name (no suffix).
     */
    @Test
    void testEnsureCheckpointTableCreatesNewTableWhenNotFound() {
        final AtomicInteger mainTableDescribeCount = new AtomicInteger();
        when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenAnswer(invocation -> {
            final DescribeTableRequest req = invocation.getArgument(0);
            if ("test-table".equals(req.tableName())) {
                if (mainTableDescribeCount.incrementAndGet() <= 2) {
                    throw ResourceNotFoundException.builder().build();
                }
                return newSchemaActiveResponse();
            }
            throw ResourceNotFoundException.builder().build();
        });
        when(dynamoDb.createTable(any(CreateTableRequest.class))).thenReturn(CreateTableResponse.builder().build());

        manager.ensureCheckpointTableExists();

        final ArgumentCaptor<CreateTableRequest> captor = ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(dynamoDb).createTable(captor.capture());
        assertEquals("test-table", captor.getValue().tableName(), "Fresh table should use the configured name, not a migration suffix");
    }

    /**
     * Verifies that when the configured table already exists with the new composite-key
     * schema and no migration table is lingering, no creation or migration occurs.
     */
    @Test
    void testEnsureCheckpointTableUsesExistingNewTable() {
        when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenAnswer(invocation -> {
            final DescribeTableRequest req = invocation.getArgument(0);
            if ("test-table".equals(req.tableName())) {
                return newSchemaActiveResponse();
            }
            throw ResourceNotFoundException.builder().build();
        });

        manager.ensureCheckpointTableExists();

        verify(dynamoDb, never()).createTable(any(CreateTableRequest.class));
        verify(dynamoDb, never()).deleteTable(any(DeleteTableRequest.class));
    }

    /**
     * Verifies that when the configured table is NOT_FOUND but an orphaned migration table
     * exists (crash during a previous migration), the migration table is renamed to the
     * configured name by creating a new table, copying items, and deleting the migration table.
     */
    @Test
    void testEnsureCheckpointTableRenamesOrphanedMigrationTable() {
        final AtomicInteger mainTableDescribeCount = new AtomicInteger();
        when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenAnswer(invocation -> {
            final DescribeTableRequest req = invocation.getArgument(0);
            if ("test-table".equals(req.tableName())) {
                if (mainTableDescribeCount.incrementAndGet() <= 3) {
                    throw ResourceNotFoundException.builder().build();
                }

                return newSchemaActiveResponse();
            }

            if ("test-table_migration".equals(req.tableName())) {
                return newSchemaActiveResponse();
            }

            throw ResourceNotFoundException.builder().build();
        });

        when(dynamoDb.updateItem(any(UpdateItemRequest.class))).thenReturn(UpdateItemResponse.builder().build());
        when(dynamoDb.deleteTable(any(DeleteTableRequest.class))).thenAnswer(invocation -> {
            final DeleteTableRequest req = invocation.getArgument(0);
            if ("test-table".equals(req.tableName())) {
                throw ResourceNotFoundException.builder().build();
            }
            return DeleteTableResponse.builder().build();
        });

        when(dynamoDb.createTable(any(CreateTableRequest.class))).thenReturn(CreateTableResponse.builder().build());

        final Map<String, AttributeValue> checkpointItem = Map.of(
                "streamName", AttributeValue.builder().s("test-stream").build(),
                "shardId", AttributeValue.builder().s("shard-1").build(),
                "sequenceNumber", AttributeValue.builder().s("12345").build());
        when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(checkpointItem).build());
        when(dynamoDb.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());

        manager.ensureCheckpointTableExists();

        final ArgumentCaptor<CreateTableRequest> createCaptor = ArgumentCaptor.forClass(CreateTableRequest.class);
        verify(dynamoDb).createTable(createCaptor.capture());
        assertEquals("test-table", createCaptor.getValue().tableName(), "Renamed table should use the configured name");

        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDb).putItem(putCaptor.capture());
        assertEquals("test-table", putCaptor.getValue().tableName(), "Items should be copied to the configured table name");
        assertEquals("12345", putCaptor.getValue().item().get("sequenceNumber").s(), "Checkpoint data should be preserved during rename");

        final ArgumentCaptor<DeleteTableRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteTableRequest.class);
        verify(dynamoDb, times(2)).deleteTable(deleteCaptor.capture());
        final String deletedMigrationTable = deleteCaptor.getAllValues().stream()
                .map(DeleteTableRequest::tableName)
                .filter("test-table_migration"::equals)
                .findFirst()
                .orElseThrow();

        assertEquals("test-table_migration", deletedMigrationTable, "Migration table should be deleted after rename");
    }

    /**
     * Verifies that when the configured table has the new schema but a lingering migration
     * table exists (crash after copy but before migration table deletion), the items are
     * copied and the migration table is cleaned up.
     */
    @Test
    void testEnsureCheckpointTableCleansUpLingeringMigrationTable() {
        when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenAnswer(invocation -> {
            final DescribeTableRequest req = invocation.getArgument(0);
            if ("test-table".equals(req.tableName()) || "test-table_migration".equals(req.tableName())) {
                return newSchemaActiveResponse();
            }
            throw ResourceNotFoundException.builder().build();
        });

        when(dynamoDb.deleteTable(any(DeleteTableRequest.class))).thenReturn(DeleteTableResponse.builder().build());
        when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().build());

        manager.ensureCheckpointTableExists();

        verify(dynamoDb, never()).createTable(any(CreateTableRequest.class));
        final ArgumentCaptor<DeleteTableRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteTableRequest.class);
        verify(dynamoDb).deleteTable(deleteCaptor.capture());
        assertEquals("test-table_migration", deleteCaptor.getValue().tableName(), "Lingering migration table should be deleted");
    }

    private static DescribeTableResponse newSchemaActiveResponse() {
        final KeySchemaElement hashKey = KeySchemaElement.builder()
                .attributeName("streamName")
                .keyType(KeyType.HASH)
                .build();
        final KeySchemaElement rangeKey = KeySchemaElement.builder()
                .attributeName("shardId")
                .keyType(KeyType.RANGE)
                .build();
        final TableDescription table = TableDescription.builder()
                .keySchema(hashKey, rangeKey)
                .tableStatus(TableStatus.ACTIVE)
                .build();
        return DescribeTableResponse.builder().table(table).build();
    }
}
