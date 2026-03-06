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
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CheckpointTableUtilsTest {

    @Test
    void testCopyCheckpointItemsConvertsNewSchemaItemsForLegacyDestination() {
        final DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
        final ComponentLog logger = mock(ComponentLog.class);

        when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenAnswer(invocation -> {
            final DescribeTableRequest request = invocation.getArgument(0);
            if ("legacy-table".equals(request.tableName())) {
                return legacySchemaResponse();
            }
            return newSchemaResponse();
        });

        final Map<String, AttributeValue> newSchemaItem = Map.of(
                "streamName", AttributeValue.builder().s("my-stream").build(),
                "shardId", AttributeValue.builder().s("shardId-0001").build(),
                "sequenceNumber", AttributeValue.builder().s("12345").build());
        when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(newSchemaItem).build());
        when(dynamoDb.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());

        CheckpointTableUtils.copyCheckpointItems(dynamoDb, logger, "migration-table", "legacy-table");

        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDb, times(1)).putItem(putCaptor.capture());

        final Map<String, AttributeValue> copiedItem = putCaptor.getValue().item();
        assertEquals("my-stream:shardId-0001", copiedItem.get("leaseKey").s());
        assertEquals("12345", copiedItem.get("checkpoint").s());
    }

    @Test
    void testCopyCheckpointItemsSkipsNodeAndMigrationMarkersForLegacyDestination() {
        final DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
        final ComponentLog logger = mock(ComponentLog.class);

        when(dynamoDb.describeTable(any(DescribeTableRequest.class))).thenReturn(legacySchemaResponse());

        final Map<String, AttributeValue> nodeItem = Map.of(
                "streamName", AttributeValue.builder().s("my-stream").build(),
                "shardId", AttributeValue.builder().s("__node__#node-a").build());
        final Map<String, AttributeValue> migrationMarkerItem = Map.of(
                "streamName", AttributeValue.builder().s("my-stream").build(),
                "shardId", AttributeValue.builder().s("__migration__").build());
        final Map<String, AttributeValue> shardItem = Map.of(
                "streamName", AttributeValue.builder().s("my-stream").build(),
                "shardId", AttributeValue.builder().s("shardId-0002").build(),
                "sequenceNumber", AttributeValue.builder().s("67890").build());

        when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(
                ScanResponse.builder().items(List.of(nodeItem, migrationMarkerItem, shardItem)).build());
        when(dynamoDb.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());

        CheckpointTableUtils.copyCheckpointItems(dynamoDb, logger, "migration-table", "legacy-table");

        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDb, times(1)).putItem(putCaptor.capture());
        assertEquals("my-stream:shardId-0002", putCaptor.getValue().item().get("leaseKey").s());
    }

    private static DescribeTableResponse newSchemaResponse() {
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

    private static DescribeTableResponse legacySchemaResponse() {
        final KeySchemaElement hashKey = KeySchemaElement.builder()
                .attributeName("leaseKey")
                .keyType(KeyType.HASH)
                .build();
        final TableDescription table = TableDescription.builder()
                .keySchema(hashKey)
                .tableStatus(TableStatus.ACTIVE)
                .build();
        return DescribeTableResponse.builder().table(table).build();
    }
}
