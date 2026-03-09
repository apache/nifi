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
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CheckpointTableUtilsTest {

    @Test
    void testCopyCheckpointItemsCopiesShardItems() {
        final DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
        final ComponentLog logger = mock(ComponentLog.class);

        final Map<String, AttributeValue> item = Map.of(
                "streamName", AttributeValue.builder().s("my-stream").build(),
                "shardId", AttributeValue.builder().s("shardId-0001").build(),
                "sequenceNumber", AttributeValue.builder().s("12345").build());
        when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().items(item).build());
        when(dynamoDb.putItem(any(PutItemRequest.class))).thenReturn(PutItemResponse.builder().build());

        CheckpointTableUtils.copyCheckpointItems(dynamoDb, logger, "source-table", "dest-table");

        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDb, times(1)).putItem(putCaptor.capture());
        assertEquals(item, putCaptor.getValue().item());
    }

    @Test
    void testCopyCheckpointItemsSkipsNodeAndMigrationMarkers() {
        final DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
        final ComponentLog logger = mock(ComponentLog.class);

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

        CheckpointTableUtils.copyCheckpointItems(dynamoDb, logger, "source-table", "dest-table");

        final ArgumentCaptor<PutItemRequest> putCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDb, times(1)).putItem(putCaptor.capture());
        assertEquals(shardItem, putCaptor.getValue().item());
    }

    @Test
    void testCopyCheckpointItemsSkipsAllMarkers() {
        final DynamoDbClient dynamoDb = mock(DynamoDbClient.class);
        final ComponentLog logger = mock(ComponentLog.class);

        final Map<String, AttributeValue> nodeItem = Map.of(
                "streamName", AttributeValue.builder().s("my-stream").build(),
                "shardId", AttributeValue.builder().s("__node__#node-b").build());

        when(dynamoDb.scan(any(ScanRequest.class))).thenReturn(
                ScanResponse.builder().items(List.of(nodeItem)).build());

        CheckpointTableUtils.copyCheckpointItems(dynamoDb, logger, "source-table", "dest-table");

        verify(dynamoDb, never()).putItem(any(PutItemRequest.class));
    }
}
