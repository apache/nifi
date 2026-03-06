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

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumeKinesisTest {

    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ConsumeKinesis.class);

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("json-reader", reader);
        runner.enableControllerService(reader);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", writer);
        runner.enableControllerService(writer);
    }

    private void setCommonProperties() throws Exception {
        final AWSCredentialsProviderControllerService credentialsService = new AWSCredentialsProviderControllerService();
        runner.addControllerService("creds", credentialsService);
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, "AK_STUB");
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.SECRET_KEY, "SK_STUB");
        runner.enableControllerService(credentialsService);

        runner.setProperty(ConsumeKinesis.APPLICATION_NAME, "test-app");
        runner.setProperty(ConsumeKinesis.STREAM_NAME, "test-stream");
        runner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "creds");
    }

    @Test
    void testProcessingStrategyValidation() throws Exception {
        setCommonProperties();

        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runner.assertValid();

        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "LINE_DELIMITED");
        runner.assertValid();

        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.assertNotValid();

        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.assertNotValid();

        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runner.assertValid();
    }

    @Test
    void testAllValidRecordsRoutedToSuccess() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "{\"name\":\"Alice\"}"),
                testRecord("2", "{\"name\":\"Bob\"}"),
                testRecord("3", "{\"name\":\"Charlie\"}"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 0);
        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "3");
    }

    @Test
    void testSingleInvalidRecordRoutedToParseFailure() throws Exception {
        assertInvalidRecordAtPosition("1", "THIS IS NOT JSON",
                testRecord("1", "THIS IS NOT JSON"), testRecord("2", "{\"name\":\"Bob\"}"), testRecord("3", "{\"name\":\"Charlie\"}"));
        assertInvalidRecordAtPosition("2", "CORRUPT DATA HERE",
                testRecord("1", "{\"name\":\"Alice\"}"), testRecord("2", "CORRUPT DATA HERE"), testRecord("3", "{\"name\":\"Charlie\"}"));
        assertInvalidRecordAtPosition("3", "NOT VALID JSON!!!",
                testRecord("1", "{\"name\":\"Alice\"}"), testRecord("2", "{\"name\":\"Bob\"}"), testRecord("3", "NOT VALID JSON!!!"));
    }

    @Test
    void testMultipleInvalidRecordsInBatch() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "BAD FIRST"),
                testRecord("2", "{\"name\":\"Bob\"}"),
                testRecord("3", "BAD THIRD"),
                testRecord("4", "{\"name\":\"Dave\"}"),
                testRecord("5", "BAD FIFTH"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 3);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "2");

        final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE);
        final List<String> failureSequences = new ArrayList<>();
        for (final MockFlowFile flowFile : failures) {
            failureSequences.add(flowFile.getAttribute(ConsumeKinesis.ATTR_FIRST_SEQUENCE));
        }
        assertTrue(failureSequences.contains("1"));
        assertTrue(failureSequences.contains("3"));
        assertTrue(failureSequences.contains("5"));
    }

    @Test
    void testAllInvalidRecordsRoutedToParseFailure() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "BAD1"),
                testRecord("2", "BAD2"),
                testRecord("3", "BAD3"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 3);
    }

    @Test
    void testFlowFilePerRecordDeliversAllRecords() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "record-one"),
                testRecord("2", "record-two"),
                testRecord("3", "record-three"));

        triggerWithStrategy(records, "FLOW_FILE", "shardId-000000000001");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 3);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeEquals("record.count", "1");
            flowFile.assertAttributeEquals(ConsumeKinesis.ATTR_STREAM_NAME, "test-stream");
            flowFile.assertAttributeEquals(ConsumeKinesis.ATTR_SHARD_ID, "shardId-000000000001");
            final String firstSequence = flowFile.getAttribute(ConsumeKinesis.ATTR_FIRST_SEQUENCE);
            final String lastSequence = flowFile.getAttribute(ConsumeKinesis.ATTR_LAST_SEQUENCE);
            assertEquals(firstSequence, lastSequence);
            assertNotNull(flowFile.getAttribute(ConsumeKinesis.ATTR_PARTITION_KEY));
            assertNotNull(flowFile.getAttribute(ConsumeKinesis.ATTR_FIRST_SUBSEQUENCE));
            assertNotNull(flowFile.getAttribute(ConsumeKinesis.ATTR_LAST_SUBSEQUENCE));
        }

        flowFiles.get(0).assertContentEquals("record-one");
        flowFiles.get(1).assertContentEquals("record-two");
        flowFiles.get(2).assertContentEquals("record-three");
    }

    @Test
    void testDemarcatorDeliversAllRecords() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "line-one"),
                testRecord("2", "line-two"),
                testRecord("3", "line-three"));

        triggerWithStrategy(records, "LINE_DELIMITED", "shardId-000000000001");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertContentEquals("line-one\nline-two\nline-three");
        success.assertAttributeEquals("record.count", "3");
    }

    @Test
    void testMultipleShardsNoDataLoss() throws Exception {
        final ShardFetchResult shard1Result = new ShardFetchResult("shard-A",
                List.of(testRecord("10", "{\"id\":1}"), testRecord("20", "{\"id\":2}")), 0L);
        final ShardFetchResult shard2Result = new ShardFetchResult("shard-B",
                List.of(testRecord("30", "{\"id\":3}"), testRecord("40", "{\"id\":4}")), 0L);

        triggerWithResults(List.of(shard1Result, shard2Result), "RECORD");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 2);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 0);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        final Set<String> shardsSeen = new LinkedHashSet<>();
        long totalRecords = 0;
        for (final MockFlowFile flowFile : flowFiles) {
            shardsSeen.add(flowFile.getAttribute(ConsumeKinesis.ATTR_SHARD_ID));
            totalRecords += Long.parseLong(flowFile.getAttribute("record.count"));
        }
        assertEquals(Set.of("shard-A", "shard-B"), shardsSeen);
        assertEquals(4, totalRecords);
    }

    @Test
    void testRecordMetadataInjectionPreservesRecordCount() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "{\"name\":\"Alice\"}"),
                testRecord("2", "{\"name\":\"Bob\"}"),
                testRecord("3", "{\"name\":\"Charlie\"}"));

        triggerWithOutputStrategy(records, "INJECT_METADATA");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 0);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "3");

        final String content = success.getContent();
        assertTrue(content.contains("kinesisMetadata"));
        assertTrue(content.contains("\"stream\""));
        assertTrue(content.contains("\"shardId\""));
        assertTrue(content.contains("\"sequenceNumber\""));
        assertTrue(content.contains("\"partitionKey\""));
    }

    @Test
    void testUseWrapperOutputStrategy() throws Exception {
        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "{\"name\":\"Alice\"}"),
                testRecord("2", "{\"name\":\"Bob\"}"));

        triggerWithOutputStrategy(records, "USE_WRAPPER");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 0);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "2");

        final String content = success.getContent();
        assertTrue(content.contains("kinesisMetadata"));
        assertTrue(content.contains("value"));
        assertTrue(content.contains("Alice"));
    }

    @Test
    void testAtTimestampInitialPositionRequiresTimestamp() throws Exception {
        setCommonProperties();
        runner.setProperty(ConsumeKinesis.INITIAL_STREAM_POSITION, "AT_TIMESTAMP");
        runner.assertNotValid();

        runner.setProperty(ConsumeKinesis.STREAM_POSITION_TIMESTAMP, "2025-01-15T00:00:00Z");
        runner.assertValid();
    }

    @Test
    void testPropertyMigrationRenamesMaxBytesToBuffer() throws Exception {
        runner = TestRunners.newTestRunner(ConsumeKinesis.class);

        setCommonProperties();
        runner.setProperty("Max Bytes to Buffer", "5 MB");

        final PropertyMigrationResult result = runner.migrateProperties();
        assertTrue(result.getPropertiesRenamed().containsKey("Max Bytes to Buffer"));
        assertEquals("Max Batch Size", result.getPropertiesRenamed().get("Max Bytes to Buffer"));
        assertEquals("5 MB", runner.getProcessContext().getProperty(ConsumeKinesis.MAX_BATCH_SIZE).getValue());
    }

    @Test
    void testPropertyMigrationRemovesCheckpointInterval() throws Exception {
        runner = TestRunners.newTestRunner(ConsumeKinesis.class);

        setCommonProperties();
        runner.setProperty("Checkpoint Interval", "5 min");

        final PropertyMigrationResult result = runner.migrateProperties();
        assertTrue(result.getPropertiesRemoved().contains("Checkpoint Interval"));
    }

    @Test
    void testDynamicRelationships() throws Exception {
        setCommonProperties();

        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        assertEquals(Set.of("success"), collectRelationshipNames());

        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        assertEquals(Set.of("success", "parse.failure"), collectRelationshipNames());
    }

    @Test
    void testEmptyRecordDoesNotCauseStuckState() throws Exception {
        final DeaggregatedRecord emptyRecord = new DeaggregatedRecord("shardId-000000000001", "2", 0, "pk-2", new byte[0], Instant.now());

        final List<DeaggregatedRecord> records = List.of(
                testRecord("1", "{\"name\":\"Alice\"}"),
                emptyRecord,
                testRecord("3", "{\"name\":\"Charlie\"}"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "2");
    }

    private void triggerWithRecords(final List<DeaggregatedRecord> records) throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager("shardId-000000000001");
        final ShardFetchResult fetchResult = new ShardFetchResult("shardId-000000000001", records, 0L);
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, fetchResult);
        runner = TestRunners.newTestRunner(processor);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", jsonWriter);
        runner.enableControllerService(jsonWriter);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");

        runner.run();
    }

    private Set<String> collectRelationshipNames() {
        final Set<String> names = new LinkedHashSet<>();
        for (final Relationship relationship : runner.getProcessor().getRelationships()) {
            names.add(relationship.getName());
        }
        return names;
    }

    private void assertInvalidRecordAtPosition(final String expectedFailureSequence, final String expectedFailureContent,
            final DeaggregatedRecord... records) throws Exception {
        triggerWithRecords(List.of(records));

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 1);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "2");

        final MockFlowFile failure = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE).getFirst();
        failure.assertContentEquals(expectedFailureContent);
        failure.assertAttributeEquals(ConsumeKinesis.ATTR_FIRST_SEQUENCE, expectedFailureSequence);
    }

    private void triggerWithOutputStrategy(final List<DeaggregatedRecord> records, final String outputStrategy) throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager("shardId-000000000001");
        final ShardFetchResult fetchResult = new ShardFetchResult("shardId-000000000001", records, 0L);
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, fetchResult);
        runner = TestRunners.newTestRunner(processor);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", jsonWriter);
        runner.enableControllerService(jsonWriter);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runner.setProperty(ConsumeKinesis.OUTPUT_STRATEGY, outputStrategy);

        runner.run();
    }

    private void triggerWithStrategy(final List<DeaggregatedRecord> records, final String processingStrategy,
            final String shardId) throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager(shardId);
        final ShardFetchResult fetchResult = new ShardFetchResult(shardId, records, 0L);
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, fetchResult);
        runner = TestRunners.newTestRunner(processor);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, processingStrategy);

        runner.run();
    }

    private void triggerWithResults(final List<ShardFetchResult> results, final String processingStrategy) throws Exception {
        final Set<String> shardIds = new LinkedHashSet<>();
        for (final ShardFetchResult fetchResult : results) {
            shardIds.add(fetchResult.shardId());
        }

        final KinesisShardManager mockShardManager = buildShardManager(shardIds.toArray(new String[0]));
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, results);
        runner = TestRunners.newTestRunner(processor);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", jsonWriter);
        runner.enableControllerService(jsonWriter);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, processingStrategy);
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");

        runner.run();
    }

    private static KinesisShardManager buildShardManager(final String... shardIds) {
        final KinesisShardManager mockShardManager = mock(KinesisShardManager.class);
        final List<Shard> shards = new ArrayList<>();
        for (final String id : shardIds) {
            shards.add(Shard.builder().shardId(id).build());
        }
        when(mockShardManager.getOwnedShards()).thenReturn(shards);
        when(mockShardManager.getCachedShardCount()).thenReturn(shardIds.length);
        when(mockShardManager.shouldProcessFetchedResult(anyString())).thenReturn(true);
        return mockShardManager;
    }

    private static DeaggregatedRecord testRecord(final String sequenceNumber, final String data) {
        return new DeaggregatedRecord(
                "shardId-000000000001",
                sequenceNumber,
                0,
                "pk-" + sequenceNumber,
                data.getBytes(StandardCharsets.UTF_8),
                Instant.now());
    }

    static class TestableConsumeKinesis extends ConsumeKinesis {
        private final KinesisShardManager mockShardManager;
        private final List<ShardFetchResult> preloadedResults;

        TestableConsumeKinesis(final KinesisShardManager mockShardManager, final ShardFetchResult preloadedResult) {
            this(mockShardManager, List.of(preloadedResult));
        }

        TestableConsumeKinesis(final KinesisShardManager mockShardManager, final List<ShardFetchResult> preloadedResults) {
            this.mockShardManager = mockShardManager;
            this.preloadedResults = preloadedResults;
        }

        @Override
        protected KinesisShardManager createShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient,
                final ComponentLog logger, final String checkpointTableName, final String streamName) {
            return mockShardManager;
        }

        @Override
        protected KinesisConsumerClient createConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger,
                final boolean efoMode) {
            final KinesisConsumerClient client = new StubConsumerClient(mock(KinesisClient.class), logger);
            for (final ShardFetchResult result : preloadedResults) {
                client.enqueueResult(result);
            }
            return client;
        }
    }

    static class StubConsumerClient extends KinesisConsumerClient {
        StubConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger) {
            super(kinesisClient, logger);
        }

        @Override
        void startFetches(final List<Shard> shards, final String streamName, final int batchSize,
                final String initialStreamPosition, final KinesisShardManager shardManager) {
        }

        @Override
        boolean hasPendingFetches() {
            return hasQueuedResults();
        }

        @Override
        void acknowledgeResults(final List<ShardFetchResult> results) {
        }

        @Override
        void rollbackResults(final List<ShardFetchResult> results) {
        }

        @Override
        void removeUnownedShards(final Set<String> ownedShards) {
        }

        @Override
        void logDiagnostics(final int ownedCount, final int cachedShardCount) {
        }
    }
}
