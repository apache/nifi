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

import org.apache.nifi.components.Backlog;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumeKinesisTest {

    private static final String TEST_STREAM_NAME = "test-stream";

    private static final String DEFAULT_SHARD_ID = "shardId-000000000001";

    private static final String FIRST_SHARD_ID = "shard-A";

    private static final String SECOND_SHARD_ID = "shard-B";

    private static final String FIRST_JSON_RECORD = "{\"id\":1}";

    private static final String SECOND_JSON_RECORD = "{\"id\":2}";

    private static final String THIRD_JSON_RECORD = "{\"id\":3}";

    private static final String FOURTH_JSON_RECORD = "{\"id\":4}";

    private static final String FIRST_FLOW_FILE_RECORD = "record-one";

    private static final String SECOND_FLOW_FILE_RECORD = "record-two";

    private static final long FIRST_SHARD_BEHIND_MS = 1000L;

    private static final long SECOND_SHARD_BEHIND_MS = 2500L;

    private static final long FLOW_FILE_BEHIND_MS = 5000L;

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
        setCommonProperties(runner);
    }

    private void setCommonProperties(final TestRunner targetRunner) throws Exception {
        final AWSCredentialsProviderControllerService credentialsService = new AWSCredentialsProviderControllerService();
        targetRunner.addControllerService("creds", credentialsService);
        targetRunner.setProperty(credentialsService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, "AK_STUB");
        targetRunner.setProperty(credentialsService, AWSCredentialsProviderControllerService.SECRET_KEY, "SK_STUB");
        targetRunner.enableControllerService(credentialsService);

        targetRunner.setProperty(ConsumeKinesis.APPLICATION_NAME, "test-app");
        targetRunner.setProperty(ConsumeKinesis.STREAM_NAME, TEST_STREAM_NAME);
        targetRunner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "creds");
    }

    @Test
    void testPartialShardLagReportingPresumesUnreportedShardsAreBehind() throws Exception {
        // The node owns three shards. Lag is reported only for shard-A (behind) and shard-B (caught up);
        // shard-C never reports a lag value. getBacklog must count both shard-A (reported behind) and
        // shard-C (not yet reported, so presumed behind) and return a lower-bound (AT_LEAST) estimate.
        final Map<String, Long> someBehind = new LinkedHashMap<>();
        someBehind.put("shard-A", 500L);
        someBehind.put("shard-B", 0L);
        final Backlog someBehindBacklog = reportBacklogWithShardLag(List.of("shard-A", "shard-B", "shard-C"), someBehind);
        assertEquals(Backlog.Precision.AT_LEAST, someBehindBacklog.getPrecision());
        assertEquals(OptionalLong.of(2L), someBehindBacklog.getFlowFileCount());
        assertEquals(OptionalLong.of(2L), someBehindBacklog.getRecordCount());

        // Every shard that has reported is caught up, but shard-C still has not reported. Because
        // shard-C's true state is unknown, it is presumed behind, so getBacklog must not emit the exact
        // caught-up Backlog, which would wrongly claim the whole stream is drained. It reports a
        // conservative lower-bound of one (the presumed-behind shard-C) instead.
        final Map<String, Long> allReportedCaughtUp = new LinkedHashMap<>();
        allReportedCaughtUp.put("shard-A", 0L);
        allReportedCaughtUp.put("shard-B", 0L);
        final Backlog reportedCaughtUpBacklog = reportBacklogWithShardLag(List.of("shard-A", "shard-B", "shard-C"), allReportedCaughtUp);
        assertEquals(Backlog.Precision.AT_LEAST, reportedCaughtUpBacklog.getPrecision());
        assertEquals(OptionalLong.of(1L), reportedCaughtUpBacklog.getFlowFileCount());
        assertEquals(OptionalLong.of(1L), reportedCaughtUpBacklog.getRecordCount());
    }

    @Test
    void testShardLagReportingClaimsCaughtUpOnlyWhenEveryOwnedShardHasReportedZeroLag() throws Exception {
        final Map<String, Long> allCaughtUp = new LinkedHashMap<>();
        allCaughtUp.put("shard-A", 0L);
        allCaughtUp.put("shard-B", 0L);
        final Backlog backlog = reportBacklogWithShardLag(List.of("shard-A", "shard-B"), allCaughtUp);
        assertEquals(Backlog.Precision.EXACT, backlog.getPrecision());
        assertEquals(OptionalLong.of(0L), backlog.getFlowFileCount());
        assertEquals(OptionalLong.of(0L), backlog.getRecordCount());
    }

    private Backlog reportBacklogWithShardLag(final List<String> ownedShardIds, final Map<String, Long> shardLag) throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager(ownedShardIds.toArray(new String[0]));
        final LagReportingConsumeKinesis processor = new LagReportingConsumeKinesis(mockShardManager, shardLag);
        final TestRunner lagRunner = TestRunners.newTestRunner(processor);
        setCommonProperties(lagRunner);
        lagRunner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        lagRunner.setProperty(ConsumeKinesis.CONSUMER_TYPE, "SHARED_THROUGHPUT");

        lagRunner.run(1, false, true);

        final Optional<Backlog> backlog = processor.getBacklog(lagRunner.getProcessContext());
        assertTrue(backlog.isPresent());
        return backlog.get();
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
        final List<UserRecord> records = List.of(
                testRecord("1", "{\"name\":\"Alice\"}"),
                testRecord("2", "{\"name\":\"Bob\"}"),
                testRecord("3", "{\"name\":\"Charlie\"}"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 0);
        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "3");

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertEquals(3L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(List.of(0.0), runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), attributes));
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
        final List<UserRecord> records = List.of(
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

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertEquals(5L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(3L, runner.getCounterValue(KinesisMetricName.RECORDS_PARSED_ERRORS.getMetricName(), attributes));
    }

    @Test
    void testAllInvalidRecordsRoutedToParseFailure() throws Exception {
        final List<UserRecord> records = List.of(
                testRecord("1", "BAD1"),
                testRecord("2", "BAD2"),
                testRecord("3", "BAD3"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 3);

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertEquals(3L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(3L, runner.getCounterValue(KinesisMetricName.RECORDS_PARSED_ERRORS.getMetricName(), attributes));
    }

    @Test
    void testFlowFilePerRecordDeliversAllRecords() throws Exception {
        final List<UserRecord> records = List.of(
                testRecord("1", "record-one"),
                testRecord("2", "record-two"),
                testRecord("3", "record-three"));

        triggerWithStrategy(records, "FLOW_FILE", "shardId-000000000001");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 3);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        for (final MockFlowFile flowFile : flowFiles) {
            flowFile.assertAttributeEquals("record.count", "1");
            flowFile.assertAttributeEquals(ConsumeKinesis.ATTR_STREAM_NAME, TEST_STREAM_NAME);
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
        final List<UserRecord> records = List.of(
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
    void testDelimitedUsesLatestArrivalTimestamp() throws Exception {
        final Instant firstArrival = Instant.parse("2025-01-15T00:00:00Z");
        final Instant secondArrival = Instant.parse("2025-01-15T00:00:05Z");
        final Instant thirdArrival = Instant.parse("2025-01-15T00:00:03Z");
        final List<UserRecord> records = List.of(
                testRecord("1", "line-one", firstArrival),
                testRecord("2", "line-two", secondArrival),
                testRecord("3", "line-three", thirdArrival));

        triggerWithStrategy(records, "LINE_DELIMITED", "shardId-000000000001");

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals(ConsumeKinesis.ATTR_ARRIVAL_TIMESTAMP, String.valueOf(secondArrival.toEpochMilli()));
    }

    @Test
    void testMultipleShardsNoDataLoss() throws Exception {
        final ShardFetchResult shard1Result = new ShardFetchResult(FIRST_SHARD_ID,
                List.of(testRecord("10", FIRST_JSON_RECORD), testRecord("20", SECOND_JSON_RECORD)), FIRST_SHARD_BEHIND_MS);
        final ShardFetchResult shard2Result = new ShardFetchResult(SECOND_SHARD_ID,
                List.of(testRecord("30", THIRD_JSON_RECORD), testRecord("40", FOURTH_JSON_RECORD)), SECOND_SHARD_BEHIND_MS);

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
        assertEquals(Set.of(FIRST_SHARD_ID, SECOND_SHARD_ID), shardsSeen);
        assertEquals(4, totalRecords);

        final Map<String, String> firstAttributes = getMetricAttributes(FIRST_SHARD_ID);
        final Map<String, String> secondAttributes = getMetricAttributes(SECOND_SHARD_ID);
        final long firstShardBytes = payloadBytes(FIRST_JSON_RECORD) + payloadBytes(SECOND_JSON_RECORD);
        final long secondShardBytes = payloadBytes(THIRD_JSON_RECORD) + payloadBytes(FOURTH_JSON_RECORD);

        assertEquals(2L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), firstAttributes));
        assertEquals(2L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), secondAttributes));
        assertEquals(firstShardBytes, runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), firstAttributes));
        assertEquals(secondShardBytes, runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), secondAttributes));
        assertEquals(List.of((double) FIRST_SHARD_BEHIND_MS),
                runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), firstAttributes));
        assertEquals(List.of((double) SECOND_SHARD_BEHIND_MS),
                runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), secondAttributes));
    }

    @Test
    void testEmptyConsumeDoesNotRecordMetrics() throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, List.of());
        runner = TestRunners.newTestRunner(processor);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runner.setProperty(ConsumeKinesis.CONSUMER_TYPE, "SHARED_THROUGHPUT");

        runner.run();

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertNull(runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertNull(runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), attributes));
        assertTrue(runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), attributes).isEmpty());
        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 0);
    }

    @Test
    void testEmptyPollRecordsBehindGaugeWhenStillLagging() throws Exception {
        final Map<String, Long> shardLag = new LinkedHashMap<>();
        shardLag.put(DEFAULT_SHARD_ID, FLOW_FILE_BEHIND_MS);
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final LagReportingConsumeKinesis processor = new LagReportingConsumeKinesis(mockShardManager, shardLag);
        runner = TestRunners.newTestRunner(processor);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runner.setProperty(ConsumeKinesis.CONSUMER_TYPE, "SHARED_THROUGHPUT");

        runner.run(1, false, true);

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertNull(runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertNull(runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), attributes));
        assertEquals(List.of((double) FLOW_FILE_BEHIND_MS),
                runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), attributes));
        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 0);
    }

    @Test
    void testEmptyPollUpdatesBehindGaugeToCaughtUp() throws Exception {
        final List<UserRecord> records = List.of(
                testRecord("1", FIRST_FLOW_FILE_RECORD),
                testRecord("2", SECOND_FLOW_FILE_RECORD));
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final ShardFetchResult firstFetch = new ShardFetchResult(DEFAULT_SHARD_ID, records, FLOW_FILE_BEHIND_MS);
        final SequentialFetchConsumeKinesis processor = new SequentialFetchConsumeKinesis(mockShardManager, firstFetch, 0L);
        runner = TestRunners.newTestRunner(processor);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runner.setProperty(ConsumeKinesis.CONSUMER_TYPE, "SHARED_THROUGHPUT");

        runner.run(2, false, true);

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        final long expectedBytes = payloadBytes(FIRST_FLOW_FILE_RECORD) + payloadBytes(SECOND_FLOW_FILE_RECORD);
        assertEquals(2L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(expectedBytes, runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), attributes));
        assertEquals(List.of((double) FLOW_FILE_BEHIND_MS, 0.0),
                runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), attributes));
        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 2);
    }

    @Test
    void testAbsentMillisBehindDoesNotRecordGauge() throws Exception {
        final List<UserRecord> records = List.of(testRecord("1", FIRST_FLOW_FILE_RECORD));
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final ShardFetchResult fetchResult = new ShardFetchResult(DEFAULT_SHARD_ID, records, -1L);
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, fetchResult);
        runner = TestRunners.newTestRunner(processor);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runner.setProperty(ConsumeKinesis.CONSUMER_TYPE, "SHARED_THROUGHPUT");

        runner.run();

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertEquals(1L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(payloadBytes(FIRST_FLOW_FILE_RECORD),
                runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), attributes));
        assertTrue(runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), attributes).isEmpty());
    }

    @Test
    void testFlowFileStrategyRecordsBytesAndBehindMetrics() throws Exception {
        final List<UserRecord> records = List.of(
                testRecord("1", FIRST_FLOW_FILE_RECORD),
                testRecord("2", SECOND_FLOW_FILE_RECORD));
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final ShardFetchResult fetchResult = new ShardFetchResult(DEFAULT_SHARD_ID, records, FLOW_FILE_BEHIND_MS);
        final TestableConsumeKinesis processor = new TestableConsumeKinesis(mockShardManager, fetchResult);
        runner = TestRunners.newTestRunner(processor);

        setCommonProperties();
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runner.setProperty(ConsumeKinesis.CONSUMER_TYPE, "SHARED_THROUGHPUT");

        runner.run();

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 2);

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        final long expectedBytes = payloadBytes(FIRST_FLOW_FILE_RECORD) + payloadBytes(SECOND_FLOW_FILE_RECORD);
        assertEquals(2L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(expectedBytes, runner.getCounterValue(KinesisMetricName.BYTES_CONSUMED.getMetricName(), attributes));
        assertEquals(2L, runner.getCounterValue("Records Consumed"));
        assertEquals(List.of((double) FLOW_FILE_BEHIND_MS),
                runner.getGaugeValues(KinesisMetricName.CONSUMER_MILLISECONDS_BEHIND.getMetricName(), attributes));
    }

    @Test
    void testRecordMetadataInjectionPreservesRecordCount() throws Exception {
        final List<UserRecord> records = List.of(
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
        final List<UserRecord> records = List.of(
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
        final UserRecord emptyRecord = new UserRecord("shardId-000000000001", "2", 0, "pk-2", new byte[0], Instant.now());

        final List<UserRecord> records = List.of(
                testRecord("1", "{\"name\":\"Alice\"}"),
                emptyRecord,
                testRecord("3", "{\"name\":\"Charlie\"}"));

        triggerWithRecords(records);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "2");
    }

    private void triggerWithRecords(final List<UserRecord> records) throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final ShardFetchResult fetchResult = new ShardFetchResult(DEFAULT_SHARD_ID, records, 0L);
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
            final UserRecord... records) throws Exception {
        triggerWithRecords(List.of(records));

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 1);

        final MockFlowFile success = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).getFirst();
        success.assertAttributeEquals("record.count", "2");

        final MockFlowFile failure = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE).getFirst();
        failure.assertContentEquals(expectedFailureContent);
        failure.assertAttributeEquals(ConsumeKinesis.ATTR_FIRST_SEQUENCE, expectedFailureSequence);
        assertNotNull(failure.getAttribute(ConsumeKinesis.ATTR_RECORD_ERROR_MESSAGE));

        final Map<String, String> attributes = getMetricAttributes(DEFAULT_SHARD_ID);
        assertEquals(3L, runner.getCounterValue(KinesisMetricName.RECORDS_CONSUMED.getMetricName(), attributes));
        assertEquals(1L, runner.getCounterValue(KinesisMetricName.RECORDS_PARSED_ERRORS.getMetricName(), attributes));
    }

    private void triggerWithOutputStrategy(final List<UserRecord> records, final String outputStrategy) throws Exception {
        final KinesisShardManager mockShardManager = buildShardManager(DEFAULT_SHARD_ID);
        final ShardFetchResult fetchResult = new ShardFetchResult(DEFAULT_SHARD_ID, records, 0L);
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

    private void triggerWithStrategy(final List<UserRecord> records, final String processingStrategy,
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

    private static UserRecord testRecord(final String sequenceNumber, final String data) {
        return testRecord(sequenceNumber, data, Instant.now());
    }

    private static UserRecord testRecord(final String sequenceNumber, final String data, final Instant arrivalTimestamp) {
        return new UserRecord(
                DEFAULT_SHARD_ID,
                sequenceNumber,
                0,
                "pk-" + sequenceNumber,
                data.getBytes(StandardCharsets.UTF_8),
                arrivalTimestamp);
    }

    private static Map<String, String> getMetricAttributes(final String shardId) {
        return Map.of(
                ConsumeKinesis.ATTR_STREAM_NAME, TEST_STREAM_NAME,
                ConsumeKinesis.ATTR_SHARD_ID, shardId
        );
    }

    private static long payloadBytes(final String payload) {
        return payload.getBytes(StandardCharsets.UTF_8).length;
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

    static class SequentialFetchConsumeKinesis extends ConsumeKinesis {
        private final KinesisShardManager mockShardManager;
        private final ShardFetchResult firstFetch;
        private final long emptyPollMillisBehind;

        SequentialFetchConsumeKinesis(final KinesisShardManager mockShardManager, final ShardFetchResult firstFetch,
                final long emptyPollMillisBehind) {
            this.mockShardManager = mockShardManager;
            this.firstFetch = firstFetch;
            this.emptyPollMillisBehind = emptyPollMillisBehind;
        }

        @Override
        protected KinesisShardManager createShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient,
                final ComponentLog logger, final String checkpointTableName, final String streamName) {
            return mockShardManager;
        }

        @Override
        protected KinesisConsumerClient createConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger,
                final boolean efoMode) {
            return new SequentialFetchConsumerClient(mock(KinesisClient.class), logger, firstFetch, emptyPollMillisBehind);
        }
    }

    static class SequentialFetchConsumerClient extends StubConsumerClient {
        private final ShardFetchResult firstFetch;
        private final long emptyPollMillisBehind;
        private int fetchCycles;

        SequentialFetchConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger,
                final ShardFetchResult firstFetch, final long emptyPollMillisBehind) {
            super(kinesisClient, logger);
            this.firstFetch = firstFetch;
            this.emptyPollMillisBehind = emptyPollMillisBehind;
        }

        @Override
        void startFetches(final List<Shard> shards, final String streamName, final int batchSize,
                final String initialStreamPosition, final KinesisShardManager shardManager) {
            if (fetchCycles++ == 0) {
                enqueueResult(firstFetch);
                recordShardLag(firstFetch.shardId(), firstFetch.millisBehindLatest());
            } else {
                recordShardLag(firstFetch.shardId(), emptyPollMillisBehind);
            }
        }
    }

    static class LagReportingConsumeKinesis extends ConsumeKinesis {
        private final KinesisShardManager mockShardManager;
        private final Map<String, Long> shardLag;

        LagReportingConsumeKinesis(final KinesisShardManager mockShardManager, final Map<String, Long> shardLag) {
            this.mockShardManager = mockShardManager;
            this.shardLag = shardLag;
        }

        @Override
        protected KinesisShardManager createShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient,
                final ComponentLog logger, final String checkpointTableName, final String streamName) {
            return mockShardManager;
        }

        @Override
        protected KinesisConsumerClient createConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger,
                final boolean efoMode) {
            return new LagReportingConsumerClient(mock(KinesisClient.class), logger, shardLag);
        }
    }

    static class LagReportingConsumerClient extends StubConsumerClient {
        private final Map<String, Long> shardLag;

        LagReportingConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger, final Map<String, Long> shardLag) {
            super(kinesisClient, logger);
            this.shardLag = shardLag;
        }

        @Override
        void startFetches(final List<Shard> shards, final String streamName, final int batchSize,
                final String initialStreamPosition, final KinesisShardManager shardManager) {
            for (final Map.Entry<String, Long> entry : shardLag.entrySet()) {
                recordShardLag(entry.getKey(), entry.getValue());
            }
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
