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

import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.kinesis.retrieval.kpl.Messages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@EnabledIfDockerAvailable
class ConsumeKinesisIT {

    @Container
    private static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:4"));

    private static final String AVRO_SCHEMA_A = """
            {
              "type": "record",
              "name": "A",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
              ]
            }""";

    private static final String AVRO_SCHEMA_B = """
            {
              "type": "record",
              "name": "B",
              "fields": [
                {"name": "code", "type": "string"},
                {"name": "value", "type": "double"}
              ]
            }""";

    private TestRunner runner;
    private KinesisClient kinesisClient;
    private int credentialServiceCounter = 0;

    @BeforeEach
    void setUp() throws Exception {
        kinesisClient = KinesisClient.builder()
                .endpointOverride(LOCALSTACK.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .build();

        runner = TestRunners.newTestRunner(FastTimingConsumeKinesis.class);

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("json-reader", reader);
        runner.enableControllerService(reader);

        final JsonRecordSetWriter normalWriter = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", normalWriter);
        runner.enableControllerService(normalWriter);

        final FailingRecordSetWriterFactory failingWriter = new FailingRecordSetWriterFactory();
        runner.addControllerService("failing-writer", failingWriter);
        runner.enableControllerService(failingWriter);

        addCredentialService(runner, "creds");
        runner.setProperty(ConsumeKinesis.APPLICATION_NAME, "test-app-" + System.currentTimeMillis());
        runner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "creds");
        runner.setProperty(RegionUtil.REGION, LOCALSTACK.getRegion());
        runner.setProperty(ConsumeKinesis.ENDPOINT_OVERRIDE, LOCALSTACK.getEndpoint().toString());
        runner.setProperty(ConsumeKinesis.MAX_BATCH_DURATION, "200 ms");
    }

    @AfterEach
    void tearDown() {
        if (kinesisClient != null) {
            kinesisClient.close();
        }
    }

    @Test
    void testFlowFilePerRecordStrategy() throws Exception {
        final String streamName = "per-record-test";
        final int recordCount = 5;

        createStream(streamName);
        publishRecords(streamName, recordCount);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertEquals(recordCount, flowFiles.size(), "Expected one FlowFile per Kinesis record");

        for (final MockFlowFile ff : flowFiles) {
            assertEquals("1", ff.getAttribute("record.count"));
            assertEquals(streamName, ff.getAttribute(ConsumeKinesis.ATTR_STREAM_NAME));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_SHARD_ID));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_FIRST_SEQUENCE));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_LAST_SEQUENCE));
            assertEquals(ff.getAttribute(ConsumeKinesis.ATTR_FIRST_SEQUENCE), ff.getAttribute(ConsumeKinesis.ATTR_LAST_SEQUENCE));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_PARTITION_KEY));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_FIRST_SUBSEQUENCE));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_LAST_SUBSEQUENCE));

            final String content = ff.getContent();
            assertTrue(content.startsWith("{"), "Expected raw JSON content: " + content);
        }

        final Set<String> emittedShardIds = flowFiles.stream()
                .map(ff -> ff.getAttribute(ConsumeKinesis.ATTR_SHARD_ID))
                .collect(Collectors.toSet());
        final List<ProvenanceEventRecord> receiveEvents = runner.getProvenanceEvents().stream()
                .filter(event -> "RECEIVE".equals(event.getEventType().name()))
                .toList();
        assertEquals(recordCount, receiveEvents.size(), "Expected one RECEIVE event per emitted FlowFile");
        for (final ProvenanceEventRecord receiveEvent : receiveEvents) {
            final String transitUri = receiveEvent.getTransitUri();
            assertNotNull(transitUri, "RECEIVE event should include a transit URI");
            assertTrue(emittedShardIds.stream().anyMatch(shardId -> transitUri.endsWith("/" + shardId)),
                    "RECEIVE transit URI should include one of the emitted shard IDs: " + transitUri);
        }

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter, "Records Consumed counter should be set");
        assertEquals(recordCount, counter.longValue());
    }

    @Test
    void testRecordOrientedStrategy() throws Exception {
        final String streamName = "record-oriented-test";
        final int recordCount = 5;

        createStream(streamName);
        publishRecords(streamName, recordCount);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty(), "Expected at least one FlowFile");

        long totalRecords = 0;
        for (final MockFlowFile ff : flowFiles) {
            totalRecords += Long.parseLong(ff.getAttribute("record.count"));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_STREAM_NAME));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_FIRST_SEQUENCE));
            assertNotNull(ff.getAttribute(ConsumeKinesis.ATTR_LAST_SEQUENCE));
        }
        assertEquals(recordCount, totalRecords, "Total record count across all FlowFiles");

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter, "Records Consumed counter should be set");
        assertEquals(recordCount, counter.longValue());
    }

    @Test
    void testRecordOrientedStrategyWithInjectedMetadata() throws Exception {
        final String streamName = "record-oriented-metadata-test";
        final int recordCount = 5;

        createStream(streamName);
        publishRecords(streamName, recordCount);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runner.setProperty(ConsumeKinesis.OUTPUT_STRATEGY, "INJECT_METADATA");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty(), "Expected at least one FlowFile");

        long totalRecords = 0;
        for (final MockFlowFile flowFile : flowFiles) {
            totalRecords += Long.parseLong(flowFile.getAttribute("record.count"));

            final String content = flowFile.getContent();
            assertTrue(content.contains("\"kinesisMetadata\""), "Expected injected kinesisMetadata object");
            assertTrue(content.contains("\"stream\":\"" + streamName + "\""), "Expected stream in injected metadata");
            assertTrue(content.contains("\"shardId\":\""), "Expected shardId in injected metadata");
            assertTrue(content.contains("\"sequenceNumber\":\""), "Expected sequenceNumber in injected metadata");
            assertTrue(content.contains("\"subSequenceNumber\":0"), "Expected default subSequenceNumber in injected metadata");
            assertTrue(content.contains("\"partitionKey\":\""), "Expected partitionKey in injected metadata");
        }

        assertEquals(recordCount, totalRecords, "Total record count across all FlowFiles");

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter, "Records Consumed counter should be set");
        assertEquals(recordCount, counter.longValue());
    }

    @Test
    void testDemarcatorStrategy() throws Exception {
        final String streamName = "demarcator-test";
        final int recordCount = 3;

        createStream(streamName);
        publishRecords(streamName, recordCount);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "LINE_DELIMITED");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty(), "Expected at least one FlowFile");

        final StringBuilder allContent = new StringBuilder();
        long totalRecords = 0;
        for (final MockFlowFile ff : flowFiles) {
            allContent.append(ff.getContent());
            totalRecords += Long.parseLong(ff.getAttribute("record.count"));
        }
        assertEquals(recordCount, totalRecords, "Total record count across all FlowFiles");

        final String[] lines = allContent.toString().split("\n");
        assertEquals(recordCount, lines.length, "Expected one line per Kinesis record");
        for (final String line : lines) {
            assertTrue(line.startsWith("{"), "Expected JSON content: " + line);
        }

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter, "Records Consumed counter should be set");
        assertEquals(recordCount, counter.longValue());
    }

    @Test
    void testDemarcatorStrategyWithCustomDelimiter() throws Exception {
        final String streamName = "custom-delim-test";
        final int recordCount = 3;
        final String delimiter = "|||";

        createStream(streamName);
        publishRecords(streamName, recordCount);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "DEMARCATOR");
        runner.setProperty(ConsumeKinesis.MESSAGE_DEMARCATOR, delimiter);
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty(), "Expected at least one FlowFile");

        final StringBuilder allContent = new StringBuilder();
        long totalRecords = 0;
        for (final MockFlowFile ff : flowFiles) {
            allContent.append(ff.getContent());
            totalRecords += Long.parseLong(ff.getAttribute("record.count"));
        }
        assertEquals(recordCount, totalRecords, "Total record count across all FlowFiles");

        final String[] parts = allContent.toString().split("\\|\\|\\|");
        assertEquals(recordCount, parts.length, "Expected records separated by custom delimiter");
        for (final String part : parts) {
            assertTrue(part.startsWith("{"), "Expected JSON content: " + part);
        }

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter, "Records Consumed counter should be set");
        assertEquals(recordCount, counter.longValue());
    }

    @Test
    void testFailedWriteRollsBackAndRecordsAreReConsumed() throws Exception {
        final String streamName = "rollback-test";
        final int recordCount = 5;

        createStream(streamName);
        publishRecords(streamName, recordCount);
        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");

        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "failing-writer");
        runUntilOutput(runner);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumeKinesis.REL_PARSE_FAILURE, 0);

        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty(), "Expected at least one FlowFile transferred to success");

        long totalRecords = 0;
        for (final MockFlowFile ff : flowFiles) {
            totalRecords += Long.parseLong(ff.getAttribute("record.count"));
        }
        assertEquals(recordCount, totalRecords, "All records should be re-consumed after rollback");

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter, "Records Consumed counter should be set");
        assertEquals(recordCount, counter.longValue());
    }

    @Test
    void testClusterSimulationDistributesShards() throws Exception {
        final String streamName = "cluster-sim-test";
        final int shardCount = 4;
        final String appName = "cluster-app-" + System.currentTimeMillis();

        createStream(streamName, shardCount);

        final TestRunner runner1 = createConfiguredRunner(streamName, appName);
        final TestRunner runner2 = createConfiguredRunner(streamName, appName);

        runner1.run(1, false, true);
        runner2.run(1, false, true);

        int recordId = 0;
        final long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            publishRecord(streamName, recordId++);
            Thread.sleep(10);

            runner1.run(1, false, false);
            runner2.run(1, false, false);

            if (hasFlowFiles(runner1) && hasFlowFiles(runner2)) {
                break;
            }
        }

        runner1.run(1, true, false);
        runner2.run(1, true, false);

        final List<MockFlowFile> flowFiles1 = runner1.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        final List<MockFlowFile> flowFiles2 = runner2.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);

        assertFalse(flowFiles1.isEmpty(), "Runner 1 should have received data");
        assertFalse(flowFiles2.isEmpty(), "Runner 2 should have received data");

        final Set<String> uniqueRecords = new HashSet<>();
        for (final MockFlowFile ff : flowFiles1) {
            uniqueRecords.add(ff.getContent());
        }
        for (final MockFlowFile ff : flowFiles2) {
            uniqueRecords.add(ff.getContent());
        }
        assertEquals(flowFiles1.size() + flowFiles2.size(), uniqueRecords.size(),
            "No duplicate records should be consumed");
    }

    @Test
    void testClusterScaleDownAndScaleUpRebalancesShards() throws Exception {
        final String streamName = "cluster-rebalance-test";
        final int shardCount = 6;
        final String appName = "cluster-rebalance-app-" + System.currentTimeMillis();

        createStream(streamName, shardCount);

        final TestRunner runner1 = createConfiguredRunner(streamName, appName);
        final TestRunner runner2 = createConfiguredRunner(streamName, appName);

        runner1.run(1, false, true);
        runner2.run(1, false, true);

        int recordId = 0;
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            publishRecord(streamName, recordId++);
            Thread.sleep(10);
            runner1.run(1, false, false);
            runner2.run(1, false, false);
            if (hasFlowFiles(runner1) && hasFlowFiles(runner2)) {
                break;
            }
        }
        assertFalse(runner1.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty(),
                "Runner 1 should receive data in stage one");
        assertFalse(runner2.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty(),
                "Runner 2 should receive data in stage one");

        final int runner1Checkpoint = runner1.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).size();
        runner2.run(1, true, false);

        deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            publishRecord(streamName, recordId++);
            Thread.sleep(10);
            runner1.run(1, false, false);

            final Set<String> shards = new HashSet<>();
            for (final MockFlowFile ff : getNewFlowFiles(runner1, runner1Checkpoint)) {
                shards.add(ff.getAttribute(ConsumeKinesis.ATTR_SHARD_ID));
            }
            if (shards.size() == shardCount) {
                break;
            }
        }

        final Set<String> stageTwoShards = new HashSet<>();
        for (final MockFlowFile ff : getNewFlowFiles(runner1, runner1Checkpoint)) {
            stageTwoShards.add(ff.getAttribute(ConsumeKinesis.ATTR_SHARD_ID));
        }
        assertEquals(shardCount, stageTwoShards.size(), "Single active runner should consume from all shards");

        final TestRunner runner3 = createConfiguredRunner(streamName, appName);
        final TestRunner runner4 = createConfiguredRunner(streamName, appName);
        final int runner1StageThreeCheckpoint = runner1.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).size();

        runner3.run(1, false, true);
        runner4.run(1, false, true);

        deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            publishRecord(streamName, recordId++);
            Thread.sleep(10);
            runner1.run(1, false, false);
            runner3.run(1, false, false);
            runner4.run(1, false, false);

            if (!getNewFlowFiles(runner1, runner1StageThreeCheckpoint).isEmpty()
                    && hasFlowFiles(runner3) && hasFlowFiles(runner4)) {
                break;
            }
        }

        runner1.run(1, true, false);
        runner3.run(1, true, false);
        runner4.run(1, true, false);

        assertFalse(getNewFlowFiles(runner1, runner1StageThreeCheckpoint).isEmpty(),
                "Runner 1 should receive data in stage three");
        assertFalse(runner3.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty(),
                "Runner 3 should receive data in stage three");
        assertFalse(runner4.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty(),
                "Runner 4 should receive data in stage three");
    }

    @Test
    void testKplAggregatedRecordsFlowFilePerRecord() throws Exception {
        final String streamName = "kpl-per-record-test";
        createStream(streamName);

        publishAggregatedRecord(streamName, "agg-pk-1",
                List.of("pk-a", "pk-b"),
                List.of("{\"id\":1,\"name\":\"Alice\"}", "{\"id\":2,\"name\":\"Bob\"}", "{\"id\":3,\"name\":\"Charlie\"}"),
                List.of(0, 1, 0));

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertEquals(3, flowFiles.size(), "Each sub-record in the aggregate should produce its own FlowFile");

        final Set<String> contents = new HashSet<>();
        for (final MockFlowFile ff : flowFiles) {
            assertEquals("1", ff.getAttribute("record.count"));
            assertEquals(streamName, ff.getAttribute(ConsumeKinesis.ATTR_STREAM_NAME));
            contents.add(ff.getContent());
        }

        assertTrue(contents.contains("{\"id\":1,\"name\":\"Alice\"}"));
        assertTrue(contents.contains("{\"id\":2,\"name\":\"Bob\"}"));
        assertTrue(contents.contains("{\"id\":3,\"name\":\"Charlie\"}"));

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter);
        assertEquals(3, counter.longValue());
    }

    @Test
    void testKplAggregatedRecordsRecordOrientedWithMetadata() throws Exception {
        final String streamName = "kpl-metadata-test";
        createStream(streamName);

        publishAggregatedRecord(streamName, "agg-pk-1",
                List.of("pk-inner"),
                List.of("{\"id\":10,\"name\":\"Alpha\"}", "{\"id\":20,\"name\":\"Beta\"}"),
                List.of(0, 0));

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runner.setProperty(ConsumeKinesis.OUTPUT_STRATEGY, "INJECT_METADATA");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty());

        long totalRecords = 0;
        for (final MockFlowFile ff : flowFiles) {
            totalRecords += Long.parseLong(ff.getAttribute("record.count"));
            final String content = ff.getContent();
            assertTrue(content.contains("\"kinesisMetadata\""), "Expected injected metadata");
            assertTrue(content.contains("\"stream\":\"" + streamName + "\""));
            assertTrue(content.contains("\"partitionKey\":\"pk-inner\""), "Expected inner partition key, not the outer one");
        }
        assertEquals(2, totalRecords);

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter);
        assertEquals(2, counter.longValue());
    }

    @Test
    void testKplAggregatedMixedWithPlainRecords() throws Exception {
        final String streamName = "kpl-mixed-test";
        createStream(streamName);

        publishRecords(streamName, 2);

        publishAggregatedRecord(streamName, "agg-pk",
                List.of("pk-agg"),
                List.of("{\"id\":100,\"name\":\"Aggregated-1\"}", "{\"id\":101,\"name\":\"Aggregated-2\"}", "{\"id\":102,\"name\":\"Aggregated-3\"}"),
                List.of(0, 0, 0));

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertEquals(5, flowFiles.size(), "2 plain + 3 deaggregated sub-records");

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter);
        assertEquals(5, counter.longValue());
    }

    @Test
    void testKplMultipleAggregatedRecords() throws Exception {
        final String streamName = "kpl-multi-agg-test";
        createStream(streamName);

        for (int batch = 0; batch < 3; batch++) {
            final List<String> payloads = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final int id = batch * 5 + i;
                payloads.add("{\"id\":" + id + ",\"name\":\"rec-" + id + "\"}");
            }
            publishAggregatedRecord(streamName, "agg-pk-" + batch,
                    List.of("pk-" + batch), payloads, payloads.stream().map(p -> 0).toList());
        }

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertEquals(15, flowFiles.size(), "3 aggregated records x 5 sub-records each");

        final Set<String> contents = new HashSet<>();
        for (final MockFlowFile ff : flowFiles) {
            contents.add(ff.getContent());
        }
        for (int i = 0; i < 15; i++) {
            assertTrue(contents.contains("{\"id\":" + i + ",\"name\":\"rec-" + i + "\"}"),
                    "Missing deaggregated record with id=" + i);
        }

        final Long counter = runner.getCounterValue("Records Consumed");
        assertNotNull(counter);
        assertEquals(15, counter.longValue());
    }

    @Test
    void testAvroSameSchemaProducesSingleFlowFile() throws Exception {
        final String streamName = "avro-same-schema-test";
        createStream(streamName);

        final Schema schemaA = new Schema.Parser().parse(AVRO_SCHEMA_A);

        for (int i = 0; i < 4; i++) {
            final GenericRecord rec = new GenericData.Record(schemaA);
            rec.put("id", i);
            rec.put("name", "record-" + i);
            publishAvroRecord(streamName, "pk-" + i, schemaA, rec);
        }

        configureAvroRecordOriented(streamName);
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertFalse(flowFiles.isEmpty(), "Expected at least one FlowFile");

        long totalRecords = 0;
        for (final MockFlowFile ff : flowFiles) {
            totalRecords += Long.parseLong(ff.getAttribute("record.count"));
        }
        assertEquals(4, totalRecords, "All 4 same-schema Avro records should be consumed");
    }

    @Test
    void testAvroDifferentSchemasSplitFlowFiles() throws Exception {
        final String streamName = "avro-diff-schema-test";
        createStream(streamName);

        final Schema schemaA = new Schema.Parser().parse(AVRO_SCHEMA_A);
        final Schema schemaB = new Schema.Parser().parse(AVRO_SCHEMA_B);

        for (int i = 0; i < 2; i++) {
            final GenericRecord rec = new GenericData.Record(schemaA);
            rec.put("id", i);
            rec.put("name", "a-" + i);
            publishAvroRecord(streamName, "pk-a-" + i, schemaA, rec);
        }
        for (int i = 0; i < 2; i++) {
            final GenericRecord rec = new GenericData.Record(schemaB);
            rec.put("code", "code-" + i);
            rec.put("value", i * 1.5);
            publishAvroRecord(streamName, "pk-b-" + i, schemaB, rec);
        }

        configureAvroRecordOriented(streamName);
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertEquals(2, flowFiles.size(), "A,A,B,B should produce 2 FlowFiles");

        assertEquals("2", flowFiles.get(0).getAttribute("record.count"));
        assertEquals("2", flowFiles.get(1).getAttribute("record.count"));
    }

    @Test
    void testAvroInterleavedSchemasSplitFlowFiles() throws Exception {
        final String streamName = "avro-interleaved-test";
        createStream(streamName);

        final Schema schemaA = new Schema.Parser().parse(AVRO_SCHEMA_A);
        final Schema schemaB = new Schema.Parser().parse(AVRO_SCHEMA_B);

        for (int i = 0; i < 2; i++) {
            final GenericRecord rec = new GenericData.Record(schemaA);
            rec.put("id", i);
            rec.put("name", "a1-" + i);
            publishAvroRecord(streamName, "pk-a1-" + i, schemaA, rec);
        }
        for (int i = 0; i < 2; i++) {
            final GenericRecord rec = new GenericData.Record(schemaB);
            rec.put("code", "code-" + i);
            rec.put("value", i * 2.0);
            publishAvroRecord(streamName, "pk-b-" + i, schemaB, rec);
        }
        for (int i = 0; i < 2; i++) {
            final GenericRecord rec = new GenericData.Record(schemaA);
            rec.put("id", 100 + i);
            rec.put("name", "a2-" + i);
            publishAvroRecord(streamName, "pk-a2-" + i, schemaA, rec);
        }

        configureAvroRecordOriented(streamName);
        runUntilOutput(runner);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        assertEquals(3, flowFiles.size(), "A,A,B,B,A,A should produce 3 FlowFiles (no demux)");

        assertEquals("2", flowFiles.get(0).getAttribute("record.count"));
        assertEquals("2", flowFiles.get(1).getAttribute("record.count"));
        assertEquals("2", flowFiles.get(2).getAttribute("record.count"));
    }

    @Test
    void testAvroMixedWithCorruptDataRoutesToParseFailure() throws Exception {
        final String streamName = "avro-corrupt-mix-test";
        createStream(streamName);

        final Schema schemaA = new Schema.Parser().parse(AVRO_SCHEMA_A);
        final Schema schemaB = new Schema.Parser().parse(AVRO_SCHEMA_B);

        final GenericRecord recA = new GenericData.Record(schemaA);
        recA.put("id", 1);
        recA.put("name", "valid-a");
        publishAvroRecord(streamName, "pk-a", schemaA, recA);

        publishCorruptRecord(streamName, "pk-corrupt", "THIS_IS_NOT_AVRO_DATA");

        final GenericRecord recB = new GenericData.Record(schemaB);
        recB.put("code", "valid-b");
        recB.put("value", 3.14);
        publishAvroRecord(streamName, "pk-b", schemaB, recB);

        configureAvroRecordOriented(streamName);
        runUntilOutput(runner);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE);

        long totalSuccessRecords = 0;
        for (final MockFlowFile ff : successFlowFiles) {
            totalSuccessRecords += Long.parseLong(ff.getAttribute("record.count"));
        }
        assertEquals(2, totalSuccessRecords, "Both valid Avro records should be in success");
        assertEquals(1, failureFlowFiles.size(), "Corrupt record should be routed to parse failure");
        failureFlowFiles.getFirst().assertContentEquals("THIS_IS_NOT_AVRO_DATA");
    }

    @Test
    void testParseFailureWithSomeValidJsonRecords() throws Exception {
        final String streamName = "json-parse-failure-test";
        createStream(streamName);

        publishRecord(streamName, 0);
        publishCorruptRecord(streamName, "pk-bad-1", "CORRUPT_DATA_1");
        publishRecord(streamName, 1);
        publishCorruptRecord(streamName, "pk-bad-2", "CORRUPT_DATA_2");
        publishRecord(streamName, 2);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runUntilOutput(runner);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE);

        long totalSuccessRecords = 0;
        for (final MockFlowFile ff : successFlowFiles) {
            totalSuccessRecords += Long.parseLong(ff.getAttribute("record.count"));
        }
        assertEquals(3, totalSuccessRecords, "All valid JSON records should route to success");
        assertEquals(2, failureFlowFiles.size(), "Both corrupt records should route to parse failure");
    }

    @Test
    void testAllCorruptRecordsRouteToParseFailure() throws Exception {
        final String streamName = "all-corrupt-test";
        createStream(streamName);

        publishCorruptRecord(streamName, "pk-1", "NOT_VALID_DATA_1");
        publishCorruptRecord(streamName, "pk-2", "NOT_VALID_DATA_2");
        publishCorruptRecord(streamName, "pk-3", "NOT_VALID_DATA_3");

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");
        runUntilOutput(runner);

        runner.assertTransferCount(ConsumeKinesis.REL_SUCCESS, 0);
        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE);
        assertEquals(3, failureFlowFiles.size(), "All corrupt records should route to parse failure");
    }

    private void addCredentialService(final TestRunner testRunner, final String serviceId) throws Exception {
        final AWSCredentialsProviderControllerService credsSvc = new AWSCredentialsProviderControllerService();
        testRunner.addControllerService(serviceId, credsSvc);
        testRunner.setProperty(credsSvc, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, LOCALSTACK.getAccessKey());
        testRunner.setProperty(credsSvc, AWSCredentialsProviderControllerService.SECRET_KEY, LOCALSTACK.getSecretKey());
        testRunner.enableControllerService(credsSvc);
    }

    private TestRunner createConfiguredRunner(final String streamName, final String appName) throws Exception {
        final TestRunner configuredRunner = TestRunners.newTestRunner(FastTimingConsumeKinesis.class);

        final String credsId = "creds-" + (credentialServiceCounter++);
        addCredentialService(configuredRunner, credsId);

        configuredRunner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        configuredRunner.setProperty(ConsumeKinesis.APPLICATION_NAME, appName);
        configuredRunner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, credsId);
        configuredRunner.setProperty(RegionUtil.REGION, LOCALSTACK.getRegion());
        configuredRunner.setProperty(ConsumeKinesis.ENDPOINT_OVERRIDE, LOCALSTACK.getEndpoint().toString());
        configuredRunner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "FLOW_FILE");
        configuredRunner.setProperty(ConsumeKinesis.MAX_BATCH_DURATION, "200 ms");

        return configuredRunner;
    }

    /**
     * Runs the processor with retries until at least one FlowFile appears on any output relationship
     * (success or parse failure), or a 10-second deadline is reached. This guards against
     * timing-sensitive tests on slow systems where LocalStack may not propagate records within a
     * single 200ms batch window.
     */
    private void runUntilOutput(final TestRunner testRunner) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + 10_000;
        testRunner.run(1, false, true);
        while (!hasOutput(testRunner) && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
            testRunner.run(1, false, false);
        }
        testRunner.run(1, true, false);
    }

    private boolean hasOutput(final TestRunner testRunner) {
        return !testRunner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty()
                || !testRunner.getFlowFilesForRelationship(ConsumeKinesis.REL_PARSE_FAILURE).isEmpty();
    }

    private boolean hasFlowFiles(final TestRunner testRunner) {
        return !testRunner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS).isEmpty();
    }

    private List<MockFlowFile> getNewFlowFiles(final TestRunner testRunner, final int startIndex) {
        final List<MockFlowFile> allFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeKinesis.REL_SUCCESS);
        return new ArrayList<>(allFlowFiles.subList(startIndex, allFlowFiles.size()));
    }

    private void createStream(final String streamName) {
        createStream(streamName, 1);
    }

    private void createStream(final String streamName, final int shardCount) {
        final CreateStreamRequest request = CreateStreamRequest.builder()
                .streamName(streamName)
                .shardCount(shardCount)
                .build();
        kinesisClient.createStream(request);
        waitForStreamActive(streamName);
    }

    private void waitForStreamActive(final String streamName) {
        final DescribeStreamRequest request = DescribeStreamRequest.builder().streamName(streamName).build();
        for (int i = 0; i < 300; i++) {
            if (kinesisClient.describeStream(request).streamDescription().streamStatus() == StreamStatus.ACTIVE) {
                return;
            }

            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Stream " + streamName + " did not become ACTIVE within 30 seconds");
    }

    private void publishAggregatedRecord(final String streamName, final String outerPartitionKey,
            final List<String> partitionKeys, final List<String> jsonPayloads, final List<Integer> pkIndices) {
        final Messages.AggregatedRecord.Builder builder = Messages.AggregatedRecord.newBuilder();
        for (final String pk : partitionKeys) {
            builder.addPartitionKeyTable(pk);
        }
        for (int i = 0; i < jsonPayloads.size(); i++) {
            builder.addRecords(Messages.Record.newBuilder()
                    .setPartitionKeyIndex(pkIndices.get(i))
                    .setData(ByteString.copyFromUtf8(jsonPayloads.get(i))));
        }

        final byte[] protobufBytes = builder.build().toByteArray();
        final byte[] payload;
        try {
            final byte[] md5 = MessageDigest.getInstance("MD5").digest(protobufBytes);
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(KplDeaggregator.KPL_MAGIC);
            out.write(protobufBytes);
            out.write(md5);
            payload = out.toByteArray();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        kinesisClient.putRecord(PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(outerPartitionKey)
                .data(SdkBytes.fromByteArray(payload))
                .build());
    }

    private void publishRecords(final String streamName, final int count) {
        for (int i = 0; i < count; i++) {
            publishRecord(streamName, i);
        }
    }

    private void publishRecord(final String streamName, final int recordId) {
        final String json = """
                {"id": %d, "name": "record-%d"}""".formatted(recordId, recordId);
        kinesisClient.putRecord(PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey("key-" + recordId)
                .data(SdkBytes.fromUtf8String(json))
                .build());
    }

    private void publishCorruptRecord(final String streamName, final String partitionKey, final String corruptData) {
        kinesisClient.putRecord(PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromUtf8String(corruptData))
                .build());
    }

    private static byte[] serializeAvroContainer(final Schema schema, final GenericRecord... records) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
            try (final DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(datumWriter)) {
                fileWriter.create(schema, baos);
                for (final GenericRecord record : records) {
                    fileWriter.append(record);
                }
            }
            return baos.toByteArray();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void publishAvroRecord(final String streamName, final String partitionKey, final Schema schema,
            final GenericRecord record) {
        final byte[] avroBytes = serializeAvroContainer(schema, record);
        kinesisClient.putRecord(PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(avroBytes))
                .build());
    }

    private void configureAvroRecordOriented(final String streamName) throws Exception {
        final AvroReader avroReader = new AvroReader();
        runner.addControllerService("avro-reader", avroReader);
        runner.enableControllerService(avroReader);

        final AvroRecordSetWriter avroWriter = new AvroRecordSetWriter();
        runner.addControllerService("avro-writer", avroWriter);
        runner.enableControllerService(avroWriter);

        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, "RECORD");
        runner.setProperty(ConsumeKinesis.RECORD_READER, "avro-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "avro-writer");
    }

    static class FailingRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        @Override
        public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema,
                final OutputStream out, final Map<String, String> variables) {
            return new FailingRecordSetWriter();
        }
    }

    private static class FailingRecordSetWriter implements RecordSetWriter {
        @Override
        public WriteResult write(final Record record) throws IOException {
            throw new IOException("Simulated write failure for rollback testing");
        }

        @Override
        public WriteResult write(final RecordSet recordSet) throws IOException {
            throw new IOException("Simulated write failure for rollback testing");
        }

        @Override
        public void beginRecordSet() {
        }

        @Override
        public WriteResult finishRecordSet() {
            return WriteResult.of(0, Map.of());
        }

        @Override
        public String getMimeType() {
            return "application/json";
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    public static class FastTimingConsumeKinesis extends ConsumeKinesis {
        private static final long TEST_SHARD_CACHE_MILLIS = 500;
        private static final long TEST_LEASE_DURATION_MILLIS = 3_000;
        private static final long TEST_LEASE_REFRESH_INTERVAL_MILLIS = 1_000;
        private static final long TEST_NODE_HEARTBEAT_EXPIRATION_MILLIS = 4_000;

        @Override
        protected KinesisShardManager createShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient,
                final ComponentLog logger, final String checkpointTableName, final String streamName) {
            return new KinesisShardManager(
                    kinesisClient,
                    dynamoDbClient,
                    logger,
                    checkpointTableName,
                    streamName,
                    TEST_SHARD_CACHE_MILLIS,
                    TEST_LEASE_DURATION_MILLIS,
                    TEST_LEASE_REFRESH_INTERVAL_MILLIS,
                    TEST_NODE_HEARTBEAT_EXPIRATION_MILLIS);
        }
    }
}
