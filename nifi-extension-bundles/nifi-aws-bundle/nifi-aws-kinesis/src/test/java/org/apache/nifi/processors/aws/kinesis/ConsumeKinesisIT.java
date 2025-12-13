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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.REL_PARSE_FAILURE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.REL_SUCCESS;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_COUNT;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_ERROR_MESSAGE;
import static org.apache.nifi.processors.aws.kinesis.JsonRecordAssert.assertFlowFileRecordPayloads;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Timeout.ThreadMode.SEPARATE_THREAD;

/**
 * Tests run in parallel to optimize execution time as Kinesis consumer coordination takes a lot.
 */
@Execution(ExecutionMode.CONCURRENT)
@Timeout(value = 5, unit = MINUTES, threadMode = SEPARATE_THREAD)
class ConsumeKinesisIT {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeKinesisIT.class);
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:4.12.0");

    private static final LocalStackContainer localstack = new LocalStackContainer(LOCALSTACK_IMAGE).withServices("kinesis", "dynamodb", "cloudwatch");

    private static KinesisClient kinesisClient;
    private static DynamoDbClient dynamoDbClient;

    private String streamName;
    private String applicationName;
    private TestRunner runner;
    private TestKinesisStreamClient streamClient;

    @BeforeAll
    static void oneTimeSetup() {
        localstack.start();

        final AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
        );

        kinesisClient = KinesisClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(credentialsProvider)
                .region(Region.of(localstack.getRegion()))
                .build();

        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(credentialsProvider)
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    @AfterAll
    static void tearDown() {
        if (kinesisClient != null) {
            kinesisClient.close();
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        localstack.stop();
    }

    @BeforeEach
    void setUp() throws InitializationException {
        final UUID testId = UUID.randomUUID();
        streamName = "%s-kinesis-stream-%s".formatted(getClass().getSimpleName(), testId);
        streamClient = new TestKinesisStreamClient(kinesisClient, streamName);
        applicationName = "%s-test-kinesis-app-%s".formatted(getClass().getSimpleName(), testId);
        runner = createTestRunner(streamName, applicationName);
    }

    @AfterEach
    void tearDownEach() {
        runner.stop();

        if (streamClient != null) {
            try {
                streamClient.deleteStream();
            } catch (final Exception e) {
                logger.warn("Failed to delete stream {}: {}", streamName, e.getMessage());
            }
        }

        // Removing tables generated by KCL.
        deleteTable(applicationName);
        deleteTable(applicationName + "-CoordinatorState");
        deleteTable(applicationName + "-WorkerMetricStats");

    }

    private void deleteTable(final String tableName) {
        try {
            dynamoDbClient.deleteTable(req -> req.tableName(tableName));
        } catch (final Exception e) {
            logger.warn("Failed to delete DynamoDB table {}: {}", tableName, e.getMessage());
        }
    }

    @Test
    void testConsumeSingleMessageFromSingleShard() {
        streamClient.createStream(1);

        final String testMessage = "Hello, Kinesis!";
        streamClient.putRecord("test-partition-key", testMessage);

        runProcessorWithInitAndWaitForFiles(runner, 1);

        runner.assertTransferCount(REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals(testMessage);
        flowFile.assertAttributeEquals("aws.kinesis.partition.key", "test-partition-key");
        assertNotNull(flowFile.getAttribute("aws.kinesis.first.sequence.number"));
        assertNotNull(flowFile.getAttribute("aws.kinesis.last.sequence.number"));
        assertNotNull(flowFile.getAttribute("aws.kinesis.shard.id"));

        assertReceiveProvenanceEvents(runner.getProvenanceEvents(), flowFile);

        // Creates an enhanced fan-out consumer by default.
        assertEquals(
                List.of(applicationName),
                streamClient.getEnhancedFanOutConsumerNames(),
                "Expected a single enhanced fan-out consumer with an application name");
    }

    @Test
    void testConsumeSingleMessageFromSingleShard_withoutEnhancedFanOut() {
        runner.setProperty(ConsumeKinesis.CONSUMER_TYPE, ConsumeKinesis.ConsumerType.SHARED_THROUGHPUT);

        streamClient.createStream(1);

        final String testMessage = "Hello, Kinesis!";
        streamClient.putRecord("test-partition-key", testMessage);

        runProcessorWithInitAndWaitForFiles(runner, 1);

        runner.assertTransferCount(REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile flowFile = flowFiles.getFirst();

        flowFile.assertContentEquals(testMessage);
        flowFile.assertAttributeEquals("aws.kinesis.partition.key", "test-partition-key");
        assertNotNull(flowFile.getAttribute("aws.kinesis.first.sequence.number"));
        assertNotNull(flowFile.getAttribute("aws.kinesis.last.sequence.number"));
        assertNotNull(flowFile.getAttribute("aws.kinesis.shard.id"));

        assertReceiveProvenanceEvents(runner.getProvenanceEvents(), flowFile);

        assertTrue(
                streamClient.getEnhancedFanOutConsumerNames().isEmpty(),
                "No enhanced fan-out consumers should be created for Shared Throughput consumer type");
    }

    @Test
    void testConsumeManyMessagesFromSingleShardWithOrdering() {
        final int messageCount = 10;

        streamClient.createStream(1);

        final List<String> messages = IntStream.range(0, messageCount).mapToObj(i -> "Message-" + i).toList();
        streamClient.putRecords("partition-key", messages);

        runProcessorWithInitAndWaitForFiles(runner, messageCount);

        runner.assertTransferCount(REL_SUCCESS, messageCount);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final List<String> flowFilesContent = flowFiles.stream().map(MockFlowFile::getContent).toList();

        assertEquals(messages, flowFilesContent);

        assertReceiveProvenanceEvents(runner.getProvenanceEvents(), flowFiles);
    }

    @Test
    void testConsumeMessagesFromMultipleShardsStream() {
        final int shardCount = 5;
        final int messagesPerPartitionKey = 5;

        streamClient.createStream(shardCount);

        // Every shard has message with the same payload.
        final List<String> shardMessages = IntStream.range(0, messagesPerPartitionKey).mapToObj(String::valueOf).toList();

        IntStream.range(0, shardCount).forEach(shard -> streamClient.putRecords(String.valueOf(shard), shardMessages));

        // Run processor and wait for all records
        final int totalMessages = shardCount * messagesPerPartitionKey;
        runProcessorWithInitAndWaitForFiles(runner, totalMessages);

        // Verify results
        runner.assertTransferCount(REL_SUCCESS, totalMessages);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        final Map<String, List<String>> partitionKey2Messages = flowFiles.stream()
                .collect(groupingBy(
                        f -> f.getAttribute("aws.kinesis.partition.key"),
                        mapping(MockFlowFile::getContent, toList())
                ));

        assertEquals(shardCount, partitionKey2Messages.size());

        assertAll(
                partitionKey2Messages.values().stream()
                        .map(actual -> () -> assertEquals(shardMessages, actual))
        );

        assertReceiveProvenanceEvents(runner.getProvenanceEvents(), flowFiles);
    }

    @Test
    @Disabled("Does not work with LocalStack: https://github.com/localstack/localstack/issues/12833. Enable only when using real Kinesis.")
    void testResharding_inParallelWithConsumption() {
        // Using partition keys with uniformally distributed hashes to ensure the data is distributed across split shards.
        final List<String> partitionKeys = List.of(
                "pk-0-14", // 035517ff4ca68849589f43842c07362f
                "pk-1-14", // 5f045ae51eea9bd124d76041a6a27073
                "pk-2-2",  // 85fb9a2b01b009904eb8a6fa13a21d6c
                "pk-3-2"   // dbf24a6e26910143c60188e2fcb53b4f
        );

        // Data to be produced at each stage
        final int partitionRecordsPerStage = 3;
        final int totalStages = 5; // initial + 4 resharding stages with data
        final int totalRecordsPerPartition = partitionRecordsPerStage * totalStages;
        final int totalRecords = partitionKeys.size() * totalRecordsPerPartition;

        // Start resharding and data production operations in background thread.
        final Thread reshardingThread = new Thread(() -> {
            int messageSeq = 0; // For each partition key the message content are sequential numbers.

            streamClient.createStream(1);
            putRecords(partitionKeys, partitionRecordsPerStage, messageSeq);
            messageSeq += partitionRecordsPerStage;

            streamClient.reshardStream(2);
            putRecords(partitionKeys, partitionRecordsPerStage, messageSeq);
            messageSeq += partitionRecordsPerStage;

            streamClient.reshardStream(4);
            putRecords(partitionKeys, partitionRecordsPerStage, messageSeq);
            messageSeq += partitionRecordsPerStage;

            streamClient.reshardStream(3);
            putRecords(partitionKeys, partitionRecordsPerStage, messageSeq);
            messageSeq += partitionRecordsPerStage;

            streamClient.reshardStream(2);
            putRecords(partitionKeys, partitionRecordsPerStage, messageSeq);
        });

        reshardingThread.start();

        runProcessorWithInitAndWaitForFiles(runner, totalRecords);

        runner.assertTransferCount(REL_SUCCESS, totalRecords);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);

        final Map<String, List<String>> partitionKeyToMessages = flowFiles.stream()
                .collect(groupingBy(
                        f -> f.getAttribute("aws.kinesis.partition.key"),
                        mapping(MockFlowFile::getContent, toList())
                ));

        final List<String> expectedPartitionMessages = IntStream.range(0, totalRecordsPerPartition).mapToObj(Integer::toString).toList();
        assertAll(
                partitionKeyToMessages.entrySet().stream()
                        .map(actual -> () -> assertEquals(
                                expectedPartitionMessages,
                                actual.getValue(),
                                "Partition messages do not match expected for partition key: " + actual.getKey()))
        );
    }

    private void putRecords(final Collection<String> partitionKeys, final int count, final int startIndex) {
        IntStream.range(startIndex, startIndex + count).forEach(i -> {
            final String message = Integer.toString(i);
            partitionKeys.forEach(partitionKey -> streamClient.putRecord(partitionKey, message));
        });
    }

    @Test
    void testSessionRollback() throws InterruptedException {
        streamClient.createStream(1);

        // Initialize the processor.
        runner.run(1, false, true);

        final ConsumeKinesis processor = (ConsumeKinesis) runner.getProcessor();

        final String firstMessage = "Initial-Rollback-Message";
        streamClient.putRecord("key", firstMessage);

        // First attempt with a failing session - should rollback.
        while (true) {
            final MockProcessSession failingSession = createFailingSession(processor);
            try {
                processor.onTrigger(runner.getProcessContext(), failingSession);
            } catch (final FlowFileHandlingException __) {
                failingSession.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
                break; // Expected rollback occurred
            }
            Thread.sleep(1000);
        }

        // Write another message.
        final String secondMessage = "Another-Test-Message";
        streamClient.putRecord("key", secondMessage);

        runProcessorAndWaitForFiles(runner, 2);

        // Verify the messages are written in the correct order.
        runner.assertTransferCount(REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        flowFiles.getFirst().assertContentEquals(firstMessage);
        flowFiles.getLast().assertContentEquals(secondMessage);

        assertReceiveProvenanceEvents(runner.getProvenanceEvents(), flowFiles.getFirst(), flowFiles.getLast());
    }

    @Test
    void testRecordProcessingWithSchemaChangesAndInvalidRecords() throws InitializationException {
        streamClient.createStream(1);

        final TestRunner recordRunner = createRecordTestRunner(streamName, applicationName);

        final List<String> testRecords = List.of(
                "{\"name\":\"John\",\"age\":30}", // Schema A
                "{\"name\":\"Jane\",\"age\":25}", // Schema A
                "{invalid json}",
                "{\"id\":\"123\",\"value\":\"test\"}" // Schema B
        );

        testRecords.forEach(record -> streamClient.putRecord("key", record));

        runProcessorWithInitAndWaitForFiles(recordRunner, 3);

        // Verify successful records.
        recordRunner.assertTransferCount(REL_SUCCESS, 2);
        final List<MockFlowFile> successFlowFiles = recordRunner.getFlowFilesForRelationship(REL_SUCCESS);

        final MockFlowFile firstFlowFile = successFlowFiles.getFirst();
        assertEquals("2", firstFlowFile.getAttribute(RECORD_COUNT));
        assertFlowFileRecordPayloads(firstFlowFile, testRecords.getFirst(), testRecords.get(1));

        final MockFlowFile secondFlowFile = successFlowFiles.get(1);
        assertEquals("1", secondFlowFile.getAttribute(RECORD_COUNT));
        assertFlowFileRecordPayloads(secondFlowFile, testRecords.getLast());

        // Verify failure record.
        recordRunner.assertTransferCount(REL_PARSE_FAILURE, 1);
        final List<MockFlowFile> parseFailureFlowFiles = recordRunner.getFlowFilesForRelationship(REL_PARSE_FAILURE);
        final MockFlowFile parseFailureFlowFile = parseFailureFlowFiles.getFirst();

        parseFailureFlowFile.assertContentEquals(testRecords.get(2));
        assertNotNull(parseFailureFlowFile.getAttribute(RECORD_ERROR_MESSAGE));

        // Verify provenance events.
        assertReceiveProvenanceEvents(recordRunner.getProvenanceEvents(), firstFlowFile, secondFlowFile, parseFailureFlowFile);
    }

    private static void assertReceiveProvenanceEvents(final List<ProvenanceEventRecord> actualEvents, final FlowFile... expectedFlowFiles) {
        assertReceiveProvenanceEvents(actualEvents, List.of(expectedFlowFiles));
    }

    private static void assertReceiveProvenanceEvents(final List<ProvenanceEventRecord> actualEvents, final Collection<? extends FlowFile> expectedFlowFiles) {
        assertEquals(expectedFlowFiles.size(), actualEvents.size(), "Each produced FlowFile must have a provenance event");

        assertAll(
                actualEvents.stream().map(event -> () ->
                        assertEquals(ProvenanceEventType.RECEIVE, event.getEventType(), "Unexpected Provenance Event Type"))
        );

        final Set<String> eventFlowFileUuids = actualEvents.stream()
                .map(ProvenanceEventRecord::getFlowFileUuid)
                .collect(toSet());

        assertAll(
                expectedFlowFiles.stream()
                        .map(flowFile -> flowFile.getAttribute(CoreAttributes.UUID.key()))
                        .map(uuid -> () ->
                            assertTrue(eventFlowFileUuids.contains(uuid), "Expected Provenance Event for FlowFile UUID: %s was not present".formatted(uuid)))
        );
    }

    private TestRunner createTestRunner(final String streamName, final String applicationName) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestConsumeKinesis.class);

        final AWSCredentialsProviderControllerService credentialsService = new AWSCredentialsProviderControllerService();
        runner.addControllerService("credentials", credentialsService);
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.ACCESS_KEY_ID, localstack.getAccessKey());
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerService.SECRET_KEY, localstack.getSecretKey());
        runner.enableControllerService(credentialsService);

        runner.setProperty(ConsumeKinesis.AWS_CREDENTIALS_PROVIDER_SERVICE, "credentials");
        runner.setProperty(ConsumeKinesis.STREAM_NAME, streamName);
        runner.setProperty(ConsumeKinesis.APPLICATION_NAME, applicationName);
        runner.setProperty(RegionUtil.REGION, localstack.getRegion());
        runner.setProperty(ConsumeKinesis.INITIAL_STREAM_POSITION, ConsumeKinesis.InitialPosition.TRIM_HORIZON);
        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, ConsumeKinesis.ProcessingStrategy.FLOW_FILE);

        runner.setProperty(ConsumeKinesis.METRICS_PUBLISHING, ConsumeKinesis.MetricsPublishing.CLOUDWATCH);

        runner.setProperty(ConsumeKinesis.MAX_BYTES_TO_BUFFER, "10 MB");

        runner.assertValid();
        return runner;
    }

    private TestRunner createRecordTestRunner(final String streamName, final String applicationName) throws InitializationException {
        final TestRunner runner = createTestRunner(streamName, applicationName);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA.getValue());
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA.getValue());
        runner.enableControllerService(jsonWriter);

        runner.setProperty(ConsumeKinesis.PROCESSING_STRATEGY, ConsumeKinesis.ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeKinesis.RECORD_READER, "json-reader");
        runner.setProperty(ConsumeKinesis.RECORD_WRITER, "json-writer");

        runner.assertValid();
        return runner;
    }

    private void runProcessorWithInitAndWaitForFiles(final TestRunner runner, final int expectedFlowFileCount) {
        runProcessorAndWaitForFiles(runner, expectedFlowFileCount, true);
    }

    private void runProcessorAndWaitForFiles(final TestRunner runner, final int expectedFlowFileCount) {
        runProcessorAndWaitForFiles(runner, expectedFlowFileCount, false);
    }

    private void runProcessorAndWaitForFiles(final TestRunner runner, final int expectedFlowFileCount, final boolean withInit) {
        logger.info("Running processor and waiting for {} files", expectedFlowFileCount);

        if (withInit) {
            runner.run(1, false, true);
        }

        final Set<Relationship> relationships = runner.getProcessor().getRelationships();

        while (true) {
            runner.run(1, false, false);

            final int currentCount = relationships.stream()
                    .map(runner::getFlowFilesForRelationship)
                    .mapToInt(Collection::size)
                    .sum();
            logger.info("Current files count: {}, expected: {}", currentCount, expectedFlowFileCount);

            if (currentCount >= expectedFlowFileCount) {
                return;
            }

            try {
                Thread.sleep(5_000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread interrupted while waiting for files", e);
            }
        }
    }

    private MockProcessSession createFailingSession(final Processor processor) {
        final SharedSessionState sharedState = new SharedSessionState(processor, new AtomicLong());

        return MockProcessSession.builder(sharedState, processor)
                .failCommit()
                .build();
    }

    public static class TestConsumeKinesis extends ConsumeKinesis {

        @Override
        URI getKinesisEndpointOverride() {
            return localstack.getEndpoint();
        }

        @Override
        URI getDynamoDbEndpointOverride() {
            return localstack.getEndpoint();
        }

        @Override
        URI getCloudwatchEndpointOverride() {
            return localstack.getEndpoint();
        }
    }

    /**
     * Test client wrapper for Kinesis operations with built-in retry logic and error handling.
     */
    private static class TestKinesisStreamClient {
        private static final Logger logger = LoggerFactory.getLogger(TestKinesisStreamClient.class);

        private static final int MAX_RETRIES = 10;
        private static final long INITIAL_RETRY_DELAY_MILLIS = 1_000;
        private static final long MAX_RETRY_DELAY_MILLIS = 60 * 1_000;

        private static final Duration STREAM_WAIT_TIMEOUT = Duration.ofMinutes(2);

        private final KinesisClient kinesisClient;
        private final String streamName;

        TestKinesisStreamClient(KinesisClient kinesisClient, String streamName) {
            this.kinesisClient = kinesisClient;
            this.streamName = streamName;
        }

        void createStream(final int shardCount) {
            logger.info("Creating stream: {} with {} shards", streamName, shardCount);

            executeWithRetry(
                    "createStream",
                    () -> kinesisClient.createStream(req -> req.streamName(streamName).shardCount(shardCount)));

            waitForStreamActive();
            logger.info("Stream {} is now active", streamName);
        }

        List<String> getEnhancedFanOutConsumerNames() {
            final String arn = describeStream().streamARN();

            final ListStreamConsumersResponse response = executeWithRetry(
                    "listStreamConsumers",
                    () -> kinesisClient.listStreamConsumers(req -> req.streamARN(arn))
            );

            return response.consumers().stream()
                    .map(Consumer::consumerName)
                    .toList();
        }

        private StreamDescription describeStream() {
            final DescribeStreamResponse response = executeWithRetry(
                    "describeStream",
                    () -> kinesisClient.describeStream(req -> req.streamName(streamName))
            );

            return response.streamDescription();
        }

        void deleteStream() {
            logger.info("Deleting stream: {}", streamName);

            executeWithRetry(
                    "deleteStream",
                    () -> kinesisClient.deleteStream(req -> req.streamName(streamName).enforceConsumerDeletion(true)));
        }

        void putRecord(final String partitionKey, final String data) {
            final SdkBytes bytes = SdkBytes.fromString(data, UTF_8);

            executeWithRetry(
                    "putRecord",
                    () -> kinesisClient.putRecord(req -> req.streamName(streamName).partitionKey(partitionKey).data(bytes)));
        }

        void putRecords(final String partitionKey, final List<String> data) {
            final List<PutRecordsRequestEntry> records = data.stream()
                    .map(it -> PutRecordsRequestEntry.builder()
                            .data(SdkBytes.fromString(it, UTF_8))
                            .partitionKey(partitionKey)
                            .build())
                    .toList();

            executeWithRetry(
                    "putRecords",
                    () -> kinesisClient.putRecords(req -> req.streamName(streamName).records(records)));
        }

        /**
         * Adjusts a number of shards for the stream.
         * <b>Note: in order to ensure new shards become active, the method waits for 30 seconds.</b>
         */
        void reshardStream(final int targetShardCount) {
            logger.info("Resharding stream {} to {} shards", streamName, targetShardCount);

            executeWithRetry(
                    "reshardStream",
                    () -> kinesisClient.updateShardCount(req -> req.streamName(streamName).targetShardCount(targetShardCount).scalingType(ScalingType.UNIFORM_SCALING)));

            waitForStreamActive();

            try {
                // After resharding new messages can still be put into the older shards for some time, so we wait a bit.
                Thread.sleep(30_000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            logger.info("Stream {} resharding completed", streamName);
        }

        private void waitForStreamActive() {
            final long timeoutMillis = System.currentTimeMillis() + STREAM_WAIT_TIMEOUT.toMillis();

            while (System.currentTimeMillis() < timeoutMillis) {
                try {
                    final StreamStatus status = describeStream().streamStatus();
                    if (status == StreamStatus.ACTIVE) {
                        return;
                    }

                    logger.info("Stream {} status: {}, waiting...", streamName, status);
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Thread interrupted while waiting for stream to be active", e);
                } catch (final RuntimeException e) {
                    logger.warn("Error checking stream status for {}: {}", streamName, e.getMessage());
                    try {
                        Thread.sleep(1000);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Thread interrupted while waiting for stream to be active", ie);
                    }
                }
            }

            throw new IllegalStateException("Stream " + streamName + " did not become active within timeout");
        }

        private <T> T executeWithRetry(final String operation, final Callable<T> op) {
            Exception lastException = null;

            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    return op.call();
                } catch (final Exception e) {
                    lastException = e;
                    logger.warn("Attempt {} of {} failed for operation {}: {}",
                            attempt, MAX_RETRIES, operation, e.getMessage());

                    if (attempt < MAX_RETRIES) {
                        try {
                            final long delayMillis = INITIAL_RETRY_DELAY_MILLIS * (1 << (attempt - 1));
                            Thread.sleep(Math.min(delayMillis, MAX_RETRY_DELAY_MILLIS));
                        } catch (final InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("Thread interrupted during retry delay", ie);
                        }
                    }
                }
            }

            throw new IllegalStateException("Operation " + operation + " failed after " + MAX_RETRIES + " attempts", lastException);
        }
    }
}
