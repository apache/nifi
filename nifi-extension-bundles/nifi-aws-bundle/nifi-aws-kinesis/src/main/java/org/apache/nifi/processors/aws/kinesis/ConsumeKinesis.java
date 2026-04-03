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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"amazon", "aws", "kinesis", "consume", "stream", "record"})
@CapabilityDescription("""
        Consumes records from an Amazon Kinesis Data Stream. Uses \
        DynamoDB-based checkpointing for reliable resumption after restarts.

        Note: when a shard is split or multiple shards are merged, this processor will consume from \
        child and parent shards concurrently. It does not wait for parent shards to be fully consumed \
        before reading child shards, so record ordering is not guaranteed across a split or merge \
        boundary.""")
@WritesAttributes({
        @WritesAttribute(attribute = "aws.kinesis.stream.name",
                description = "The name of the Kinesis Stream from which records were read"),
        @WritesAttribute(attribute = "aws.kinesis.shard.id",
                description = "Shard ID from which records were read"),
        @WritesAttribute(attribute = "aws.kinesis.partition.key",
                description = "Partition key of the last Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = "aws.kinesis.first.sequence.number",
                description = "Sequence Number of the first Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = "aws.kinesis.first.subsequence.number",
                description = "Sub-Sequence Number of the first Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = "aws.kinesis.last.sequence.number",
                description = "Sequence Number of the last Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = "aws.kinesis.last.subsequence.number",
                description = "Sub-Sequence Number of the last Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = "aws.kinesis.approximate.arrival.timestamp.ms",
                description = "Approximate arrival timestamp associated with the Kinesis Record or records in the FlowFile"),
        @WritesAttribute(attribute = "mime.type",
                description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer (if configured)"),
        @WritesAttribute(attribute = "record.count",
                description = "Number of records written to the FlowFile"),
        @WritesAttribute(attribute = "record.error.message",
                description = "Error message encountered by the Record Reader or Record Writer (if configured)"),
        @WritesAttribute(attribute = "kinesis.millis.behind",
                description = "How far behind the stream tail we are, in milliseconds")
})
@DefaultSettings(yieldDuration = "100 millis")
@SystemResourceConsideration(resource = SystemResource.CPU, description = """
        The processor uses additional CPU resources when consuming data from Kinesis.""")
@SystemResourceConsideration(resource = SystemResource.NETWORK, description = """
        The processor will continually poll for new Records.""")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = """
        Records are fetched from Kinesis in the background and buffered in memory until they \
        can be written to FlowFiles. Up to 200 fetch responses may be buffered per shard for \
        both Shared Throughput and Enhanced Fan-Out. Each Shared Throughput response can \
        contain up to the value of 'Max Records Per Request' records (default 100) and up \
        to 10 MB, so the theoretical maximum is 20,000 records or approximately 2 GB per \
        shard at default settings. Each Enhanced Fan-Out push event can hold up to roughly \
        2 MB, for a theoretical maximum of approximately 400 MB per shard. In practice the \
        buffer is typically much smaller because fetch threads block when the queue is \
        full and most responses are well below the maximum size.
        """)
public class ConsumeKinesis extends AbstractProcessor {

    static final String ATTR_STREAM_NAME = "aws.kinesis.stream.name";
    static final String ATTR_SHARD_ID = "aws.kinesis.shard.id";
    static final String ATTR_FIRST_SEQUENCE = "aws.kinesis.first.sequence.number";
    static final String ATTR_LAST_SEQUENCE = "aws.kinesis.last.sequence.number";
    static final String ATTR_FIRST_SUBSEQUENCE = "aws.kinesis.first.subsequence.number";
    static final String ATTR_LAST_SUBSEQUENCE = "aws.kinesis.last.subsequence.number";
    static final String ATTR_PARTITION_KEY = "aws.kinesis.partition.key";
    static final String ATTR_ARRIVAL_TIMESTAMP = "aws.kinesis.approximate.arrival.timestamp.ms";
    static final String ATTR_MILLIS_BEHIND = "kinesis.millis.behind";
    static final String ATTR_RECORD_ERROR_MESSAGE = "record.error.message";

    private static final long QUEUE_POLL_TIMEOUT_MILLIS = 100;
    private static final Duration API_CALL_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration API_CALL_ATTEMPT_TIMEOUT = Duration.ofSeconds(10);
    private static final byte[] NEWLINE_DELIMITER = new byte[] {'\n'};
    private static final String WRAPPER_VALUE_FIELD = "value";

    static final PropertyDescriptor STREAM_NAME = new PropertyDescriptor.Builder()
            .name("Stream Name")
            .description("The name of the Kinesis stream to consume from.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .name("Application Name")
            .description("""
                    The name of the Kinesis application. Used as the DynamoDB table name for checkpoint storage. \
                    When Consumer Type is Enhanced Fan-Out, this value is also used as the registered consumer \
                    name. This value should be unique for the stream.""")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider Service")
            .description("The Controller Service used to obtain AWS credentials provider.")
            .required(true)
            .identifiesControllerService(AwsCredentialsProviderService.class)
            .build();

    static final PropertyDescriptor CONSUMER_TYPE = new PropertyDescriptor.Builder()
            .name("Consumer Type")
            .description("Strategy for reading records from Amazon Kinesis streams.")
            .required(true)
            .allowableValues(ConsumerType.class)
            .defaultValue(ConsumerType.ENHANCED_FAN_OUT)
            .build();

    static final PropertyDescriptor PROCESSING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Processing Strategy")
            .description("Strategy for processing Kinesis Records and writing serialized output to FlowFiles.")
            .required(true)
            .allowableValues(ProcessingStrategy.class)
            .defaultValue(ProcessingStrategy.FLOW_FILE)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for parsing the data received from Kinesis.")
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer to use for serializing records.")
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Strategy")
            .description("The format used to output a Kinesis Record into a FlowFile Record.")
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .defaultValue(OutputStrategy.USE_VALUE)
            .allowableValues(OutputStrategy.class)
            .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .description("""
                    Specifies the string (interpreted as UTF-8) used to separate multiple Kinesis messages \
                    within a single FlowFile when Processing Strategy is DEMARCATOR.""")
            .required(true)
            .addValidator(Validator.VALID)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR)
            .build();

    static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .name("Initial Stream Position")
            .description("The position in the stream where the processor should start reading.")
            .required(true)
            .allowableValues(InitialPosition.class)
            .defaultValue(InitialPosition.TRIM_HORIZON)
            .build();

    static final PropertyDescriptor STREAM_POSITION_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Stream Position Timestamp")
            .description("Timestamp position in stream from which to start reading Kinesis Records. Must be in ISO 8601 format.")
            .required(true)
            .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
            .dependsOn(INITIAL_STREAM_POSITION, InitialPosition.AT_TIMESTAMP)
            .build();

    static final PropertyDescriptor MAX_RECORDS_PER_REQUEST = new PropertyDescriptor.Builder()
            .name("Max Records Per Request")
            .description("The maximum number of records to retrieve per GetRecords call. Maximum is 10,000.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.createLongValidator(1, 10000, true))
            .dependsOn(CONSUMER_TYPE, ConsumerType.SHARED_THROUGHPUT)
            .build();

    static final PropertyDescriptor MAX_BATCH_DURATION = new PropertyDescriptor.Builder()
            .name("Max Batch Duration")
            .description("""
                    The maximum amount of time to spend consuming records in a single invocation before \
                    committing the session and checkpointing.""")
            .required(true)
            .defaultValue("5 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max Batch Size")
            .description("""
                    The maximum amount of data to consume in a single invocation before committing the \
                    session and checkpointing.""")
            .required(true)
            .defaultValue("10 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Endpoint Override URL")
            .description("An optional endpoint override URL for both the Kinesis and DynamoDB clients.")
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE =
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP, ProxySpec.HTTP_AUTH);

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            STREAM_NAME,
            APPLICATION_NAME,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            CUSTOM_REGION,
            CONSUMER_TYPE,
            PROCESSING_STRATEGY,
            RECORD_READER,
            RECORD_WRITER,
            OUTPUT_STRATEGY,
            MESSAGE_DEMARCATOR,
            INITIAL_STREAM_POSITION,
            STREAM_POSITION_TIMESTAMP,
            MAX_RECORDS_PER_REQUEST,
            MAX_BATCH_DURATION,
            MAX_BATCH_SIZE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are created when records are successfully read from Kinesis and parsed")
            .build();

    static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("FlowFiles that failed to parse using the configured Record Reader")
            .build();

    private static final Set<Relationship> RAW_FILE_RELATIONSHIPS = Set.of(REL_SUCCESS);
    private static final Set<Relationship> RECORD_FILE_RELATIONSHIPS = Set.of(REL_SUCCESS, REL_PARSE_FAILURE);

    private volatile SdkHttpClient kinesisHttpClient;
    private volatile SdkHttpClient dynamoHttpClient;
    private volatile KinesisClient kinesisClient;
    private volatile DynamoDbClient dynamoDbClient;
    private volatile SdkAsyncHttpClient asyncHttpClient;
    private volatile KinesisShardManager shardManager;
    private volatile KinesisConsumerClient consumerClient;
    private volatile String streamName;
    private volatile int maxRecordsPerRequest;
    private volatile String initialStreamPosition;
    private volatile long maxBatchNanos;
    private volatile long maxBatchBytes;

    private volatile ProcessingStrategy processingStrategy = ProcessingStrategy.valueOf(PROCESSING_STRATEGY.getDefaultValue());
    private volatile String efoConsumerArn;
    private final AtomicLong shardRoundRobinCounter = new AtomicLong();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return switch (processingStrategy) {
            case FLOW_FILE, LINE_DELIMITED, DEMARCATOR -> RAW_FILE_RELATIONSHIPS;
            case RECORD -> RECORD_FILE_RELATIONSHIPS;
        };
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(PROCESSING_STRATEGY)) {
            processingStrategy = ProcessingStrategy.valueOf(newValue);
        }
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        ProxyServiceMigration.renameProxyConfigurationServiceProperty(config);
        config.renameProperty("Max Bytes to Buffer", "Max Batch Size");
        config.removeProperty("Checkpoint Interval");
        config.removeProperty("Metrics Publishing");
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Region region = RegionUtil.getRegion(context);
        final AwsCredentialsProvider credentialsProvider = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AwsCredentialsProviderService.class).getAwsCredentialsProvider();
        final String endpointOverride = context.getProperty(ENDPOINT_OVERRIDE).getValue();

        final ClientOverrideConfiguration clientConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(API_CALL_TIMEOUT)
                .apiCallAttemptTimeout(API_CALL_ATTEMPT_TIMEOUT)
                .build();

        final KinesisClientBuilder kinesisBuilder = KinesisClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(clientConfig);

        final DynamoDbClientBuilder dynamoBuilder = DynamoDbClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(clientConfig);

        if (endpointOverride != null && !endpointOverride.isEmpty()) {
            final URI endpointUri = URI.create(endpointOverride);
            kinesisBuilder.endpointOverride(endpointUri);
            dynamoBuilder.endpointOverride(endpointUri);
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context);

        kinesisHttpClient = buildApacheHttpClient(proxyConfig, PollingKinesisClient.MAX_CONCURRENT_FETCHES + 10);
        dynamoHttpClient = buildApacheHttpClient(proxyConfig, 50);
        kinesisBuilder.httpClient(kinesisHttpClient);
        dynamoBuilder.httpClient(dynamoHttpClient);

        kinesisClient = kinesisBuilder.build();
        dynamoDbClient = dynamoBuilder.build();

        final String checkpointTableName = context.getProperty(APPLICATION_NAME).getValue();
        streamName = context.getProperty(STREAM_NAME).getValue();
        initialStreamPosition = context.getProperty(INITIAL_STREAM_POSITION).getValue();
        maxBatchNanos = context.getProperty(MAX_BATCH_DURATION).asTimePeriod(TimeUnit.NANOSECONDS);
        maxBatchBytes = context.getProperty(MAX_BATCH_SIZE).asDataSize(DataUnit.B).longValue();

        final boolean efoMode = ConsumerType.ENHANCED_FAN_OUT.equals(context.getProperty(CONSUMER_TYPE).asAllowableValue(ConsumerType.class));
        maxRecordsPerRequest = efoMode ? 0 : context.getProperty(MAX_RECORDS_PER_REQUEST).asInteger();

        shardManager = createShardManager(kinesisClient, dynamoDbClient, getLogger(), checkpointTableName, streamName);
        shardManager.ensureCheckpointTableExists();
        consumerClient = createConsumerClient(kinesisClient, getLogger(), efoMode);

        final Instant timestampForPosition = resolveTimestampPosition(context);
        if (timestampForPosition != null) {
            consumerClient.setTimestampForInitialPosition(timestampForPosition);
        }

        if (efoMode) {
            final NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder()
                    .protocol(Protocol.HTTP2)
                    .maxConcurrency(500)
                    .connectionAcquisitionTimeout(Duration.ofSeconds(60));

            if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
                final software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder nettyProxyBuilder = software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
                    .host(proxyConfig.getProxyServerHost())
                    .port(proxyConfig.getProxyServerPort());

                if (proxyConfig.hasCredential()) {
                    nettyProxyBuilder.username(proxyConfig.getProxyUserName());
                    nettyProxyBuilder.password(proxyConfig.getProxyUserPassword());
                }

                nettyBuilder.proxyConfiguration(nettyProxyBuilder.build());
            }

            asyncHttpClient = nettyBuilder.build();

            final KinesisAsyncClientBuilder asyncBuilder = KinesisAsyncClient.builder()
                    .region(region)
                    .credentialsProvider(credentialsProvider)
                    .httpClient(asyncHttpClient);

            if (endpointOverride != null && !endpointOverride.isEmpty()) {
                asyncBuilder.endpointOverride(URI.create(endpointOverride));
            }

            final String consumerName = context.getProperty(APPLICATION_NAME).getValue();
            consumerClient.initialize(asyncBuilder.build(), streamName, consumerName);
        }
    }

    private static Instant resolveTimestampPosition(final ProcessContext context) {
        final InitialPosition position = context.getProperty(INITIAL_STREAM_POSITION).asAllowableValue(InitialPosition.class);
        if (position == InitialPosition.AT_TIMESTAMP) {
            return Instant.parse(context.getProperty(STREAM_POSITION_TIMESTAMP).getValue());
        }
        return null;
    }

    /**
     * Builds an {@link Apache5HttpClient} with the given connection pool size and optional proxy
     * configuration. Each AWS service client (Kinesis, DynamoDB) should receive its own HTTP client
     * so their connection pools are isolated and cannot starve each other under high shard counts.
     */
    private static SdkHttpClient buildApacheHttpClient(final ProxyConfiguration proxyConfig, final int maxConnections) {
        final Apache5HttpClient.Builder builder = Apache5HttpClient.builder()
                .maxConnections(maxConnections);

        if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
            final URI proxyEndpoint = URI.create(String.format("http://%s:%s", proxyConfig.getProxyServerHost(), proxyConfig.getProxyServerPort()));
            final software.amazon.awssdk.http.apache5.ProxyConfiguration.Builder proxyBuilder =
                    software.amazon.awssdk.http.apache5.ProxyConfiguration.builder().endpoint(proxyEndpoint);

            if (proxyConfig.hasCredential()) {
                proxyBuilder.username(proxyConfig.getProxyUserName());
                proxyBuilder.password(proxyConfig.getProxyUserPassword());
            }

            builder.proxyConfiguration(proxyBuilder.build());
        }

        return builder.build();
    }

    @OnStopped
    public void onStopped() {
        if (shardManager != null) {
            shardManager.releaseAllLeases();
            shardManager.close();
            shardManager = null;
        }

        if (consumerClient instanceof EnhancedFanOutClient efo) {
            efoConsumerArn = efo.getConsumerArn();
        }
        if (consumerClient != null) {
            consumerClient.close();
            consumerClient = null;
        }

        if (asyncHttpClient != null) {
            asyncHttpClient.close();
            asyncHttpClient = null;
        }

        if (kinesisClient != null) {
            kinesisClient.close();
            kinesisClient = null;
        }

        if (dynamoDbClient != null) {
            dynamoDbClient.close();
            dynamoDbClient = null;
        }

        closeQuietly(kinesisHttpClient);
        kinesisHttpClient = null;
        closeQuietly(dynamoHttpClient);
        dynamoHttpClient = null;
    }

    @OnRemoved
    public void onRemoved(final ProcessContext context) {
        final String arn = efoConsumerArn;
        efoConsumerArn = null;
        if (arn == null) {
            return;
        }

        final Region region = RegionUtil.getRegion(context);
        final AwsCredentialsProvider credentialsProvider = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AwsCredentialsProviderService.class).getAwsCredentialsProvider();
        final String endpointOverride = context.getProperty(ENDPOINT_OVERRIDE).getValue();

        final KinesisClientBuilder builder = KinesisClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider);

        if (endpointOverride != null && !endpointOverride.isEmpty()) {
            builder.endpointOverride(URI.create(endpointOverride));
        }

        try (final KinesisClient tempClient = builder.build()) {
            tempClient.deregisterStreamConsumer(DeregisterStreamConsumerRequest.builder()
                    .consumerARN(arn)
                    .build());
            getLogger().info("Deregistered EFO consumer [{}]", arn);
        } catch (final Exception e) {
            getLogger().warn("Failed to deregister EFO consumer [{}]; manual cleanup may be required", arn, e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final NodeTypeProvider nodeTypeProvider = getNodeTypeProvider();
        final int clusterMemberCount = nodeTypeProvider.isClustered() ? 0 : Math.max(1, nodeTypeProvider.getClusterMembers().size());
        shardManager.refreshLeasesIfNecessary(clusterMemberCount);
        final List<Shard> ownedShards = shardManager.getOwnedShards();

        if (ownedShards.isEmpty()) {
            context.yield();
            return;
        }

        final Set<String> ownedShardIds = new HashSet<>();
        for (final Shard shard : ownedShards) {
            ownedShardIds.add(shard.shardId());
        }

        consumerClient.removeUnownedShards(ownedShardIds);
        consumerClient.startFetches(ownedShards, streamName, maxRecordsPerRequest, initialStreamPosition, shardManager);
        consumerClient.logDiagnostics(ownedShards.size(), shardManager.getCachedShardCount());

        final Set<String> claimedShards = new HashSet<>();
        List<ShardFetchResult> consumed = List.of();
        try {
            consumed = consumeRecords(claimedShards);
            final List<ShardFetchResult> accepted = discardRelinquishedResults(consumed, claimedShards);

            if (accepted.isEmpty()) {
                consumerClient.releaseShards(claimedShards);
                context.yield();
                return;
            }

            final PartitionedBatch batch = partitionByShardAndCheckpoint(accepted);

            final WriteResult output;
            try {
                output = writeResults(session, context, batch.resultsByShard());
            } catch (final Exception e) {
                handleWriteFailure(e, accepted, claimedShards, context);
                return;
            }

            if (output.produced().isEmpty() && output.parseFailures().isEmpty()) {
                consumerClient.releaseShards(claimedShards);
                context.yield();
                return;
            }

            session.transfer(output.produced(), REL_SUCCESS);
            if (!output.parseFailures().isEmpty()) {
                session.transfer(output.parseFailures(), REL_PARSE_FAILURE);
                session.adjustCounter("Records Parse Failure", output.parseFailures().size(), false);
            }
            session.adjustCounter("Records Consumed", output.totalRecordCount(), false);
            final long dedupEvents = consumerClient.drainDeduplicatedEventCount();
            if (dedupEvents > 0) {
                session.adjustCounter("EFO Deduplicated Events", dedupEvents, false);
            }

            consumed = List.of();
            session.commitAsync(
                () -> {
                    try {
                        shardManager.writeCheckpoints(batch.checkpoints());
                    } finally {
                        try {
                            consumerClient.acknowledgeResults(accepted);
                        } finally {
                            consumerClient.releaseShards(claimedShards);
                        }
                    }
                },
                failure -> {
                    try {
                        getLogger().error("Session commit failed; resetting shard iterators for re-consumption", failure);
                        consumerClient.rollbackResults(accepted);
                    } finally {
                        consumerClient.releaseShards(claimedShards);
                    }
                });
        } catch (final Exception e) {
            if (!consumed.isEmpty()) {
                consumerClient.rollbackResults(consumed);
            }
            consumerClient.releaseShards(claimedShards);
            throw e;
        }
    }

    private List<ShardFetchResult> discardRelinquishedResults(final List<ShardFetchResult> consumedResults, final Set<String> claimedShards) {
        final List<ShardFetchResult> accepted = new ArrayList<>();
        final List<ShardFetchResult> discarded = new ArrayList<>();
        for (final ShardFetchResult result : consumedResults) {
            if (shardManager.shouldProcessFetchedResult(result.shardId())) {
                accepted.add(result);
            } else {
                discarded.add(result);
            }
        }

        if (!discarded.isEmpty()) {
            getLogger().debug("Discarding {} fetched shard result(s) for relinquished shards", discarded.size());
            consumerClient.rollbackResults(discarded);
            for (final ShardFetchResult result : discarded) {
                claimedShards.remove(result.shardId());
            }
            consumerClient.releaseShards(discarded.stream().map(ShardFetchResult::shardId).toList());
        }

        return accepted;
    }

    private PartitionedBatch partitionByShardAndCheckpoint(final List<ShardFetchResult> accepted) {
        final Map<String, List<ShardFetchResult>> resultsByShard = new LinkedHashMap<>();
        for (final ShardFetchResult result : accepted) {
            resultsByShard.computeIfAbsent(result.shardId(), k -> new ArrayList<>()).add(result);
        }
        for (final List<ShardFetchResult> shardResults : resultsByShard.values()) {
            shardResults.sort(Comparator.comparing(ShardFetchResult::firstSequenceNumber));
        }

        final Map<String, BigInteger> checkpoints = new HashMap<>();
        for (final List<ShardFetchResult> shardResults : resultsByShard.values()) {
            final ShardFetchResult last = shardResults.getLast();
            checkpoints.put(last.shardId(), last.lastSequenceNumber());
        }

        return new PartitionedBatch(resultsByShard, checkpoints);
    }

    private List<ShardFetchResult> consumeRecords(final Set<String> claimedShards) {
        final List<ShardFetchResult> results = new ArrayList<>();
        final long startNanos = System.nanoTime();
        long estimatedBytes = 0;

        while (isScheduled() && System.nanoTime() < startNanos + maxBatchNanos && estimatedBytes < maxBatchBytes) {
            final List<String> readyShards = consumerClient.getShardIdsWithResults();
            if (readyShards.isEmpty()) {
                if (!consumerClient.hasPendingFetches()) {
                    break;
                }

                try {
                    consumerClient.awaitResults(QUEUE_POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }

            boolean foundAny = false;
            final int shardCount = readyShards.size();
            final int startOffset = (int) (shardRoundRobinCounter.getAndIncrement() % shardCount);
            for (int i = 0; i < shardCount && estimatedBytes < maxBatchBytes; i++) {
                final String shardId = readyShards.get((startOffset + i) % shardCount);
                if (!claimedShards.contains(shardId) && !consumerClient.claimShard(shardId)) {
                    continue;
                }
                claimedShards.add(shardId);

                final ShardFetchResult result = consumerClient.pollShardResult(shardId);
                if (result != null) {
                    results.add(result);
                    estimatedBytes += estimateResultBytes(result);
                    foundAny = true;
                }
            }

            if (!foundAny) {
                break;
            }
        }

        return results;
    }

    private void handleWriteFailure(final Exception cause, final List<ShardFetchResult> accepted,
                                    final Set<String> claimedShards, final ProcessContext context) {
        getLogger().error("Failed to write consumed Kinesis records", cause);
        consumerClient.rollbackResults(accepted);
        consumerClient.releaseShards(claimedShards);
        context.yield();
    }

    private WriteResult writeResults(final ProcessSession session, final ProcessContext context,
                                     final Map<String, List<ShardFetchResult>> resultsByShard) {
        final List<FlowFile> produced = new ArrayList<>();
        final List<FlowFile> parseFailures = new ArrayList<>();
        long totalRecordCount = 0;
        long totalBytesConsumed = 0;
        long maxMillisBehind = -1;

        try {
            if (processingStrategy == ProcessingStrategy.FLOW_FILE) {
                final BatchAccumulator batch = new BatchAccumulator();
                for (final List<ShardFetchResult> shardResults : resultsByShard.values()) {
                    for (final ShardFetchResult result : shardResults) {
                        batch.updateMillisBehind(result.millisBehindLatest());
                        for (final UserRecord record : result.records()) {
                            batch.addBytes(record.data().length);
                        }
                    }
                    writeFlowFilePerRecord(session, shardResults, streamName, batch, produced);
                }
                totalRecordCount = batch.getRecordCount();
                totalBytesConsumed = batch.getBytesConsumed();
                maxMillisBehind = batch.getMaxMillisBehind();
            } else {
                for (final Map.Entry<String, List<ShardFetchResult>> entry : resultsByShard.entrySet()) {
                    final BatchAccumulator batch = new BatchAccumulator();
                    batch.setLastShardId(entry.getKey());
                    for (final ShardFetchResult result : entry.getValue()) {
                        batch.updateMillisBehind(result.millisBehindLatest());
                        batch.updateSequenceRange(result);
                        for (final UserRecord record : result.records()) {
                            batch.addBytes(record.data().length);
                            batch.updateRecordRange(record);
                        }
                    }

                    if (processingStrategy == ProcessingStrategy.LINE_DELIMITED || processingStrategy == ProcessingStrategy.DEMARCATOR) {
                        final byte[] delimiter;
                        if (processingStrategy == ProcessingStrategy.LINE_DELIMITED) {
                            delimiter = NEWLINE_DELIMITER;
                        } else {
                            final String demarcatorValue = context.getProperty(MESSAGE_DEMARCATOR).getValue();
                            delimiter = demarcatorValue.getBytes(StandardCharsets.UTF_8);
                        }
                        writeDelimited(session, entry.getValue(), streamName, batch, delimiter, produced);
                    } else {
                        writeRecordOriented(session, context, entry.getValue(), streamName, batch, produced, parseFailures);
                    }

                    totalRecordCount += batch.getRecordCount();
                    totalBytesConsumed += batch.getBytesConsumed();
                    maxMillisBehind = Math.max(maxMillisBehind, batch.getMaxMillisBehind());
                }
            }
        } catch (final Exception e) {
            session.remove(produced);
            session.remove(parseFailures);
            throw e;
        }

        return new WriteResult(produced, parseFailures, totalRecordCount, totalBytesConsumed, maxMillisBehind);
    }

    private void writeFlowFilePerRecord(final ProcessSession session, final List<ShardFetchResult> results,
                                        final String streamName, final BatchAccumulator batch, final List<FlowFile> output) {
        for (final ShardFetchResult result : results) {
            for (final UserRecord record : result.records()) {
                final byte[] recordBytes = record.data();
                FlowFile flowFile = session.create();
                try {
                    flowFile = session.write(flowFile, out -> out.write(recordBytes));

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put(ATTR_STREAM_NAME, streamName);
                    attributes.put(ATTR_SHARD_ID, result.shardId());
                    attributes.put(ATTR_FIRST_SEQUENCE, record.sequenceNumber());
                    attributes.put(ATTR_LAST_SEQUENCE, record.sequenceNumber());
                    attributes.put(ATTR_FIRST_SUBSEQUENCE, String.valueOf(record.subSequenceNumber()));
                    attributes.put(ATTR_LAST_SUBSEQUENCE, String.valueOf(record.subSequenceNumber()));
                    attributes.put(ATTR_PARTITION_KEY, record.partitionKey());
                    if (record.approximateArrivalTimestamp() != null) {
                        attributes.put(ATTR_ARRIVAL_TIMESTAMP, String.valueOf(record.approximateArrivalTimestamp().toEpochMilli()));
                    }
                    attributes.put("record.count", "1");
                    if (result.millisBehindLatest() >= 0) {
                        attributes.put(ATTR_MILLIS_BEHIND, String.valueOf(result.millisBehindLatest()));
                    }

                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.getProvenanceReporter().receive(flowFile, buildTransitUri(streamName, result.shardId()));
                    output.add(flowFile);
                    batch.incrementRecordCount();
                } catch (final Exception e) {
                    session.remove(flowFile);
                    throw e;
                }
            }
        }
    }

    private void writeDelimited(final ProcessSession session, final List<ShardFetchResult> results,
                                final String streamName, final BatchAccumulator batch, final byte[] delimiter,
                                final List<FlowFile> output) {
        FlowFile flowFile = session.create();
        try {
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    boolean first = true;
                    for (final ShardFetchResult result : results) {
                        for (final UserRecord record : result.records()) {
                            if (!first) {
                                out.write(delimiter);
                            }
                            out.write(record.data());
                            first = false;
                            batch.incrementRecordCount();
                        }
                    }
                }
            });

            flowFile = session.putAllAttributes(flowFile, createFlowFileAttributes(streamName, batch));
            session.getProvenanceReporter().receive(flowFile, buildTransitUri(streamName, batch.getLastShardId()));
            output.add(flowFile);
        } catch (final Exception e) {
            session.remove(flowFile);
            throw e;
        }
    }

    /**
     * Writes Kinesis records as NiFi records using the configured Record Reader and Record Writer.
     *
     * <p>This method may appear unnecessarily complex, but it is intended to address specific requirements:</p>
     * <ul>
     *   <li>Keep records ordered in the same order they are received from Kinesis</li>
     *   <li>Create as few FlowFiles as necessary, keeping many records together in larger FlowFiles for performance reasons.</li>
     * </ul>
     *
     * <p>Alternative options have been considered, as well:</p>
     * <ul>
     *   <li>Read each Record one at a time with a separate RecordReader. If its schema is different than the previous
     *       record, create a new FlowFile. However, when the stream is filled with JSON and many fields are nullable, this
     *       can look like a different schema for each Record when inference is used, thus creating many tiny FlowFiles.</li>
     *   <li>Read each Record one at a time with a separate RecordReader. Map the RecordSchema to the existing RecordWriter
     *       for that schema, if one exists, and write to that writer; if none exists, create a new one. This results in better
     *       grouping in many cases, but it results in the output being reordered, as we may write records 1, 2, 3 to writers
     *       A, B, A.</li>
     *   <li>Create a single InputStream and RecordReader for the entire batch. Create a single Writer for the entire batch.
     *       This way, we infer a single schema for the entire batch that is appropriate for all records. This bundles all records
     *       in the batch into a single FlowFile, which is ideal. However, this approach fails when we are not inferring the schema
     *       and the records do not all have the same schema. In that case, we can fail when attempting to read the records or when
     *       we attempt to write the records due to schema incompatibility.</li>
     * </ul>
     *
     * <p>
     *   Additionally, the existing RecordSchema API does not tell us whether or not a schema was inferred,
     *   so we cannot easily make a decision based on that knowledge. Therefore, we have taken an approach that
     *   attempts to process data using our preferred method, falling back as necessary to other options.
     * </p>
     *
     * <p>
     *   The primary path ({@link #writeRecordBatch}) combines all records into a single InputStream
     *   via {@link KinesisRecordInputStream} and creates one RecordReader. This is optimal for formats
     *   like JSON where the schema is inferred from the data: a single InputStream lets the reader see
     *   all records and produce a unified schema for the writer.
     * </p>
     *
     * <p>
     *   However, this approach fails when records carry incompatible embedded schemas (e.g. Avro
     *   containers with different field sets). The single reader sees only the first schema and cannot
     *   parse subsequent records that differ from it. When this happens, the method falls back to
     *   {@link #writeRecordBatchPerRecord}, which processes each record individually and splits output
     *   across multiple FlowFiles when schemas change.
     * </p>
     */
    private void writeRecordOriented(final ProcessSession session, final ProcessContext context,
                                     final List<ShardFetchResult> results, final String streamName,
                                     final BatchAccumulator batch, final List<FlowFile> output,
                                     final List<FlowFile> parseFailureOutput) {

        final List<UserRecord> allRecords = new ArrayList<>();
        for (final ShardFetchResult result : results) {
            allRecords.addAll(result.records());
        }

        try {
            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
            final OutputStrategy outputStrategy = context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class);
            writeRecordBatch(session, readerFactory, writerFactory, outputStrategy,
                    allRecords, streamName, batch, output);
        } catch (final Exception e) {
            getLogger().debug("Combined-stream record processing failed; falling back to per-record processing", e);
            batch.resetRecordCount();
            final RecordBatchResult result = writeRecordBatchPerRecord(session, context, allRecords, streamName, batch);
            output.addAll(result.output());
            parseFailureOutput.addAll(result.parseFailures());
        }
    }

    private void writeRecordBatch(final ProcessSession session, final RecordReaderFactory readerFactory,
                                  final RecordSetWriterFactory writerFactory, final OutputStrategy outputStrategy,
                                  final List<UserRecord> records,
                                  final String streamName, final BatchAccumulator batch, final List<FlowFile> output) {

        FlowFile flowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();

        try {
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    try (final InputStream kinesisInput = new KinesisRecordInputStream(records);
                         final RecordReader reader = readerFactory.createRecordReader(Map.of(), kinesisInput, -1, getLogger())) {

                        RecordSchema writeSchema = reader.getSchema();
                        if (outputStrategy == OutputStrategy.INJECT_METADATA) {
                            final List<RecordField> fields = new ArrayList<>(writeSchema.getFields());
                            fields.add(KinesisRecordMetadata.FIELD_METADATA);
                            writeSchema = new SimpleRecordSchema(fields);
                        } else if (outputStrategy == OutputStrategy.USE_WRAPPER) {
                            writeSchema = new SimpleRecordSchema(List.of(
                                    KinesisRecordMetadata.FIELD_METADATA,
                                    new RecordField(WRAPPER_VALUE_FIELD, RecordFieldType.RECORD.getRecordDataType(writeSchema))));
                        }

                        try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, Map.of())) {
                            writer.beginRecordSet();

                            int recordIndex = 0;
                            org.apache.nifi.serialization.record.Record nifiRecord;
                            while ((nifiRecord = reader.nextRecord()) != null) {
                                final UserRecord record = records.get(recordIndex++);
                                nifiRecord = decorateRecord(nifiRecord, record, record.shardId(), streamName, outputStrategy, writeSchema);

                                writer.write(nifiRecord);
                                batch.incrementRecordCount();
                            }

                            final org.apache.nifi.serialization.WriteResult writeResult = writer.finishRecordSet();
                            attributes.putAll(writeResult.getAttributes());
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        }
                    } catch (final MalformedRecordException | SchemaNotFoundException e) {
                        throw new IOException(e);
                    }
                }
            });

            attributes.putAll(createFlowFileAttributes(streamName, batch));
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.getProvenanceReporter().receive(flowFile, buildTransitUri(streamName, batch.getLastShardId()));
            output.add(flowFile);
        } catch (final Exception e) {
            session.remove(flowFile);
            throw e;
        }
    }

    /**
     * Fallback path that processes each Kinesis record individually, splitting output across multiple
     * FlowFiles when the record schema changes between consecutive records.
     *
     * <p>This is invoked when the combined-stream approach ({@link #writeRecordBatch}) fails, which
     * typically happens when the batch contains records with incompatible embedded schemas (e.g. Avro
     * containers whose field sets differ). Rather than grouping or buffering records up front, this
     * method makes a single pass: for each record it creates a RecordReader, compares the schema to
     * the current writer's schema, and either continues writing to the same FlowFile or finalizes the
     * current FlowFile and starts a new one. This preserves record ordering without demultiplexing.</p>
     *
     * <p>Records that cannot be parsed (empty data, malformed content, missing schema) are collected
     * and routed to the parse-failure relationship at the end.</p>
     *
     * @param session    the current process session
     * @param context    the current process context (used to resolve Record Reader, Record Writer, and Output Strategy)
     * @param records    the Kinesis records to process, in order
     * @param streamName the Kinesis stream name, used for FlowFile attributes
     * @param batch      accumulator for batch-level attributes and record counting
     * @return a {@link RecordBatchResult} containing the successfully written FlowFiles and any parse-failure FlowFiles
     */
    private RecordBatchResult writeRecordBatchPerRecord(final ProcessSession session, final ProcessContext context,
                                                        final List<UserRecord> records,
                                                        final String streamName, final BatchAccumulator batch) {

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final OutputStrategy outputStrategy = context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class);

        final List<FlowFile> output = new ArrayList<>();
        final List<FlowFile> parseFailureOutput = new ArrayList<>();
        final List<ParseFailureRecord> unparseable = new ArrayList<>();
        FlowFile currentFlowFile = null;
        OutputStream currentOut = null;
        RecordSetWriter currentWriter = null;
        RecordSchema currentReadSchema = null;
        RecordSchema currentWriteSchema = null;

        try {
            for (final UserRecord record : records) {

                if (record.data().length == 0) {
                    unparseable.add(new ParseFailureRecord(record, "Record content is empty"));
                    continue;
                }

                RecordSchema readSchema = null;
                final List<org.apache.nifi.serialization.record.Record> parsedRecords = new ArrayList<>();
                RecordReader reader = null;
                try {
                    reader = readerFactory.createRecordReader(Map.of(), new ByteArrayInputStream(record.data()), record.data().length, getLogger());
                    readSchema = reader.getSchema();
                    org.apache.nifi.serialization.record.Record nifiRecord;
                    while ((nifiRecord = reader.nextRecord()) != null) {
                        parsedRecords.add(nifiRecord);
                    }
                } catch (final MalformedRecordException | SchemaNotFoundException | IOException e) {
                    getLogger().debug("Kinesis record seq {} classified as unparseable: {}", record.sequenceNumber(), e.getMessage());
                    unparseable.add(new ParseFailureRecord(record, e.toString()));
                    continue;
                } finally {
                    closeQuietly(reader);
                }

                if (parsedRecords.isEmpty()) {
                    unparseable.add(new ParseFailureRecord(record, "Record content produced no parsed records"));
                    continue;
                }

                if (currentWriter == null || !readSchema.equals(currentReadSchema)) {
                    if (currentWriter != null) {
                        final org.apache.nifi.serialization.WriteResult writeResult = currentWriter.finishRecordSet();

                        currentWriter.close();
                        currentOut.close();

                        final Map<String, String> attributes = createFlowFileAttributes(streamName, batch);
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.putAll(writeResult.getAttributes());
                        attributes.put(CoreAttributes.MIME_TYPE.key(), currentWriter.getMimeType());
                        currentFlowFile = session.putAllAttributes(currentFlowFile, attributes);

                        session.getProvenanceReporter().receive(currentFlowFile, buildTransitUri(streamName, batch.getLastShardId()));
                        output.add(currentFlowFile);
                        currentFlowFile = null;
                    }

                    currentReadSchema = readSchema;
                    currentWriteSchema = buildWriteSchema(readSchema, outputStrategy);
                    currentFlowFile = session.create();
                    currentOut = session.write(currentFlowFile);
                    currentWriter = writerFactory.createWriter(getLogger(), currentWriteSchema, currentOut, Map.of());
                    currentWriter.beginRecordSet();
                    batch.resetRanges();
                }

                batch.updateRecordRange(record);

                for (final org.apache.nifi.serialization.record.Record parsed : parsedRecords) {
                    final org.apache.nifi.serialization.record.Record decorated =
                            decorateRecord(parsed, record, record.shardId(), streamName, outputStrategy, currentWriteSchema);
                    currentWriter.write(decorated);
                    batch.incrementRecordCount();
                }
            }

            if (currentWriter != null) {
                final org.apache.nifi.serialization.WriteResult writeResult = currentWriter.finishRecordSet();
                currentWriter.close();
                currentOut.close();

                final Map<String, String> attributes = createFlowFileAttributes(streamName, batch);
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.putAll(writeResult.getAttributes());
                attributes.put(CoreAttributes.MIME_TYPE.key(), currentWriter.getMimeType());

                currentFlowFile = session.putAllAttributes(currentFlowFile, attributes);
                session.getProvenanceReporter().receive(currentFlowFile, buildTransitUri(streamName, batch.getLastShardId()));
                output.add(currentFlowFile);
                currentFlowFile = null;
            }
        } catch (final Exception e) {
            closeQuietly(currentWriter);
            closeQuietly(currentOut);
            if (currentFlowFile != null) {
                session.remove(currentFlowFile);
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new ProcessException(e);
        }

        if (!unparseable.isEmpty()) {
            getLogger().warn("Encountered {} unparseable record(s) in shard {}; routing to parse failure",
                    unparseable.size(), batch.getLastShardId());
            writeParseFailures(session, unparseable, streamName, batch, parseFailureOutput);
        }

        return new RecordBatchResult(output, parseFailureOutput);
    }

    /**
     * Adjusts a read schema to the write schema required by the configured OutputStrategy. For
     * {@code INJECT_METADATA} the metadata field is appended; for {@code USE_WRAPPER} a two-field
     * wrapper schema is created; for {@code USE_VALUE} the read schema is returned unchanged.
     */
    private static RecordSchema buildWriteSchema(final RecordSchema readSchema, final OutputStrategy outputStrategy) {
        return switch (outputStrategy) {
            case INJECT_METADATA -> {
                final List<RecordField> fields = new ArrayList<>(readSchema.getFields());
                fields.add(KinesisRecordMetadata.FIELD_METADATA);
                yield new SimpleRecordSchema(fields);
            }
            case USE_WRAPPER -> {
                yield new SimpleRecordSchema(List.of(
                    KinesisRecordMetadata.FIELD_METADATA,
                    new RecordField(WRAPPER_VALUE_FIELD, RecordFieldType.RECORD.getRecordDataType(readSchema))));
            }
            case USE_VALUE -> readSchema;
        };
    }

    /**
     * Attaches Kinesis metadata to a NiFi record according to the configured OutputStrategy.
     */
    private static org.apache.nifi.serialization.record.Record decorateRecord(
            final org.apache.nifi.serialization.record.Record nifiRecord,
            final UserRecord kinesisRecord, final String shardId,
            final String streamName, final OutputStrategy outputStrategy,
            final RecordSchema writeSchema) {
        return switch (outputStrategy) {
            case INJECT_METADATA -> {
                final Map<String, Object> values = new HashMap<>(nifiRecord.toMap());
                values.put(KinesisRecordMetadata.METADATA,
                        KinesisRecordMetadata.composeMetadataObject(kinesisRecord, streamName, shardId));
                yield new MapRecord(writeSchema, values);
            }
            case USE_WRAPPER -> {
                final Map<String, Object> wrapperValues = new HashMap<>(2, 1.0f);
                wrapperValues.put(KinesisRecordMetadata.METADATA,
                        KinesisRecordMetadata.composeMetadataObject(kinesisRecord, streamName, shardId));
                wrapperValues.put(WRAPPER_VALUE_FIELD, nifiRecord);
                yield new MapRecord(writeSchema, wrapperValues);
            }
            case USE_VALUE -> nifiRecord;
        };
    }

    private void writeParseFailures(final ProcessSession session, final List<ParseFailureRecord> unparseable,
                                    final String streamName, final BatchAccumulator batch, final List<FlowFile> parseFailureOutput) {

        for (final ParseFailureRecord parseFailureRecord : unparseable) {
            final UserRecord record = parseFailureRecord.record();
            FlowFile flowFile = session.create();
            try {
                final byte[] rawBytes = record.data();
                flowFile = session.write(flowFile, out -> out.write(rawBytes));

                final Map<String, String> attributes = new HashMap<>();
                attributes.put(ATTR_STREAM_NAME, streamName);
                attributes.put(ATTR_FIRST_SEQUENCE, record.sequenceNumber());
                attributes.put(ATTR_LAST_SEQUENCE, record.sequenceNumber());
                attributes.put(ATTR_FIRST_SUBSEQUENCE, String.valueOf(record.subSequenceNumber()));
                attributes.put(ATTR_LAST_SUBSEQUENCE, String.valueOf(record.subSequenceNumber()));
                attributes.put(ATTR_PARTITION_KEY, record.partitionKey());
                if (record.approximateArrivalTimestamp() != null) {
                    attributes.put(ATTR_ARRIVAL_TIMESTAMP, String.valueOf(record.approximateArrivalTimestamp().toEpochMilli()));
                }
                attributes.put("record.count", "1");
                attributes.put(ATTR_RECORD_ERROR_MESSAGE, parseFailureRecord.reason());
                if (batch.getLastShardId() != null) {
                    attributes.put(ATTR_SHARD_ID, batch.getLastShardId());
                }
                flowFile = session.putAllAttributes(flowFile, attributes);
                parseFailureOutput.add(flowFile);
            } catch (final Exception e) {
                session.remove(flowFile);
                throw e;
            }
        }
    }

    private static long estimateResultBytes(final ShardFetchResult result) {
        long bytes = 0;
        for (final UserRecord record : result.records()) {
            bytes += record.data().length;
        }
        return bytes;
    }

    private static Map<String, String> createFlowFileAttributes(final String streamName, final BatchAccumulator batch) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_STREAM_NAME, streamName);
        attributes.put("record.count", String.valueOf(batch.getRecordCount()));

        if (batch.getMaxMillisBehind() >= 0) {
            attributes.put(ATTR_MILLIS_BEHIND, String.valueOf(batch.getMaxMillisBehind()));
        }
        if (batch.getLastShardId() != null) {
            attributes.put(ATTR_SHARD_ID, batch.getLastShardId());
        }
        if (batch.getMinSequenceNumber() != null) {
            attributes.put(ATTR_FIRST_SEQUENCE, batch.getMinSequenceNumber());
        }
        if (batch.getMaxSequenceNumber() != null) {
            attributes.put(ATTR_LAST_SEQUENCE, batch.getMaxSequenceNumber());
        }
        if (batch.getMinSubSequenceNumber() != Long.MAX_VALUE) {
            attributes.put(ATTR_FIRST_SUBSEQUENCE, String.valueOf(batch.getMinSubSequenceNumber()));
        }
        if (batch.getMaxSubSequenceNumber() != Long.MIN_VALUE) {
            attributes.put(ATTR_LAST_SUBSEQUENCE, String.valueOf(batch.getMaxSubSequenceNumber()));
        }
        if (batch.getLastPartitionKey() != null) {
            attributes.put(ATTR_PARTITION_KEY, batch.getLastPartitionKey());
        }
        if (batch.getLatestArrivalTimestamp() != null) {
            attributes.put(ATTR_ARRIVAL_TIMESTAMP, String.valueOf(batch.getLatestArrivalTimestamp().toEpochMilli()));
        }

        return attributes;
    }

    private void closeQuietly(final AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close resource", e);
            }
        }
    }

    private static String buildTransitUri(final String streamName, final String shardId) {
        return "kinesis://" + streamName + "/" + shardId;
    }

    // Exposed for testing to allow injection of mock Shard Manager
    protected KinesisShardManager createShardManager(final KinesisClient kinesisClient, final DynamoDbClient dynamoDbClient,
            final ComponentLog logger, final String checkpointTableName, final String streamName) {
        return new KinesisShardManager(kinesisClient, dynamoDbClient, logger, checkpointTableName, streamName);
    }

    // Exposed for testing to allow injection of a mock client
    protected KinesisConsumerClient createConsumerClient(final KinesisClient kinesisClient, final ComponentLog logger, final boolean efoMode) {
        if (efoMode) {
            return new EnhancedFanOutClient(kinesisClient, logger);
        }
        return new PollingKinesisClient(kinesisClient, logger);
    }

    private record RecordBatchResult(List<FlowFile> output, List<FlowFile> parseFailures) {
    }

    private record ParseFailureRecord(UserRecord record, String reason) {
    }

    private static final class KinesisRecordInputStream extends InputStream {
        private final List<byte[]> chunks;
        private int chunkIndex;
        private int positionInChunk;
        private int markChunkIndex = -1;
        private int markPositionInChunk;

        KinesisRecordInputStream(final List<UserRecord> records) {
            this.chunks = new ArrayList<>(records.size());
            for (final UserRecord record : records) {
                final byte[] data = record.data();
                if (data.length > 0) {
                    chunks.add(data);
                }
            }
        }

        @Override
        public int read() {
            while (chunkIndex < chunks.size()) {
                final byte[] current = chunks.get(chunkIndex);
                if (positionInChunk < current.length) {
                    return current[positionInChunk++] & 0xFF;
                }
                chunkIndex++;
                positionInChunk = 0;
            }
            return -1;
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length) {
            if (chunkIndex >= chunks.size()) {
                return -1;
            }
            if (length == 0) {
                return 0;
            }

            int totalRead = 0;
            while (totalRead < length && chunkIndex < chunks.size()) {
                final byte[] current = chunks.get(chunkIndex);
                final int remaining = current.length - positionInChunk;
                if (remaining <= 0) {
                    chunkIndex++;
                    positionInChunk = 0;
                    continue;
                }

                final int toRead = Math.min(length - totalRead, remaining);
                System.arraycopy(current, positionInChunk, buffer, offset + totalRead, toRead);
                positionInChunk += toRead;
                totalRead += toRead;
            }

            return totalRead == 0 ? -1 : totalRead;
        }

        @Override
        public int available() {
            if (chunkIndex >= chunks.size()) {
                return 0;
            }
            return chunks.get(chunkIndex).length - positionInChunk;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public void mark(final int readLimit) {
            markChunkIndex = chunkIndex;
            markPositionInChunk = positionInChunk;
        }

        @Override
        public void reset() throws IOException {
            if (markChunkIndex < 0) {
                throw new IOException("Stream not marked");
            }
            chunkIndex = markChunkIndex;
            positionInChunk = markPositionInChunk;
        }
    }

    private record PartitionedBatch(Map<String, List<ShardFetchResult>> resultsByShard, Map<String, BigInteger> checkpoints) {
    }

    private record WriteResult(List<FlowFile> produced, List<FlowFile> parseFailures,
            long totalRecordCount, long totalBytesConsumed, long maxMillisBehind) {
    }

    private static final class BatchAccumulator {
        private long bytesConsumed;
        private long recordCount;
        private long maxMillisBehind = -1;
        private BigInteger minSequenceNumber;
        private BigInteger maxSequenceNumber;
        private long minSubSequenceNumber = Long.MAX_VALUE;
        private long maxSubSequenceNumber = Long.MIN_VALUE;
        private String lastPartitionKey;
        private Instant latestArrivalTimestamp;
        private String lastShardId;

        long getBytesConsumed() {
            return bytesConsumed;
        }

        long getRecordCount() {
            return recordCount;
        }

        long getMaxMillisBehind() {
            return maxMillisBehind;
        }

        String getMinSequenceNumber() {
            return minSequenceNumber == null ? null : minSequenceNumber.toString();
        }

        String getMaxSequenceNumber() {
            return maxSequenceNumber == null ? null : maxSequenceNumber.toString();
        }

        long getMinSubSequenceNumber() {
            return minSubSequenceNumber;
        }

        long getMaxSubSequenceNumber() {
            return maxSubSequenceNumber;
        }

        String getLastPartitionKey() {
            return lastPartitionKey;
        }

        Instant getLatestArrivalTimestamp() {
            return latestArrivalTimestamp;
        }

        String getLastShardId() {
            return lastShardId;
        }

        void setLastShardId(final String shardId) {
            lastShardId = shardId;
        }

        void addBytes(final long bytes) {
            bytesConsumed += bytes;
        }

        void incrementRecordCount() {
            recordCount++;
        }

        void resetRecordCount() {
            recordCount = 0;
        }

        void updateMillisBehind(final long millisBehindLatest) {
            maxMillisBehind = Math.max(maxMillisBehind, millisBehindLatest);
        }

        void updateSequenceRange(final ShardFetchResult result) {
            final BigInteger firstSeq = result.firstSequenceNumber();
            final BigInteger lastSeq = result.lastSequenceNumber();
            if (minSequenceNumber == null || firstSeq.compareTo(minSequenceNumber) < 0) {
                minSequenceNumber = firstSeq;
            }
            if (maxSequenceNumber == null || lastSeq.compareTo(maxSequenceNumber) > 0) {
                maxSequenceNumber = lastSeq;
            }
        }

        void updateRecordRange(final UserRecord record) {
            updateSequenceFromRecord(record);
            final long subSeq = record.subSequenceNumber();
            if (subSeq < minSubSequenceNumber) {
                minSubSequenceNumber = subSeq;
            }
            if (subSeq > maxSubSequenceNumber) {
                maxSubSequenceNumber = subSeq;
            }
            lastPartitionKey = record.partitionKey();
            final Instant arrival = record.approximateArrivalTimestamp();
            if (arrival != null && (latestArrivalTimestamp == null || arrival.isAfter(latestArrivalTimestamp))) {
                latestArrivalTimestamp = arrival;
            }
        }

        void updateSequenceFromRecord(final UserRecord record) {
            final BigInteger seqNum = new BigInteger(record.sequenceNumber());
            if (minSequenceNumber == null || seqNum.compareTo(minSequenceNumber) < 0) {
                minSequenceNumber = seqNum;
            }
            if (maxSequenceNumber == null || seqNum.compareTo(maxSequenceNumber) > 0) {
                maxSequenceNumber = seqNum;
            }
        }

        void resetRanges() {
            minSequenceNumber = null;
            maxSequenceNumber = null;
            minSubSequenceNumber = Long.MAX_VALUE;
            maxSubSequenceNumber = Long.MIN_VALUE;
            lastPartitionKey = null;
            latestArrivalTimestamp = null;
        }
    }

    enum ConsumerType implements DescribedValue {
        SHARED_THROUGHPUT("Shared Throughput", "A consumer shares the read throughput limits with other consumers"),
        ENHANCED_FAN_OUT("Enhanced Fan-Out", "A consumer is granted a dedicated read throughput with a lower latency");

        private final String displayName;
        private final String description;

        ConsumerType(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    enum ProcessingStrategy implements DescribedValue {
        FLOW_FILE("Write one FlowFile for each consumed Kinesis Record"),
        LINE_DELIMITED("Write one FlowFile containing multiple consumed Kinesis Records separated by line delimiters"),
        RECORD("Write one FlowFile containing multiple consumed Kinesis Records processed with Record Reader and Record Writer"),
        DEMARCATOR("Write one FlowFile containing multiple consumed Kinesis Records separated by a configurable demarcator");

        private final String description;

        ProcessingStrategy(final String description) {
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return name();
        }

        @Override
        public String getDescription() {
            return description;
        }

    }

    enum InitialPosition implements DescribedValue {
        TRIM_HORIZON("Trim Horizon", "Start reading at the last untrimmed record in the shard."),
        LATEST("Latest", "Start reading just after the most recent record in the shard."),
        AT_TIMESTAMP("At Timestamp", "Start reading at the record with the specified timestamp.");

        private final String displayName;
        private final String description;

        InitialPosition(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    enum OutputStrategy implements DescribedValue {
        USE_VALUE("Use Content as Value", "Write only the Kinesis Record value to the FlowFile record."),
        USE_WRAPPER("Use Wrapper", "Write the Kinesis Record value and metadata into the FlowFile record."),
        INJECT_METADATA("Inject Metadata", "Write the Kinesis Record value to the FlowFile record and add a sub-record with metadata.");

        private final String displayName;
        private final String description;

        OutputStrategy(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
