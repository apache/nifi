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

import jakarta.annotation.Nullable;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.kinesis.MemoryBoundRecordBuffer.Lease;
import org.apache.nifi.processors.aws.kinesis.ReaderRecordProcessor.ProcessingResult;
import org.apache.nifi.processors.aws.kinesis.RecordBuffer.ShardBufferId;
import org.apache.nifi.processors.aws.kinesis.converter.InjectMetadataRecordConverter;
import org.apache.nifi.processors.aws.kinesis.converter.KinesisRecordConverter;
import org.apache.nifi.processors.aws.kinesis.converter.ValueRecordConverter;
import org.apache.nifi.processors.aws.kinesis.converter.WrapperRecordConverter;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.metrics.LogMetricsFactory;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.RetrievalSpecificConfig;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.APPROXIMATE_ARRIVAL_TIMESTAMP;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.FIRST_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.FIRST_SUB_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.LAST_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.LAST_SUB_SEQUENCE_NUMBER;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.MIME_TYPE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.PARTITION_KEY;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_COUNT;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.RECORD_ERROR_MESSAGE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesisAttributes.SHARD_ID;
import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"amazon", "aws", "kinesis", "consume", "stream", "record"})
@CapabilityDescription("""
        Consumes data from the specified AWS Kinesis stream and outputs a FlowFile for every processed Record (raw)
        or a FlowFile for a batch of processed records if a Record Reader and Record Writer are configured.
        The processor may take a few minutes on the first start and several seconds on subsequent starts
        to initialize before starting to fetch data.
        Uses DynamoDB for check pointing and coordination, and (optional) CloudWatch for metrics.
        """)
@WritesAttributes({
        @WritesAttribute(attribute = ConsumeKinesisAttributes.STREAM_NAME,
                description = "The name of the Kinesis Stream from which all Kinesis Records in the FlowFile were read"),
        @WritesAttribute(attribute = SHARD_ID,
                description = "Shard ID from which all Kinesis Records in the FlowFile were read"),
        @WritesAttribute(attribute = PARTITION_KEY,
                description = "Partition key of the last Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = FIRST_SEQUENCE_NUMBER,
                description = "A Sequence Number of the first Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = FIRST_SUB_SEQUENCE_NUMBER,
                description = "A SubSequence Number of the first Kinesis Record in the FlowFile. Generated by KPL when aggregating records into a single Kinesis Record"),
        @WritesAttribute(attribute = LAST_SEQUENCE_NUMBER,
                description = "A Sequence Number of the last Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = LAST_SUB_SEQUENCE_NUMBER,
                description = "A SubSequence Number of the last Kinesis Record in the FlowFile. Generated by KPL when aggregating records into a single Kinesis Record"),
        @WritesAttribute(attribute = APPROXIMATE_ARRIVAL_TIMESTAMP,
                description = "Approximate arrival timestamp of the last Kinesis Record in the FlowFile"),
        @WritesAttribute(attribute = MIME_TYPE,
                description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer (if configured)"),
        @WritesAttribute(attribute = RECORD_COUNT,
                description = "Number of records written to the FlowFiles by the Record Writer (if configured)"),
        @WritesAttribute(attribute = RECORD_ERROR_MESSAGE,
                description = "This attribute provides on failure the error message encountered by the Record Reader or Record Writer (if configured)")
})
@DefaultSettings(yieldDuration = "100 millis")
@SystemResourceConsideration(resource = SystemResource.CPU, description = """
        The processor uses additional CPU resources when consuming data from Kinesis.
        The consumption is started immediately after this Processor is scheduled. The consumption ends only when the Processor is stopped.""")
@SystemResourceConsideration(resource = SystemResource.NETWORK, description = """
        The processor will continually poll for new Records,
        requesting up to a maximum number of Records/bytes per call. This can result in sustained network usage.""")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = """
        ConsumeKinesis buffers Kinesis Records in memory until they can be processed.
        The maximum size of the buffer is controlled by the 'Max Bytes to Buffer' property.
        In addition, the processor may cache some amount of data for each shard when the processor's buffer is full.""")
public class ConsumeKinesis extends AbstractProcessor {

    private static final Duration HTTP_CLIENTS_CONNECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration HTTP_CLIENTS_READ_TIMEOUT = Duration.ofMinutes(3);

    private static final int KINESIS_HTTP_CLIENT_WINDOW_SIZE_BYTES = 512 * 1024; // 512 KiB
    private static final Duration KINESIS_HTTP_HEALTH_CHECK_PERIOD = Duration.ofMinutes(1);

    /**
     * How long to wait for a Scheduler initialization to complete in the OnScheduled method.
     * If the initialization takes longer than this, the processor will continue initialization checks in the onTrigger method.
     */
    private static final Duration KINESIS_SCHEDULER_ON_SCHEDULED_INITIALIZATION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration KINESIS_SCHEDULER_GRACEFUL_SHUTDOWN_TIMEOUT = Duration.ofMinutes(3);

    static final PropertyDescriptor STREAM_NAME = new PropertyDescriptor.Builder()
            .name("Stream Name")
            .description("The name of the Kinesis stream to consume from.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .name("Application Name")
            .description("The name of the Kinesis application. This is used for DynamoDB table naming and worker coordination.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("AWS Credentials Provider Service")
            .description("""
                    The Controller Service that is used to obtain AWS credentials provider.
                    Ensure that the credentials provided have access to Kinesis, DynamoDB and (optional) CloudWatch.
                    """)
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
            .description("""
                    The Record Reader to use for parsing the data received from Kinesis.

                    The Record Reader is responsible for providing schemas for the records. If the schemas change frequently,
                    it might hinder performance of the processor.
                    """)
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

    static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .name("Initial Stream Position")
            .description("The position in the stream where the processor should start reading.")
            .required(true)
            .allowableValues(InitialPosition.class)
            .defaultValue(InitialPosition.TRIM_HORIZON)
            .build();

    static final PropertyDescriptor STREAM_POSITION_TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Stream Position Timestamp")
            .description("Timestamp position in stream from which to start reading Kinesis Records. The timestamp must be in ISO 8601 format.")
            .required(true)
            .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
            .dependsOn(INITIAL_STREAM_POSITION, InitialPosition.AT_TIMESTAMP)
            .build();

    static final PropertyDescriptor MAX_BYTES_TO_BUFFER = new PropertyDescriptor.Builder()
            .name("Max Bytes to Buffer")
            .description("""
                    The maximum size of Kinesis Records that can be buffered in memory before being processed by NiFi.
                    If the buffer size exceeds the limit, the processor will stop consuming new records until free space is available.

                    Using a larger value may increase the throughput, but will do so at the expense of using more memory.
                    """)
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("100 MB")
            .build();

    static final PropertyDescriptor CHECKPOINT_INTERVAL = new PropertyDescriptor.Builder()
            .name("Checkpoint Interval")
            .description("""
                    Interval between checkpointing consumed Kinesis records. To checkpoint records each time the Processor is run, set this value to 0 seconds.

                    More frequent checkpoint may reduce performance and increase DynamoDB costs,
                    but less frequent checkpointing may result in duplicates when a Shard lease is lost or NiFi is restarted.
                    """)
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 sec")
            .build();

    static final PropertyDescriptor METRICS_PUBLISHING = new PropertyDescriptor.Builder()
            .name("Metrics Publishing")
            .description("Specifies where Kinesis usage metrics are published to.")
            .required(true)
            .allowableValues(MetricsPublishing.class)
            .defaultValue(MetricsPublishing.DISABLED)
            .build();

    static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP, ProxySpec.HTTP_AUTH);

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
            INITIAL_STREAM_POSITION,
            STREAM_POSITION_TIMESTAMP,
            MAX_BYTES_TO_BUFFER,
            CHECKPOINT_INTERVAL,
            PROXY_CONFIGURATION_SERVICE,
            METRICS_PUBLISHING
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

    private volatile DynamoDbAsyncClient dynamoDbClient;
    private volatile CloudWatchAsyncClient cloudWatchClient;
    private volatile KinesisAsyncClient kinesisClient;
    private volatile Scheduler kinesisScheduler;

    private volatile String streamName;
    private volatile RecordBuffer.ForProcessor<Lease> recordBuffer;

    private volatile @Nullable ReaderRecordProcessor readerRecordProcessor;

    private volatile Future<InitializationResult> initializationResultFuture;
    private final AtomicBoolean initialized = new AtomicBoolean();

    // An instance filed, so that it can be read in getRelationships.
    private volatile ProcessingStrategy processingStrategy = ProcessingStrategy.from(
            PROCESSING_STRATEGY.getDefaultValue());

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        ProxyServiceMigration.renameProxyConfigurationServiceProperty(config);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return switch (processingStrategy) {
            case FLOW_FILE -> RAW_FILE_RELATIONSHIPS;
            case RECORD -> RECORD_FILE_RELATIONSHIPS;
        };
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(PROCESSING_STRATEGY)) {
            processingStrategy = ProcessingStrategy.from(newValue);
        }
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        readerRecordProcessor = switch (processingStrategy) {
            case FLOW_FILE -> null;
            case RECORD -> createReaderRecordProcessor(context);
        };

        final Region region = RegionUtil.getRegion(context);
        final AwsCredentialsProvider credentialsProvider = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AwsCredentialsProviderService.class).getAwsCredentialsProvider();

        kinesisClient = KinesisAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(getKinesisEndpointOverride())
                .httpClient(createKinesisHttpClient(context))
                .build();

        dynamoDbClient = DynamoDbAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(getDynamoDbEndpointOverride())
                .httpClient(createHttpClientBuilder(context).build())
                .build();

        cloudWatchClient = CloudWatchAsyncClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .endpointOverride(getCloudwatchEndpointOverride())
                .httpClient(createHttpClientBuilder(context).build())
                .build();

        streamName = context.getProperty(STREAM_NAME).getValue();
        final InitialPositionInStreamExtended initialPositionExtended = getInitialPosition(context);
        final SingleStreamTracker streamTracker = new SingleStreamTracker(streamName, initialPositionExtended);

        final long maxBytesToBuffer = context.getProperty(MAX_BYTES_TO_BUFFER).asDataSize(DataUnit.B).longValue();
        final Duration checkpointInterval = context.getProperty(CHECKPOINT_INTERVAL).asDuration();
        final MemoryBoundRecordBuffer memoryBoundRecordBuffer = new MemoryBoundRecordBuffer(getLogger(), maxBytesToBuffer, checkpointInterval);
        recordBuffer = memoryBoundRecordBuffer;
        final ShardRecordProcessorFactory recordProcessorFactory = () -> new ConsumeKinesisRecordProcessor(memoryBoundRecordBuffer);

        final String applicationName = context.getProperty(APPLICATION_NAME).getValue();
        final String workerId = generateWorkerId();
        final ConfigsBuilder configsBuilder = new ConfigsBuilder(streamTracker, applicationName, kinesisClient, dynamoDbClient, cloudWatchClient, workerId, recordProcessorFactory);

        final MetricsFactory metricsFactory = configureMetricsFactory(context);
        final RetrievalSpecificConfig retrievalSpecificConfig = configureRetrievalSpecificConfig(context, kinesisClient, streamName, applicationName);

        final InitializationStateChangeListener initializationListener = new InitializationStateChangeListener(getLogger());
        initialized.set(false);
        initializationResultFuture = initializationListener.result();

        kinesisScheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig().workerStateChangeListener(initializationListener),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsFactory(metricsFactory),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(retrievalSpecificConfig)
        );

        final String schedulerThreadName = "%s-Scheduler-%s".formatted(getClass().getSimpleName(), getIdentifier());
        final Thread schedulerThread = new Thread(kinesisScheduler, schedulerThreadName);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
        // The thread is stopped when kinesisScheduler is shutdown in the onStopped method.

        try {
            final InitializationResult result = initializationResultFuture.get(
                    KINESIS_SCHEDULER_ON_SCHEDULED_INITIALIZATION_TIMEOUT.getSeconds(), SECONDS);
            checkInitializationResult(result);
        } catch (final TimeoutException e) {
            // During a first run the processor will take more time to initialize. We return from OnSchedule and continue waiting in the onTrigger method.
            getLogger().warn("Kinesis Scheduler initialization may take up to 10 minutes on a first run, which is caused by AWS resources initialization");
        } catch (final InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            cleanUpState();
            throw new ProcessException("Initialization failed for stream [%s]".formatted(streamName), e);
        }
    }

    /**
     * Creating Kinesis HTTP client, as per
     * {@link software.amazon.kinesis.common.KinesisClientUtil#adjustKinesisClientBuilder(KinesisAsyncClientBuilder)}.
     */
    private static SdkAsyncHttpClient createKinesisHttpClient(final ProcessContext context) {
        return createHttpClientBuilder(context)
                .protocol(Protocol.HTTP2)
                // Since we're using HTTP/2, multiple concurrent requests will reuse the same HTTP connection.
                // Therefore, the number of real connections is going to be relatively small.
                .maxConcurrency(Integer.MAX_VALUE)
                .http2Configuration(Http2Configuration.builder()
                        .initialWindowSize(KINESIS_HTTP_CLIENT_WINDOW_SIZE_BYTES)
                        .healthCheckPingPeriod(KINESIS_HTTP_HEALTH_CHECK_PERIOD)
                        .build())
                .build();
    }

    private static NettyNioAsyncHttpClient.Builder createHttpClientBuilder(final ProcessContext context) {
        final NettyNioAsyncHttpClient.Builder builder = NettyNioAsyncHttpClient.builder()
                .connectionTimeout(HTTP_CLIENTS_CONNECTION_TIMEOUT)
                .readTimeout(HTTP_CLIENTS_READ_TIMEOUT);

        final ProxyConfigurationService proxyConfigService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
        if (proxyConfigService != null) {
            final ProxyConfiguration proxyConfig = proxyConfigService.getConfiguration();

            final software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
                    .host(proxyConfig.getProxyServerHost())
                    .port(proxyConfig.getProxyServerPort());

            if (proxyConfig.hasCredential()) {
                proxyConfigBuilder.username(proxyConfig.getProxyUserName());
                proxyConfigBuilder.password(proxyConfig.getProxyUserPassword());
            }

            builder.proxyConfiguration(proxyConfigBuilder.build());
        }

        return builder;
    }

    private ReaderRecordProcessor createReaderRecordProcessor(final ProcessContext context) {
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory recordWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final OutputStrategy outputStrategy = context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class);
        final KinesisRecordConverter converter = switch (outputStrategy) {
            case USE_VALUE -> new ValueRecordConverter();
            case USE_WRAPPER -> new WrapperRecordConverter();
            case INJECT_METADATA -> new InjectMetadataRecordConverter();
        };

        return new ReaderRecordProcessor(recordReaderFactory, converter, recordWriterFactory, getLogger());
    }

    private static InitialPositionInStreamExtended getInitialPosition(final ProcessContext context) {
        final InitialPosition initialPosition = context.getProperty(INITIAL_STREAM_POSITION).asAllowableValue(InitialPosition.class);
        return switch (initialPosition) {
            case TRIM_HORIZON ->
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
            case LATEST -> InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
            case AT_TIMESTAMP -> {
                final String timestampValue = context.getProperty(STREAM_POSITION_TIMESTAMP).getValue();
                final Instant timestamp = Instant.parse(timestampValue);
                yield InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(timestamp));
            }
        };
    }

    private String generateWorkerId() {
        final String processorId = getIdentifier();
        final NodeTypeProvider nodeTypeProvider = getNodeTypeProvider();

        final String workerId;

        if (nodeTypeProvider.isClustered()) {
            // If a node id is not available for some reason, generating a random UUID helps to avoid collisions.
            final String nodeId = nodeTypeProvider.getCurrentNode().orElse(UUID.randomUUID().toString());
            workerId = "%s@%s".formatted(processorId, nodeId);
        } else {
            workerId = processorId;
        }

        return workerId;
    }

    private static @Nullable MetricsFactory configureMetricsFactory(final ProcessContext context) {
        final MetricsPublishing metricsPublishing = context.getProperty(METRICS_PUBLISHING).asAllowableValue(MetricsPublishing.class);
        return switch (metricsPublishing) {
            case DISABLED -> new NullMetricsFactory();
            case LOGS -> new LogMetricsFactory();
            case CLOUDWATCH -> null; // If no metrics factory was provided, CloudWatch metrics factory is used by default.
        };
    }

    private static RetrievalSpecificConfig configureRetrievalSpecificConfig(
            final ProcessContext context,
            final KinesisAsyncClient kinesisClient,
            final String streamName,
            final String applicationName) {
        final ConsumerType consumerType = context.getProperty(CONSUMER_TYPE).asAllowableValue(ConsumerType.class);
        return switch (consumerType) {
            case SHARED_THROUGHPUT -> new PollingConfig(kinesisClient).streamName(streamName);
            case ENHANCED_FAN_OUT -> new FanOutConfig(kinesisClient).streamName(streamName).applicationName(applicationName);
        };
    }

    @OnStopped
    public void onStopped() {
        cleanUpState();

        initialized.set(false);
        initializationResultFuture = null;
    }

    private void cleanUpState() {
        if (kinesisScheduler != null) {
            shutdownScheduler();
            kinesisScheduler = null;
        }

        if (kinesisClient != null) {
            kinesisClient.close();
            kinesisClient = null;
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
            dynamoDbClient = null;
        }
        if (cloudWatchClient != null) {
            cloudWatchClient.close();
            cloudWatchClient = null;
        }

        recordBuffer = null;
        readerRecordProcessor = null;
    }

    private void shutdownScheduler() {
        if (kinesisScheduler.shutdownComplete()) {
            return;
        }

        final long start = System.nanoTime();
        getLogger().debug("Shutting down Kinesis Scheduler");

        boolean gracefulShutdownSucceeded;
        try {
            gracefulShutdownSucceeded = kinesisScheduler.startGracefulShutdown().get(KINESIS_SCHEDULER_GRACEFUL_SHUTDOWN_TIMEOUT.getSeconds(), SECONDS);
            if (!gracefulShutdownSucceeded) {
                getLogger().warn("Failed to shutdown Kinesis Scheduler gracefully. See the logs for more details");
            }
        } catch (final RuntimeException | InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof TimeoutException) {
                getLogger().warn("Failed to shutdown Kinesis Scheduler gracefully after {} seconds", KINESIS_SCHEDULER_GRACEFUL_SHUTDOWN_TIMEOUT.getSeconds(), e);
            } else {
                getLogger().warn("Failed to shutdown Kinesis Scheduler gracefully", e);
            }
            gracefulShutdownSucceeded = false;
        }

        if (!gracefulShutdownSucceeded) {
            kinesisScheduler.shutdown();
        }

        final long finish = System.nanoTime();
        getLogger().debug("Kinesis Scheduler shutdown finished after {} seconds", NANOSECONDS.toSeconds(finish - start));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (!initialized.get()) {
            if (!initializationResultFuture.isDone()) {
                getLogger().debug("Waiting for Kinesis Scheduler to finish initialization");
                context.yield();
                return;
            }

            checkInitializationResult(initializationResultFuture.resultNow());
        }

        final Optional<Lease> leaseAcquired = recordBuffer.acquireBufferLease();

        leaseAcquired.ifPresentOrElse(
                lease -> processRecordsFromBuffer(session, lease),
                context::yield
        );
    }

    private void checkInitializationResult(final InitializationResult initializationResult) {
        switch (initializationResult) {
            case InitializationResult.Success ignored -> {
                final boolean wasInitialized = initialized.getAndSet(true);
                if (!wasInitialized) {
                    getLogger().info(
                            "Started Kinesis Scheduler for stream [{}] with application name [{}] and workerId [{}]",
                            streamName, kinesisScheduler.applicationName(), kinesisScheduler.leaseManagementConfig().workerIdentifier());
                }
            }
            case InitializationResult.Failure failure -> {
                cleanUpState();

                final ProcessException ex = failure.error()
                        .map(err -> new ProcessException("Initialization failed for stream [%s]".formatted(streamName), err))
                        // This branch is active only when a scheduler was shutdown, but no initialization error was provided.
                        // This behavior isn't typical and wasn't observed.
                        .orElseGet(() -> new ProcessException("Initialization failed for stream [%s]".formatted(streamName)));

                throw ex;
            }
        }
    }

    private void processRecordsFromBuffer(final ProcessSession session, final Lease lease) {
        try {
            final List<KinesisClientRecord> records = recordBuffer.consumeRecords(lease);

            if (records.isEmpty()) {
                recordBuffer.returnBufferLease(lease);
                return;
            }

            final String shardId = lease.shardId();
            final ReaderRecordProcessor processor = readerRecordProcessor;
            if (processor != null) {
                processRecordsWithReader(processor, session, shardId, records);
            } else {
                processRecordsAsRaw(session, shardId, records);
            }

            session.adjustCounter("Records Processed", records.size(), false);

            session.commitAsync(
                    () -> commitRecords(lease),
                    __ -> rollbackRecords(lease)
            );
        } catch (final RuntimeException e) {
            rollbackRecords(lease);
            throw e;
        }
    }

    private void commitRecords(final Lease lease) {
        try {
            recordBuffer.commitConsumedRecords(lease);
        } finally {
            recordBuffer.returnBufferLease(lease);
        }
    }

    private void rollbackRecords(final Lease lease) {
        try {
            recordBuffer.rollbackConsumedRecords(lease);
        } finally {
            recordBuffer.returnBufferLease(lease);
        }
    }

    private void processRecordsAsRaw(final ProcessSession session, final String shardId, final List<KinesisClientRecord> records) {
        for (final KinesisClientRecord record : records) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, ConsumeKinesisAttributes.fromKinesisRecords(streamName, shardId, record, record));

            flowFile = session.write(flowFile, out -> {
                try (final WritableByteChannel channel = Channels.newChannel(out)) {
                    channel.write(record.data());
                }
            });

            session.getProvenanceReporter().receive(flowFile, ProvenanceTransitUriFormat.toTransitUri(streamName, shardId));

            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    private void processRecordsWithReader(final ReaderRecordProcessor recordProcessor, final ProcessSession session, final String shardId, final List<KinesisClientRecord> records) {
        final ProcessingResult result = recordProcessor.processRecords(session, streamName, shardId, records);

        session.transfer(result.successFlowFiles(), REL_SUCCESS);
        session.transfer(result.parseFailureFlowFiles(), REL_PARSE_FAILURE);
    }

    /**
     * An adapter between Kinesis Consumer Library and {@link RecordBuffer}.
     */
    private static class ConsumeKinesisRecordProcessor implements ShardRecordProcessor {

        private final RecordBuffer.ForKinesisClientLibrary recordBuffer;
        private volatile @Nullable ShardBufferId bufferId;

        ConsumeKinesisRecordProcessor(final MemoryBoundRecordBuffer recordBuffer) {
            this.recordBuffer = recordBuffer;
        }

        @Override
        public void initialize(final InitializationInput initializationInput) {
            bufferId = recordBuffer.createBuffer(initializationInput.shardId());
        }

        @Override
        public void processRecords(final ProcessRecordsInput processRecordsInput) {
            if (bufferId == null) {
                throw new IllegalStateException("Buffer ID not found: Record Processor not initialized");
            }
            recordBuffer.addRecords(bufferId, processRecordsInput.records(), processRecordsInput.checkpointer());
        }

        @Override
        public void leaseLost(final LeaseLostInput leaseLostInput) {
            if (bufferId != null) {
                recordBuffer.consumerLeaseLost(bufferId);
            }
        }

        @Override
        public void shardEnded(final ShardEndedInput shardEndedInput) {
            if (bufferId != null) {
                recordBuffer.checkpointEndedShard(bufferId, shardEndedInput.checkpointer());
            }
        }

        @Override
        public void shutdownRequested(final ShutdownRequestedInput shutdownRequestedInput) {
            if (bufferId != null) {
                recordBuffer.shutdownShardConsumption(bufferId, shutdownRequestedInput.checkpointer());
            }
        }
    }

    private static final class InitializationStateChangeListener implements WorkerStateChangeListener {

        private final ComponentLog logger;

        private final CompletableFuture<InitializationResult> resultFuture = new CompletableFuture<>();

        private volatile @Nullable Throwable initializationFailure;

        InitializationStateChangeListener(final ComponentLog logger) {
            this.logger = logger;
        }

        @Override
        public void onWorkerStateChange(final WorkerState newState) {
            logger.info("Worker state changed to [{}]", newState);

            if (newState == WorkerState.STARTED) {
                resultFuture.complete(new InitializationResult.Success());
            } else if (newState == WorkerState.SHUT_DOWN) {
                resultFuture.complete(new InitializationResult.Failure(Optional.ofNullable(initializationFailure)));
            }
        }

        @Override
        public void onAllInitializationAttemptsFailed(final Throwable e) {
            // This method is called before the SHUT_DOWN_STARTED phase.
            // Memorizing the error until the Scheduler is SHUT_DOWN.
            initializationFailure = e;
        }

        Future<InitializationResult> result() {
            return resultFuture;
        }
    }

    private sealed interface InitializationResult {
        record Success() implements InitializationResult {
        }

        record Failure(Optional<Throwable> error) implements InitializationResult {
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
        RECORD("Write one FlowFile containing multiple consumed Kinesis Records processed with Record Reader and Record Writer");

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

        private static ProcessingStrategy from(final String name) {
            // As long as getValue() returns name(), using valueOf is fine.
            return ProcessingStrategy.valueOf(name);
        }
    }

    enum InitialPosition implements DescribedValue {
        TRIM_HORIZON("Trim Horizon", "Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard."),
        LATEST("Latest", "Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard."),
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

    enum MetricsPublishing implements DescribedValue {
        DISABLED("Disabled", "No metrics are published"),
        LOGS("Logs", "Metrics are published to application logs"),
        CLOUDWATCH("CloudWatch", "Metrics are published to Amazon CloudWatch");

        private final String displayName;
        private final String description;

        MetricsPublishing(final String displayName, final String description) {
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
        INJECT_METADATA("Inject Metadata", "Write the Kinesis Record value to the FlowFile record and add a sub-record to it with metadata.");

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

    // Visible for tests only.
    @Nullable URI getKinesisEndpointOverride() {
        return null;
    }

    // Visible for tests only.
    @Nullable URI getDynamoDbEndpointOverride() {
        return null;
    }

    // Visible for tests only.
    @Nullable URI getCloudwatchEndpointOverride() {
        return null;
    }
}
