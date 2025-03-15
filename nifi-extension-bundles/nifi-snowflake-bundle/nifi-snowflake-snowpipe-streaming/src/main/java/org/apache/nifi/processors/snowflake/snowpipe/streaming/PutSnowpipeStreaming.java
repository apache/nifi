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
package org.apache.nifi.processors.snowflake.snowpipe.streaming;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.OpenChannelRequest.OnErrorOption;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ParameterProvider;
import net.snowflake.ingest.utils.SFException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ChannelPromise;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ConcurrencyClaim;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ConcurrencyClaimRequest;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ConcurrencyManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.BoundedConcurrencyManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.QueuedSnowpipeStreamingChannelManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.SingleSnowpipeStreamingChannelManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.SnowpipeStreamingChannel;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.ChannelCoordinates;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.SnowpipeStreamingChannelFactory;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.SnowpipeStreamingChannelManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.StandardSnowpipeStreamingChannel;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.channel.UnboundedConcurrencyManager;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.converter.RuntimeExceptionConverter;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.converter.RecordRowConverter;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.converter.StandardRuntimeExceptionConverter;
import org.apache.nifi.processors.snowflake.snowpipe.streaming.converter.StandardRecordRowConverter;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"openflow", "snowflake", "experimental", "snowpipe streaming", "jdbc", "database", "connection"})
@CapabilityDescription("""
    Streams records into a Snowflake table. The table must be created in the Snowflake account beforehand.
    """)
@UseCase(
    description = "Write record-oriented data to a Snowflake table as fast as possible, accepting the possible of occasional duplicates.",
    keywords = {"snowflake", "snowpipe", "at least once", "streaming", "ingest"},
    notes = """
            This configuration assumes that incoming data is properly mapped such that the Record keys match the names of the columns in the configured Snowflake database and the values
            in the Record are of the appropriate type for the Snowflake columns.""",
    configuration = """
        Set the appropriate values for each of the following properties, according to the Snowflake account and table to which you want to write:
        - Account
        - User
        - Role
        - Schema
        - Database
        - Table

        Configure the "Private Key Service" property to a Controller Service that provides the RSA Private Key for authenticating connections.
        Configure the "Record Reader" property to a Record Reader that can read the data to be written to Snowflake.
        Set the "Delivery Guarantee" to "At least once".
        Leave the "Snowpipe Channel Prefix" property set to the default value of `${hostname(false)}`.
        Set the "Error Handling Strategy" property to `Abort`.
        Set the "Client Lag" property to `1 sec`.
        """
)
public class PutSnowpipeStreaming extends AbstractProcessor implements VerifiableProcessor {
    private static final int MAX_FLOWFILES_PER_BATCH = 1000;
    private static final long MAX_BYTES_PER_BATCH = 1024 * 1024 * 1024; // 1 GiB
    private static final long MAX_BYTES_TOTAL_MINI_BATCH = 100 * 1024 * 1024;
    private static final long MAX_MINI_BATCHES = 25;

    private static final Pattern ATTRIBUTE_PATTERN = Pattern.compile("\\$\\{([^#():}]+)}");

    static final AllowableValue DELIVERY_AT_LEAST_ONCE = new AllowableValue("At least once", "At least once",
        "Guarantees that records are delivered to Snowflake at least once, but may be delivered multiple times " +
        "if there is a failure sending to Snowflake while in the middle of transferring a FlowFile.");
    // Require stateless execution engine because it guarantees FIFO ordering of Records, and it also ensures single-threadedness from source to destination so that two FlowFiles from the same
    // source and partition (in the case of Kafka, at least) are not processed in parallel, and it ensures that data is not left in the queue when a node is scaled down or stopped.
    // If data is left in a queue, and a Kafka broker is rebalanced, the data would be processed by another node and then when the node originally holding the data restarted, it would be duplicated.
    static final AllowableValue DELIVERY_EXACTLY_ONCE = new AllowableValue("Exactly once", "Exactly once",
        "Guarantees that records are delivered to Snowflake exactly once. This requires specifying the Channel that a FlowFile belongs to, the offset of the first record in the FlowFile, " +
        "a guarantee that records in a FlowFile are already ordered correctly, and use of the Stateless Execution Engine. Additionally, the configured Record Reader must use an explicit " +
        "schema instead of inferring the schema.");

    static final AllowableValue OFFSET_FROM_EXPRESSION_LANGUAGE = new AllowableValue("Use Expression Language", "Use Expression Language",
        "The offset of each record in a FlowFile is determined by evaluating an Expression Language expression to determine the offset of the first " +
        "record and then adding to it the index of the record in the FlowFile.");
    static final AllowableValue OFFSET_FROM_RECORD_PATH = new AllowableValue("Use Record Path", "Use Record Path",
        "The offset each record in a FlowFile is determined by evaluating a RecordPath expression against the record.");

    static final PropertyDescriptor ACCOUNT = new PropertyDescriptor.Builder()
            .name("Account")
            .description("Snowflake Account Identifier with Organization Name and Account Name formatted as [organization-name]-[account-name]")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User")
            .description("Snowflake User for authenticating connections")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ROLE = new PropertyDescriptor.Builder()
            .name("Role")
            .description("Snowflake Role the user will assume when authenticating connections")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database")
            .description("Snowflake Database destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Schema")
            .description("Snowflake Schema destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table")
            .description("Snowflake Table destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("Private Key Service")
            .description("RSA Private Key Service for authenticating connections")
            .required(true)
            .identifiesControllerService(PrivateKeyService.class)
            .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name("Delivery Guarantee")
            .description("Specifies the delivery guarantee for the records being sent to Snowflake.")
            .required(true)
            .allowableValues(DELIVERY_AT_LEAST_ONCE, DELIVERY_EXACTLY_ONCE)
            .defaultValue(DELIVERY_AT_LEAST_ONCE)
            .build();

    static final PropertyDescriptor RECORD_OFFSET_STRATEGY = new PropertyDescriptor.Builder()
            .name("Record Offset Strategy")
            .description("Specifies the strategy for determining the offset of each record.")
            .required(true)
            .allowableValues(OFFSET_FROM_EXPRESSION_LANGUAGE, OFFSET_FROM_RECORD_PATH)
            .dependsOn(DELIVERY_GUARANTEE, DELIVERY_EXACTLY_ONCE)
            .build();

    static final PropertyDescriptor RECORD_OFFSET_EL = new PropertyDescriptor.Builder()
            .name("Record Offset")
            .description("The Expression Language expression to use to determine the offset of the first record in a FlowFile.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .dependsOn(RECORD_OFFSET_STRATEGY, OFFSET_FROM_EXPRESSION_LANGUAGE)
            .build();

    static final PropertyDescriptor RECORD_OFFSET_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("Record Offset Record Path")
            .description("The Record Path expression to use to determine the offset of the first record in a FlowFile.")
            .required(true)
            .addValidator(new RecordPathValidator())
            .dependsOn(RECORD_OFFSET_STRATEGY, OFFSET_FROM_RECORD_PATH)
            .build();

    static final PropertyDescriptor SNOWPIPE_CHANNEL_PREFIX = new PropertyDescriptor.Builder()
            .name("Snowpipe Channel Prefix")
            .description("The prefix to use for the Snowpipe channel name. The full channel name will be constructed as " +
                    "openflow.[prefix].[index]. The default value is ${hostname(false)}, which ensures that each NiFi node in the cluster writes to a unique channel " +
                    "by incorporating the hostname of the NiFi instance into the channel name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${hostname(false)}")
            .build();

    static final PropertyDescriptor SNOWPIPE_CHANNEL_INDEX = new PropertyDescriptor.Builder()
            .name("Snowpipe Channel Index")
            .description("The index to use for the Snowpipe channel name. The full channel name will be constructed as " +
                    "openflow.[prefix].[index]. This is necessary in order to provide Exactly Once delivery to Snowflake, as any retry must be " +
                    "tried against the same channel as was previously used.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .dependsOn(DELIVERY_GUARANTEE, DELIVERY_EXACTLY_ONCE)
            .build();

    static final PropertyDescriptor CONCURRENCY_GROUP = new PropertyDescriptor.Builder()
            .name("Concurrency Group")
            .description("Allows specifying a 'Concurrency Group' that a given FlowFile belongs to, so that the number of Concurrent Tasks that write to tables in a given group can be limited.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(DELIVERY_GUARANTEE, DELIVERY_AT_LEAST_ONCE)
            .build();

    static final PropertyDescriptor MAX_TASKS_PER_GROUP = new PropertyDescriptor.Builder()
            .name("Max Tasks Per Group")
            .description("The maximum number of channels to create for a given Snowpipe Channel Prefix. This allows limiting the number of " +
                    "concurrent tasks that can be writing to a given Snowflake table.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(CONCURRENCY_GROUP)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for reading the input")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor CLIENT_LAG = new PropertyDescriptor.Builder()
            .name("Client Lag")
            .description("The maximum amount of time that the client will wait before flushing records to Snowflake. A larger value can increase latency " +
                    "while sending to Snowflake, but for tables that are not constantly updated it can result in queries that are faster and more cost efficient.")
            .required(true)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, 10, TimeUnit.MINUTES))
            .defaultValue("1 sec")
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max Batch Size")
            .description("Maximum number of records to ingest in a single call. Multiple ingest calls will be made if the number of records exceeds the max batch size. " +
                    "Current guidance recommends batch sizes less than 16MB. " +
                    "The Max Batch Size can be tuned based on the average record size such that batches are generally less than 16MB.")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor ICEBERG_ENABLED = new PropertyDescriptor.Builder()
            .name("Iceberg Enabled")
            .description("Specifies whether the processor ingests data into an Iceberg table. The processor fails if " +
                    "this property doesn’t match the actual table type.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles successfully uploaded to Snowflake")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("For FlowFiles that failed to upload to Snowflake")
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ACCOUNT,
            USER,
            ROLE,
            DATABASE,
            SCHEMA,
            TABLE,
            PRIVATE_KEY_SERVICE,
            DELIVERY_GUARANTEE,
            RECORD_OFFSET_STRATEGY,
            RECORD_OFFSET_EL,
            RECORD_OFFSET_RECORD_PATH,
            SNOWPIPE_CHANNEL_PREFIX,
            SNOWPIPE_CHANNEL_INDEX,
            CONCURRENCY_GROUP,
            MAX_TASKS_PER_GROUP,
            RECORD_READER,
            CLIENT_LAG,
            MAX_BATCH_SIZE,
            ICEBERG_ENABLED
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final String ACCOUNT_HOST_FORMAT = "%s.snowflakecomputing.com";
    private static final char SPACE_SEPARATOR = ' ';
    private static final String CLIENT_CONNECTION_STEP = "Client Connection";
    private static final RuntimeExceptionConverter RUNTIME_EXCEPTION_CONVERTER = new StandardRuntimeExceptionConverter();

    private volatile RecordReaderFactory recordReaderFactory;
    private volatile SnowflakeStreamingIngestClient client;

    private final ConcurrentMap<ChannelCoordinates, SnowpipeStreamingChannelManager> channelManagers = new ConcurrentHashMap<>();
    private volatile PropertyEvaluator databasePropertyValue;
    private volatile PropertyEvaluator schemaPropertyValue;
    private volatile PropertyEvaluator tablePropertyValue;
    private volatile PropertyEvaluator prefixPropertyValue;
    private volatile PropertyEvaluator channelIndexPropertyValue;
    private volatile PropertyEvaluator concurrencyGroupEvaluator;
    private volatile int maxBatchSize;
    private volatile boolean exactlyOnce;
    private volatile RecordOffsetFunction recordOffsetFunction;
    private volatile ConcurrencyManager concurrencyManager;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext processContext, final ComponentLog componentLog, final Map<String, String> map) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try (final SnowflakeStreamingIngestClient client = getClient(processContext)) {
            final String explanation = "Connection authenticated [%s]".formatted(client.getName());
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(CLIENT_CONNECTION_STEP)
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation(explanation)
                    .build()
            );
        } catch (final Exception e) {
            final String explanation = getExplanation(e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(CLIENT_CONNECTION_STEP)
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation(explanation)
                    .build()
            );
            return results;
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws UnknownHostException {
        recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        client = getClient(context);

        databasePropertyValue = createEvaluator(context.getProperty(DATABASE));
        schemaPropertyValue = createEvaluator(context.getProperty(SCHEMA));
        tablePropertyValue = createEvaluator(context.getProperty(TABLE));
        prefixPropertyValue = createEvaluator(context.getProperty(SNOWPIPE_CHANNEL_PREFIX));
        channelIndexPropertyValue = createEvaluator(context.getProperty(SNOWPIPE_CHANNEL_INDEX));
        maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        exactlyOnce = DELIVERY_EXACTLY_ONCE.getValue().equalsIgnoreCase(context.getProperty(DELIVERY_GUARANTEE).getValue());

        if (!exactlyOnce && context.getProperty(CONCURRENCY_GROUP).isSet()) {
            concurrencyGroupEvaluator = createEvaluator(context.getProperty(CONCURRENCY_GROUP));
        } else {
            concurrencyGroupEvaluator = new ExplicitValueEvaluator("");
        }

        if (!exactlyOnce) {
            final boolean concurrencyGroupSet = context.getProperty(CONCURRENCY_GROUP).isSet();
            if (concurrencyGroupSet) {
                final int maxTasksPerGroup = context.getProperty(MAX_TASKS_PER_GROUP).asInteger();
                concurrencyManager = new BoundedConcurrencyManager(maxTasksPerGroup, getLogger());
            } else {
                concurrencyManager = new UnboundedConcurrencyManager();
            }
        }

        final String offsetStrategy = context.getProperty(RECORD_OFFSET_STRATEGY).getValue();
        if (!exactlyOnce || offsetStrategy == null) {
            recordOffsetFunction = new NopRecordOffsetFunction();
        } else if (offsetStrategy.equalsIgnoreCase(OFFSET_FROM_EXPRESSION_LANGUAGE.getValue())) {
            final PropertyValue recordOffsetPropertyValue = context.getProperty(RECORD_OFFSET_EL);
            recordOffsetFunction = new ExpressionLanguageOffsetFunction(recordOffsetPropertyValue);
        } else if (offsetStrategy.equalsIgnoreCase(OFFSET_FROM_RECORD_PATH.getValue())) {
            final String recordPath = context.getProperty(RECORD_OFFSET_RECORD_PATH).getValue();
            recordOffsetFunction = new RecordPathRecordOffsetFunction(recordPath);
        }
    }

    @OnStopped
    public void onStopped() {
        if (client != null) {
            try {
                client.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close Snowpipe Streaming Client", e);
            }
        }

        channelManagers.clear();
    }

    ConcurrencyManager getConcurrencyManager() {
        return concurrencyManager;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<Batch> batches = new ArrayList<>();
        long totalBytesConsumed = 0L;
        int totalBatches = 0;
        long totalRecords = 0;
        int totalFlowFiles = 0;
        final long startMillis = System.currentTimeMillis();
        final ConcurrencyClaim concurrencyClaim = new ConcurrencyClaim();

        PublishRequest publishRequest = null;
        try {
            while (true) {
                publishRequest = createPublishRequest(session, concurrencyClaim);

                try {
                    if (publishRequest.flowFiles().isEmpty()) {
                        context.yield();
                        break;
                    }

                    final Throwable failureCause = publishRequest.failureCause();
                    if (failureCause != null) {
                        failRequest(publishRequest, session, failureCause, "open a channel");
                        break;
                    }

                    final PublishResult publishResult = publish(publishRequest);
                    if (publishResult.getFailureCause() != null) {
                        failRequest(publishRequest, session, publishResult.getFailureCause(), "publish records");
                        break;
                    }

                    // Trigger close of the channel in the background. We do not want to block for it to complete.
                    publishRequest.channel().closeAsync();

                    batches.add(new Batch(publishRequest, publishResult));

                    // Keep track of what we've sent and check if we're under the threshold to send more data.
                    totalBytesConsumed += getDataSize(publishRequest);
                    totalBatches++;
                    totalRecords += publishResult.getUploadResult().recordsSent();
                    totalFlowFiles += publishRequest.flowFiles().size();

                    if (totalBytesConsumed >= MAX_BYTES_TOTAL_MINI_BATCH || totalBatches >= MAX_MINI_BATCHES) {
                        break;
                    }
                } catch (final Exception e) {
                    final List<FlowFile> flowFiles = publishRequest.flowFiles();

                    // Because we could have many FlowFiles, we don't want the verbosity of the toString() for each FlowFile. Instead, just list the UUIDs.
                    final String flowFilesDescription = flowFiles.size() == 1
                        ? flowFiles.getFirst().toString()
                        : "FlowFiles [" + flowFiles.stream().map(ff -> ff.getAttribute(CoreAttributes.UUID.key())).collect(Collectors.joining(", ")) + "]";
                    getLogger().error("Failed to send records to Snowpipe Streaming table for {}; routing to failure", flowFilesDescription, e);
                    session.transfer(flowFiles, REL_FAILURE);

                    break;
                }
            }

            for (final Batch batch : batches) {
                processPublishResult(batch.result(), context, session);
            }

            if (totalFlowFiles > 0 && getLogger().isInfoEnabled()) {
                final long millis = System.currentTimeMillis() - startMillis;
                final double seconds = (double) millis / 1000.0;
                final String formattedBytes = String.format("%,d", totalBytesConsumed);
                final double mbps = (totalBytesConsumed / 1024.0 / 1024.0) / seconds;
                final String formattedMbps = String.format("%.2f", mbps);
                final int recordsPerSecond = (int) (totalRecords / seconds);
                final long numTables = batches.stream()
                    .map(Batch::request)
                    .map(request -> request.channel().getCoordinates().table())
                    .distinct()
                    .count();

                getLogger().info("Successfully sent {} FlowFiles ({} bytes / {} records) to {} tables in {} millis at a rate of {} MB/sec ({} records/sec)",
                    totalFlowFiles, formattedBytes, totalRecords, numTables, millis, formattedMbps, recordsPerSecond);
            }
        } finally {
            final Set<SnowpipeStreamingChannel> channelsReturned = new HashSet<>();
            final Set<String> ownedConcurrencyGroups = new HashSet<>();

            for (final Batch batch : batches) {
                final SnowpipeStreamingChannel channel = batch.request().channel();
                channelsReturned.add(channel);

                try {
                    if (channel != null) {
                        ownedConcurrencyGroups.add(channel.getConcurrencyGroup());
                        channel.getManager().returnChannel(channel);
                    }
                } catch (final Exception e) {
                    getLogger().warn("Failed to close Snowpipe Streaming Channel {}", channel.getName(), e);
                }
            }

            // If we have created a PublishRequest and have not yet returned its channel, do so now. This should
            // happen only when an Exception is thrown. We need to ensure that we do not attempt to return the
            // channel multiple times, however, so we need to track the channels that we've already closed and not
            // return the channel associated with the Publish Request if it's already been returned in the block above.
            if (publishRequest != null) {
                ownedConcurrencyGroups.add(publishRequest.concurrencyGroup());

                final SnowpipeStreamingChannel channel = publishRequest.channel();
                if (channel != null && !channelsReturned.contains(channel)) {
                    try {
                        channel.getManager().returnChannel(channel);
                    } catch (final IOException e) {
                        getLogger().warn("Failed to close Snowpipe Streaming Channel {}", channel.getName(), e);
                    }
                }
            }

            // Commit the session and whether we are successful or not, ensure that we release the claims on our concurrency groups.
            // It is important that we do this in the session commit callback, however, because if we route FlowFiles to failure, and the Processor
            // is configured to retry failure with Yield, we want to ensure that we re-queue those FlowFiles before releasing the claim. This ensures
            // that a second thread is not able to process another FlowFile in the same Concurrency Group.
            // I.e., it avoids the following scenario:
            //
            // Thread 1, time t = 1
            // --------------------
            // Get FlowFIle 1 - Table my_table, generation 5
            // Obtain Concurrency Claim
            // Fail to open channel because Table Does Not Exist
            // Release Concurrency Claim
            //
            // Thread 2, time t = 2
            // --------------------
            // Get FlowFile 2 - Table my_table generation 7
            // Obtain Concurrency Claim
            // Open Channel
            // Write Data
            // Release Concurrency Claim
            //
            // Now, data is not in the correct order.
            if (concurrencyManager != null) {
                session.commitAsync(
                    () -> ownedConcurrencyGroups.forEach(concurrencyManager::releaseClaim),
                    failureReason -> ownedConcurrencyGroups.forEach(concurrencyManager::releaseClaim));
            }
        }
    }

    private void failRequest(final PublishRequest publishRequest, final ProcessSession session, final Throwable failureCause, final String action) {
        final List<FlowFile> flowFiles = publishRequest.flowFiles();
        final ChannelCoordinates coordinates = publishRequest.coordinates();
        getLogger().error("Failed to {} for {}.{}.{} with prefix [{}] and index {} for {}; routing to failure",
            action, coordinates.database(), coordinates.schema(), coordinates.table(), coordinates.prefix(), coordinates.index(), flowFiles, failureCause);

        session.transfer(flowFiles, REL_FAILURE);
    }

    private long getDataSize(final PublishRequest publishRequest) {
        long total = 0L;
        for (final FlowFile flowFile : publishRequest.flowFiles()) {
            total += flowFile.getSize();
        }
        return total;
    }

    PropertyEvaluator createEvaluator(final PropertyValue propertyValue) throws UnknownHostException {
        if (!propertyValue.isExpressionLanguagePresent()) {
            return new ExplicitValueEvaluator(propertyValue.getValue());
        }

        final String explicitValue = propertyValue.getValue();
        if (explicitValue.isEmpty()) {
            return new ExplicitValueEvaluator("");
        }

        if (explicitValue.equals("${hostname(false)}")) {
            return new ExplicitValueEvaluator(getHostname());
        }

        final Matcher matcher = ATTRIBUTE_PATTERN.matcher(explicitValue);
        if (matcher.matches()) {
            return new AttributeValueEvaluator(matcher.group(1));
        }

        return new PropertyValueEvaluator(propertyValue);
    }

    private String getHostname() throws UnknownHostException {
        final String hostname = InetAddress.getLocalHost().getHostName();
        if (hostname.contains(".")) {
            return hostname.substring(0, hostname.indexOf('.'));
        }
        return hostname;
    }

    private static String getChannelName(final String prefix, final String channelIndex) {
        return prefix + "." + channelIndex;
    }

    private void processPublishResult(final PublishResult publishResult, final ProcessContext context, final ProcessSession session) {
        final PublishRequest request = publishResult.getPublishRequest();
        final SnowpipeStreamingChannel snowpipeStreamingChannel = request.channel();
        final List<FlowFile> flowFiles = request.flowFiles();
        final Throwable failureCause = publishResult.getFailureCause();

        // Ensure that we close the channel before transferring to 'success' because if there is a failure during the flush/close of the channel,
        // we have to instead route to 'failure'.
        try {
            snowpipeStreamingChannel.close();
        } catch (final IOException e) {
            getLogger().error("Failed to close Snowpipe Streaming Channel for {}; routing {} FlowFiles to failure",
                snowpipeStreamingChannel.getCoordinates(), flowFiles.size(), e);

            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        if (failureCause != null) {
            getLogger().error("Failed to send records to Snowpipe Streaming Table [{}]", snowpipeStreamingChannel.getDestinationName(), failureCause);
            session.transfer(flowFiles, REL_FAILURE);
            return;
        }

        session.transfer(flowFiles, REL_SUCCESS);

        final String account = context.getProperty(ACCOUNT).evaluateAttributeExpressions().getValue();
        final String hostname = ACCOUNT_HOST_FORMAT.formatted(account);
        final ChannelCoordinates coordinates = snowpipeStreamingChannel.getCoordinates();
        final String snowflakeUri = "https://%s/%s/%s/%s".formatted(hostname, coordinates.database(), coordinates.schema(), coordinates.table());

        for (final FlowFile flowFile : flowFiles) {
            session.getProvenanceReporter().send(flowFile, snowflakeUri);
        }
    }

    private PublishResult publish(final PublishRequest publishRequest) {
        try {
            final UploadResult uploadResult = uploadRecordsToChannel(publishRequest);
            publishRequest.channel().updateLocalOffset(uploadResult.offset());
            return new PublishResult(publishRequest, uploadResult);
        } catch (final Throwable t) {
            return new PublishResult(publishRequest, t);
        }
    }

    private UploadResult uploadRecordsToChannel(final PublishRequest publishRequest) throws IOException {
        final List<FlowFile> flowFiles = publishRequest.flowFiles();
        final ProcessSession session = publishRequest.session();
        final SnowpipeStreamingChannel channel = publishRequest.channel();

        try (final FlowFileRecordHolderIterator recordIterator = new FlowFileRecordHolderIterator(getLogger(), flowFiles, recordReaderFactory, session)) {
            final RecordRowConverter recordRowConverter = new StandardRecordRowConverter(recordIterator.getSchema(), channel.getIngestChannel().getTableSchema());
            final List<Map<String, Object>> snowflakeRows = new ArrayList<>();
            final Queue<Map<String, Object>> reusableSnowflakeRows = new LinkedBlockingQueue<>(maxBatchSize);

            BigInteger channelOffset = channel.getLocalOffset();
            long recordsSent = 0;
            long recordIndex = 0;
            long recordsSkipped = 0;

            try {
                FlowFile lastFlowFile = null;
                while (recordIterator.hasNext()) {
                    final FlowFileRecordHolder flowFileRecordHolder = recordIterator.next();

                    // Determine the index for this Record
                    final Record record = flowFileRecordHolder.record();
                    final FlowFile flowFile = flowFileRecordHolder.flowFile();
                    if (flowFile != lastFlowFile) {
                        recordIndex = 0;
                        lastFlowFile = flowFile;
                    }

                    // If the offset is less than the current channel offset, skip this record.
                    final BigInteger offset = recordOffsetFunction.getOffset(record, flowFileRecordHolder.flowFile(), recordIndex++);
                    if (exactlyOnce && offset.compareTo(channelOffset) <= 0) {
                        getLogger().debug("Skipping record for {} because its offset is less than the current channel offset", flowFileRecordHolder.flowFile());
                        recordsSkipped++;
                        continue;
                    }

                    if (snowflakeRows.size() >= maxBatchSize) {
                        final InsertValidationResponse validationResponse = channel.getIngestChannel().insertRows(snowflakeRows, String.valueOf(channelOffset));

                        // If response indicates that there are failures, throw an Exception
                        if (validationResponse.hasErrors()) {
                            throw new ProcessException("Error inserting records into Snowflake for " + flowFiles,
                                validationResponse.getInsertErrors().getFirst().getException());
                        }

                        recordsSent += snowflakeRows.size();

                        // Re-queue the Map objects for reuse.
                        for (final Map<String, Object> row : snowflakeRows) {
                            row.clear();
                            reusableSnowflakeRows.offer(row);
                        }
                        snowflakeRows.clear();
                    }

                    Map<String, Object> rowData = reusableSnowflakeRows.poll();
                    if (rowData == null) {
                        rowData = new HashMap<>();
                    }

                    recordRowConverter.convertRecord(record, rowData);
                    snowflakeRows.add(rowData);
                    channelOffset = offset;
                }

                if (!snowflakeRows.isEmpty()) {
                    recordsSent += snowflakeRows.size();
                    final InsertValidationResponse validationResponse = channel.getIngestChannel().insertRows(snowflakeRows, String.valueOf(channelOffset));

                    // Ensure that there were no errors and throw an Exception if there are.
                    if (validationResponse.hasErrors()) {
                        throw new ProcessException("Error inserting records into Snowflake for " + flowFiles,
                            validationResponse.getInsertErrors().getFirst().getException());
                    }
                }

                return new UploadResult(recordsSent, channelOffset, recordsSkipped);
            } finally {
                session.adjustCounter("Records sent", recordsSent, true);
                session.adjustCounter("FlowFiles sent", flowFiles.size(), true);
                session.adjustCounter("Records sent to " + channel.getDestinationName(), recordsSent, true);
                session.adjustCounter("FlowFiles sent to " + channel.getDestinationName(), flowFiles.size(), true);
                session.adjustCounter("Duplicate Records skipped", recordsSkipped, true);
            }
        }
    }

    protected SnowflakeStreamingIngestClient getClient(final ProcessContext context) {
        final Properties props = new Properties();
        final String user = context.getProperty(USER).evaluateAttributeExpressions().getValue();
        final String role = context.getProperty(ROLE).evaluateAttributeExpressions().getValue();
        final String account = context.getProperty(ACCOUNT).evaluateAttributeExpressions().getValue();
        final String accountUrl = ACCOUNT_HOST_FORMAT.formatted(account);
        props.put(Constants.USER, user);
        props.put(Constants.ACCOUNT_URL, accountUrl);
        props.put(Constants.ROLE, role);

        final PrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class);
        final String privateKeyEncoded = Base64.getEncoder().encodeToString(privateKeyService.getPrivateKey().getEncoded());
        props.put(Constants.AUTHORIZATION_TYPE, Constants.JWT);
        props.put(Constants.PRIVATE_KEY, privateKeyEncoded);
        props.put(ParameterProvider.ENABLE_ICEBERG_STREAMING, context.getProperty(ICEBERG_ENABLED));
        final Map<String, Object> parameterOverrides = Map.of(
            ParameterProvider.MAX_CLIENT_LAG, context.getProperty(CLIENT_LAG).asTimePeriod(TimeUnit.MILLISECONDS),
            ParameterProvider.IO_TIME_CPU_RATIO, 0 // Avoid creating more threads than are needed
        );

        return SnowflakeStreamingIngestClientFactory.builder(context.getName())
                .setProperties(props)
                .setParameterOverrides(parameterOverrides)
                .build();
    }

    private SnowpipeStreamingChannelManager getChannelManager(final ChannelCoordinates channelCoordinates) {
        final String channelIndex = channelCoordinates.index();
        if (channelIndex != null && !channelIndex.isEmpty()) {
            return channelManagers.computeIfAbsent(channelCoordinates,
                k -> new SingleSnowpipeStreamingChannelManager(new StandardSnowpipeStreamingChannelFactory(client, channelCoordinates, OnErrorOption.ABORT), channelIndex));
        }

        final ComponentLog logger = getLogger();
        return channelManagers.computeIfAbsent(channelCoordinates, k -> new QueuedSnowpipeStreamingChannelManager(
                new StandardSnowpipeStreamingChannelFactory(client, channelCoordinates, OnErrorOption.ABORT),
                concurrencyManager,
                logger
                )
        );
    }

    private PublishRequest createPublishRequest(final ProcessSession session, final ConcurrencyClaim concurrencyClaim) {
        final PrefixFlowFileFilter flowFileFilter = createFlowFileFilter(concurrencyClaim);
        final List<FlowFile> flowFiles = session.get(flowFileFilter);

        final ChannelPromise promise = flowFileFilter.getChannelPromise();
        final String concurrencyGroup;
        final SnowpipeStreamingChannel channel;
        if (promise == null) {
            channel = null;
            concurrencyGroup = null;
        } else {
            concurrencyGroup = promise.getConcurrencyGroup();

            try {
                channel = promise.getChannel();
            } catch (final Exception e) {
                return new PublishRequest(session, flowFiles, flowFileFilter.getChosenCoordinates(), null, promise.getConcurrencyGroup(), e);
            }
        }

        return new PublishRequest(session, flowFiles, flowFileFilter.getChosenCoordinates(), channel, concurrencyGroup, flowFileFilter.getFailureCause());
    }

    PrefixFlowFileFilter createFlowFileFilter(final ConcurrencyClaim concurrencyClaim) {
        return new PrefixFlowFileFilter(concurrencyClaim);
    }

    private ChannelCoordinates getChannelCoordinates(final FlowFile flowFile) {
        final String database = databasePropertyValue.evaluate(flowFile);
        final String schema = schemaPropertyValue.evaluate(flowFile);
        final String table = tablePropertyValue.evaluate(flowFile);
        final String prefix = prefixPropertyValue.evaluate(flowFile);
        // If using exactly once, we need to specify a channel index so that we can ensure that the same FlowFile goes to the same channel on retry.
        final String channelIndex = exactlyOnce ? channelIndexPropertyValue.evaluate(flowFile) : null;

        return new ChannelCoordinates(database, schema, table, prefix, channelIndex);
    }

    private ChannelPromise getAvailableChannel(final FlowFile flowFile, final ConcurrencyClaimRequest claimRequest) {
        final ChannelCoordinates coordinates = getChannelCoordinates(flowFile);
        final SnowpipeStreamingChannelManager channelManager = getChannelManager(coordinates);
        return channelManager.getChannel(claimRequest).orElse(null);
    }

    private static class StandardSnowpipeStreamingChannelFactory implements SnowpipeStreamingChannelFactory {
        private final SnowflakeStreamingIngestClient client;
        private final ChannelCoordinates channelCoordinates;
        private final OnErrorOption onErrorOption;

        public StandardSnowpipeStreamingChannelFactory(final SnowflakeStreamingIngestClient client, final ChannelCoordinates channelCoordinates,
                                                       final OnErrorOption onErrorOption) {

            this.client = client;
            this.channelCoordinates = channelCoordinates;
            this.onErrorOption = onErrorOption;
        }

        @Override
        public SnowpipeStreamingChannel openChannel(final String index, final String concurrencyGroup, final SnowpipeStreamingChannelManager manager) {
            final String prefix = channelCoordinates.prefix();
            final String database = channelCoordinates.database();
            final String schema = channelCoordinates.schema();
            final String table = channelCoordinates.table();

            final String channelName = getChannelName(prefix, index);
            final OpenChannelRequest openChannelRequest = OpenChannelRequest.builder(channelName)
                .setDBName(database)
                .setSchemaName(schema)
                .setTableName(table)
                .setOnErrorOption(onErrorOption)
                .build();

            try {
                final SnowflakeStreamingIngestChannel ingestChannel = client.openChannel(openChannelRequest);
                final String destinationName = database + "." + schema + "." + table;
                final ChannelCoordinates coordinates = new ChannelCoordinates(database, schema, table, prefix, index);

                return new StandardSnowpipeStreamingChannel(ingestChannel, coordinates, manager, destinationName, concurrencyGroup);
            } catch (final SFException e) {
                throw RUNTIME_EXCEPTION_CONVERTER.convertException(e);
            }
        }

        @Override
        public ChannelCoordinates getCoordinates(final String channelIndex) {
            return new ChannelCoordinates(
                channelCoordinates.database(),
                channelCoordinates.schema(),
                channelCoordinates.table(),
                channelCoordinates.prefix(),
                channelIndex
            );
        }
    }

    private String getExplanation(final Exception e) {
        final StringBuilder explanation = new StringBuilder();
        explanation.append(e.getClass().getSimpleName());

        final String message = e.getMessage();
        if (message != null && !message.isBlank()) {
            explanation.append(SPACE_SEPARATOR);
            explanation.append(message);
        }

        return explanation.toString();
    }

    /**
     * A mechanism for determining the channel offset that applies to a given Record. This is necessary when using Exactly Once delivery to Snowflake.
     * to ensure that any duplicates are not sent to Snowflake.
     */
    private interface RecordOffsetFunction {
        BigInteger getOffset(Record record, FlowFile flowFile, long recordIndex);
    }

    private static class NopRecordOffsetFunction implements RecordOffsetFunction {
        @Override
        public BigInteger getOffset(final Record record, final FlowFile flowFile, final long recordIndex) {
            return BigInteger.ZERO;
        }
    }

    private static class ExpressionLanguageOffsetFunction implements RecordOffsetFunction {
        private final PropertyValue firstOffsetPropertyValue;

        public ExpressionLanguageOffsetFunction(final PropertyValue firstOffsetPropertyValue) {
            this.firstOffsetPropertyValue = firstOffsetPropertyValue;
        }

        @Override
        public BigInteger getOffset(final Record record, final FlowFile flowFile, final long recordIndex) {
            final String firstOffset = firstOffsetPropertyValue.evaluateAttributeExpressions(flowFile).getValue();
            try {
                return new BigInteger(firstOffset).add(BigInteger.valueOf(recordIndex));
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("Failed to parse offset [%s] for record [%d] of [%s]".formatted(firstOffset, recordIndex, flowFile), e);
            }
        }
    }

    static class RecordPathRecordOffsetFunction implements RecordOffsetFunction {
        private final RecordPath recordPath;

        public RecordPathRecordOffsetFunction(final String recordPath) {
            this.recordPath = RecordPath.compile(recordPath);
        }

        @Override
        public BigInteger getOffset(final Record record, final FlowFile flowFile, final long recordIndex) {
            return recordPath.evaluate(record).getSelectedFields()
                .map(field -> getOffsetValue(field.getValue(), flowFile, recordIndex))
                .findFirst()
                .orElse(BigInteger.ZERO);
        }

        private BigInteger getOffsetValue(final Object value, final FlowFile flowFile, final long recordIndex) {
            return switch (value) {
                case null -> BigInteger.ZERO;
                case final Number number -> BigInteger.valueOf(number.longValue());
                case final String string -> new BigInteger(string);
                default -> throw new IllegalArgumentException("Found unexpected type for offset [%s] for record %s of %s".formatted(value, recordIndex, flowFile));
            };
        }
    }

    /**
     * A mechanism for evaluating a property value based on a FlowFile. Because we may need to evaluate Expression Language
     * from within a FlowFileFilter, we need to ensure that we evaluate it as quickly as possible. A value such as `${my.attribute}` can
     * be evaluated more trivially than evaluating the entire Expression Language by determining that it's a simple attribute reference and
     * simply returning the value of that attribute. While EL Evaluation performance is not typically a concern, it is here because we could
     * constantly evaluate a flowfile, reject it, and move on to the next - meaning we could evaluate EL millions of times even for a small
     * number of (selected) FlowFiles.
     */
    interface PropertyEvaluator {
        String evaluate(FlowFile flowFile);
    }

    private static class PropertyValueEvaluator implements PropertyEvaluator {
        private final PropertyValue propertyValue;

        public PropertyValueEvaluator(final PropertyValue propertyValue) {
            this.propertyValue = propertyValue;
        }

        @Override
        public String evaluate(final FlowFile flowFile) {
            return propertyValue.evaluateAttributeExpressions(flowFile).getValue();
        }
    }

    private static class AttributeValueEvaluator implements PropertyEvaluator {
        private final String attributeName;

        public AttributeValueEvaluator(final String attributeName) {
            this.attributeName = attributeName;
        }

        @Override
        public String evaluate(final FlowFile flowFile) {
            final String value = flowFile.getAttribute(attributeName);
            return value == null ? "" : value;
        }
    }

    private static class ExplicitValueEvaluator implements PropertyEvaluator {
        private final String value;

        public ExplicitValueEvaluator(final String value) {
            this.value = value;
        }

        @Override
        public String evaluate(final FlowFile flowFile) {
            return value;
        }
    }

    private record UploadResult(long recordsSent, BigInteger offset, long recordsSkipped) {
    }

    private record PublishRequest(ProcessSession session, List<FlowFile> flowFiles, ChannelCoordinates coordinates, SnowpipeStreamingChannel channel, String concurrencyGroup,
                                  Throwable failureCause) {
    }

    private record Batch(PublishRequest request, PublishResult result) {
    }

    private static class PublishResult {
        private final PublishRequest publishRequest;
        private final Throwable failureCause;
        private final UploadResult uploadResult;

        public PublishResult(final PublishRequest publishRequest, final Throwable failureCause) {
            this.publishRequest = publishRequest;
            this.failureCause = failureCause;
            this.uploadResult = null;
        }

        public PublishResult(final PublishRequest publishRequest, final UploadResult uploadResult) {
            this.publishRequest = publishRequest;
            this.failureCause = null;
            this.uploadResult = uploadResult;
        }

        public PublishRequest getPublishRequest() {
            return this.publishRequest;
        }

        public Throwable getFailureCause() {
            return failureCause;
        }

        public UploadResult getUploadResult() {
            return uploadResult;
        }
    }

    class PrefixFlowFileFilter implements FlowFileFilter {
        private final ConcurrencyClaim concurrencyClaim;

        private final Set<String> rejectedConcurrencyGroups = new HashSet<>();
        private ChannelPromise channelPromise = null;
        private int flowFileCount = 0;
        private long byteCount = 0L;
        private Throwable failureCause;

        public PrefixFlowFileFilter(final ConcurrencyClaim concurrencyClaim) {
            this.concurrencyClaim = concurrencyClaim;
        }

        public Throwable getFailureCause() {
            return failureCause;
        }

        public ChannelPromise getChannelPromise() {
            return channelPromise;
        }

        public ChannelCoordinates getChosenCoordinates() {
            return channelPromise == null ? null : channelPromise.getCoordinates();
        }

        @Override
        public FlowFileFilterResult filter(final FlowFile flowFile) {
            // If we have not yet chosen a channel, reject FlowFiles until we find one whose channel is available.
            if (channelPromise == null) {
                // If a Concurrency Group is specified, don't try to get a channel for a Concurrency Group we've already been rejected from.
                // This is necessary to ensure that we don't change the ordering of incoming FlowFiles.
                final String concurrencyGroup;
                if (concurrencyGroupEvaluator == null) {
                    concurrencyGroup = "";
                } else {
                    concurrencyGroup = concurrencyGroupEvaluator.evaluate(flowFile);
                    if (rejectedConcurrencyGroups.contains(concurrencyGroup)) {
                        return FlowFileFilterResult.REJECT_AND_CONTINUE;
                    }
                }

                final ConcurrencyClaimRequest claimRequest = new ConcurrencyClaimRequest(concurrencyGroup, concurrencyClaim);

                final ChannelPromise channelPromise;
                try {
                    channelPromise = getAvailableChannel(flowFile, claimRequest);
                } catch (final Throwable t) {
                    failureCause = t;
                    return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                }

                if (channelPromise == null) {
                    if (concurrencyGroupEvaluator != null) {
                        rejectedConcurrencyGroups.add(concurrencyGroup);
                    }

                    return FlowFileFilterResult.REJECT_AND_CONTINUE;
                }

                this.channelPromise = channelPromise;
                flowFileCount++;
                byteCount += flowFile.getSize();

                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }

            // We now have an available channel. Accept any and all FlowFiles that belong to that channel, up to the maximum batch size.
            final ChannelCoordinates flowFileCoordinates = getChannelCoordinates(flowFile);
            final boolean compatible;
            if (exactlyOnce) {
                compatible = channelPromise.getCoordinates().equals(flowFileCoordinates);
            } else {
                compatible = channelPromise.getCoordinates().equalsIgnoringIndex(flowFileCoordinates);
            }

            if (!compatible) {
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }

            flowFileCount++;
            byteCount += flowFile.getSize();

            if (flowFileCount >= MAX_FLOWFILES_PER_BATCH || byteCount >= MAX_BYTES_PER_BATCH) {
                return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
            }

            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
        }
    }
}

