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
package org.apache.nifi.kafka.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.components.ConnectorMethod;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.GroupType;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.processors.consumer.bundle.ByteRecordBundler;
import org.apache.nifi.kafka.processors.consumer.convert.FlowFileStreamKafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.InjectOffsetRecordStreamKafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.KafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.RecordStreamKafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.WrapperRecordStreamKafkaMessageConverter;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.consumer.RebalanceCallback;
import org.apache.nifi.kafka.service.api.consumer.SessionContext;
import org.apache.nifi.kafka.service.api.consumer.share.Acknowledgement;
import org.apache.nifi.kafka.service.api.consumer.share.KafkaShareConsumerService;
import org.apache.nifi.kafka.service.api.consumer.share.ShareAcknowledgementMode;
import org.apache.nifi.kafka.service.api.consumer.share.ShareGroupContext;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

@CapabilityDescription("Consumes messages from Apache Kafka Consumer API. "
        + "The complementary NiFi processor for sending messages is PublishKafka. The Processor supports consumption of Kafka messages, optionally interpreted as NiFi records. "
        + "By default the Processor uses a classic consumer group, which assigns whole partitions to consumers. "
        + "When configured for 'Share Group' the Processor uses Kafka share groups (KIP-932, requires Kafka 4.2+ brokers configured for share-group operation), "
        + "which distribute records cooperatively across the consumers of a share group with per-record acknowledgement instead of per-partition offset commits. "
        + "Please note that, at this time (in read record mode), the Processor assumes that "
        + "all records that are retrieved from a given partition have the same schema. For this mode, if any of the Kafka messages are pulled but cannot be parsed or written with the "
        + "configured Record Reader or Record Writer, the contents of the message will be written to a separate FlowFile, and that FlowFile will be transferred to the "
        + "'parse.failure' relationship. Otherwise, each FlowFile is sent to the 'success' relationship and may contain many individual messages within the single FlowFile. "
        + "A 'record.count' attribute is added to indicate how many messages are contained in the FlowFile. No two Kafka messages will be placed into the same FlowFile if they "
        + "have different schemas, or if they have different values for a message header that is included by the <Headers to Add as Attributes> property.")
@Tags({"Kafka", "Get", "Record", "csv", "avro", "json", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records received"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that is provided by the configured Record Writer"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_COUNT, description = "The number of records in the FlowFile for a batch of records"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_KEY, description = "The key of message if present and if single message. "
                + "How the key is encoded depends on the value of the 'Key Attribute Encoding' property."),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_OFFSET, description = "The offset of the record in the partition or the minimum value of the offset in a batch of records"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TIMESTAMP, description = "The timestamp of the message consumed from the topic or the minimum value of the timestamp "
                + "in a batch of messages. The value of this timestamp depends on 'log.message.timestamp.type` kafka broker config (LOG_APPEND_TIME, CREATE_TIME, NO_TIMESTAMP_TYPE)"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_PARTITION, description = "The partition of the topic for a record or batch of records"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TOPIC, description = "The topic the for a record or batch of records"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TOMBSTONE, description = "Set to true if the consumed message is a tombstone message"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_MAX_OFFSET, description = "The maximum value of the Kafka offset in batch of records")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SeeAlso({PublishKafka.class})
public class ConsumeKafka extends AbstractProcessor implements VerifiableProcessor {

    static final AllowableValue TOPIC_NAME = new AllowableValue("names", "names", "Topic is a full topic name or comma separated list of names");
    static final AllowableValue TOPIC_PATTERN = new AllowableValue("pattern", "pattern", "Topic is a regular expression according to the Java Pattern syntax");

    static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Kafka Connection Service")
            .description("Provides connections to Kafka Broker for publishing Kafka Records")
            .identifiesControllerService(KafkaConnectionService.class)
            .expressionLanguageSupported(NONE)
            .required(true)
            .build();

    static final PropertyDescriptor GROUP_TYPE = new PropertyDescriptor.Builder()
            .name("Group Type")
            .description("Selects the Kafka consumer group model. Choose 'Consumer Group' for the classic offset-committed model used by NiFi historically. "
                    + "Choose 'Share Group' to use Kafka share groups (KIP-932), which require Kafka 4.1+ brokers configured for share-group operation and "
                    + "deliver records cooperatively with per-record acknowledgement instead of per-partition offset commits. "
                    + "When 'Share Group' is selected the [Acknowledgement Mode] property controls how records are acknowledged to the broker.")
            .required(true)
            .allowableValues(GroupType.class)
            .defaultValue(GroupType.CONSUMER)
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("Group ID")
            .description("Kafka group identifier. For 'Consumer Group' this corresponds to the Kafka group.id property. "
                    + "For 'Share Group' this is the share-group identifier used by the broker to track per-record acknowledgements.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor ACKNOWLEDGEMENT_MODE = new PropertyDescriptor.Builder()
            .name("Acknowledgement Mode")
            .description("Controls how share-group records are acknowledged to the broker. "
                    + "'Explicit' (the default) acknowledges every delivered record individually so a session rollback can release records back to the share group "
                    + "for immediate redelivery to another consumer. "
                    + "'Implicit' relies on the broker's default behavior of accepting all delivered records on the next poll or commit; "
                    + "in this mode a session rollback cannot actively release records, and they only become eligible for redelivery once the broker's "
                    + "acquisition lock expires (controlled by the broker-level 'group.share.record.lock.duration.ms' configuration).")
            .required(true)
            .allowableValues(ShareAcknowledgementMode.class)
            .defaultValue(ShareAcknowledgementMode.EXPLICIT)
            .dependsOn(GROUP_TYPE, GroupType.SHARE)
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor TOPIC_FORMAT = new PropertyDescriptor.Builder()
            .name("Topic Format")
            .description("Specifies whether the Topics provided are a comma separated list of names or a single regular expression. "
                    + "Pattern subscription is not available for share groups in Kafka 4.1.")
            .required(true)
            .allowableValues(TOPIC_NAME, TOPIC_PATTERN)
            .defaultValue(TOPIC_NAME)
            .dependsOn(GROUP_TYPE, GroupType.CONSUMER)
            .build();

    static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("Topics")
            .description("The name or pattern of the Kafka Topics from which the Processor consumes Kafka Records. More than one can be supplied if comma separated.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name("auto.offset.reset")
            .displayName("Auto Offset Reset")
            .description("Automatic offset configuration applied when no previous consumer offset found corresponding to Kafka auto.offset.reset property. "
                    + "Not applicable to share groups, which manage starting positions at the group level via Kafka admin tools.")
            .required(true)
            .allowableValues(AutoOffsetReset.class)
            .defaultValue(AutoOffsetReset.LATEST)
            .expressionLanguageSupported(NONE)
            .dependsOn(GROUP_TYPE, GroupType.CONSUMER)
            .build();

    static final PropertyDescriptor COMMIT_OFFSETS = new PropertyDescriptor.Builder()
            .name("Commit Offsets")
            .description("Specifies whether this Processor should commit the offsets to Kafka after receiving messages. Typically, this value should be set to true " +
                    "so that messages that are received are not duplicated. However, in certain scenarios, we may want to avoid committing the offsets, that the data can be " +
                    "processed and later acknowledged by PublishKafka in order to provide Exactly Once semantics. "
                    + "Not applicable to share groups, which acknowledge per record rather than committing offsets.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .dependsOn(GROUP_TYPE, GroupType.CONSUMER)
            .build();

    static final PropertyDescriptor MAX_UNCOMMITTED_SIZE = new PropertyDescriptor.Builder()
            .name("Max Uncommitted Size")
            .description("""
                    Maximum total size of records to consume from Kafka before transferring FlowFiles to an output
                    relationship. Evaluated when specified based on the size of serialized keys and values from each
                    Kafka record, before reaching the [Max Uncommitted Time].
                    """
            )
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .dependsOn(GROUP_TYPE, GroupType.CONSUMER)
            .build();

    static final PropertyDescriptor MAX_UNCOMMITTED_TIME = new PropertyDescriptor.Builder()
            .name("Max Uncommitted Time")
            .description("""
                    Maximum amount of time to spend consuming records from Kafka before transferring FlowFiles to an
                    output relationship. Longer amounts of time may produce larger FlowFiles and increase processing
                    latency for individual records.
                    """
            )
            .required(true)
            .defaultValue("100 millis")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("Header Encoding")
            .description("Character encoding applied when reading Kafka Record Header values and writing FlowFile attributes")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .required(true)
            .build();

    static final PropertyDescriptor HEADER_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("Header Name Pattern")
            .description("Regular Expression Pattern applied to Kafka Record Header Names for selecting Header Values to be written as FlowFile attributes")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor PROCESSING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Processing Strategy")
            .description("Strategy for processing Kafka Records and writing serialized output to FlowFiles")
            .required(true)
            .allowableValues(ProcessingStrategy.class)
            .defaultValue(ProcessingStrategy.FLOW_FILE)
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor HEADER_NAME_PREFIX = new PropertyDescriptor.Builder()
            .name("Header Name Prefix")
            .description("""
                    A prefix to apply to the FlowFile attribute name when writing Kafka Record Header values.
                    This is useful to avoid conflicts with reserved FlowFile attribute names such as 'uuid'.
                    For example, if set to 'kafka.header.', a Kafka header named 'uuid' would be written as 'kafka.header.uuid'.
                    """)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.FLOW_FILE)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for incoming Kafka messages")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer to use in order to serialize the outgoing FlowFiles")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Strategy")
            .description("The format used to output the Kafka Record into a FlowFile Record.")
            .required(true)
            .defaultValue(OutputStrategy.USE_VALUE)
            .allowableValues(OutputStrategy.class)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
            .name("Key Attribute Encoding")
            .description("Encoding for value of configured FlowFile attribute containing Kafka Record Key.")
            .required(true)
            .defaultValue(KeyEncoding.UTF8)
            .allowableValues(KeyEncoding.class)
            .build();

    static final PropertyDescriptor KEY_FORMAT = new PropertyDescriptor.Builder()
            .name("Key Format")
            .description("Specifies how to represent the Kafka Record Key in the output FlowFile")
            .required(true)
            .defaultValue(KeyFormat.BYTE_ARRAY)
            .allowableValues(KeyFormat.class)
            .dependsOn(OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER, OutputStrategy.INJECT_METADATA)
            .build();

    static final PropertyDescriptor KEY_RECORD_READER = new PropertyDescriptor.Builder()
            .name("Key Record Reader")
            .description("The Record Reader to use for parsing the Kafka Record Key into a Record")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .dependsOn(KEY_FORMAT, KeyFormat.RECORD)
            .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .required(true)
            .addValidator(Validator.VALID)
            .description("Since KafkaConsumer receives messages in batches, this Processor has an option to output FlowFiles which contains "
                    + "all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use "
                    + "for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received "
                    + "will result in a single FlowFile which  "
                    + "time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR)
            .build();

    static final PropertyDescriptor SEPARATE_BY_KEY = new PropertyDescriptor.Builder()
            .name("Separate By Key")
            .description("When this property is enabled, two messages will only be added to the same FlowFile if both of the Kafka Messages have identical keys.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(MESSAGE_DEMARCATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing one or more serialized Kafka Records")
            .build();

    public static final Relationship PARSE_FAILURE = new Relationship.Builder()
            .name("parse failure")
            .description("If configured to use a Record Reader, a Kafka message that cannot be parsed using the configured Record Reader will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_SERVICE,
            GROUP_TYPE,
            GROUP_ID,
            ACKNOWLEDGEMENT_MODE,
            TOPIC_FORMAT,
            TOPICS,
            AUTO_OFFSET_RESET,
            COMMIT_OFFSETS,
            MAX_UNCOMMITTED_SIZE,
            MAX_UNCOMMITTED_TIME,
            HEADER_NAME_PATTERN,
            HEADER_ENCODING,
            PROCESSING_STRATEGY,
            HEADER_NAME_PREFIX,
            RECORD_READER,
            RECORD_WRITER,
            OUTPUT_STRATEGY,
            KEY_ATTRIBUTE_ENCODING,
            KEY_FORMAT,
            KEY_RECORD_READER,
            MESSAGE_DEMARCATOR,
            SEPARATE_BY_KEY
    );

    private static final Set<Relationship> SUCCESS_RELATIONSHIP = Set.of(SUCCESS);
    private static final Set<Relationship> SUCCESS_FAILURE_RELATIONSHIPS = Set.of(SUCCESS, PARSE_FAILURE);

    private volatile Charset headerEncoding;
    private volatile Pattern headerNamePattern;
    private volatile String headerNamePrefix;
    private volatile ProcessingStrategy processingStrategy;
    private volatile KeyEncoding keyEncoding;
    private volatile OutputStrategy outputStrategy;
    private volatile KeyFormat keyFormat;
    private volatile boolean commitOffsets;
    private volatile boolean useReader;
    private volatile String brokerUri;
    private volatile GroupType groupType;
    private volatile PollingContext pollingContext;
    private volatile ShareGroupContext shareGroupContext;
    private volatile ShareAcknowledgementMode shareAcknowledgementMode;
    private volatile int maxConsumerCount;
    private volatile boolean maxUncommittedSizeConfigured;
    private volatile long maxUncommittedSize;

    private final Queue<KafkaConsumerService> consumerServices = new LinkedBlockingQueue<>();
    private final Queue<KafkaShareConsumerService> shareConsumerServices = new LinkedBlockingQueue<>();
    private final AtomicInteger activeConsumerCount = new AtomicInteger();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return useReader ? SUCCESS_FAILURE_RELATIONSHIPS : SUCCESS_RELATIONSHIP;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(RECORD_READER)) {
            useReader = newValue != null;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        groupType = context.getProperty(GROUP_TYPE).asAllowableValue(GroupType.class);
        if (groupType == GroupType.SHARE) {
            shareAcknowledgementMode = context.getProperty(ACKNOWLEDGEMENT_MODE).asAllowableValue(ShareAcknowledgementMode.class);
            shareGroupContext = createShareGroupContext(context, shareAcknowledgementMode);
            pollingContext = null;
        } else {
            pollingContext = createPollingContext(context);
            shareGroupContext = null;
            shareAcknowledgementMode = null;
        }
        headerEncoding = Charset.forName(context.getProperty(HEADER_ENCODING).getValue());

        final String headerNamePatternProperty = context.getProperty(HEADER_NAME_PATTERN).getValue();
        if (StringUtils.isNotBlank(headerNamePatternProperty)) {
            headerNamePattern = Pattern.compile(headerNamePatternProperty);
        } else {
            headerNamePattern = null;
        }

        keyEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).asAllowableValue(KeyEncoding.class);
        commitOffsets = groupType == GroupType.SHARE || context.getProperty(COMMIT_OFFSETS).asBoolean();
        processingStrategy = context.getProperty(PROCESSING_STRATEGY).asAllowableValue(ProcessingStrategy.class);

        // Only read HEADER_NAME_PREFIX when PROCESSING_STRATEGY is FLOW_FILE (property dependency)
        headerNamePrefix = processingStrategy == ProcessingStrategy.FLOW_FILE
                ? context.getProperty(HEADER_NAME_PREFIX).getValue()
                : null;
        outputStrategy = processingStrategy == ProcessingStrategy.RECORD ? context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class) : null;
        keyFormat = (outputStrategy == OutputStrategy.USE_WRAPPER || outputStrategy == OutputStrategy.INJECT_METADATA)
                ? context.getProperty(KEY_FORMAT).asAllowableValue(KeyFormat.class)
                : KeyFormat.BYTE_ARRAY;
        brokerUri = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class).getBrokerUri();
        maxConsumerCount = context.getMaxConcurrentTasks();
        activeConsumerCount.set(0);

        if (groupType == GroupType.CONSUMER) {
            final PropertyValue maxUncommittedSizeProperty = context.getProperty(MAX_UNCOMMITTED_SIZE);
            maxUncommittedSizeConfigured = maxUncommittedSizeProperty.isSet();
            if (maxUncommittedSizeConfigured) {
                maxUncommittedSize = maxUncommittedSizeProperty.asDataSize(DataUnit.B).longValue();
            }
        } else {
            maxUncommittedSizeConfigured = false;
        }
    }

    @OnStopped
    public void onStopped() {
        KafkaConsumerService service;
        while ((service = consumerServices.poll()) != null) {
            close(service, "Processor stopped");
        }

        KafkaShareConsumerService shareService;
        while ((shareService = shareConsumerServices.poll()) != null) {
            closeShareConsumer(shareService, "Processor stopped");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        if (groupType == GroupType.SHARE) {
            triggerShareGroup(context, session);
            return;
        }

        final KafkaConsumerService consumerService = getConsumerService(context);
        if (consumerService == null) {
            getLogger().debug("No Kafka Consumer Service available; will yield and return immediately");
            context.yield();
            return;
        }

        final long maxUncommittedMillis = context.getProperty(MAX_UNCOMMITTED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final long stopTime = System.currentTimeMillis() + maxUncommittedMillis;
        final OffsetTracker offsetTracker = new OffsetTracker();
        boolean recordsReceived = false;

        final RebalanceSessionHolder sessionHolder = new RebalanceSessionHolder(session, offsetTracker);
        consumerService.setSessionContext(sessionHolder);

        try {
            while (System.currentTimeMillis() < stopTime) {
                try {
                    final Duration maxWaitDuration = Duration.ofMillis(stopTime - System.currentTimeMillis());
                    if (maxWaitDuration.toMillis() <= 0) {
                        break;
                    }

                    final Iterator<ByteRecord> consumerRecords = consumerService.poll(maxWaitDuration).iterator();
                    if (!consumerRecords.hasNext()) {
                        getLogger().trace("No Kafka Records consumed: {}", pollingContext);
                        // Check if a rebalance occurred during poll - if so, break to commit what we have
                        if (consumerService.hasRevokedPartitions()) {
                            getLogger().debug("Rebalance detected with revoked partitions, breaking to commit session");
                            break;
                        }
                        continue;
                    }

                    recordsReceived = true;
                    processConsumerRecords(context, session, offsetTracker, consumerRecords);

                    // Check if a rebalance occurred during poll - if so, break to commit what we have
                    if (consumerService.hasRevokedPartitions()) {
                        getLogger().debug("Rebalance detected with revoked partitions, breaking to commit session");
                        break;
                    }

                    if (maxUncommittedSizeConfigured) {
                        // Stop consuming before reaching Max Uncommitted Time when exceeding Max Uncommitted Size
                        final long totalRecordSize = offsetTracker.getTotalRecordSize();
                        if (totalRecordSize > maxUncommittedSize) {
                            break;
                        }
                    }
                } catch (final Exception e) {
                    getLogger().error("Failed to consume Kafka Records", e);
                    consumerService.rollback();
                    close(consumerService, "Encountered Exception while consuming or writing out Kafka Records");
                    context.yield();
                    // If there are any FlowFiles already created and transferred, roll them back because we're rolling back offsets and
                    // because we will consume the data again, we don't want to transfer out the FlowFiles.
                    session.rollback();
                    return;
                }
            }

            if (!recordsReceived && !consumerService.hasRevokedPartitions()) {
                getLogger().trace("No Kafka Records consumed, re-queuing consumer");
                consumerServices.offer(consumerService);
                return;
            }

            // If no records received but we have revoked partitions, we still need to commit their offsets.
            // Note: When a rebalance callback is registered (which is the case in this processor), offsets for
            // revoked partitions are committed synchronously during onPartitionsRevoked(), so hasRevokedPartitions()
            // will return false. This code path exists for backward compatibility when no callback is registered.
            if (!recordsReceived && consumerService.hasRevokedPartitions()) {
                getLogger().debug("No records received but rebalance occurred, committing offsets for revoked partitions");
                try {
                    consumerService.commitOffsetsForRevokedPartitions();
                } catch (final Exception e) {
                    getLogger().warn("Failed to commit offsets for revoked partitions", e);
                }
                consumerServices.offer(consumerService);
                return;
            }

            session.commitAsync(
                () -> commitOffsets(consumerService, offsetTracker, pollingContext, session),
                throwable -> {
                    getLogger().error("Failed to commit session; will roll back any uncommitted records", throwable);
                    rollback(consumerService, offsetTracker, session);
                    context.yield();
                });
        } finally {
            consumerService.setSessionContext(null);
        }
    }

    private void commitOffsets(final KafkaConsumerService consumerService, final OffsetTracker offsetTracker, final PollingContext pollingContext, final ProcessSession session) {
        try {
            if (commitOffsets) {
                consumerService.commit(offsetTracker.getPollingSummary(pollingContext));

                offsetTracker.getRecordCounts().forEach((topic, count) -> {
                    session.adjustCounter("Records Acknowledged for " + topic, count, true);
                });
            }

            // After successful session commit, also commit offsets for any partitions that were revoked during rebalance.
            // Note: When a rebalance callback is registered, this check will always be false since offsets are
            // committed synchronously during onPartitionsRevoked(). This code path is for backward compatibility.
            if (consumerService.hasRevokedPartitions()) {
                getLogger().debug("Committing offsets for partitions revoked during rebalance");
                consumerService.commitOffsetsForRevokedPartitions();
            }

            consumerServices.offer(consumerService);
            getLogger().debug("Committed offsets for Kafka Consumer Service");
        } catch (final Exception e) {
            getLogger().error("Failed to commit offsets for Kafka Consumer Service; will attempt to rollback to latest committed offsets", e);
            rollback(consumerService, offsetTracker, session);
        }
    }

    private void rollback(final KafkaConsumerService consumerService, final OffsetTracker offsetTracker, final ProcessSession session) {
        if (!consumerService.isClosed()) {
            try {
                // Clear any pending revoked partitions since we're rolling back
                consumerService.clearRevokedPartitions();
                consumerService.rollback();
                consumerServices.offer(consumerService);
                getLogger().debug("Rolled back offsets for Kafka Consumer Service");
            } catch (final Exception e) {
                getLogger().warn("Failed to rollback offsets for Kafka Consumer", e);
                close(consumerService, "Failed to rollback offsets");
            }

            offsetTracker.getRecordCounts().forEach((topic, count) -> {
                session.adjustCounter("Records Rolled Back for " + topic, count, true);
            });
        }
    }

    private void close(final KafkaConsumerService consumerService, final String reason) {
        if (consumerService.isClosed()) {
            getLogger().debug("Asked to close Kafka Consumer Service but consumer already closed");
            return;
        }

        getLogger().info("Closing Kafka Consumer due to: {}", reason);

        try {
            consumerService.close();
            activeConsumerCount.decrementAndGet();
        } catch (final IOException ioe) {
            getLogger().warn("Failed to close Kafka Consumer Service", ioe);
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();

        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        final GroupType verifyGroupType = context.getProperty(GROUP_TYPE).asAllowableValue(GroupType.class);

        if (verifyGroupType == GroupType.SHARE) {
            // Verification always samples-and-RELEASEs records, which requires EXPLICIT acknowledgement
            // regardless of the user-selected acknowledgement mode for runtime processing.
            final ShareGroupContext verifyShareContext = createShareGroupContext(context, ShareAcknowledgementMode.EXPLICIT);
            try (final KafkaShareConsumerService shareConsumerService = connectionService.getShareConsumerService(verifyShareContext)) {
                verificationResults.add(verifyShareGroup(shareConsumerService, verifyShareContext));
            } catch (final UnsupportedOperationException e) {
                verificationResults.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Verify Share Group Subscription")
                        .outcome(Outcome.FAILED)
                        .explanation("Configured Kafka Connection Service does not support share groups: " + e.getMessage())
                        .build());
            } catch (final IOException e) {
                verificationResults.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Communicate with Kafka Broker")
                        .outcome(Outcome.FAILED)
                        .explanation("There was an I/O failure when communicating with Kafka: " + e)
                        .build());
            } catch (final RuntimeException e) {
                verificationResults.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Verify Share Group Subscription")
                        .outcome(Outcome.FAILED)
                        .explanation("Failed to verify the share group subscription against the broker. The broker may not support Kafka share groups or share-group operation may be disabled: " + e)
                        .build());
            }
            return verificationResults;
        }

        final PollingContext pollingContext = createPollingContext(context, null, AutoOffsetReset.EARLIEST);
        try (final KafkaConsumerService consumerService = connectionService.getConsumerService(pollingContext)) {
            final ConfigVerificationResult partitionVerification = verifyPartitions(consumerService, pollingContext);
            verificationResults.add(partitionVerification);

            final ConfigVerificationResult parsingResult = verifyCanParse(context, consumerService, verificationLogger);
            verificationResults.add(parsingResult);
        } catch (final IOException e) {
            verificationResults.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Communicate with Kafka Broker")
                .outcome(Outcome.FAILED)
                .explanation("There was an I/O failure when communicating with Kafka: " + e)
                .build());
        }

        return verificationResults;
    }

    private ConfigVerificationResult verifyShareGroup(final KafkaShareConsumerService shareConsumerService, final ShareGroupContext shareContext) {
        final ConfigVerificationResult.Builder builder = new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Share Group Subscription");

        try {
            final Iterable<ByteRecord> records = shareConsumerService.poll(Duration.ofSeconds(5));
            int sampled = 0;
            for (final ByteRecord byteRecord : records) {
                sampled++;
                shareConsumerService.acknowledge(byteRecord, Acknowledgement.RELEASE);
            }
            // Always commit pending acknowledgements; if no records, this is a no-op.
            shareConsumerService.commit();

            if (sampled == 0) {
                builder.outcome(Outcome.SUCCESSFUL).explanation(
                        "Successfully subscribed to topics %s for share group [%s]. No records were available within the verification window; "
                                .formatted(shareContext.getTopics(), shareContext.getGroupId())
                                + "ensure the share group's starting offset has been set via Kafka admin tools (kafka-share-groups.sh) if records are expected.");
            } else {
                builder.outcome(Outcome.SUCCESSFUL).explanation(
                        "Successfully subscribed to topics %s for share group [%s] and sampled [%d] records (released back to the share group)."
                                .formatted(shareContext.getTopics(), shareContext.getGroupId(), sampled));
            }
        } catch (final Exception e) {
            builder.outcome(Outcome.FAILED).explanation("Share group subscription failed: " + e);
        }

        return builder.build();
    }

    private ConfigVerificationResult verifyPartitions(final KafkaConsumerService consumerService, final PollingContext pollingContext) {
        final ConfigVerificationResult.Builder partitionVerification = new ConfigVerificationResult.Builder()
            .verificationStepName("Verify Topic Partitions");

        try {
            final List<PartitionState> partitionStates = consumerService.getPartitionStates();
            partitionVerification
                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                .explanation(String.format("Found [%d] partitions for Topics %s", partitionStates.size(), pollingContext.getTopics()));
        } catch (final Exception e) {
            getLogger().error("Topics {} Partition verification failed", pollingContext.getTopics(), e);
            partitionVerification
                .outcome(ConfigVerificationResult.Outcome.FAILED)
                .explanation(String.format("Topics %s Partition access failed: %s", pollingContext.getTopics(), e));
        }

        return partitionVerification.build();
    }

    private ConfigVerificationResult verifyCanParse(final ProcessContext context, final KafkaConsumerService consumerService, final ComponentLog verificationLogger) {
        final Iterable<ByteRecord> records = consumerService.poll(Duration.ofSeconds(60));
        final ProcessingStrategy processingStrategy = context.getProperty(PROCESSING_STRATEGY).asAllowableValue(ProcessingStrategy.class);
        if (processingStrategy != ProcessingStrategy.RECORD) {
            return new ConfigVerificationResult.Builder()
                .verificationStepName("Parse Records")
                .outcome(Outcome.SKIPPED)
                .explanation("Processing Strategy is set to " + processingStrategy.getValue() + " so skipping record parsing verification")
                .build();
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        int recordIndex = 0;
        for (final ByteRecord byteRecord : records) {
            recordIndex++;
            final Map<String, String> recordAttributes = KafkaUtils.toAttributes(
                byteRecord, keyEncoding, headerNamePattern, headerEncoding, commitOffsets);

            try (final InputStream inputStream = new ByteArrayInputStream(byteRecord.getValue());
                 final RecordReader reader = readerFactory.createRecordReader(recordAttributes, inputStream, byteRecord.getValue().length, verificationLogger)) {

                while (reader.nextRecord() != null) {
                }
            } catch (final Exception e) {
                return new ConfigVerificationResult.Builder()
                    .verificationStepName("Parse Records")
                    .outcome(Outcome.FAILED)
                    .explanation("Failed to parse Record number " + recordIndex + ": " + e)
                    .build();
            }
        }

        if (recordIndex == 0) {
            return new ConfigVerificationResult.Builder()
                .verificationStepName("Parse Records")
                .outcome(Outcome.SKIPPED)
                .explanation("No records were received to parse")
                .build();
        }

        return new ConfigVerificationResult.Builder()
            .verificationStepName("Parse Records")
            .outcome(Outcome.SUCCESSFUL)
            .explanation("Successfully parsed " + recordIndex + " records")
            .build();
    }


    @ConnectorMethod(
        name = "sampleTopics",
        description = "Returns a list of sample data from the topics that would be consumed by this processor."
    )
    public List<byte[]> sampleTopics(final ProcessContext context) throws IOException {
        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        final GroupType sampleGroupType = context.getProperty(GROUP_TYPE).asAllowableValue(GroupType.class);

        if (sampleGroupType == GroupType.SHARE) {
            // Sampling RELEASEs records back to the share group so they remain available to real consumers,
            // which requires EXPLICIT acknowledgement regardless of the user-selected acknowledgement mode.
            final ShareGroupContext sampleShareContext = createShareGroupContext(context, ShareAcknowledgementMode.EXPLICIT);
            try (final KafkaShareConsumerService shareConsumerService = connectionService.getShareConsumerService(sampleShareContext)) {
                final Iterable<ByteRecord> records = shareConsumerService.poll(Duration.ofSeconds(60));
                final List<byte[]> samples = new ArrayList<>();
                for (final ByteRecord record : records) {
                    samples.add(record.getValue());
                    // Release sampled records so they remain available to real consumers
                    shareConsumerService.acknowledge(record, Acknowledgement.RELEASE);
                    if (samples.size() >= 10) {
                        break;
                    }
                }
                shareConsumerService.commit();
                return samples;
            }
        }

        final PollingContext pollingContext = createPollingContext(context, "nifi-validation-" + System.currentTimeMillis(), AutoOffsetReset.EARLIEST);
        try (final KafkaConsumerService consumerService = connectionService.getConsumerService(pollingContext)) {
            final Iterable<ByteRecord> records = consumerService.poll(Duration.ofSeconds(60));
            final List<byte[]> samples = new ArrayList<>();
            for (final ByteRecord record : records) {
                samples.add(record.getValue());
                if (samples.size() >= 10) {
                    break;
                }
            }

            return samples;
        }
    }

    private KafkaConsumerService getConsumerService(final ProcessContext context) {
        final KafkaConsumerService consumerService = consumerServices.poll();
        if (consumerService != null) {
            return consumerService;
        }

        final int activeCount = activeConsumerCount.incrementAndGet();
        if (activeCount > getMaxConsumerCount()) {
            getLogger().trace("No Kafka Consumer Service available; have already reached max count of {} so will not create a new one", getMaxConsumerCount());
            activeConsumerCount.decrementAndGet();
            return null;
        }

        getLogger().info("No Kafka Consumer Service available; creating a new one. Active count: {}", activeCount);
        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        final KafkaConsumerService newService = connectionService.getConsumerService(pollingContext);
        newService.setRebalanceCallback(createRebalanceCallback());
        return newService;
    }

    private RebalanceCallback createRebalanceCallback() {
        return new RebalanceCallback() {
            @Override
            public void onPartitionsRevoked(final Collection<PartitionState> revokedPartitions, final SessionContext sessionContext) {
                if (sessionContext == null) {
                    getLogger().debug("No session context during rebalance callback, nothing to commit");
                    return;
                }

                final RebalanceSessionHolder holder = (RebalanceSessionHolder) sessionContext;
                final ProcessSession session = holder.session;
                final OffsetTracker offsetTracker = holder.offsetTracker;

                getLogger().info("Rebalance callback invoked for {} revoked partitions, committing session synchronously",
                        revokedPartitions.size());

                try {
                    session.commit();
                    getLogger().debug("Session committed successfully during rebalance callback");

                    if (offsetTracker != null) {
                        offsetTracker.getRecordCounts().forEach((topic, count) -> {
                            session.adjustCounter("Records Acknowledged for " + topic, count, true);
                        });
                        offsetTracker.clear();
                    }
                } catch (final Exception e) {
                    getLogger().error("Failed to commit session during rebalance callback", e);
                    throw new RuntimeException("Failed to commit session during rebalance", e);
                }
            }
        };
    }

    private int getMaxConsumerCount() {
        return maxConsumerCount;
    }

    private void processConsumerRecords(final ProcessContext context, final ProcessSession session, final OffsetTracker offsetTracker,
            final Iterator<ByteRecord> consumerRecords) {
        switch (processingStrategy) {
            case RECORD -> processInputRecords(context, session, offsetTracker, consumerRecords);
            case FLOW_FILE -> processInputFlowFile(session, offsetTracker, consumerRecords);
            case DEMARCATOR -> {
                final Iterator<ByteRecord> demarcatedRecords = transformDemarcator(context, consumerRecords);
                processInputFlowFile(session, offsetTracker, demarcatedRecords);
            }
        }
    }

    private Iterator<ByteRecord> transformDemarcator(final ProcessContext context, final Iterator<ByteRecord> consumerRecords) {
        final String demarcatorValue = context.getProperty(ConsumeKafka.MESSAGE_DEMARCATOR).getValue();
        if (demarcatorValue == null) {
            return consumerRecords;
        }

        final byte[] demarcator = demarcatorValue.getBytes(StandardCharsets.UTF_8);
        final boolean separateByKey = context.getProperty(SEPARATE_BY_KEY).asBoolean();
        return new ByteRecordBundler(demarcator, separateByKey, keyEncoding, headerNamePattern, headerEncoding, commitOffsets).bundle(consumerRecords);
    }

    private void processInputRecords(final ProcessContext context, final ProcessSession session, final OffsetTracker offsetTracker, final Iterator<ByteRecord> consumerRecords) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final KafkaMessageConverter converter;
        if (outputStrategy == OutputStrategy.USE_VALUE) {
            converter = new RecordStreamKafkaMessageConverter(readerFactory, writerFactory, headerEncoding, headerNamePattern,
                    keyEncoding, commitOffsets, offsetTracker, getLogger(), brokerUri);
        } else if (outputStrategy == OutputStrategy.INJECT_OFFSET) {
            converter = new InjectOffsetRecordStreamKafkaMessageConverter(
                    readerFactory,
                    writerFactory,
                    headerEncoding,
                    headerNamePattern,
                    keyEncoding,
                    commitOffsets,
                    offsetTracker,
                    getLogger(),
                    brokerUri
            );
        } else {
            final RecordReaderFactory keyReaderFactory = keyFormat == KeyFormat.RECORD
                ? context.getProperty(KEY_RECORD_READER).asControllerService(RecordReaderFactory.class) : null;

            converter = new WrapperRecordStreamKafkaMessageConverter(readerFactory, writerFactory, keyReaderFactory,
                headerEncoding, headerNamePattern, keyFormat, keyEncoding, commitOffsets, offsetTracker, getLogger(), brokerUri, outputStrategy);
        }

        converter.toFlowFiles(session, consumerRecords);
    }

    private void processInputFlowFile(final ProcessSession session, final OffsetTracker offsetTracker, final Iterator<ByteRecord> consumerRecords) {
        final KafkaMessageConverter converter = new FlowFileStreamKafkaMessageConverter(
            headerEncoding, headerNamePattern, headerNamePrefix, keyEncoding, commitOffsets, offsetTracker, brokerUri);
        converter.toFlowFiles(session, consumerRecords);
    }

    private PollingContext createPollingContext(final ProcessContext context) {
        final String groupId = context.getProperty(GROUP_ID).getValue();
        final String offsetReset = context.getProperty(AUTO_OFFSET_RESET).getValue();
        final AutoOffsetReset autoOffsetReset = AutoOffsetReset.valueOf(offsetReset.toUpperCase());
        return createPollingContext(context, groupId, autoOffsetReset);
    }

    private ShareGroupContext createShareGroupContext(final ProcessContext context, final ShareAcknowledgementMode acknowledgementMode) {
        final String groupId = context.getProperty(GROUP_ID).getValue();
        final String topics = context.getProperty(TOPICS).evaluateAttributeExpressions().getValue();
        final Collection<String> topicList = KafkaUtils.toTopicList(topics);
        return new ShareGroupContext(groupId, topicList, acknowledgementMode);
    }

    private void triggerShareGroup(final ProcessContext context, final ProcessSession session) {
        final KafkaShareConsumerService shareConsumerService = getShareConsumerService(context);
        if (shareConsumerService == null) {
            getLogger().debug("No Kafka Share Consumer Service available; will yield and return immediately");
            context.yield();
            return;
        }

        final long maxUncommittedMillis = context.getProperty(MAX_UNCOMMITTED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final OffsetTracker offsetTracker = new OffsetTracker();

        // The share consumer requires every in-flight record to be acknowledged (explicitly or implicitly)
        // before the next poll. Acknowledgement happens in the session-commit callback below, after
        // FlowFiles are durably committed. To respect this contract we perform a single poll per trigger;
        // the poll itself blocks up to Max Uncommitted Time waiting for records, so a retry loop is
        // unnecessary and would violate the share consumer contract by polling again before the
        // previous batch has been acknowledged.
        try {
            final Iterator<ByteRecord> consumerRecords = shareConsumerService.poll(Duration.ofMillis(maxUncommittedMillis)).iterator();
            if (!consumerRecords.hasNext()) {
                getLogger().trace("No Kafka share-group records consumed for {}; re-queuing share consumer", shareGroupContext);
                shareConsumerServices.offer(shareConsumerService);
                return;
            }

            processConsumerRecords(context, session, offsetTracker, consumerRecords);

            session.commitAsync(
                    () -> commitShareConsumer(shareConsumerService, offsetTracker, session),
                    throwable -> {
                        getLogger().error("Failed to commit session for share group; will roll back any uncommitted records", throwable);
                        rollbackShareConsumer(shareConsumerService, offsetTracker, session);
                        context.yield();
                    });
        } catch (final Exception e) {
            getLogger().error("Failed to consume Kafka share-group records", e);
            // Best-effort rollback so EXPLICIT-mode consumers can release records immediately;
            // in IMPLICIT mode this is a no-op and we rely on the close+lock-expiry below for redelivery.
            if (!shareConsumerService.isClosed()) {
                try {
                    shareConsumerService.rollback();
                } catch (final Exception rollbackException) {
                    getLogger().warn("Failed to release records back to the share group", rollbackException);
                }
            }
            closeShareConsumer(shareConsumerService, "Encountered Exception while consuming or writing out Kafka share-group records");
            context.yield();
            session.rollback();
        }
    }

    private void commitShareConsumer(final KafkaShareConsumerService shareConsumerService, final OffsetTracker offsetTracker, final ProcessSession session) {
        try {
            shareConsumerService.commit();

            offsetTracker.getRecordCounts().forEach((topic, count) -> session.adjustCounter("Records Acknowledged for " + topic, count, true));

            shareConsumerServices.offer(shareConsumerService);
            getLogger().debug("Committed acknowledgements for Kafka Share Consumer Service");
        } catch (final Exception e) {
            getLogger().error("Failed to commit acknowledgements for Kafka Share Consumer Service; will release records back to the share group", e);
            rollbackShareConsumer(shareConsumerService, offsetTracker, session);
        }
    }

    private void rollbackShareConsumer(final KafkaShareConsumerService shareConsumerService, final OffsetTracker offsetTracker, final ProcessSession session) {
        if (shareConsumerService.isClosed()) {
            return;
        }

        try {
            shareConsumerService.rollback();
            if (shareAcknowledgementMode == ShareAcknowledgementMode.EXPLICIT) {
                // EXPLICIT-mode rollback actively RELEASEs records to the broker, so the consumer can be re-pooled.
                shareConsumerServices.offer(shareConsumerService);
                getLogger().debug("Released records back to the share group for Kafka Share Consumer Service");
            } else {
                // IMPLICIT-mode rollback cannot actively release records. Close the consumer so the broker's
                // acquisition lock expires and the records become eligible for redelivery to another consumer.
                closeShareConsumer(shareConsumerService, "IMPLICIT-mode session rollback - awaiting broker acquisition lock expiry");
            }
        } catch (final Exception e) {
            getLogger().warn("Failed to release records back to the share group", e);
            closeShareConsumer(shareConsumerService, "Failed to release records back to the share group");
        }

        offsetTracker.getRecordCounts().forEach((topic, count) -> session.adjustCounter("Records Released for " + topic, count, true));
    }

    private KafkaShareConsumerService getShareConsumerService(final ProcessContext context) {
        final KafkaShareConsumerService shareConsumerService = shareConsumerServices.poll();
        if (shareConsumerService != null) {
            return shareConsumerService;
        }

        final int activeCount = activeConsumerCount.incrementAndGet();
        if (activeCount > getMaxConsumerCount()) {
            getLogger().trace("No Kafka Share Consumer Service available; have already reached max count of {} so will not create a new one", getMaxConsumerCount());
            activeConsumerCount.decrementAndGet();
            return null;
        }

        getLogger().info("No Kafka Share Consumer Service available; creating a new one. Active count: {}", activeCount);
        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        return connectionService.getShareConsumerService(shareGroupContext);
    }

    private void closeShareConsumer(final KafkaShareConsumerService shareConsumerService, final String reason) {
        if (shareConsumerService.isClosed()) {
            getLogger().debug("Asked to close Kafka Share Consumer Service but consumer already closed");
            return;
        }

        getLogger().info("Closing Kafka Share Consumer due to: {}", reason);

        try {
            shareConsumerService.close();
            activeConsumerCount.decrementAndGet();
        } catch (final IOException ioe) {
            getLogger().warn("Failed to close Kafka Share Consumer Service", ioe);
        }
    }

    private PollingContext createPollingContext(final ProcessContext context, final String groupId, final AutoOffsetReset autoOffsetReset) {
        final String topics = context.getProperty(TOPICS).evaluateAttributeExpressions().getValue();
        final String topicFormat = context.getProperty(TOPIC_FORMAT).getValue();

        final PollingContext pollingContext;
        if (topicFormat.equals(TOPIC_PATTERN.getValue())) {
            final Pattern topicPattern = Pattern.compile(topics.trim());
            pollingContext = new PollingContext(groupId, topicPattern, autoOffsetReset);
        } else if (topicFormat.equals(TOPIC_NAME.getValue())) {
            final Collection<String> topicList = KafkaUtils.toTopicList(topics);
            pollingContext = new PollingContext(groupId, topicList, autoOffsetReset);
        } else {
            throw new ProcessException(String.format("Topic Format [%s] not supported", topicFormat));
        }

        return pollingContext;
    }

    private static class RebalanceSessionHolder implements SessionContext {
        private final ProcessSession session;
        private final OffsetTracker offsetTracker;

        RebalanceSessionHolder(final ProcessSession session, final OffsetTracker offsetTracker) {
            this.session = session;
            this.offsetTracker = offsetTracker;
        }
    }
}
