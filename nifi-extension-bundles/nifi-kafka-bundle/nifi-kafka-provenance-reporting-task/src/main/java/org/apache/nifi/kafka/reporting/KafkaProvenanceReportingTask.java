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
package org.apache.nifi.kafka.reporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ComponentMapHolder;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@Tags({"provenance", "lineage", "tracking", "kafka", "publish", "streaming", "avro", "record"})
@CapabilityDescription(
        "Publishes NiFi Provenance events directly to a Kafka topic using the Kafka Connection Service.")
@Stateful(
        scopes = Scope.LOCAL,
        description = "Stores the ID of the last Provenance Event published to Kafka so that " +
                "the task resumes from the correct position after a restart."
)
public class KafkaProvenanceReportingTask extends AbstractReportingTask {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT).withZone(ZoneOffset.UTC);

    private static final String NIFI_API_PATH = "/nifi";

    private static final String DEFAULT_PLATFORM = "nifi";

    private static final String BOOLEAN_TRUE = "true";
    private static final String BOOLEAN_FALSE = "false";

    static final int NIFI_DEFAULT_MAX_ATTR_LENGTH = 65536;

    static final int ATTR_LENGTH_MIN = 36;

    private static final String FIELD_EVENT_ID = "eventId";
    private static final String FIELD_EVENT_ORDINAL = "eventOrdinal";
    private static final String FIELD_EVENT_TYPE = "eventType";
    private static final String FIELD_TIMESTAMP_MILLIS = "timestampMillis";

    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_DURATION_MILLIS = "durationMillis";
    private static final String FIELD_LINEAGE_START = "lineageStart";
    private static final String FIELD_DETAILS = "details";
    private static final String FIELD_COMPONENT_ID = "componentId";
    private static final String FIELD_COMPONENT_TYPE = "componentType";
    private static final String FIELD_COMPONENT_NAME = "componentName";
    private static final String FIELD_PROCESS_GROUP_ID = "processGroupId";
    private static final String FIELD_PROCESS_GROUP_NAME = "processGroupName";
    private static final String FIELD_ENTITY_ID = "entityId";
    private static final String FIELD_ENTITY_TYPE = "entityType";
    private static final String FIELD_ENTITY_SIZE = "entitySize";
    private static final String FIELD_PREV_ENTITY_SIZE = "previousEntitySize";
    private static final String FIELD_UPDATED_ATTRIBUTES = "updatedAttributes";
    private static final String FIELD_PREV_ATTRIBUTES = "previousAttributes";
    private static final String FIELD_ACTOR_HOSTNAME = "actorHostname";
    private static final String FIELD_CONTENT_URI = "contentURI";
    private static final String FIELD_PREV_CONTENT_URI = "previousContentURI";
    private static final String FIELD_PARENT_IDS = "parentIds";
    private static final String FIELD_CHILD_IDS = "childIds";
    private static final String FIELD_PLATFORM = "platform";
    private static final String FIELD_APPLICATION = "application";
    private static final String FIELD_REMOTE_IDENTIFIER = "remoteIdentifier";
    private static final String FIELD_ALT_IDENTIFIER = "alternateIdentifier";
    private static final String FIELD_TRANSIT_URI = "transitUri";

    private static final RecordSchema PROVENANCE_SCHEMA = buildProvenanceSchema();

    private static RecordSchema buildProvenanceSchema() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(FIELD_EVENT_ID, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(FIELD_EVENT_ORDINAL, RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField(FIELD_EVENT_TYPE, RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField(FIELD_TIMESTAMP_MILLIS, RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField(FIELD_DURATION_MILLIS, RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField(FIELD_LINEAGE_START, RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField(FIELD_DETAILS, RecordFieldType.STRING.getDataType()));
        addStringFields(fields, FIELD_COMPONENT_ID, FIELD_COMPONENT_TYPE, FIELD_COMPONENT_NAME,
                FIELD_PROCESS_GROUP_ID, FIELD_PROCESS_GROUP_NAME, FIELD_ENTITY_ID, FIELD_ENTITY_TYPE);
        fields.add(new RecordField(FIELD_ENTITY_SIZE, RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField(FIELD_PREV_ENTITY_SIZE, RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField(FIELD_UPDATED_ATTRIBUTES,
                RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField(FIELD_PREV_ATTRIBUTES,
                RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        addStringFields(fields, FIELD_ACTOR_HOSTNAME, FIELD_CONTENT_URI, FIELD_PREV_CONTENT_URI);
        fields.add(new RecordField(FIELD_PARENT_IDS,
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField(FIELD_CHILD_IDS,
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        addStringFields(fields, FIELD_PLATFORM, FIELD_APPLICATION,
                FIELD_REMOTE_IDENTIFIER, FIELD_ALT_IDENTIFIER, FIELD_TRANSIT_URI);
        return new SimpleRecordSchema(fields);
    }

    private static void addStringFields(final List<RecordField> fields, final String... names) {
        for (final String name : names) {
            fields.add(new RecordField(name, RecordFieldType.STRING.getDataType()));
        }
    }

    static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue(
            "beginning-of-stream", "Beginning of Stream",
            "Start reading from the oldest event in the stream.");
    static final AllowableValue END_OF_STREAM = new AllowableValue(
            "end-of-stream", "End of Stream",
            "Start reading from the current end of the stream, ignoring historical events.");

    static final AllowableValue KEY_NONE = new AllowableValue(
            "none", "None",
            "No key is set on Kafka messages. Kafka assigns messages to partitions using its default strategy.");
    static final AllowableValue KEY_LINEAGE_START = new AllowableValue(
            FIELD_LINEAGE_START, "Lineage Start",
            "Uses the lineage start timestamp (epoch millis) as the key. " +
                    "All events belonging to the same lineage chain share the same key and land on the same partition, " +
                    "enabling ordered consumption of a complete data lineage.");
    static final AllowableValue KEY_ENTITY_ID = new AllowableValue(
            FIELD_ENTITY_ID, "FlowFile UUID",
            "Uses the FlowFile UUID as the key. All provenance events for the same FlowFile share the same key.");
    static final AllowableValue KEY_COMPONENT_ID = new AllowableValue(
            FIELD_COMPONENT_ID, "Component ID",
            "Uses the component ID (processor/connection UUID) as the key. Groups all events from the same component.");
    static final AllowableValue KEY_EVENT_TYPE = new AllowableValue(
            FIELD_EVENT_TYPE, "Event Type",
            "Uses the event type name (e.g. CREATE, SEND, RECEIVE) as the key. Useful for per-type topic compaction.");
    static final AllowableValue KEY_EVENT_ORDINAL = new AllowableValue(
            FIELD_EVENT_ORDINAL, "Event Ordinal",
            "Uses the event's unique sequential ordinal ID (long) as the key. Each message has a distinct key.");

    static final AllowableValue DELIVERY_REPLICATED = new AllowableValue(
            "all", "Guarantee Replicated Delivery",
            "Producer waits for acknowledgment from all in-sync replicas. Strongest guarantee.");
    static final AllowableValue DELIVERY_ONE_NODE = new AllowableValue(
            "1", "Guarantee Single Node Delivery",
            "Producer waits for acknowledgment from the partition leader only.");
    static final AllowableValue DELIVERY_BEST_EFFORT = new AllowableValue(
            "0", "Best Effort",
            "No acknowledgment required. Highest throughput, possible data loss on broker failure.");

    static final PropertyDescriptor KAFKA_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("kafka-connection-service")
            .displayName("Kafka Connection Service")
            .description("The Kafka Connection Service to use for connecting to Kafka brokers.")
            .identifiesControllerService(KafkaConnectionService.class)
            .required(true)
            .build();

    static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("topic-name")
            .displayName("Topic Name")
            .description("The Kafka topic to which Provenance Events are published.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description(
                    "Specifies the Controller Service to use for serializing Provenance Events before publishing to Kafka. " +
                            "When set, all events in a batch are encoded as a record set using the configured writer " +
                            "(e.g. AvroRecordSetWriter, JsonRecordSetWriter) and published as a single Kafka message per batch. " +
                            "Configure AvroRecordSetWriter with a Schema Registry and 'Schema Write Strategy = Confluent encoded' " +
                            "to emit the standard Confluent wire format (magic byte 0x00 + 4-byte schema ID + Avro binary), " +
                            "which allows consumers to resolve the schema by ID from the registry. " +
                            "The record schema is identical to the one used by SiteToSiteProvenanceReportingTask. " +
                            "When not set, each event is serialized as a JSON object.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(false)
            .build();

    static final PropertyDescriptor MESSAGE_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("message-key-field")
            .displayName("Message Key Field")
            .description(
                    "Specifies which Provenance Event field to use as the Kafka message key. " +
                            "A meaningful key enables Kafka to co-locate related events on the same partition, " +
                            "which is important for ordered consumption and log compaction. " +
                            "The key value is serialized as a UTF-8 string.")
            .required(true)
            .allowableValues(KEY_NONE, KEY_LINEAGE_START, KEY_ENTITY_ID,
                    KEY_COMPONENT_ID, KEY_EVENT_TYPE, KEY_EVENT_ORDINAL)
            .defaultValue(KEY_LINEAGE_START.getValue())
            .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name("delivery-guarantee")
            .displayName("Delivery Guarantee")
            .description("Level of delivery guarantee required (maps to Kafka producer 'acks').")
            .required(true)
            .allowableValues(DELIVERY_REPLICATED, DELIVERY_ONE_NODE, DELIVERY_BEST_EFFORT)
            .defaultValue(DELIVERY_REPLICATED.getValue())
            .build();

    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("compression-type")
            .displayName("Compression Type")
            .description("Compression codec for Kafka producer batches.")
            .required(true)
            .allowableValues("none", "gzip", "snappy", "lz4", "zstd")
            .defaultValue("none")
            .build();

    static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder()
            .name("max-request-size")
            .displayName("Max Request Size")
            .description("Maximum Kafka producer request size (maps to 'max.request.size').")
            .required(true)
            .defaultValue("1 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSACTIONS_ENABLED = new PropertyDescriptor.Builder()
            .name("transactions-enabled")
            .displayName("Transactions Enabled")
            .description("Whether to use Kafka transactions. Each batch is published atomically when enabled.")
            .required(true)
            .allowableValues(BOOLEAN_TRUE, BOOLEAN_FALSE)
            .defaultValue(BOOLEAN_TRUE)
            .build();

    static final PropertyDescriptor TRANSACTIONAL_ID_PREFIX = new PropertyDescriptor.Builder()
            .name("transactional-id-prefix")
            .displayName("Transactional ID Prefix")
            .description("Prefix for the auto-generated transactional.id (used when Transactions Enabled is true).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("nifi-provenance-")
            .dependsOn(TRANSACTIONS_ENABLED, BOOLEAN_TRUE)
            .build();

    static final PropertyDescriptor INSTANCE_URL = new PropertyDescriptor.Builder()
            .name("instance-url")
            .displayName("Instance URL")
            .description("URL of this NiFi instance (ending with /nifi). Used to generate contentURI fields. Optional.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("https://${hostname(true)}:8443/nifi")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
            .name(FIELD_PLATFORM)
            .displayName("Platform")
            .description("Environment label embedded in each record (e.g. 'prod-nifi-cluster-01'). Must be non-empty.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue(DEFAULT_PLATFORM)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("Maximum number of Provenance Events to collect per scheduling trigger. " +
                    "Each event is published as an individual Kafka message; all messages from one batch are sent in a single producer call.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor ALLOW_NULL_VALUES = new PropertyDescriptor.Builder()
            .name("allow-null-values")
            .displayName("Include Null Values")
            .description(
                    "When true, null-valued fields are included in the JSON output. " +
                            "Has no effect when Record Writer is configured.")
            .required(true)
            .allowableValues(BOOLEAN_TRUE, BOOLEAN_FALSE)
            .defaultValue(BOOLEAN_FALSE)
            .build();

    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder()
            .name("start-position")
            .displayName("Start Position")
            .description("Where to begin reading if no prior state exists.")
            .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
            .defaultValue(BEGINNING_OF_STREAM.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("Event Type to Include")
            .displayName("Event Type to Include")
            .description("Comma-separated list of ProvenanceEventType values to include. Available: "
                    + Arrays.deepToString(ProvenanceEventType.values()))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("Event Type to Exclude")
            .displayName("Event Type to Exclude")
            .description("Comma-separated list of ProvenanceEventType values to exclude (takes precedence over include).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .name("Component Type to Include")
            .displayName("Component Type to Include")
            .description("Regex to include events by component type.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("Component Type to Exclude")
            .displayName("Component Type to Exclude")
            .description("Regex to exclude events by component type (takes precedence over include).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
            .name("Component ID to Include")
            .displayName("Component ID to Include")
            .description("Comma-separated processor/connection UUIDs to include.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID_EXCLUDE = new PropertyDescriptor.Builder()
            .name("Component ID to Exclude")
            .displayName("Component ID to Exclude")
            .description("Comma-separated processor/connection UUIDs to exclude (takes precedence over include).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME = new PropertyDescriptor.Builder()
            .name("Component Name to Include")
            .displayName("Component Name to Include")
            .description("Regex to include events by component name.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME_EXCLUDE = new PropertyDescriptor.Builder()
            .name("Component Name to Exclude")
            .displayName("Component Name to Exclude")
            .description("Regex to exclude events by component name (takes precedence over include).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_ATTRIBUTES_INCLUDE = new PropertyDescriptor.Builder()
            .name("Attributes to Include")
            .displayName("Attributes to Include")
            .description("Comma-separated list of attribute names or regular expressions. " +
                    "When set, only attributes whose names match any entry are included in " +
                    "previousAttributes and updatedAttributes. " +
                    "Cannot be configured together with attributes to exclude.")
            .required(false)
            .addValidator(createAttributeFilterValidator())
            .build();

    static final PropertyDescriptor FILTER_ATTRIBUTES_EXCLUDE = new PropertyDescriptor.Builder()
            .name("Attributes to Exclude")
            .displayName("Attributes to Exclude")
            .description("Comma-separated list of attribute names or regular expressions. " +
                    "When set, attributes whose names match any entry are excluded from " +
                    "previousAttributes and updatedAttributes; all others are included. " +
                    "Cannot be configured together with attributes to include.")
            .required(false)
            .addValidator(createAttributeFilterValidator())
            .build();

    static final PropertyDescriptor ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("attribute-max-length")
            .displayName("Attribute Max Length")
            .description("Maximum number of characters to include in each attribute value sent in Kafka provenance events. " +
                    "Values exceeding this limit are trimmed. " +
                    "Must be at least " + ATTR_LENGTH_MIN + " to preserve UUID attributes. " +
                    "If set to " + NIFI_DEFAULT_MAX_ATTR_LENGTH + " or higher, trimming is not applied because " +
                    "NiFi's global provenance repository limit already enforces that ceiling. " +
                    "Note: this task compares against the NiFi default global limit of " + NIFI_DEFAULT_MAX_ATTR_LENGTH + " characters. " +
                    "If the NiFi property nifi.provenance.repository.max.attribute.length has been set to a lower value by an administrator, " +
                    "attribute values will already be capped at that lower global limit before they reach this task. " +
                    "In that case, setting Attribute Max Length to a value larger than the actual global limit has no trimming effect, " +
                    "even if it is below " + NIFI_DEFAULT_MAX_ATTR_LENGTH + ", " +
                    "and no warning will be logged because this task cannot read the runtime value of nifi.provenance.repository.max.attribute.length. " +
                    "To ensure trimming takes effect, set Attribute Max Length to a value strictly smaller than " +
                    "the configured nifi.provenance.repository.max.attribute.length.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            KAFKA_CONNECTION_SERVICE,
            TOPIC_NAME,
            RECORD_WRITER,
            MESSAGE_KEY_FIELD,
            DELIVERY_GUARANTEE,
            COMPRESSION_TYPE,
            MAX_REQUEST_SIZE,
            TRANSACTIONS_ENABLED,
            TRANSACTIONAL_ID_PREFIX,
            INSTANCE_URL,
            PLATFORM,
            BATCH_SIZE,
            ALLOW_NULL_VALUES,
            START_POSITION,
            FILTER_EVENT_TYPE,
            FILTER_EVENT_TYPE_EXCLUDE,
            FILTER_COMPONENT_TYPE,
            FILTER_COMPONENT_TYPE_EXCLUDE,
            FILTER_COMPONENT_ID,
            FILTER_COMPONENT_ID_EXCLUDE,
            FILTER_COMPONENT_NAME,
            FILTER_COMPONENT_NAME_EXCLUDE,
            FILTER_ATTRIBUTES_INCLUDE,
            FILTER_ATTRIBUTES_EXCLUDE,
            ATTRIBUTE_MAX_LENGTH
    );

    private record EncodingContext(
            String hostname,
            String nifiUrlBase,
            String applicationName,
            String platform,
            String nodeIdentifier,
            boolean allowNullValues) {
    }

    private record KafkaOutputConfig(
            String topicName,
            String messageKeyField,
            RecordSetWriterFactory writerFactory) {
    }

    private final AtomicReference<ProvenanceEventConsumer> consumerRef = new AtomicReference<>();
    private final AtomicReference<KafkaProducerService> producerRef = new AtomicReference<>();

    final AtomicReference<List<Pattern>> attributeIncludePatterns = new AtomicReference<>(Collections.emptyList());
    final AtomicReference<List<Pattern>> attributeExcludePatterns = new AtomicReference<>(Collections.emptyList());
    volatile int attributeMaxLength = -1;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final boolean hasIncludeFilter = context.getProperty(FILTER_ATTRIBUTES_INCLUDE).isSet();
        final boolean hasExcludeFilter = context.getProperty(FILTER_ATTRIBUTES_EXCLUDE).isSet();
        if (hasIncludeFilter && hasExcludeFilter) {
            results.add(new ValidationResult.Builder()
                    .subject(FILTER_ATTRIBUTES_INCLUDE.getDisplayName() + " / " + FILTER_ATTRIBUTES_EXCLUDE.getDisplayName())
                    .valid(false)
                    .explanation("'Attributes to Include' and 'Attributes to Exclude' are mutually exclusive. Configure only one.")
                    .build());
        }
        return results;
    }

    /**
     * Returns a {@link Validator} that accepts a comma-separated list of attribute names or regular
     * expressions. Each entry is compiled as a {@link Pattern} to verify it is syntactically valid.
     * Literal attribute names are also valid regular expressions, so this validator covers both cases.
     */
    private static Validator createAttributeFilterValidator() {
        return (subject, value, context) -> {
            if (value == null || value.isBlank()) {
                return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
            }
            for (final String entry : value.split(",")) {
                final String trimmed = entry.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try {
                    Pattern.compile(trimmed);
                } catch (final PatternSyntaxException e) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(value)
                            .valid(false)
                            .explanation("'" + trimmed + "' is not a valid regular expression: " + e.getMessage())
                            .build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
        };
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        final KafkaConnectionService connectionService =
                context.getProperty(KAFKA_CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        producerRef.set(connectionService.getProducerService(buildProducerConfiguration(context)));

        final ProvenanceEventConsumer consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        consumer.setLogger(getLogger());

        consumer.setComponentTypeRegex(
                context.getProperty(FILTER_COMPONENT_TYPE).evaluateAttributeExpressions().getValue());
        consumer.setComponentTypeRegexExclude(
                context.getProperty(FILTER_COMPONENT_TYPE_EXCLUDE).evaluateAttributeExpressions().getValue());
        consumer.setComponentNameRegex(
                context.getProperty(FILTER_COMPONENT_NAME).evaluateAttributeExpressions().getValue());
        consumer.setComponentNameRegexExclude(
                context.getProperty(FILTER_COMPONENT_NAME_EXCLUDE).evaluateAttributeExpressions().getValue());

        final String[] includeEventTypes = StringUtils.stripAll(StringUtils.split(
                context.getProperty(FILTER_EVENT_TYPE).evaluateAttributeExpressions().getValue(), ','));
        if (includeEventTypes != null) {
            for (final String type : includeEventTypes) {
                try {
                    consumer.addTargetEventType(ProvenanceEventType.valueOf(type));
                } catch (final IllegalArgumentException e) {
                    getLogger().warn("'{}' is not a valid ProvenanceEventType; ignored from include filter.", type);
                }
            }
        }

        final String[] excludeEventTypes = StringUtils.stripAll(StringUtils.split(
                context.getProperty(FILTER_EVENT_TYPE_EXCLUDE).evaluateAttributeExpressions().getValue(), ','));
        if (excludeEventTypes != null) {
            for (final String type : excludeEventTypes) {
                try {
                    consumer.addTargetEventTypeExclude(ProvenanceEventType.valueOf(type));
                } catch (final IllegalArgumentException e) {
                    getLogger().warn("'{}' is not a valid ProvenanceEventType; ignored from exclude filter.", type);
                }
            }
        }

        final String[] includeComponentIds = StringUtils.stripAll(StringUtils.split(
                context.getProperty(FILTER_COMPONENT_ID).evaluateAttributeExpressions().getValue(), ','));
        if (includeComponentIds != null) {
            consumer.addTargetComponentId(includeComponentIds);
        }

        final String[] excludeComponentIds = StringUtils.stripAll(StringUtils.split(
                context.getProperty(FILTER_COMPONENT_ID_EXCLUDE).evaluateAttributeExpressions().getValue(), ','));
        if (excludeComponentIds != null) {
            consumer.addTargetComponentIdExclude(excludeComponentIds);
        }

        attributeIncludePatterns.set(parsePatterns(context.getProperty(FILTER_ATTRIBUTES_INCLUDE).getValue()));
        attributeExcludePatterns.set(parsePatterns(context.getProperty(FILTER_ATTRIBUTES_EXCLUDE).getValue()));
        attributeMaxLength = resolveAttributeMaxLength(context.getProperty(ATTRIBUTE_MAX_LENGTH).asInteger());

        consumer.setScheduled(true);
        consumerRef.set(consumer);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        final ProvenanceEventConsumer consumer = consumerRef.get();
        if (consumer != null) {
            consumer.setScheduled(false);
        }
    }

    @OnStopped
    public void onStopped() {
        final KafkaProducerService producer = producerRef.getAndSet(null);
        if (producer != null) {
            try {
                producer.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close KafkaProducerService cleanly", e);
            }
        }
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final boolean isClustered = context.isClustered();
        final String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            getLogger().debug("Cluster Node Identifier not yet established; skipping trigger.");
            return;
        }

        final KafkaProducerService producer = producerRef.get();
        if (producer == null || producer.isClosed()) {
            getLogger().warn("KafkaProducerService is not available; skipping trigger.");
            return;
        }

        final ProcessGroupStatus rootStatus = context.getEventAccess().getControllerStatus();
        final String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
        final String messageKeyField = context.getProperty(MESSAGE_KEY_FIELD).getValue();
        final boolean useRecordWriter = context.getProperty(RECORD_WRITER).isSet();
        final RecordSetWriterFactory writerFactory = useRecordWriter
                ? context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class) : null;

        final EncodingContext encodingCtx = buildEncodingContext(context, rootStatus, nodeId);
        final KafkaOutputConfig kafkaConfig = new KafkaOutputConfig(topicName, messageKeyField, writerFactory);

        try {
            consumerRef.get().consumeEvents(context, (mapHolder, events) ->
                    publishBatch(mapHolder, events, producer, kafkaConfig, encodingCtx));
        } catch (final ProcessException pe) {
            getLogger().error("Failed to publish Provenance Events to Kafka topic '{}'", topicName, pe);
        }
    }

    private void publishBatch(
            final ComponentMapHolder mapHolder,
            final List<ProvenanceEventRecord> events,
            final KafkaProducerService producer,
            final KafkaOutputConfig kafkaConfig,
            final EncodingContext encodingCtx) {

        final List<KafkaRecord> kafkaRecords = new ArrayList<>(events.size());
        for (final ProvenanceEventRecord event : events) {
            final String componentName = mapHolder.getComponentName(event.getComponentId());
            final String processGroupId = mapHolder.getProcessGroupId(event.getComponentId(), event.getComponentType());
            final String processGroupName = mapHolder.getComponentName(processGroupId);
            try {
                kafkaRecords.add(toKafkaRecord(event, componentName, processGroupId, processGroupName,
                        kafkaConfig, encodingCtx));
            } catch (final IOException e) {
                throw new ProcessException("Failed to encode Provenance Event", e);
            }
        }

        final PublishContext publishContext = new PublishContext(kafkaConfig.topicName(), null, null, null);
        producer.send(kafkaRecords.iterator(), publishContext);
        producer.complete();

        if (publishContext.getException() != null) {
            throw new ProcessException(
                    "Kafka producer reported an error after send", publishContext.getException());
        }

        getLogger().debug("Published {} Provenance Events to Kafka topic '{}' (format: {})",
                events.size(), kafkaConfig.topicName(),
                kafkaConfig.writerFactory() != null ? "record-writer" : "json");
    }

    private KafkaRecord toKafkaRecord(
            final ProvenanceEventRecord event,
            final String componentName,
            final String processGroupId,
            final String processGroupName,
            final KafkaOutputConfig kafkaConfig,
            final EncodingContext ctx) throws IOException {

        final byte[] key = resolveMessageKey(event, kafkaConfig.messageKeyField());
        final byte[] payload;
        if (kafkaConfig.writerFactory() != null) {
            final MapRecord mapRecord = buildProvenanceRecord(
                    event, componentName, processGroupId, processGroupName, ctx);
            payload = encodeRecord(kafkaConfig.writerFactory(), mapRecord);
        } else {
            payload = serializeToJson(event, componentName, processGroupId, processGroupName, ctx)
                    .toString().getBytes(StandardCharsets.UTF_8);
        }
        return new KafkaRecord(kafkaConfig.topicName(), null, event.getEventTime(), key, payload,
                Collections.emptyList());
    }

    private EncodingContext buildEncodingContext(
            final ReportingContext context,
            final ProcessGroupStatus rootStatus,
            final String nodeId) {

        final String nifiUrlString = context.getProperty(INSTANCE_URL).evaluateAttributeExpressions().getValue();
        final String platform = context.getProperty(PLATFORM).evaluateAttributeExpressions().getValue();
        final boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();
        final String rootGroupName = rootStatus == null ? null : rootStatus.getName();

        String hostname = null;
        String nifiUrlBase = null;
        if (nifiUrlString != null && !nifiUrlString.isBlank()) {
            try {
                final URL nifiUrl = URI.create(nifiUrlString).toURL();
                hostname = nifiUrl.getHost();
                nifiUrlBase = resolveUrlBase(nifiUrl);
            } catch (final IllegalArgumentException | MalformedURLException e) {
                getLogger().warn("Configured Instance URL '{}' is invalid; contentURI fields will be omitted.",
                        nifiUrlString);
            }
        }

        return new EncodingContext(hostname, nifiUrlBase, rootGroupName, platform, nodeId, allowNullValues);
    }

    private MapRecord buildProvenanceRecord(
            final ProvenanceEventRecord event,
            final String componentName,
            final String processGroupId,
            final String processGroupName,
            final EncodingContext ctx) {

        final String contentBase = resolveContentBase(event, ctx);

        final Map<String, Object> values = new LinkedHashMap<>();
        values.put(FIELD_EVENT_ID, UUID.randomUUID().toString());
        values.put(FIELD_EVENT_ORDINAL, event.getEventId());
        values.put(FIELD_EVENT_TYPE, event.getEventType().name());
        values.put(FIELD_TIMESTAMP_MILLIS, event.getEventTime());
        values.put(FIELD_DURATION_MILLIS, event.getEventDuration());
        values.put(FIELD_LINEAGE_START, event.getLineageStartDate());
        values.put(FIELD_DETAILS, event.getDetails());
        values.put(FIELD_COMPONENT_ID, event.getComponentId());
        values.put(FIELD_COMPONENT_TYPE, event.getComponentType());
        values.put(FIELD_COMPONENT_NAME, componentName);
        values.put(FIELD_PROCESS_GROUP_ID, processGroupId);
        values.put(FIELD_PROCESS_GROUP_NAME, processGroupName);
        values.put(FIELD_ENTITY_ID, event.getFlowFileUuid());
        values.put(FIELD_ENTITY_TYPE, "org.apache.nifi.flowfile.FlowFile");
        values.put(FIELD_ENTITY_SIZE, event.getFileSize());
        values.put(FIELD_PREV_ENTITY_SIZE, event.getPreviousFileSize());
        values.put(FIELD_UPDATED_ATTRIBUTES,
                filterAttributes(event.getUpdatedAttributes() != null
                        ? event.getUpdatedAttributes() : Collections.emptyMap()));
        values.put(FIELD_PREV_ATTRIBUTES,
                filterAttributes(event.getPreviousAttributes() != null
                        ? event.getPreviousAttributes() : Collections.emptyMap()));
        values.put(FIELD_ACTOR_HOSTNAME, ctx.hostname());
        if (contentBase != null) {
            final String clusterSuffix = resolveClusterSuffix(ctx);
            values.put(FIELD_CONTENT_URI, contentBase + "output" + clusterSuffix);
            values.put(FIELD_PREV_CONTENT_URI, contentBase + "input" + clusterSuffix);
        } else {
            values.put(FIELD_CONTENT_URI, null);
            values.put(FIELD_PREV_CONTENT_URI, null);
        }
        values.put(FIELD_PARENT_IDS,
                event.getParentUuids() != null ? new ArrayList<>(event.getParentUuids()) : Collections.emptyList());
        values.put(FIELD_CHILD_IDS,
                event.getChildUuids() != null ? new ArrayList<>(event.getChildUuids()) : Collections.emptyList());
        values.put(FIELD_PLATFORM, ctx.platform());
        values.put(FIELD_APPLICATION, ctx.applicationName() != null ? ctx.applicationName() : "");
        values.put(FIELD_REMOTE_IDENTIFIER, event.getSourceSystemFlowFileIdentifier());
        values.put(FIELD_ALT_IDENTIFIER, event.getAlternateIdentifierUri());
        values.put(FIELD_TRANSIT_URI, event.getTransitUri());

        return new MapRecord(PROVENANCE_SCHEMA, values);
    }

    private static List<Pattern> parsePatterns(final String value) {
        if (value == null || value.isBlank()) {
            return Collections.emptyList();
        }
        final List<Pattern> patterns = new ArrayList<>();
        for (final String entry : value.split(",")) {
            final String trimmed = entry.trim();
            if (!trimmed.isEmpty()) {
                patterns.add(Pattern.compile(trimmed));
            }
        }
        return Collections.unmodifiableList(patterns);
    }

    private int resolveAttributeMaxLength(final Integer configured) {
        if (configured == null) {
            return -1;
        }
        int effective = configured;
        if (effective < ATTR_LENGTH_MIN) {
            getLogger().warn(
                    "Configured '{}' is {}; enforcing minimum of {} to preserve UUID attributes.",
                    ATTRIBUTE_MAX_LENGTH.getDisplayName(), effective, ATTR_LENGTH_MIN);
            effective = ATTR_LENGTH_MIN;
        }
        if (effective >= NIFI_DEFAULT_MAX_ATTR_LENGTH) {
            getLogger().info(
                    "Configured '{}' ({}) is greater than or equal to the NiFi global provenance limit ({}). " +
                            "Attribute value trimming will not be applied because NiFi already enforces that ceiling.",
                    ATTRIBUTE_MAX_LENGTH.getDisplayName(), effective, NIFI_DEFAULT_MAX_ATTR_LENGTH);
            return -1;
        }
        return effective;
    }

    private Map<String, String> filterAttributes(final Map<String, String> attributes) {
        if (attributes == null) {
            return Collections.emptyMap();
        }
        final List<Pattern> includePatterns = attributeIncludePatterns.get();
        final List<Pattern> excludePatterns = attributeExcludePatterns.get();
        final int maxLength = attributeMaxLength;

        if (includePatterns.isEmpty() && excludePatterns.isEmpty() && maxLength < 0) {
            return attributes;
        }

        final Map<String, String> filtered = new LinkedHashMap<>();
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            if (key == null
                    || (!includePatterns.isEmpty() && !matchesAny(key, includePatterns))
                    || (!excludePatterns.isEmpty() && matchesAny(key, excludePatterns))) {
                continue;
            }
            String attrValue = entry.getValue();
            if (attrValue != null && maxLength > 0 && attrValue.length() > maxLength) {
                attrValue = attrValue.substring(0, maxLength);
            }
            filtered.put(key, attrValue);
        }
        return filtered;
    }

    private static boolean matchesAny(final String key, final List<Pattern> patterns) {
        for (final Pattern pattern : patterns) {
            if (pattern.matcher(key).matches()) {
                return true;
            }
        }
        return false;
    }

    private byte[] encodeRecord(
            final RecordSetWriterFactory writerFactory,
            final MapRecord mapRecord) throws IOException {

        try {
            final RecordSchema writeSchema = writerFactory.getSchema(Collections.emptyMap(), PROVENANCE_SCHEMA);
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (final RecordSetWriter writer =
                         writerFactory.createWriter(getLogger(), writeSchema, baos, Collections.emptyMap())) {
                writer.write(mapRecord);
            }
            return baos.toByteArray();
        } catch (final SchemaNotFoundException e) {
            throw new IOException("Schema lookup failed for Provenance record", e);
        }
    }

    private ObjectNode serializeToJson(
            final ProvenanceEventRecord event,
            final String componentName,
            final String processGroupId,
            final String processGroupName,
            final EncodingContext ctx) {

        final boolean allowNullValues = ctx.allowNullValues();
        final String contentBase = resolveContentBase(event, ctx);
        final ObjectNode node = OBJECT_MAPPER.createObjectNode();

        addField(node, FIELD_EVENT_ID, UUID.randomUUID().toString(), allowNullValues);
        addField(node, FIELD_EVENT_ORDINAL, event.getEventId(), allowNullValues);
        addField(node, FIELD_EVENT_TYPE, event.getEventType().name(), allowNullValues);
        addField(node, FIELD_TIMESTAMP_MILLIS, event.getEventTime(), allowNullValues);
        addField(node, FIELD_TIMESTAMP,
                DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(event.getEventTime())), allowNullValues);
        addField(node, FIELD_DURATION_MILLIS, event.getEventDuration(), allowNullValues);
        addField(node, FIELD_LINEAGE_START, event.getLineageStartDate(), allowNullValues);
        addField(node, FIELD_DETAILS, event.getDetails(), allowNullValues);
        addField(node, FIELD_COMPONENT_ID, event.getComponentId(), allowNullValues);
        addField(node, FIELD_COMPONENT_TYPE, event.getComponentType(), allowNullValues);
        addField(node, FIELD_COMPONENT_NAME, componentName, allowNullValues);
        addField(node, FIELD_PROCESS_GROUP_ID, processGroupId, allowNullValues);
        addField(node, FIELD_PROCESS_GROUP_NAME, processGroupName, allowNullValues);
        addField(node, FIELD_ENTITY_ID, event.getFlowFileUuid(), allowNullValues);
        addField(node, FIELD_ENTITY_TYPE, "org.apache.nifi.flowfile.FlowFile", allowNullValues);
        addField(node, FIELD_ENTITY_SIZE, event.getFileSize(), allowNullValues);
        addField(node, FIELD_PREV_ENTITY_SIZE, event.getPreviousFileSize(), allowNullValues);
        addMapField(node, FIELD_UPDATED_ATTRIBUTES,
                filterAttributes(event.getUpdatedAttributes()), allowNullValues);
        addMapField(node, FIELD_PREV_ATTRIBUTES,
                filterAttributes(event.getPreviousAttributes()), allowNullValues);
        addField(node, FIELD_ACTOR_HOSTNAME, ctx.hostname(), allowNullValues);
        if (contentBase != null) {
            final String clusterSuffix = resolveClusterSuffix(ctx);
            addField(node, FIELD_CONTENT_URI, contentBase + "output" + clusterSuffix, allowNullValues);
            addField(node, FIELD_PREV_CONTENT_URI, contentBase + "input" + clusterSuffix, allowNullValues);
        } else if (allowNullValues) {
            node.putNull(FIELD_CONTENT_URI);
            node.putNull(FIELD_PREV_CONTENT_URI);
        }
        addCollectionField(node, FIELD_PARENT_IDS, event.getParentUuids(), allowNullValues);
        addCollectionField(node, FIELD_CHILD_IDS, event.getChildUuids(), allowNullValues);
        addField(node, FIELD_TRANSIT_URI, event.getTransitUri(), allowNullValues);
        addField(node, FIELD_REMOTE_IDENTIFIER, event.getSourceSystemFlowFileIdentifier(), allowNullValues);
        addField(node, FIELD_ALT_IDENTIFIER, event.getAlternateIdentifierUri(), allowNullValues);
        addField(node, FIELD_PLATFORM, ctx.platform(), allowNullValues);
        addField(node, FIELD_APPLICATION, ctx.applicationName(), allowNullValues);

        return node;
    }

    private ProducerConfiguration buildProducerConfiguration(final ConfigurationContext context) {
        final boolean transactionsEnabled = context.getProperty(TRANSACTIONS_ENABLED).asBoolean();
        final String transactionalIdPrefix = transactionsEnabled
                ? context.getProperty(TRANSACTIONAL_ID_PREFIX).evaluateAttributeExpressions().getValue()
                : null;
        return new ProducerConfiguration(
                transactionsEnabled,
                transactionalIdPrefix,
                context.getProperty(DELIVERY_GUARANTEE).getValue(),
                context.getProperty(COMPRESSION_TYPE).getValue(),
                null,
                context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue()
        );
    }

    private static byte[] resolveMessageKey(final ProvenanceEventRecord event, final String keyField) {
        final String keyValue = switch (keyField) {
            case FIELD_LINEAGE_START -> String.valueOf(event.getLineageStartDate());
            case FIELD_ENTITY_ID -> event.getFlowFileUuid();
            case FIELD_COMPONENT_ID -> event.getComponentId();
            case FIELD_EVENT_TYPE -> event.getEventType().name();
            case FIELD_EVENT_ORDINAL -> String.valueOf(event.getEventId());
            default -> null;
        };
        return keyValue == null ? null : keyValue.getBytes(StandardCharsets.UTF_8);
    }

    private static String resolveUrlBase(final URL nifiUrl) {
        final String urlString = nifiUrl.toString();
        return urlString.endsWith(NIFI_API_PATH)
                ? urlString.substring(0, urlString.length() - NIFI_API_PATH.length())
                : urlString;
    }

    private static String resolveContentBase(final ProvenanceEventRecord event, final EncodingContext ctx) {
        return ctx.nifiUrlBase() == null ? null
                : ctx.nifiUrlBase() + "/nifi-api/provenance-events/" + event.getEventId() + "/content/";
    }

    private static String resolveClusterSuffix(final EncodingContext ctx) {
        return ctx.nodeIdentifier() == null ? "" : "?clusterNodeId=" + ctx.nodeIdentifier();
    }

    private static void addField(final ObjectNode node, final String key,
                                 final Object value, final boolean allowNullValues) {
        switch (value) {
            case String s -> node.put(key, s);
            case Long l -> node.put(key, l);
            case Integer i -> node.put(key, i);
            case Boolean b -> node.put(key, b);
            case null -> {
                if (allowNullValues) {
                    node.putNull(key);
                }
            }
            default -> node.put(key, value.toString());
        }
    }

    private static void addMapField(final ObjectNode node, final String key,
                                    final Map<String, String> values,
                                    final boolean allowNullValues) {
        if (values != null) {
            final ObjectNode mapNode = OBJECT_MAPPER.createObjectNode();
            for (final Map.Entry<String, String> entry : values.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                if (entry.getValue() == null) {
                    if (allowNullValues) {
                        mapNode.putNull(entry.getKey());
                    }
                } else {
                    mapNode.put(entry.getKey(), entry.getValue());
                }
            }
            node.set(key, mapNode);
        } else if (allowNullValues) {
            node.putNull(key);
        }
    }

    private static void addCollectionField(final ObjectNode node, final String key,
                                           final Collection<String> values,
                                           final boolean allowNullValues) {
        if (values != null) {
            final ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
            for (final String v : values) {
                if (v != null) {
                    arrayNode.add(v);
                }
            }
            node.set(key, arrayNode);
        } else if (allowNullValues) {
            node.putNull(key);
        }
    }
}
