/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.influxdb;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.influxdb.services.InfluxDBService;
import org.apache.nifi.influxdb.util.PropertyValueUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.influxdb.WriteOptions.MissingItemsBehaviour;
import org.apache.nifi.processors.influxdb.WriteOptions.NullValueBehaviour;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBIOException;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.influxdb.util.PropertyValueUtils.getEnumValue;
import static org.influxdb.BatchOptions.DEFAULT_BATCH_INTERVAL_DURATION;
import static org.influxdb.BatchOptions.DEFAULT_JITTER_INTERVAL_DURATION;

@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({"influxdb", "measurement", "insert", "write", "put", "record", "timeseries"})
@CapabilityDescription("PutInfluxDBRecord processor uses a specified RecordReader to write the content of a FlowFile " +
        "into InfluxDB database.")
@WritesAttributes({@WritesAttribute(
        attribute = AbstractInfluxDBProcessor.INFLUX_DB_ERROR_MESSAGE,
        description = "InfluxDB error message"),
})
public class PutInfluxDBRecord extends AbstractInfluxDBProcessor {

    protected static final String DATABASE_NAME_EMPTY_MESSAGE =
            "Cannot configure InfluxDB client because Database Name is null or empty.";

    protected static final String MEASUREMENT_NAME_EMPTY_MESSAGE =
            "Cannot write FlowFile to InfluxDB because Measurement Name is null or empty.";

    protected static final String AT_LEAST_ONE_FIELD_DEFINED_MESSAGE =
            "Cannot write FlowFile to InfluxDB because at least one field must be defined.";

    protected static final String REQUIRED_FIELD_MISSING =
            "Cannot write FlowFile to InfluxDB because the required field '%s' is not present in Record.";

    protected static final String UNSUPPORTED_FIELD_TYPE =
            "Cannot write FlowFile to InfluxDB because the field '%s' has a unsupported type '%s'.";

    protected static final String FIELD_NULL_VALUE =
            "Cannot write FlowFile to InfluxDB because the field '%s' has null value.";

    /**
     * Influx consistency levels.
     */
    private static final AllowableValue CONSISTENCY_LEVEL_ALL = new AllowableValue(
            ConsistencyLevel.ALL.name(),
            "All",
            "Return success when all nodes have responded with write success");

    private static final AllowableValue CONSISTENCY_LEVEL_ANY = new AllowableValue(
            ConsistencyLevel.ANY.name(),
            "Any",
            "Return success when any nodes have responded with write success");

    private static final AllowableValue CONSISTENCY_LEVEL_ONE = new AllowableValue(
            ConsistencyLevel.ONE.name(),
            "One",
            "Return success when one node has responded with write success");

    private static final AllowableValue CONSISTENCY_LEVEL_QUORUM = new AllowableValue(
            ConsistencyLevel.QUORUM.name(),
            "Quorum",
            "Return success when a majority of nodes have responded with write success");

    /**
     * Influx Log levels.
     */
    private static final AllowableValue NONE =
            new AllowableValue("NONE", "None", "No logging");

    private static final AllowableValue BASIC =
            new AllowableValue("BASIC", "Basic",
                    "Log only the request method and URL and the response status code and execution time.");

    private static final AllowableValue HEADERS =
            new AllowableValue("HEADERS", "Headers",
                    "Log the basic information along with request and response headers.");

    private static final AllowableValue FULL =
            new AllowableValue("FULL", "Full",
                    "Log the headers, body, and metadata for both requests and responses. "
                            + "Note: This requires that the entire request and response body be buffered in memory!");

    /**
     * Missing items behaviour.
     */
    private static final AllowableValue MISSING_ITEMS_BEHAVIOUR_IGNORE = new AllowableValue(
            MissingItemsBehaviour.IGNORE.name(),
            "Ignore",
            "The item that is not present in the document is silently ignored.");

    private static final AllowableValue MISSING_ITEMS_BEHAVIOUR_FAIL = new AllowableValue(
            MissingItemsBehaviour.FAIL.name(),
            "Fail",
            "If the item is not present in the document, the FlowFile will be routed to the failure relationship.");


    /**
     * Null values behaviour.
     */
    private static final AllowableValue NULL_VALUE_BEHAVIOUR_IGNORE = new AllowableValue(
            NullValueBehaviour.IGNORE.name(),
            "Ignore",
            "Silently skip fields with a null value.");

    private static final AllowableValue NULL_VALUE_BEHAVIOUR_FAIL = new AllowableValue(
            NullValueBehaviour.FAIL.name(),
            "Fail",
            "Fail when the field has a null value.");

    /**
     * Complex field behaviour
     */
    protected static final AllowableValue COMPLEX_FIELD_FAIL = new AllowableValue(
            WriteOptions.ComplexFieldBehaviour.FAIL.name(),
            "Fail",
            "Route entire FlowFile to failure if any elements contain complex values.");

    protected static final AllowableValue COMPLEX_FIELD_WARN = new AllowableValue(
            WriteOptions.ComplexFieldBehaviour.WARN.name(),
            "Warn",
            "Provide a warning and do not include field InfluxDB data point.");

    protected static final AllowableValue COMPLEX_FIELD_IGNORE = new AllowableValue(
            WriteOptions.ComplexFieldBehaviour.IGNORE.name(),
            "Ignore",
            "Silently ignore and do not include field InfluxDB data point.");

    protected static final AllowableValue COMPLEX_FIELD_VALUE = new AllowableValue(
            WriteOptions.ComplexFieldBehaviour.TEXT.name(),
            "Text",
            "Use the string representation of the complex field as the value of the given field.");


    protected static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written into InlfuxDB are routed to this relationship")
            .build();

    protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to InfluxDB are routed to this relationship")
            .build();

    protected static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting "
                    + "the operation again may succeed. ")
            .build();

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data "
                    + "and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INFLUX_DB_SERVICE = new PropertyDescriptor.Builder()
            .name("influxdb-service")
            .displayName("InfluxDB Controller Service")
            .description("A controller service that provides connection to InfluxDB")
            .required(true)
            .identifiesControllerService(InfluxDBService.class)
            .build();

    public static final PropertyDescriptor ENABLE_GZIP = new PropertyDescriptor.Builder()
            .name("influxdb-enable-gzip")
            .displayName("Enable gzip compression")
            .description("Enable gzip compression for InfluxDB http request body.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("false", "true")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("influxdb-log-level")
            .displayName("Log Level")
            .description("Controls the level of logging for the REST layer of InfluxDB client.")
            .required(true)
            .allowableValues(NONE, BASIC, HEADERS, FULL)
            .defaultValue(NONE.getValue())
            .build();

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .name("influxdb-consistency-level")
            .displayName("Consistency Level")
            .description("InfluxDB consistency level")
            .required(true)
            .allowableValues(
                    CONSISTENCY_LEVEL_ONE,
                    CONSISTENCY_LEVEL_ANY,
                    CONSISTENCY_LEVEL_ALL,
                    CONSISTENCY_LEVEL_QUORUM)
            .defaultValue(CONSISTENCY_LEVEL_ONE.getValue())
            .build();

    public static final PropertyDescriptor RETENTION_POLICY = new PropertyDescriptor.Builder()
            .name("influxdb-retention-policy")
            .displayName("Retention Policy")
            .description("Retention policy for the saving the records")
            .defaultValue(WriteOptions.DEFAULT_RETENTION_POLICY)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENABLE_BATCHING = new PropertyDescriptor.Builder()
            .name("influxdb-enable-batch")
            .displayName("Enable InfluxDB batching")
            .description("Enabled batching speed up writes significantly " +
                    "but in the cost of loosing reliability. Flow file can be transfered to success releation " +
                    "before the batch buffer is flushed into database. For additional information see " +
                    "processor documentation.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("false", "true")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor BATCH_ACTIONS = new PropertyDescriptor.Builder()
            .name("influxdb-batch-actions")
            .displayName("Batch actions")
            .description("The number of batch actions to collect")
            .required(false)
            .defaultValue(Integer.toString(BatchOptions.DEFAULT_BATCH_ACTIONS_LIMIT))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_FLUSH_DURATION = new PropertyDescriptor.Builder()
            .name("influxdb-batch-flush-duration")
            .displayName("Batch flush duration")
            .description("Flush at least every specified time")
            .defaultValue(Integer.toString(DEFAULT_BATCH_INTERVAL_DURATION) + " ms")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_JITTER_DURATION = new PropertyDescriptor.Builder()
            .name("influxdb-batch-jitter-duration")
            .displayName("Batch flush jitter")
            .description("Jitters the batch flush interval by a random amount. This is primarily to avoid "
                    + " large write spikes for users running a large number of client instances. "
                    + " ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s.")
            .required(false)
            .defaultValue(Integer.toString(DEFAULT_JITTER_INTERVAL_DURATION) + " ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_BUFFER_LIMIT = new PropertyDescriptor.Builder()
            .name("influxdb-batch-buffer-limit")
            .displayName("Batch flush buffer limit")
            .description("The client maintains a buffer for failed writes so that the writes will be retried "
                    + "later on. This may help to overcome temporary network problems or InfluxDB load spikes. "
                    + "When the buffer is full and new points are written, oldest entries in the buffer are lost. "
                    + "To disable this feature set buffer limit to a value smaller than getActions")
            .required(false)
            .defaultValue(Integer.toString(BatchOptions.DEFAULT_BUFFER_LIMIT))
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();


    protected static final PropertyDescriptor MEASUREMENT = new PropertyDescriptor.Builder()
            .name("influxdb-measurement")
            .displayName("Measurement")
            .description("The name of the measurement."
                    + " If the Record contains a field with measurement property value, "
                    + "then value of the Record field is use as InfluxDB measurement.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("measurement")
            .build();

    protected static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("influxdb-fields")
            .displayName("Fields")
            .description("A comma-separated list of record fields stored in InfluxDB as 'field'. "
                    + " At least one field must be defined")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("fields")
            .build();

    protected static final PropertyDescriptor TAGS = new PropertyDescriptor.Builder()
            .name("influxdb-tags")
            .displayName("Tags")
            .description("A comma-separated list of record fields stored in InfluxDB as 'tag'.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("tags")
            .build();

    protected static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("influxdb-timestamp-field")
            .displayName("Timestamp field")
            .description("A name of the record field that used as a 'timestamp'. "
                    + "If it is not specified, current system time is used. "
                    + "The support types of field value are: java.util.Date, java.lang.Number, "
                    + "java.lang.String (parsable to Long).")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("timestamp")
            .build();

    protected static final PropertyDescriptor TIMESTAMP_PRECISION = new PropertyDescriptor.Builder()
            .name("influxdb-timestamp-precision")
            .displayName("Timestamp precision")
            .description("The timestamp precision is ignore when the 'Timestamp field' value is 'java.util.Date'.")
            .defaultValue(WriteOptions.PRECISION_DEFAULT.name())
            .required(true)
            .allowableValues(Arrays.stream(TimeUnit.values()).map(Enum::name).toArray(String[]::new))
            .sensitive(false)
            .build();

    protected static final PropertyDescriptor MISSING_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-fields-field-behavior")
            .displayName("Missing Field Behavior")
            .description("If the specified field is not present in the document, "
                    + "this property specifies how to handle the situation.")
            .allowableValues(MISSING_ITEMS_BEHAVIOUR_IGNORE, MISSING_ITEMS_BEHAVIOUR_FAIL)
            .defaultValue(WriteOptions.MISSING_FIELDS_BEHAVIOUR_DEFAULT.name())
            .build();

    protected static final PropertyDescriptor MISSING_TAG_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-tags-field-behavior")
            .displayName("Missing Tag Behavior")
            .description("If the specified tag is not present in the document, "
                    + "this property specifies how to handle the situation.")
            .allowableValues(MISSING_ITEMS_BEHAVIOUR_IGNORE, MISSING_ITEMS_BEHAVIOUR_FAIL)
            .defaultValue(WriteOptions.MISSING_TAGS_BEHAVIOUR_DEFAULT.name())
            .build();

    protected static final PropertyDescriptor COMPLEX_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-complex-field-behavior")
            .displayName("Complex Field Behavior")
            .description("Indicates how to handle complex fields, i.e. fields that do not have a primitive value.")
            .required(true)
            .allowableValues(COMPLEX_FIELD_VALUE, COMPLEX_FIELD_IGNORE, COMPLEX_FIELD_WARN, COMPLEX_FIELD_FAIL)
            .defaultValue(WriteOptions.COMPLEX_FIELD_BEHAVIOUR_DEFAULT.name())
            .build();

    protected static final PropertyDescriptor NULL_VALUE_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("influxdb-null-behavior")
            .displayName("Null Values Behavior")
            .description("Indicates how to handle null fields, i.e. fields that do not have a defined value.")
            .required(true)
            .allowableValues(NULL_VALUE_BEHAVIOUR_IGNORE, NULL_VALUE_BEHAVIOUR_FAIL)
            .defaultValue(WriteOptions.NULL_FIELD_VALUE_BEHAVIOUR_DEFAULT.name())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    private static final Set<Relationship> RELATIONSHIPS;

    static {

        final Set<Relationship> relationships = new LinkedHashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_RETRY);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(RECORD_READER_FACTORY);

        propertyDescriptors.add(INFLUX_DB_SERVICE);
        propertyDescriptors.add(DB_NAME);
        propertyDescriptors.add(ENABLE_GZIP);
        propertyDescriptors.add(LOG_LEVEL);

        propertyDescriptors.add(CONSISTENCY_LEVEL);
        propertyDescriptors.add(RETENTION_POLICY);

        propertyDescriptors.add(ENABLE_BATCHING);
        propertyDescriptors.add(BATCH_FLUSH_DURATION);
        propertyDescriptors.add(BATCH_ACTIONS);
        propertyDescriptors.add(BATCH_JITTER_DURATION);
        propertyDescriptors.add(BATCH_BUFFER_LIMIT);

        propertyDescriptors.add(MEASUREMENT);

        propertyDescriptors.add(TAGS);
        propertyDescriptors.add(MISSING_TAG_BEHAVIOR);

        propertyDescriptors.add(FIELDS);
        propertyDescriptors.add(MISSING_FIELD_BEHAVIOR);

        propertyDescriptors.add(TIMESTAMP_FIELD);
        propertyDescriptors.add(TIMESTAMP_PRECISION);

        propertyDescriptors.add(COMPLEX_FIELD_BEHAVIOR);
        propertyDescriptors.add(NULL_VALUE_BEHAVIOR);
        propertyDescriptors.add(MAX_RECORDS_SIZE);

        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(propertyDescriptors);
    }

    protected InfluxDBService influxDBService;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Assigns the InfluxDB Service on scheduling.
     *
     * @param context the process context provided on scheduling the processor.
     */
    @OnScheduled
    public void onScheduled(@NonNull final ProcessContext context) {

        Objects.requireNonNull(context, "ProcessContext is required");

        super.onScheduled(context);

        influxDBService = context.getProperty(INFLUX_DB_SERVICE).asControllerService(InfluxDBService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {

            WriteOptions writeOptions = writeOptions(context, flowFile);

            // Init Mapper
            RecordToPointMapper pointMapper = RecordToPointMapper
                    .createMapper(session, context, getLogger(), writeOptions);

            // Write to InfluxDB
            pointMapper
                    .mapFlowFile(flowFile)
                    .writeToInflux(getInfluxDB(context))
                    .reportResults(influxDBService.getDatabaseURL());

            session.transfer(flowFile, REL_SUCCESS);

        } catch (InfluxDBException.DatabaseNotFoundException
                | InfluxDBException.AuthorizationFailedException
                | InfluxDBException.CacheMaxMemorySizeExceededException e) {

            flowFile = logException(flowFile, session, e);
            flowFile = session.penalize(flowFile);

            session.transfer(flowFile, REL_RETRY);

        } catch (InfluxDBException.HintedHandOffQueueNotEmptyException | InfluxDBIOException e) {

            flowFile = logException(flowFile, session, e);
            session.transfer(flowFile, REL_RETRY);

            context.yield();

        } catch (Exception e) {

            flowFile = logException(flowFile, session, e);

            if (ExceptionUtils.indexOfType(e, UnknownHostException.class) != -1) {

                session.transfer(flowFile, REL_RETRY);
                context.yield();

            } else {

                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * Helper method to create InfluxDB instance by the InfluxDBService
     * @return InfluxDB instance
     */
    @Override
    protected synchronized InfluxDB getInfluxDB(final ProcessContext context) {

        if (influxDB.get() == null) {

            try {
                InfluxDB influxDBClient = influxDBService.connect();
                configure(influxDBClient, context);

                influxDB.set(influxDBClient);

            } catch (Exception e) {

                String message = "Error while getting connection " + e.getLocalizedMessage();

                getLogger().error(message, e);

                throw new RuntimeException(message, e);
            }

            getLogger().info("InfluxDB connection created for host {}", new Object[]{influxDBService.getDatabaseURL()});
        }

        return influxDB.get();
    }

    protected void configure(@NonNull final InfluxDB influxDBClient, @NonNull final ProcessContext context) {

        Objects.requireNonNull(influxDBClient, "InfluxDB client instance is required for configuration");
        Objects.requireNonNull(context, "Context of Processor is required");

        // GZIP
        Boolean enableGzip = context.getProperty(ENABLE_GZIP).asBoolean();
        if (BooleanUtils.isTrue(enableGzip)) {

            influxDBClient.enableGzip();
        } else {

            influxDBClient.disableGzip();
        }

        // LOG Level
        InfluxDB.LogLevel logLevel = getEnumValue(LOG_LEVEL, context, InfluxDB.LogLevel.class, InfluxDB.LogLevel.NONE);
        influxDBClient.setLogLevel(logLevel);

        // Consistency Level
        ConsistencyLevel consistencyLevel = getEnumValue(CONSISTENCY_LEVEL, context, ConsistencyLevel.class, ConsistencyLevel.ONE);
        influxDBClient.setConsistency(consistencyLevel);

        // Batching
        Boolean enableBatching = context.getProperty(ENABLE_BATCHING).asBoolean();
        if (BooleanUtils.isTrue(enableBatching)) {

            Long flushDuration = context.getProperty(BATCH_FLUSH_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
            Integer actions = context.getProperty(BATCH_ACTIONS).asInteger();
            Long jitter = context.getProperty(BATCH_JITTER_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
            Integer limit = context.getProperty(BATCH_BUFFER_LIMIT).asInteger();

            BatchOptions batchOptions = BatchOptions.DEFAULTS
                    .flushDuration(flushDuration != null ? flushDuration.intValue() : DEFAULT_BATCH_INTERVAL_DURATION)
                    .actions(actions != null ? actions : BatchOptions.DEFAULT_BATCH_ACTIONS_LIMIT)
                    .jitterDuration(jitter != null ? jitter.intValue() : DEFAULT_JITTER_INTERVAL_DURATION)
                    .bufferLimit(limit != null ? limit : BatchOptions.DEFAULT_BUFFER_LIMIT)
                    .consistency(consistencyLevel);

            influxDBClient.enableBatch(batchOptions);
        } else {

            influxDBClient.disableBatch();
        }

    }

    @NonNull
    protected WriteOptions writeOptions(@NonNull final ProcessContext context, @Nullable final FlowFile flowFile)
            throws IllegalConfigurationException {

        Objects.requireNonNull(context, "Context of Processor is required");

        // Database
        String database = context.getProperty(DB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(database)) {
            throw new IllegalConfigurationException(DATABASE_NAME_EMPTY_MESSAGE);
        }

        // Retention policy
        String retentionPolicy = context.getProperty(RETENTION_POLICY).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(retentionPolicy)) {

            retentionPolicy = WriteOptions.DEFAULT_RETENTION_POLICY;
        }

        // Timestamp
        String timestamp = context.getProperty(TIMESTAMP_FIELD).evaluateAttributeExpressions(flowFile).getValue();

        // Timestamp precision
        TimeUnit precision = getEnumValue(TIMESTAMP_PRECISION, context, TimeUnit.class, WriteOptions.PRECISION_DEFAULT);

        // Measurement
        String measurement = context.getProperty(MEASUREMENT).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(measurement)) {
            throw new IllegalConfigurationException(MEASUREMENT_NAME_EMPTY_MESSAGE);
        }

        // Fields
        List<String> fields = PropertyValueUtils.getList(FIELDS, context, flowFile);
        if (fields.isEmpty()) {
            throw new IllegalConfigurationException(AT_LEAST_ONE_FIELD_DEFINED_MESSAGE);
        }

        // Missing fields
        MissingItemsBehaviour missingFields = getEnumValue(MISSING_FIELD_BEHAVIOR, context, MissingItemsBehaviour.class,
                WriteOptions.MISSING_FIELDS_BEHAVIOUR_DEFAULT);

        // Tags
        List<String> tags = PropertyValueUtils.getList(TAGS, context, flowFile);

        // Missing tags
        MissingItemsBehaviour missingTags = getEnumValue(MISSING_TAG_BEHAVIOR, context, MissingItemsBehaviour.class,
                WriteOptions.MISSING_TAGS_BEHAVIOUR_DEFAULT);

        // Complex fields behaviour
        WriteOptions.ComplexFieldBehaviour complexFieldBehaviour = getEnumValue(COMPLEX_FIELD_BEHAVIOR, context,
                WriteOptions.ComplexFieldBehaviour.class, WriteOptions.COMPLEX_FIELD_BEHAVIOUR_DEFAULT);

        // Null Field Value Behaviour
        NullValueBehaviour nullValueBehaviour = getEnumValue(NULL_VALUE_BEHAVIOR, context, NullValueBehaviour.class, NullValueBehaviour.IGNORE);

        return new WriteOptions()
                .database(database)
                .setRetentionPolicy(retentionPolicy)
                .timestamp(timestamp)
                .precision(precision)
                .measurement(measurement)
                .fields(fields)
                .missingFields(missingFields)
                .tags(tags)
                .missingTags(missingTags)
                .complexFieldBehaviour(complexFieldBehaviour)
                .nullValueBehaviour(nullValueBehaviour);
    }

    @NonNull
    private FlowFile logException(@NonNull final FlowFile flowFile,
                                  @NonNull final ProcessSession session,
                                  @Nullable final Exception e) {

        if (e == null) {
            return flowFile;
        }

        String flowFileName = flowFile.getAttributes().get(CoreAttributes.FILENAME.key());

        getLogger().error(INFLUX_DB_ERROR_MESSAGE_LOG, new Object[]{flowFileName, e.getLocalizedMessage()}, e);

        return session.putAttribute(flowFile, AbstractInfluxDBProcessor.INFLUX_DB_ERROR_MESSAGE, String.valueOf(e.getMessage()));
    }

    protected class IllegalConfigurationException extends Exception {

        protected IllegalConfigurationException(final String message) {

            super(message);
        }
    }
}
