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
package org.apache.nifi.processors.aws.kinesis.stream;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.WorkerStateChangeListener;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean2;
import org.apache.commons.beanutils.FluentPropertyBeanIntrospector;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.kinesis.stream.record.AbstractKinesisRecordProcessor;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorRaw;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorRecord;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@Tags({"amazon", "aws", "kinesis", "consume", "stream"})
@CapabilityDescription("Reads data from the specified AWS Kinesis stream and outputs a FlowFile for every processed Record (raw) " +
        " or a FlowFile for a batch of processed records if a Record Reader and Record Writer are configured. " +
        "At-least-once delivery of all Kinesis Records within the Stream while the processor is running. " +
        "AWS Kinesis Client Library can take several seconds to initialise before starting to fetch data. " +
        "Uses DynamoDB for check pointing and CloudWatch (optional) for metrics. " +
        "Ensure that the credentials provided have access to DynamoDB and CloudWatch (optional) along with Kinesis.")
@WritesAttributes({
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY,
                description = "Partition key of the (last) Kinesis Record read from the Shard"),
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_SHARD_ID,
                description = "Shard ID from which the Kinesis Record was read"),
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_SEQUENCE_NUMBER,
                description = "The unique identifier of the (last) Kinesis Record within its Shard"),
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                description = "Approximate arrival timestamp of the (last) Kinesis Record read from the stream"),
        @WritesAttribute(attribute = "mime.type",
                description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer (if configured)"),
        @WritesAttribute(attribute = "record.count",
                description = "Number of records written to the FlowFiles by the Record Writer (if configured)"),
        @WritesAttribute(attribute = "record.error.message",
                description = "This attribute provides on failure the error message encountered by the Record Reader or Record Writer (if configured)")
})
@DynamicProperties({
        @DynamicProperty(name="Kinesis Client Library (KCL) Configuration property name",
                description="Override default KCL Configuration properties with required values. Supports setting of values via the \"with\" " +
                        "methods on the KCL Configuration class. Specify the property to be set without the leading prefix, e.g. \"maxInitialisationAttempts\" " +
                        "will call \"withMaxInitialisationAttempts\" and set the provided value. Only supports setting of simple property values, e.g. String, " +
                        "int, long and boolean. Does not allow override of KCL Configuration settings handled by non-dynamic processor properties.",
                expressionLanguageScope = ExpressionLanguageScope.NONE, value="Value to set in the KCL Configuration property")
})
@SystemResourceConsideration(resource = SystemResource.CPU, description = "Kinesis Client Library is used to create a Worker thread for consumption of Kinesis Records. " +
        "The Worker is initialised and started when this Processor has been triggered. It runs continually, spawning Kinesis Record Processors as required " +
        "to fetch Kinesis Records. The Worker Thread (and any child Record Processor threads) are not controlled by the normal NiFi scheduler as part of the " +
        "Concurrent Thread pool and are not released until this processor is stopped.")
@SystemResourceConsideration(resource = SystemResource.NETWORK, description = "Kinesis Client Library will continually poll for new Records, " +
        "requesting up to a maximum number of Records/bytes per call. This can result in sustained network usage.")
@SeeAlso(PutKinesisStream.class)
public class ConsumeKinesisStream extends AbstractKinesisStreamProcessor {
    static final AllowableValue TRIM_HORIZON = new AllowableValue(
            InitialPositionInStream.TRIM_HORIZON.toString(),
            InitialPositionInStream.TRIM_HORIZON.toString(),
            "Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard."
    );
    static final AllowableValue LATEST = new AllowableValue(
            InitialPositionInStream.LATEST.toString(),
            InitialPositionInStream.LATEST.toString(),
            "Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard."
    );
    static final AllowableValue AT_TIMESTAMP = new AllowableValue(
            InitialPositionInStream.AT_TIMESTAMP.toString(),
            InitialPositionInStream.AT_TIMESTAMP.toString(), "Start reading from the position denoted by a specific time stamp, provided in the value Timestamp."
    );

    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .displayName("Application Name")
            .name("amazon-kinesis-stream-application-name")
            .description("The Kinesis stream reader application name.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true).build();

    public static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .displayName("Initial Stream Position")
            .name("amazon-kinesis-stream-initial-position")
            .description("Initial position to read Kinesis streams.")
            .allowableValues(LATEST, TRIM_HORIZON, AT_TIMESTAMP)
            .defaultValue(LATEST.getValue())
            .required(true).build();

    public static final PropertyDescriptor STREAM_POSITION_TIMESTAMP = new PropertyDescriptor.Builder()
            .displayName("Stream Position Timestamp")
            .name("amazon-kinesis-stream-position-timestamp")
            .description("Timestamp position in stream from which to start reading Kinesis Records. " +
                    "Required if " + INITIAL_STREAM_POSITION.getDescription() + " is " + AT_TIMESTAMP.getDisplayName() + ". " +
                    "Uses the Timestamp Format to parse value into a Date.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR) // customValidate checks the value against TIMESTAMP_FORMAT
            .dependsOn(INITIAL_STREAM_POSITION, AT_TIMESTAMP)
            .required(false).build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
            .displayName("Timestamp Format")
            .name("amazon-kinesis-stream-timestamp-format")
            .description("Format to use for parsing the " + STREAM_POSITION_TIMESTAMP.getDisplayName() + " into a Date " +
                    "and converting the Kinesis Record's Approximate Arrival Timestamp into a FlowFile attribute.")
            .addValidator((subject, input, context) -> {
                if (StringUtils.isNotBlank(input)) {
                    try {
                        DateTimeFormatter.ofPattern(input);
                    } catch (Exception e) {
                        return new ValidationResult.Builder().valid(false).subject(subject).input(input)
                                .explanation("Must be a valid java.time.DateTimeFormatter pattern, e.g. " + RecordFieldType.TIMESTAMP.getDefaultFormat())
                                .build();
                    }
                }
                return new ValidationResult.Builder().valid(true).subject(subject).build();
            })
            .defaultValue(RecordFieldType.TIMESTAMP.getDefaultFormat())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true).build();

    public static final PropertyDescriptor FAILOVER_TIMEOUT = new PropertyDescriptor.Builder()
            .displayName("Failover Timeout")
            .name("amazon-kinesis-stream-failover-timeout")
            .description("Kinesis Client Library failover timeout")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .required(true).build();

    public static final PropertyDescriptor GRACEFUL_SHUTDOWN_TIMEOUT = new PropertyDescriptor.Builder()
            .displayName("Graceful Shutdown Timeout")
            .name("amazon-kinesis-stream-graceful-shutdown-timeout")
            .description("Kinesis Client Library graceful shutdown timeout")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("20 secs")
            .required(true).build();

    public static final PropertyDescriptor CHECKPOINT_INTERVAL = new PropertyDescriptor.Builder()
            .displayName("Checkpoint Interval")
            .name("amazon-kinesis-stream-checkpoint-interval")
            .description("Interval between Kinesis checkpoints")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("3 secs")
            .required(true).build();

    public static final PropertyDescriptor NUM_RETRIES = new PropertyDescriptor.Builder()
            .displayName("Retry Count")
            .name("amazon-kinesis-stream-retry-count")
            .description("Number of times to retry a Kinesis operation (process record, checkpoint, shutdown)")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10")
            .required(true).build();

    public static final PropertyDescriptor RETRY_WAIT = new PropertyDescriptor.Builder()
            .displayName("Retry Wait")
            .name("amazon-kinesis-stream-retry-wait")
            .description("Interval between Kinesis operation retries (process record, checkpoint, shutdown)")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 sec")
            .required(true).build();

    public static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .displayName("DynamoDB Override")
            .name("amazon-kinesis-stream-dynamodb-override")
            .description("DynamoDB override to use non-AWS deployments")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false).build();

    public static final PropertyDescriptor REPORT_CLOUDWATCH_METRICS = new PropertyDescriptor.Builder()
            .displayName("Report Metrics to CloudWatch")
            .name("amazon-kinesis-stream-cloudwatch-flag")
            .description("Whether to report Kinesis usage metrics to CloudWatch.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true).build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("amazon-kinesis-stream-record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for reading received messages." +
                    " The Kinesis Stream name can be referred to by Expression Language '${" +
                    AbstractKinesisRecordProcessor.KINESIS_RECORD_SCHEMA_KEY + "}' to access a schema." +
                    " If Record Reader/Writer are not specified, each Kinesis Record will create a FlowFile.")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("amazon-kinesis-stream-record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use for serializing Records to an output FlowFile." +
                    " The Kinesis Stream name can be referred to by Expression Language '${" +
                    AbstractKinesisRecordProcessor.KINESIS_RECORD_SCHEMA_KEY + "}' to access a schema." +
                    " If Record Reader/Writer are not specified, each Kinesis Record will create a FlowFile.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE)
            .required(true)
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message from Kinesis cannot be parsed using the configured Record Reader" +
                    " or failed to be written by the configured Record Writer," +
                    " the contents of the message will be routed to this Relationship as its own individual FlowFile.")
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(
            Arrays.asList(
                    // Kinesis Stream specific properties
                    KINESIS_STREAM_NAME, APPLICATION_NAME, RECORD_READER, RECORD_WRITER, REGION, ENDPOINT_OVERRIDE,
                    DYNAMODB_ENDPOINT_OVERRIDE, INITIAL_STREAM_POSITION, STREAM_POSITION_TIMESTAMP, TIMESTAMP_FORMAT,
                    FAILOVER_TIMEOUT, GRACEFUL_SHUTDOWN_TIMEOUT, CHECKPOINT_INTERVAL, NUM_RETRIES, RETRY_WAIT, REPORT_CLOUDWATCH_METRICS,
                    // generic AWS processor properties
                    TIMEOUT, AWS_CREDENTIALS_PROVIDER_SERVICE, PROXY_CONFIGURATION_SERVICE
            )
    );

    private static final Map<String, PropertyDescriptor> DISALLOWED_DYNAMIC_KCL_PROPERTIES = new HashMap<String, PropertyDescriptor>(){{
        put("regionName", REGION);
        put("timestampAtInitialPositionInStream", STREAM_POSITION_TIMESTAMP);
        put("initialPositionInStream", INITIAL_STREAM_POSITION);
        put("dynamoDBEndpoint", DYNAMODB_ENDPOINT_OVERRIDE);
        put("kinesisEndpoint", ENDPOINT_OVERRIDE);
        put("failoverTimeMillis", FAILOVER_TIMEOUT);
        put("gracefulShutdownMillis", GRACEFUL_SHUTDOWN_TIMEOUT);
    }};

    private static final Object WORKER_LOCK = new Object();
    private static final String WORKER_THREAD_NAME_TEMPLATE = ConsumeKinesisStream.class.getSimpleName() + "-" + Worker.class.getSimpleName() + "-";

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);
    private static final Set<Relationship> RECORD_RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_PARSE_FAILURE)));

    private static final PropertyUtilsBean PROPERTY_UTILS_BEAN;
    private static final BeanUtilsBean BEAN_UTILS_BEAN;
    static {
        PROPERTY_UTILS_BEAN = new PropertyUtilsBean();
        PROPERTY_UTILS_BEAN.addBeanIntrospector(new FluentPropertyBeanIntrospector("with"));

        final ConvertUtilsBean2 convertUtilsBean2 = new ConvertUtilsBean2() {
            @SuppressWarnings("unchecked") // generic Enum conversion from String property values
            @Override
            public Object convert(final String value, final Class clazz) {
                if (clazz.isEnum()) {
                    return Enum.valueOf(clazz, value);
                }else{
                    return super.convert(value, clazz);
                }
            }
        };

        BEAN_UTILS_BEAN = new BeanUtilsBean(convertUtilsBean2, PROPERTY_UTILS_BEAN);
    }

    private volatile boolean isRecordReaderSet;
    private volatile boolean isRecordWriterSet;

    private volatile Worker worker;
    final AtomicReference<WorkerStateChangeListener.WorkerState> workerState = new AtomicReference<>(null);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return isRecordReaderSet && isRecordWriterSet ? RECORD_RELATIONSHIPS : RELATIONSHIPS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (RECORD_READER.equals(descriptor)) {
            isRecordReaderSet = StringUtils.isNotEmpty(newValue);
        } else if (RECORD_WRITER.equals(descriptor)) {
            isRecordWriterSet = StringUtils.isNotEmpty(newValue);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .addValidator(this::validateDynamicKCLConfigProperty)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE);

        return builder.build();
    }

    private ValidationResult validateDynamicKCLConfigProperty(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder validationResult = new ValidationResult.Builder().subject(subject).input(input);

        if (!subject.matches("^(?!with)[a-zA-Z]\\w*$")) {
            return validationResult
                    .explanation("Property name must not have a prefix of \"with\", must start with a letter and contain only letters, numbers or underscores")
                    .valid(false).build();
        }

        if (DISALLOWED_DYNAMIC_KCL_PROPERTIES.keySet().stream().anyMatch(k -> k.equalsIgnoreCase(subject))) {
            return validationResult
                    .explanation(String.format("Use \"%s\" instead of a dynamic property", DISALLOWED_DYNAMIC_KCL_PROPERTIES.get(subject).getDisplayName()))
                    .valid(false).build();
        }

        final KinesisClientLibConfiguration kclTemp = new KinesisClientLibConfiguration("validate", "validate", null, "validate");
        try {
            final String propName = StringUtils.uncapitalize(subject);
            if (!PROPERTY_UTILS_BEAN.isWriteable(kclTemp, propName)) {
                return validationResult
                        .explanation(String.format("Kinesis Client Library Configuration property with name %s does not exist or is not writable", StringUtils.capitalize(subject)))
                        .valid(false).build();
            }
            BEAN_UTILS_BEAN.setProperty(kclTemp, propName, input);
        } catch (IllegalAccessException e) {
            return validationResult
                    .explanation(String.format("Kinesis Client Library Configuration property with name %s is not accessible", StringUtils.capitalize(subject)))
                    .valid(false).build();
        } catch (InvocationTargetException e) {
            return buildDynamicPropertyBeanValidationResult(validationResult, subject, input, e.getTargetException().getLocalizedMessage());
        } catch (IllegalArgumentException e) {
            return buildDynamicPropertyBeanValidationResult(validationResult, subject, input, e.getLocalizedMessage());
        }

        return validationResult.valid(true).build();
    }

    private ValidationResult buildDynamicPropertyBeanValidationResult(final ValidationResult.Builder validationResult,
                                                                      final String subject, final String input, final String message) {
        return validationResult
                .explanation(
                        String.format("Kinesis Client Library Configuration property with name %s cannot be used with value \"%s\" : %s",
                                StringUtils.capitalize(subject), input, message)
                )
                .valid(false).build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Set<ValidationResult> validationResults = new HashSet<>(super.customValidate(validationContext));

        DateTimeFormatter dateTimeFormatter = null;
        try {
            dateTimeFormatter = getDateTimeFormatter(validationContext);
        } catch (IllegalArgumentException iae) {
            validationResults.add(new ValidationResult.Builder().valid(false)
                    .subject(TIMESTAMP_FORMAT.getName())
                    .explanation(String.format("%s must be a valid java.time.DateTimeFormatter format", TIMESTAMP_FORMAT.getDisplayName()))
                    .build()
            );
        }

        if (InitialPositionInStream.AT_TIMESTAMP == getInitialPositionInStream(validationContext)) {
            if (!validationContext.getProperty(STREAM_POSITION_TIMESTAMP).isSet()) {
                validationResults.add(new ValidationResult.Builder().valid(false)
                        .subject(STREAM_POSITION_TIMESTAMP.getName())
                        .explanation(String.format("%s must be provided when %s is %s", STREAM_POSITION_TIMESTAMP.getDisplayName(),
                                INITIAL_STREAM_POSITION.getDisplayName(), AT_TIMESTAMP.getDisplayName()))
                        .build()
                );
            } else if (dateTimeFormatter != null) {
                try {
                    // check the streamTimestamp can be formatted
                    getStartStreamTimestamp(validationContext, dateTimeFormatter);
                } catch (Exception e) {
                    validationResults.add(new ValidationResult.Builder().valid(false)
                            .subject(STREAM_POSITION_TIMESTAMP.getName())
                            .explanation(String.format("%s must be parsable by %s", STREAM_POSITION_TIMESTAMP.getDisplayName(),
                                    TIMESTAMP_FORMAT.getDisplayName()))
                            .build());
                }
            }
        }

        if (isRecordReaderSet && !isRecordWriterSet) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RECORD_WRITER.getName())
                    .explanation(String.format("%s must be set if %s is set in order to write FlowFiles as Records.",
                            RECORD_WRITER.getDisplayName(), RECORD_READER.getDisplayName()))
                    .valid(false)
                    .build());
        } else if (isRecordWriterSet && !isRecordReaderSet) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RECORD_READER.getName())
                    .explanation(String.format("%s must be set if %s is set in order to write FlowFiles as Records.",
                            RECORD_READER.getDisplayName(), RECORD_WRITER.getDisplayName()))
                    .valid(false)
                    .build());
        }

        return validationResults;
    }

    @OnScheduled
    @Override
    public void onScheduled(ProcessContext context) {
        stopped.set(false);
        workerState.set(null);
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        if (worker == null) {
            synchronized (WORKER_LOCK) {
                if (worker == null) {
                    final String workerId = generateWorkerId();
                    getLogger().info("Starting Kinesis Worker {}", workerId);
                    // create worker (WorkerState will be CREATED)
                    worker = prepareWorker(context, sessionFactory, workerId);
                    // initialise and start Worker (will set WorkerState to INITIALIZING and attempt to start)
                    new Thread(worker, WORKER_THREAD_NAME_TEMPLATE + workerId).start();
                }
            }
        } else {
            // after a Worker is registered successfully, nothing has to be done at onTrigger
            // new sessions are created when new messages are consumed by the Worker
            // and if the WorkerState is unexpectedly SHUT_DOWN, then we don't want to immediately re-enter onTrigger
            context.yield();

            if (!stopped.get() && WorkerStateChangeListener.WorkerState.SHUT_DOWN == workerState.get()) {
                throw new ProcessException("Worker has shutdown unexpectedly, possibly due to a configuration issue; check logs for details");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // intentionally blank (using onTrigger with ProcessSessionFactory above instead)
    }

    @OnStopped
    public void stopConsuming(final ProcessContext context) {
        if (worker != null) {
            synchronized (WORKER_LOCK) {
                if (worker != null) {
                    // indicate whether the processor has been Stopped; the Worker can be marked as SHUT_DOWN but still be waiting
                    // for ShardConsumers/RecordProcessors to complete, etc.
                    stopped.set(true);

                    final boolean success = shutdownWorker(context);
                    worker = null;
                    workerState.set(null);

                    if (!success) {
                        getLogger().warn("One or more problems while shutting down Kinesis Worker, see logs for details");
                    }
                }
            }
        }
    }

    private synchronized Worker prepareWorker(final ProcessContext context, final ProcessSessionFactory sessionFactory, final String workerId) {
        final IRecordProcessorFactory factory = prepareRecordProcessorFactory(context, sessionFactory);

        final KinesisClientLibConfiguration kinesisClientLibConfiguration =
                prepareKinesisClientLibConfiguration(context, workerId);

        final Worker.Builder workerBuilder = prepareWorkerBuilder(context, kinesisClientLibConfiguration, factory);

        getLogger().info("Kinesis Worker prepared for application {} to process stream {} as worker ID {}...",
                getApplicationName(context), getStreamName(context), workerId);

        return workerBuilder.build();
    }

    private IRecordProcessorFactory prepareRecordProcessorFactory(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        return () -> {
            if (isRecordReaderSet && isRecordWriterSet) {
                return new KinesisRecordProcessorRecord(
                        sessionFactory, getLogger(), getStreamName(context), getClient().getEndpointPrefix(),
                        getKinesisEndpoint(context).orElse(null), getCheckpointIntervalMillis(context),
                        getRetryWaitMillis(context), getNumRetries(context), getDateTimeFormatter(context),
                        getReaderFactory(context), getWriterFactory(context)
                );
            } else {
                return new KinesisRecordProcessorRaw(
                        sessionFactory, getLogger(), getStreamName(context), getClient().getEndpointPrefix(),
                        getKinesisEndpoint(context).orElse(null), getCheckpointIntervalMillis(context),
                        getRetryWaitMillis(context), getNumRetries(context), getDateTimeFormatter(context)
                );
            }
        };
    }

    /*
     *  Developer note: if setting KCL configuration explicitly from processor properties, be sure to add them to the
     *  DISALLOWED_DYNAMIC_KCL_PROPERTIES list so Dynamic Properties can't be used to override the static properties
     */
    KinesisClientLibConfiguration prepareKinesisClientLibConfiguration(final ProcessContext context, final String workerId) {
        @SuppressWarnings("deprecated")
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
                getApplicationName(context),
                getStreamName(context),
                getCredentialsProvider(context),
                workerId
        )
                .withCommonClientConfig(getClient().getClientConfiguration())
                .withRegionName(getRegion().getName())
                .withFailoverTimeMillis(getFailoverTimeMillis(context))
                .withShutdownGraceMillis(getGracefulShutdownMillis(context));

        final InitialPositionInStream initialPositionInStream = getInitialPositionInStream(context);
        if (InitialPositionInStream.AT_TIMESTAMP == initialPositionInStream) {
            kinesisClientLibConfiguration.withTimestampAtInitialPositionInStream(getStartStreamTimestamp(context));
        } else {
            kinesisClientLibConfiguration.withInitialPositionInStream(initialPositionInStream);
        }

        getDynamoDBOverride(context).ifPresent(kinesisClientLibConfiguration::withDynamoDBEndpoint);
        getKinesisEndpoint(context).ifPresent(kinesisClientLibConfiguration::withKinesisEndpoint);

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        dynamicProperties.forEach(descriptor -> {
            final String name = descriptor.getName();
            final String value = context.getProperty(descriptor).getValue();
            try {
                BEAN_UTILS_BEAN.setProperty(kinesisClientLibConfiguration, StringUtils.uncapitalize(name), value);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new ProcessException(String.format("Unable to set Kinesis Client Library Configuration property %s with value %s", StringUtils.capitalize(name), value), e);
            }
        });

        return kinesisClientLibConfiguration;
    }

    Worker.Builder prepareWorkerBuilder(final ProcessContext context, final KinesisClientLibConfiguration kinesisClientLibConfiguration,
                                        final IRecordProcessorFactory factory) {
        final Worker.Builder workerBuilder = new Worker.Builder()
                .config(kinesisClientLibConfiguration)
                .kinesisClient(getClient())
                .workerStateChangeListener(workerState::set)
                .recordProcessorFactory(factory);

        if (!isReportCloudWatchMetrics(context)) {
            workerBuilder.metricsFactory(new NullMetricsFactory());
        }

        return workerBuilder;
    }

    private boolean shutdownWorker(final ProcessContext context) {
        boolean success = true;
        try {
            if (!worker.hasGracefulShutdownStarted()) {
                getLogger().info("Requesting Kinesis Worker shutdown");
                final Future<Boolean> shutdown = worker.startGracefulShutdown();
                // allow 2 seconds longer than the graceful period for shutdown before cancelling the task
                if (Boolean.FALSE.equals(shutdown.get(getGracefulShutdownMillis(context) + 2_000L, TimeUnit.MILLISECONDS))) {
                    getLogger().warn("Kinesis Worker shutdown did not complete in time, cancelling");
                    success = false;
                } else {
                    getLogger().info("Kinesis Worker shutdown");
                }
            }
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            getLogger().warn("Problem while shutting down Kinesis Worker: {}", e.getLocalizedMessage(), e);
            success = false;
        }
        return success;
    }

    private String generateWorkerId() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new ProcessException(e);
        }
    }

    private String getApplicationName(final PropertyContext context) {
        return StringUtils.trimToEmpty(context.getProperty(APPLICATION_NAME).getValue());
    }

    private String getStreamName(final PropertyContext context) {
        return StringUtils.trimToEmpty(context.getProperty(KINESIS_STREAM_NAME).getValue());
    }

    private long getFailoverTimeMillis(final PropertyContext context) {
        return context.getProperty(FAILOVER_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private long getGracefulShutdownMillis(final PropertyContext context) {
        return context.getProperty(GRACEFUL_SHUTDOWN_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private long getCheckpointIntervalMillis(final PropertyContext context) {
        return context.getProperty(CHECKPOINT_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private int getNumRetries(final PropertyContext context) {
        return context.getProperty(NUM_RETRIES).asInteger();
    }

    private long getRetryWaitMillis(final PropertyContext context) {
        return context.getProperty(RETRY_WAIT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private boolean isReportCloudWatchMetrics(final PropertyContext context) {
        return context.getProperty(REPORT_CLOUDWATCH_METRICS).asBoolean();
    }

    private Optional<String> getKinesisEndpoint(final PropertyContext context) {
        return context.getProperty(ENDPOINT_OVERRIDE).isSet()
                ? Optional.of(StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue()))
                : Optional.empty();
    }

    private Optional<String> getDynamoDBOverride(final PropertyContext context) {
        return context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE).isSet()
                ? Optional.of(StringUtils.trimToEmpty(context.getProperty(DYNAMODB_ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue()))
                : Optional.empty();
    }

    private RecordReaderFactory getReaderFactory(final PropertyContext context) {
        return context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
    }

    private RecordSetWriterFactory getWriterFactory(final PropertyContext context) {
        return context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    private InitialPositionInStream getInitialPositionInStream(final PropertyContext context) {
        return InitialPositionInStream.valueOf(StringUtils.trimToEmpty(context.getProperty(INITIAL_STREAM_POSITION).getValue()));
    }

    private DateTimeFormatter getDateTimeFormatter(final PropertyContext context) {
        return DateTimeFormatter.ofPattern(context.getProperty(TIMESTAMP_FORMAT).evaluateAttributeExpressions().getValue());
    }

    private Date getStartStreamTimestamp(final PropertyContext context) {
        return getStartStreamTimestamp(context, getDateTimeFormatter(context));
    }

    private Date getStartStreamTimestamp(final PropertyContext context, final DateTimeFormatter dateTimeFormatter) {
        final String streamTimestamp = context.getProperty(STREAM_POSITION_TIMESTAMP).getValue();
        return new Date(
                LocalDateTime.parse(streamTimestamp, dateTimeFormatter).atZone(ZoneId.systemDefault()) // parse date/time with system timezone
                .withZoneSameInstant(ZoneOffset.UTC) // convert to UTC
                .toInstant().toEpochMilli() // convert to epoch milliseconds for creating Date
        );
    }
}
