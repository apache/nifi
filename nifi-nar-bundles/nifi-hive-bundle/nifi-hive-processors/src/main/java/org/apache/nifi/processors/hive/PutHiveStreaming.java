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
package org.apache.nifi.processors.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.SerializationError;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.HiveConfigurator;
import org.apache.nifi.util.hive.HiveOptions;
import org.apache.nifi.util.hive.HiveUtils;
import org.apache.nifi.util.hive.HiveWriter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * This processor utilizes the Hive Streaming capability to insert data from the flow into a Hive database table.
 */
@TriggerSerially
@Tags({"hive", "streaming", "put", "database", "store"})
@CapabilityDescription("This processor uses Hive Streaming to send flow file data to an Apache Hive table. The incoming flow file is expected to be in "
        + "Avro format and the table must exist in Hive. Please see the Hive documentation for requirements on the Hive table (format, partitions, etc.). "
        + "The partition values are extracted from the Avro record based on the names of the partition columns as specified in the processor.")
@WritesAttributes({
        @WritesAttribute(attribute = "hivestreaming.record.count", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the number of records from the incoming flow file written successfully and unsuccessfully, respectively.")
})
public class PutHiveStreaming extends AbstractProcessor {

    // Attributes
    public static final String HIVE_STREAMING_RECORD_COUNT_ATTR = "hivestreaming.record.count";

    // Validators
    private static final Validator GREATER_THAN_ONE_VALIDATOR = (subject, value, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
        }

        String reason = null;
        try {
            final int intVal = Integer.parseInt(value);

            if (intVal < 2) {
                reason = "value is less than 2";
            }
        } catch (final NumberFormatException e) {
            reason = "value is not a valid integer";
        }

        return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
    };

    // Metadata keys that are not transferred to split files when output strategy is datafile
    // Avro will write this key/values pairs on its own
    private static final Set<String> RESERVED_METADATA;

    static {
        Set<String> reservedMetadata = new HashSet<>();
        reservedMetadata.add("avro.schema");
        reservedMetadata.add("avro.codec");
        RESERVED_METADATA = Collections.unmodifiableSet(reservedMetadata);
    }

    // Properties
    public static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive-stream-metastore-uri")
            .displayName("Hive Metastore URI")
            .description("The URI location for the Hive Metastore. Note that this is not the location of the Hive Server. The default port for the "
                    + "Hive metastore is 9043.")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("(^[^/]+.*[^/]+$|^[^/]+$|^$)"))) // no start with / or end with /
            .build();

    public static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hive-config-resources")
            .displayName("Hive Configuration Resources")
            .description("A file or comma separated list of files which contains the Hive configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Note that to enable authentication "
                    + "with Kerberos e.g., the appropriate properties must be set in the configuration files. Please see the Hive documentation for more details.")
            .required(false)
            .addValidator(HiveUtils.createMultipleFilesExistValidator())
            .build();

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("hive-stream-database-name")
            .displayName("Database Name")
            .description("The name of the database in which to put the data.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hive-stream-table-name")
            .displayName("Table Name")
            .description("The name of the database table in which to put the data.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PARTITION_COLUMNS = new PropertyDescriptor.Builder()
            .name("hive-stream-partition-cols")
            .displayName("Partition Columns")
            .description("A comma-delimited list of column names on which the table has been partitioned. The order of values in this list must "
                    + "correspond exactly to the order of partition columns specified during the table creation.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("[^,]+(,[^,]+)*"))) // comma-separated list with non-empty entries
            .build();

    public static final PropertyDescriptor AUTOCREATE_PARTITIONS = new PropertyDescriptor.Builder()
            .name("hive-stream-autocreate-partition")
            .displayName("Auto-Create Partitions")
            .description("Flag indicating whether partitions should be automatically created")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor MAX_OPEN_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("hive-stream-max-open-connections")
            .displayName("Max Open Connections")
            .description("The maximum number of open connections that can be allocated from this pool at the same time, "
                    + "or negative for no limit.")
            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor HEARTBEAT_INTERVAL = new PropertyDescriptor.Builder()
            .name("hive-stream-heartbeat-interval")
            .displayName("Heartbeat Interval")
            .description("Indicates that a heartbeat should be sent when the specified number of seconds has elapsed. "
                    + "A value of 0 indicates that no heartbeat should be sent.")
            .defaultValue("60")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor TXNS_PER_BATCH = new PropertyDescriptor.Builder()
            .name("hive-stream-transactions-per-batch")
            .displayName("Transactions per Batch")
            .description("A hint to Hive Streaming indicating how many transactions the processor task will need. This value must be greater than 1.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(GREATER_THAN_ONE_VALIDATOR)
            .defaultValue("100")
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing the JSON contents of a record is routed to this relationship after the record has been successfully transmitted to Hive.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile containing the JSON contents of a record is routed to this relationship if the record could not be transmitted to Hive.")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("The incoming FlowFile is routed to this relationship if its records cannot be transmitted to Hive. Note that "
                    + "some records may have been processed successfully, they will be routed (as JSON flow files) to the success relationship. "
                    + "The combination of the retry, success, and failure relationships indicate how many records succeeded and/or failed. This "
                    + "can be used to provide a retry capability since full rollback is not possible.")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    private static final long TICKET_RENEWAL_PERIOD = 60000;

    protected KerberosProperties kerberosProperties;
    private volatile String kerberosServicePrincipal = null;
    private volatile File kerberosConfigFile = null;
    private volatile File kerberosServiceKeytab = null;

    protected volatile HiveConfigurator hiveConfigurator = new HiveConfigurator();
    protected volatile UserGroupInformation ugi;

    protected final AtomicBoolean isInitialized = new AtomicBoolean(false);

    protected HiveOptions options;
    protected ExecutorService callTimeoutPool;
    protected transient Timer heartBeatTimer;
    protected AtomicBoolean sendHeartBeat = new AtomicBoolean(false);
    protected Map<HiveEndPoint, HiveWriter> allWriters;


    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(METASTORE_URI);
        propertyDescriptors.add(HIVE_CONFIGURATION_RESOURCES);
        propertyDescriptors.add(DB_NAME);
        propertyDescriptors.add(TABLE_NAME);
        propertyDescriptors.add(PARTITION_COLUMNS);
        propertyDescriptors.add(AUTOCREATE_PARTITIONS);
        propertyDescriptors.add(MAX_OPEN_CONNECTIONS);
        propertyDescriptors.add(HEARTBEAT_INTERVAL);
        propertyDescriptors.add(TXNS_PER_BATCH);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        kerberosServicePrincipal = context.getKerberosServicePrincipal();
        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosServiceKeytab = context.getKerberosServiceKeytab();
        kerberosProperties = new KerberosProperties(kerberosConfigFile);
        propertyDescriptors.add(kerberosProperties.getKerberosPrincipal());
        propertyDescriptors.add(kerberosProperties.getKerberosKeytab());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @OnScheduled
    public void setup(final ProcessContext context) {
        ComponentLog log = getLogger();

        final String metastoreUri = context.getProperty(METASTORE_URI).getValue();
        final String dbName = context.getProperty(DB_NAME).getValue();
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        final boolean autoCreatePartitions = context.getProperty(AUTOCREATE_PARTITIONS).asBoolean();
        final Integer maxConnections = context.getProperty(MAX_OPEN_CONNECTIONS).asInteger();
        final Integer heartbeatInterval = context.getProperty(HEARTBEAT_INTERVAL).asInteger();
        final Integer txnsPerBatch = context.getProperty(TXNS_PER_BATCH).asInteger();
        final String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).getValue();
        final Configuration hiveConfig = hiveConfigurator.getConfigurationFromFiles(configFiles);

        // add any dynamic properties to the Hive configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hiveConfig.set(descriptor.getName(), entry.getValue());
            }
        }

        options = new HiveOptions(metastoreUri, dbName, tableName)
                .withTxnsPerBatch(txnsPerBatch)
                .withAutoCreatePartitions(autoCreatePartitions)
                .withMaxOpenConnections(maxConnections)
                .withHeartBeatInterval(heartbeatInterval);

        if (SecurityUtil.isSecurityEnabled(hiveConfig)) {
            final String principal = context.getProperty(kerberosProperties.getKerberosPrincipal()).getValue();
            final String keyTab = context.getProperty(kerberosProperties.getKerberosKeytab()).getValue();

            log.info("Hive Security Enabled, logging in as principal {} with keytab {}", new Object[]{principal, keyTab});
            try {
                ugi = hiveConfigurator.authenticate(hiveConfig, principal, keyTab, TICKET_RENEWAL_PERIOD, log);
            } catch (AuthenticationFailedException ae) {
                throw new ProcessException("Kerberos authentication failed for Hive Streaming", ae);
            }
            log.info("Successfully logged in as principal {} with keytab {}", new Object[]{principal, keyTab});
            options = options.withKerberosPrincipal(principal).withKerberosKeytab(keyTab);
        }

        allWriters = new ConcurrentHashMap<>();
        String timeoutName = "put-hive-streaming-%d";
        this.callTimeoutPool = Executors.newFixedThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

        sendHeartBeat.set(true);
        heartBeatTimer = new Timer();
        setupHeartBeatTimer();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog log = getLogger();
        final Integer txnsPerBatch = context.getProperty(TXNS_PER_BATCH).asInteger();

        // Store the original class loader, then explicitly set it to this class's classloader (for use by the Hive Metastore)
        ClassLoader originalClassloader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        final List<String> partitionColumnList;
        final String partitionColumns = context.getProperty(PARTITION_COLUMNS).getValue();
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            partitionColumnList = Collections.emptyList();
        } else {
            String[] partitionCols = partitionColumns.split(",");
            partitionColumnList = new ArrayList<>(partitionCols.length);
            for (String col : partitionCols) {
                partitionColumnList.add(col.trim());
            }
        }

        final AtomicInteger recordCount = new AtomicInteger(0);
        final AtomicInteger successfulRecordCount = new AtomicInteger(0);
        List<HiveStreamingRecord> successfulRecords = new LinkedList<>();
        final FlowFile inputFlowFile = flowFile;
        final AtomicBoolean incomingFlowFileTransferred = new AtomicBoolean(false);

        // Create output flow files and their Avro writers
        AtomicReference<FlowFile> successFlowFile = new AtomicReference<>(session.create(inputFlowFile));
        final DataFileWriter<GenericRecord> successAvroWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
        AtomicReference<FlowFile> failureFlowFile = new AtomicReference<>(session.create(inputFlowFile));
        final DataFileWriter<GenericRecord> failureAvroWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());

        try {
            session.read(inputFlowFile, in -> {

                try (final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {
                    GenericRecord currRecord = null;

                    // Copy codec and schema information to all writers
                    final String codec = reader.getMetaString(DataFileConstants.CODEC) == null
                            ? DataFileConstants.NULL_CODEC
                            : reader.getMetaString(DataFileConstants.CODEC);

                    Arrays.asList(successAvroWriter, failureAvroWriter)
                            .forEach((writer) -> {
                                writer.setCodec(CodecFactory.fromString(codec));
                                // Transfer metadata (this is a subset of the incoming file)
                                for (String metaKey : reader.getMetaKeys()) {
                                    if (!RESERVED_METADATA.contains(metaKey)) {
                                        writer.setMeta(metaKey, reader.getMeta(metaKey));
                                    }
                                }
                            });

                    while (reader.hasNext()) {
                        currRecord = reader.next(currRecord);
                        recordCount.incrementAndGet();

                        // Extract the partition values (they must be put separately into the Hive Streaming API)
                        List<String> partitionValues = new ArrayList<>();

                        try {
                            for (String partition : partitionColumnList) {
                                Object partitionValue = currRecord.get(partition);
                                if (partitionValue == null) {
                                    throw new IOException("Partition column '" + partition + "' not found in Avro record");
                                }
                                partitionValues.add(partitionValue.toString());
                            }
                        } catch (IOException ioe) {
                            // Add the failed record to the failure flow file
                            log.error("Error writing record to Hive Streaming transaction", ioe);
                            appendRecordsToFlowFile(session, Collections.singletonList(new HiveStreamingRecord(null, currRecord)),
                                    failureFlowFile, failureAvroWriter, reader);
                            continue;
                        }

                        List<Schema.Field> fields = currRecord.getSchema().getFields();
                        if (fields != null) {
                            JSONObject obj = new JSONObject();
                            try {
                                for (Schema.Field field : fields) {
                                    String fieldName = field.name();
                                    // Skip fields that are partition columns, we extracted those values above to create an EndPoint
                                    if (!partitionColumnList.contains(fieldName)) {
                                        Object value = currRecord.get(fieldName);
                                        try {
                                            obj.put(fieldName, value);
                                        } catch (JSONException je) {
                                            throw new IOException(je);
                                        }
                                    }
                                }
                            } catch (IOException ioe) {
                                // This really shouldn't happen since we are iterating over the schema fields, but just in case,
                                // add the failed record to the failure flow file.
                                log.error("Error writing record to Hive Streaming transaction", ioe);
                                appendRecordsToFlowFile(session, Collections.singletonList(new HiveStreamingRecord(null, currRecord)),
                                        failureFlowFile, failureAvroWriter, reader);
                                continue;
                            }
                            final HiveStreamingRecord record = new HiveStreamingRecord(partitionValues, currRecord);
                            HiveEndPoint endPoint = null;
                            HiveWriter hiveWriter = null;
                            try {
                                endPoint = makeHiveEndPoint(record.getPartitionValues(), options);
                                hiveWriter = getOrCreateWriter(endPoint);
                            } catch (ConnectionError
                                    | HiveWriter.ConnectFailure
                                    | InterruptedException connectionError) {
                                // Can't connect to Hive endpoint.
                                log.error("Error connecting to Hive endpoint: table {} at {}",
                                        new Object[]{options.getTableName(), options.getMetaStoreURI()});
                                // If we can't connect to the endpoint, exit the loop and let the outer exception handler route the original flow file to retry
                                abortAndCloseWriters();
                                throw new ProcessException(connectionError);
                            }
                            try {
                                try {
                                    hiveWriter.write(record.getRecord().toString().getBytes(StandardCharsets.UTF_8));
                                    successfulRecords.add(record);
                                } catch (InterruptedException | HiveWriter.WriteFailure wf) {
                                    // Add the failed record to the failure flow file
                                    log.error("Error writing record to Hive Streaming transaction", wf);
                                    appendRecordsToFlowFile(session, Collections.singletonList(record), failureFlowFile, failureAvroWriter, reader);
                                }

                                // If we've reached the transactions-per-batch limit, flush the Hive Writer and update the Avro Writer for successful records
                                if (hiveWriter.getTotalRecords() >= txnsPerBatch) {
                                    hiveWriter.flush(true);
                                    // Now send the records to the success relationship and update the success count
                                    try {
                                        appendRecordsToFlowFile(session, successfulRecords, successFlowFile, successAvroWriter, reader);
                                        successfulRecordCount.accumulateAndGet(successfulRecords.size(), (current, incr) -> current + incr);

                                        // Clear the list of successful records, we'll use it at the end when we flush whatever records are left
                                        successfulRecords.clear();

                                    } catch (IOException ioe) {
                                        // The records were put to Hive Streaming successfully, but there was an error while writing the
                                        // Avro records to the flow file. Log as an error and move on.
                                        getLogger().error("Error writing Avro records (which were sent successfully to Hive Streaming) to the flow file", ioe);
                                    }
                                }

                            } catch (InterruptedException
                                    | HiveWriter.CommitFailure
                                    | HiveWriter.TxnBatchFailure
                                    | HiveWriter.TxnFailure
                                    | SerializationError writeException) {

                                log.error("Error writing record to Hive Streaming transaction", writeException);
                                // Add the failed record to the failure flow file
                                appendRecordsToFlowFile(session, Collections.singletonList(record), failureFlowFile, failureAvroWriter, reader);

                                if (!(writeException instanceof SerializationError)) {
                                    try {
                                        hiveWriter.abort();
                                    } catch (Exception e) {
                                        // Can't even abort properly, throw a process exception
                                        throw new ProcessException(e);
                                    }
                                }
                            }
                        }
                    }
                    try {
                        // Finish any transactions
                        flushAllWriters(true);
                        closeAllWriters();

                        // Now send any remaining records to the success relationship and update the count
                        appendRecordsToFlowFile(session, successfulRecords, successFlowFile, successAvroWriter, reader);
                        successfulRecordCount.accumulateAndGet(successfulRecords.size(), (current, incr) -> current + incr);
                        successfulRecords.clear();

                    } catch (HiveWriter.CommitFailure
                            | HiveWriter.TxnBatchFailure
                            | HiveWriter.TxnFailure
                            | InterruptedException e) {

                        // If any records are in the successfulRecords list but ended up here, then they actually weren't transferred successfully, so
                        // route them to failure instead
                        appendRecordsToFlowFile(session, successfulRecords, failureFlowFile, failureAvroWriter, reader);
                    }
                } catch (IOException ioe) {
                    // The Avro file is invalid (or may not be an Avro file at all), send it to failure
                    log.error("The incoming flow file can not be read as an Avro file, routing to failure", ioe);
                    session.transfer(inputFlowFile, REL_FAILURE);
                    incomingFlowFileTransferred.set(true);
                }
            });


            if (recordCount.get() > 0) {
                if (successfulRecordCount.get() > 0) {
                    // Transfer the flow file with successful records
                    successFlowFile.set(
                            session.putAttribute(successFlowFile.get(), HIVE_STREAMING_RECORD_COUNT_ATTR, Integer.toString(recordCount.get())));
                    session.getProvenanceReporter().send(successFlowFile.get(), options.getMetaStoreURI());
                    session.transfer(successFlowFile.get(), REL_SUCCESS);
                } else {
                    session.remove(successFlowFile.get());
                }

                if (recordCount.get() != successfulRecordCount.get()) {
                    // There were some failed records, so transfer that flow file to failure
                    failureFlowFile.set(
                            session.putAttribute(failureFlowFile.get(), HIVE_STREAMING_RECORD_COUNT_ATTR,
                                    Integer.toString(recordCount.get() - successfulRecordCount.get())));
                    session.transfer(failureFlowFile.get(), REL_FAILURE);
                } else {
                    session.remove(failureFlowFile.get());
                }
            } else {
                // No records were processed, so remove the output flow files
                session.remove(successFlowFile.get());
                session.remove(failureFlowFile.get());
            }
            successFlowFile.set(null);
            failureFlowFile.set(null);

            // If we got here, we've processed the outgoing flow files correctly, so remove the incoming one if necessary
            if (!incomingFlowFileTransferred.get()) {
                session.remove(flowFile);
            }

        } catch (ProcessException pe) {
            abortAndCloseWriters();
            Throwable t = pe.getCause();
            if (t != null) {
                if (t instanceof ConnectionError
                        || t instanceof HiveWriter.ConnectFailure
                        || t instanceof HiveWriter.CommitFailure
                        || t instanceof HiveWriter.TxnBatchFailure
                        || t instanceof HiveWriter.TxnFailure
                        || t instanceof InterruptedException) {
                    log.error("Hive Streaming connect/write error, flow file will be penalized and routed to retry", t);
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_RETRY);
                    // Remove the ones we created
                    if (successFlowFile.get() != null) {
                        session.remove(successFlowFile.get());
                    }
                    if (failureFlowFile.get() != null) {
                        session.remove(failureFlowFile.get());
                    }
                } else {
                    throw pe;
                }
            } else {
                throw pe;
            }
        } finally {
            // Restore original class loader, might not be necessary but is good practice since the processor task changed it
            Thread.currentThread().setContextClassLoader(originalClassloader);
        }
    }

    private void appendRecordsToFlowFile(ProcessSession session,
                                         List<HiveStreamingRecord> records,
                                         AtomicReference<FlowFile> appendFlowFile,
                                         DataFileWriter<GenericRecord> avroWriter,
                                         DataFileStream<GenericRecord> reader) throws IOException {

        appendFlowFile.set(session.append(appendFlowFile.get(), (out) -> {

            try (DataFileWriter<GenericRecord> writer = avroWriter.create(reader.getSchema(), out)) {
                for (HiveStreamingRecord sRecord : records) {
                    writer.append(sRecord.getRecord());
                }
                writer.flush();
            }
        }));
    }

    @OnStopped
    public void cleanup() {
        ComponentLog log = getLogger();
        sendHeartBeat.set(false);
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                HiveWriter w = entry.getValue();
                w.flushAndClose();
            } catch (Exception ex) {
                log.warn("Error while closing writer to " + entry.getKey() + ". Exception follows.", ex);
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        callTimeoutPool.shutdown();
        try {
            while (!callTimeoutPool.isTerminated()) {
                callTimeoutPool.awaitTermination(
                        options.getCallTimeOut(), TimeUnit.MILLISECONDS);
            }
        } catch (Throwable t) {
            log.warn("shutdown interrupted on " + callTimeoutPool, t);
        }

        callTimeoutPool = null;
    }

    private void setupHeartBeatTimer() {
        if (options.getHeartBeatInterval() > 0) {
            final ComponentLog log = getLogger();
            heartBeatTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        if (sendHeartBeat.get()) {
                            log.debug("Start sending heartbeat on all writers");
                            sendHeartBeatOnAllWriters();
                            setupHeartBeatTimer();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to heartbeat on HiveWriter ", e);
                    }
                }
            }, options.getHeartBeatInterval() * 1000);
        }
    }

    private void sendHeartBeatOnAllWriters() throws InterruptedException {
        for (HiveWriter writer : allWriters.values()) {
            writer.heartBeat();
        }
    }

    private void flushAllWriters(boolean rollToNext)
            throws HiveWriter.CommitFailure, HiveWriter.TxnBatchFailure, HiveWriter.TxnFailure, InterruptedException {
        for (HiveWriter writer : allWriters.values()) {
            writer.flush(rollToNext);
        }
    }

    private void abortAndCloseWriters() {
        try {
            abortAllWriters();
            closeAllWriters();
        } catch (Exception ie) {
            getLogger().warn("unable to close hive connections. ", ie);
        }
    }

    /**
     * Abort current Txn on all writers
     */
    private void abortAllWriters() throws InterruptedException, StreamingException, HiveWriter.TxnBatchFailure {
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                entry.getValue().abort();
            } catch (Exception e) {
                getLogger().error("Failed to abort hive transaction batch, HiveEndPoint " + entry.getValue() + " due to exception ", e);
            }
        }
    }

    /**
     * Closes all writers and remove them from cache
     */
    private void closeAllWriters() {
        //1) Retire writers
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                getLogger().warn("unable to close writers. ", e);
            }
        }
        //2) Clear cache
        allWriters.clear();
    }

    private HiveWriter getOrCreateWriter(HiveEndPoint endPoint) throws HiveWriter.ConnectFailure, InterruptedException {
        ComponentLog log = getLogger();
        try {
            HiveWriter writer = allWriters.get(endPoint);
            if (writer == null) {
                log.debug("Creating Writer to Hive end point : " + endPoint);
                writer = makeHiveWriter(endPoint, callTimeoutPool, ugi, options);
                if (allWriters.size() > (options.getMaxOpenConnections() - 1)) {
                    log.info("cached HiveEndPoint size {} exceeded maxOpenConnections {} ", new Object[]{allWriters.size(), options.getMaxOpenConnections()});
                    int retired = retireIdleWriters();
                    if (retired == 0) {
                        retireEldestWriter();
                    }
                }
                allWriters.put(endPoint, writer);
                HiveUtils.logAllHiveEndPoints(allWriters);
            }
            return writer;
        } catch (HiveWriter.ConnectFailure e) {
            log.error("Failed to create HiveWriter for endpoint: " + endPoint, e);
            throw e;
        }
    }

    /**
     * Locate writer that has not been used for longest time and retire it
     */
    private void retireEldestWriter() {
        ComponentLog log = getLogger();

        log.info("Attempting close eldest writers");
        long oldestTimeStamp = System.currentTimeMillis();
        HiveEndPoint eldest = null;
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            if (entry.getValue().getLastUsed() < oldestTimeStamp) {
                eldest = entry.getKey();
                oldestTimeStamp = entry.getValue().getLastUsed();
            }
        }
        try {
            log.info("Closing least used Writer to Hive end point : " + eldest);
            allWriters.remove(eldest).flushAndClose();
        } catch (IOException e) {
            log.warn("Failed to close writer for end point: " + eldest, e);
        } catch (InterruptedException e) {
            log.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Interrupted when attempting to close writer for end point: " + eldest, e);
        }
    }

    /**
     * Locate all writers past idle timeout and retire them
     *
     * @return number of writers retired
     */
    private int retireIdleWriters() {
        ComponentLog log = getLogger();

        log.info("Attempting to close idle HiveWriters");
        int count = 0;
        long now = System.currentTimeMillis();
        ArrayList<HiveEndPoint> retirees = new ArrayList<>();

        //1) Find retirement candidates
        for (Map.Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
            if (now - entry.getValue().getLastUsed() > options.getIdleTimeout()) {
                ++count;
                retirees.add(entry.getKey());
            }
        }
        //2) Retire them
        for (HiveEndPoint ep : retirees) {
            try {
                log.info("Closing idle Writer to Hive end point : {}", new Object[]{ep});
                allWriters.remove(ep).flushAndClose();
            } catch (IOException e) {
                log.warn("Failed to close HiveWriter for end point: {}. Error: " + ep, e);
            } catch (InterruptedException e) {
                log.warn("Interrupted when attempting to close HiveWriter for end point: " + ep, e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("Interrupted when attempting to close HiveWriter for end point: " + ep, e);
            }
        }
        return count;
    }

    protected HiveEndPoint makeHiveEndPoint(List<String> partitionValues, HiveOptions options) throws ConnectionError {
        return HiveUtils.makeEndPoint(partitionValues, options);
    }

    protected HiveWriter makeHiveWriter(HiveEndPoint endPoint, ExecutorService callTimeoutPool, UserGroupInformation ugi, HiveOptions options)
            throws HiveWriter.ConnectFailure, InterruptedException {
        return HiveUtils.makeHiveWriter(endPoint, callTimeoutPool, ugi, options);
    }

    protected KerberosProperties getKerberosProperties() {
        return kerberosProperties;
    }

    protected class HiveStreamingRecord {

        private List<String> partitionValues;
        private GenericRecord record;

        public HiveStreamingRecord(List<String> partitionValues, GenericRecord record) {
            this.partitionValues = partitionValues;
            this.record = record;
        }

        public List<String> getPartitionValues() {
            return partitionValues;
        }

        public GenericRecord getRecord() {
            return record;
        }

    }
}

