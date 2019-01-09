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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.streaming.ConnectionError;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.InvalidTable;
import org.apache.hive.streaming.SerializationError;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StreamingIOFailure;
import org.apache.hive.streaming.TransactionError;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.DiscontinuedException;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.HiveConfigurator;
import org.apache.nifi.util.hive.HiveOptions;
import org.apache.hive.streaming.HiveRecordWriter;
import org.apache.nifi.util.hive.HiveUtils;
import org.apache.nifi.util.hive.ValidationResources;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.hive.AbstractHive3QLProcessor.ATTR_OUTPUT_TABLES;

@Tags({"hive", "streaming", "put", "database", "store"})
@CapabilityDescription("This processor uses Hive Streaming to send flow file records to an Apache Hive 3.0+ table. "
        + "The partition values are expected to be the 'last' fields of each record, so if the table is partitioned on column A for example, then the last field in "
        + "each record should be field A.")
@WritesAttributes({
        @WritesAttribute(attribute = "hivestreaming.record.count", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the number of records from the incoming flow file. All records in a flow file are committed as a single transaction."),
        @WritesAttribute(attribute = "query.output.tables", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the target table name in 'databaseName.tableName' format.")
})
@RequiresInstanceClassLoading
public class PutHive3Streaming extends AbstractProcessor {
    // Attributes
    public static final String HIVE_STREAMING_RECORD_COUNT_ATTR = "hivestreaming.record.count";

    private static final String CLIENT_CACHE_DISABLED_PROPERTY = "hcatalog.hive.client.cache.disabled";

    // Properties
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading records from incoming flow files.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive3-stream-metastore-uri")
            .displayName("Hive Metastore URI")
            .description("The URI location(s) for the Hive metastore. This is a comma-separated list of Hive metastore URIs; note that this is not the location of the Hive Server. "
                    + "The default port for the Hive metastore is 9043. If this field is not set, then the 'hive.metastore.uris' property from any provided configuration resources "
                    + "will be used, and if none are provided, then the default value from a default hive-site.xml will be used (usually thrift://localhost:9083).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hive3-config-resources")
            .displayName("Hive Configuration Resources")
            .description("A file or comma separated list of files which contains the Hive configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Note that to enable authentication "
                    + "with Kerberos e.g., the appropriate properties must be set in the configuration files. Also note that if Max Concurrent Tasks is set "
                    + "to a number greater than one, the 'hcatalog.hive.client.cache.disabled' property will be forced to 'true' to avoid concurrency issues. "
                    + "Please see the Hive documentation for more details.")
            .required(false)
            .addValidator(HiveUtils.createMultipleFilesExistValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("hive3-stream-database-name")
            .displayName("Database Name")
            .description("The name of the database in which to put the data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hive3-stream-table-name")
            .displayName("Table Name")
            .description("The name of the database table in which to put the data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PARTITION_VALUES = new PropertyDescriptor.Builder()
            .name("hive3-stream-part-vals")
            .displayName("Partition Values")
            .description("Specifies a comma-separated list of the values for the partition columns of the target table. If the incoming records all have the same values "
                    + "for the partition columns, those values can be entered here, resulting in a performance gain. If specified, this property will often contain "
                    + "Expression Language, for example if PartitionRecord is upstream and two partitions 'name' and 'age' are used, then this property can be set to "
                    + "${name},${age}.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AUTOCREATE_PARTITIONS = new PropertyDescriptor.Builder()
            .name("hive3-stream-autocreate-partition")
            .displayName("Auto-Create Partitions")
            .description("Flag indicating whether partitions should be automatically created")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor CALL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hive3-stream-call-timeout")
            .displayName("Call Timeout")
            .description("The number of seconds allowed for a Hive Streaming operation to complete. A value of 0 indicates the processor should wait indefinitely on operations. "
                    + "Note that although this property supports Expression Language, it will not be evaluated against incoming FlowFile attributes.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DISABLE_STREAMING_OPTIMIZATIONS = new PropertyDescriptor.Builder()
            .name("hive3-stream-disable-optimizations")
            .displayName("Disable Streaming Optimizations")
            .description("Whether to disable streaming optimizations. Disabling streaming optimizations will have significant impact to performance and memory consumption.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();


    static final PropertyDescriptor ROLLBACK_ON_FAILURE = RollbackOnFailure.createRollbackOnFailureProperty(
            "NOTE: When an error occurred after a Hive streaming transaction which is derived from the same input FlowFile is already committed," +
                    " (i.e. a FlowFile contains more records than 'Records per Transaction' and a failure occurred at the 2nd transaction or later)" +
                    " then the succeeded records will be transferred to 'success' relationship while the original input FlowFile stays in incoming queue." +
                    " Duplicated records can be created for the succeeded ones when the same FlowFile is processed again.");

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing Avro records routed to this relationship after the record has been successfully transmitted to Hive.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile containing Avro records routed to this relationship if the record could not be transmitted to Hive.")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("The incoming FlowFile is routed to this relationship if its records cannot be transmitted to Hive. Note that "
                    + "some records may have been processed successfully, they will be routed (as Avro flow files) to the success relationship. "
                    + "The combination of the retry, success, and failure relationships indicate how many records succeeded and/or failed. This "
                    + "can be used to provide a retry capability since full rollback is not possible.")
            .build();

    private List<PropertyDescriptor> propertyDescriptors;
    private Set<Relationship> relationships;

    protected volatile HiveConfigurator hiveConfigurator = new HiveConfigurator();
    protected volatile UserGroupInformation ugi;
    protected volatile HiveConf hiveConfig;

    protected volatile int callTimeout;
    protected ExecutorService callTimeoutPool;
    protected volatile boolean rollbackOnFailure;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RECORD_READER);
        props.add(METASTORE_URI);
        props.add(HIVE_CONFIGURATION_RESOURCES);
        props.add(DB_NAME);
        props.add(TABLE_NAME);
        props.add(PARTITION_VALUES);
        props.add(AUTOCREATE_PARTITIONS);
        props.add(CALL_TIMEOUT);
        props.add(DISABLE_STREAMING_OPTIMIZATIONS);
        props.add(ROLLBACK_ON_FAILURE);
        props.add(KERBEROS_CREDENTIALS_SERVICE);

        propertyDescriptors = Collections.unmodifiableList(props);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        final KerberosCredentialsService credentialsService = validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        final String resolvedPrincipal = credentialsService != null ? credentialsService.getPrincipal() : null;
        final String resolvedKeytab = credentialsService != null ? credentialsService.getKeytab() : null;
        if (confFileProvided) {
            final String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            problems.addAll(hiveConfigurator.validate(configFiles, resolvedPrincipal, resolvedKeytab, validationResourceHolder, getLogger()));
        }

        return problems;
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        ComponentLog log = getLogger();
        rollbackOnFailure = context.getProperty(ROLLBACK_ON_FAILURE).asBoolean();

        final String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        hiveConfig = hiveConfigurator.getConfigurationFromFiles(configFiles);

        // If more than one concurrent task, force 'hcatalog.hive.client.cache.disabled' to true
        if (context.getMaxConcurrentTasks() > 1) {
            hiveConfig.setBoolean(CLIENT_CACHE_DISABLED_PROPERTY, true);
        }

        // add any dynamic properties to the Hive configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hiveConfig.set(descriptor.getName(), entry.getValue());
            }
        }

        hiveConfigurator.preload(hiveConfig);

        if (SecurityUtil.isSecurityEnabled(hiveConfig)) {
            final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

            final String resolvedPrincipal = credentialsService.getPrincipal();
            final String resolvedKeytab = credentialsService.getKeytab();

            log.info("Hive Security Enabled, logging in as principal {} with keytab {}", new Object[]{resolvedPrincipal, resolvedKeytab});
            try {
                ugi = hiveConfigurator.authenticate(hiveConfig, resolvedPrincipal, resolvedKeytab);
            } catch (AuthenticationFailedException ae) {
                throw new ProcessException("Kerberos authentication failed for Hive Streaming", ae);
            }

            log.info("Successfully logged in as principal {} with keytab {}", new Object[]{resolvedPrincipal, resolvedKeytab});
        } else {
            ugi = null;
        }

        callTimeout = context.getProperty(CALL_TIMEOUT).evaluateAttributeExpressions().asInteger() * 1000; // milliseconds
        String timeoutName = "put-hive3-streaming-%d";
        this.callTimeoutPool = Executors.newFixedThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat(timeoutName).build());
    }

    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String dbName = context.getProperty(DB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final ComponentLog log = getLogger();
        String metastoreURIs = null;
        if (context.getProperty(METASTORE_URI).isSet()) {
            metastoreURIs = context.getProperty(METASTORE_URI).evaluateAttributeExpressions(flowFile).getValue();
            if (StringUtils.isEmpty(metastoreURIs)) {
                // Shouldn't be empty at this point, log an error, penalize the flow file, and return
                log.error("The '" + METASTORE_URI.getDisplayName() + "' property evaluated to null or empty, penalizing flow file, routing to failure");
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }
        }

        final String partitionValuesString = context.getProperty(PARTITION_VALUES).evaluateAttributeExpressions(flowFile).getValue();
        final boolean autoCreatePartitions = context.getProperty(AUTOCREATE_PARTITIONS).asBoolean();
        final boolean disableStreamingOptimizations = context.getProperty(DISABLE_STREAMING_OPTIMIZATIONS).asBoolean();

        // Override the Hive Metastore URIs in the config if set by the user
        if (metastoreURIs != null) {
           hiveConfig.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreURIs);
        }

        HiveOptions o = new HiveOptions(metastoreURIs, dbName, tableName)
                .withHiveConf(hiveConfig)
                .withAutoCreatePartitions(autoCreatePartitions)
                .withCallTimeout(callTimeout)
                .withStreamingOptimizations(!disableStreamingOptimizations);

        if (!StringUtils.isEmpty(partitionValuesString)) {
            List<String> staticPartitionValues = Arrays.stream(partitionValuesString.split(",")).filter(Objects::nonNull).map(String::trim).collect(Collectors.toList());
            o = o.withStaticPartitionValues(staticPartitionValues);
        }

        if (SecurityUtil.isSecurityEnabled(hiveConfig)) {
            final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
            o = o.withKerberosPrincipal(credentialsService.getPrincipal()).withKerberosKeytab(credentialsService.getKeytab());
        }

        final HiveOptions options = o;

        // Store the original class loader, then explicitly set it to this class's classloader (for use by the Hive Metastore)
        ClassLoader originalClassloader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        StreamingConnection hiveStreamingConnection = null;

        try (final InputStream rawIn = session.read(flowFile)) {
            final RecordReader reader;

            try (final BufferedInputStream in = new BufferedInputStream(rawIn)) {

                // if we fail to create the RecordReader then we want to route to failure, so we need to
                // handle this separately from the other IOExceptions which normally route to retry
                try {
                    reader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                } catch (Exception e) {
                    throw new RecordReaderFactoryException("Unable to create RecordReader", e);
                }

                hiveStreamingConnection = makeStreamingConnection(options, reader);

                // Write records to Hive streaming, then commit and close
                hiveStreamingConnection.beginTransaction();
                hiveStreamingConnection.write(in);
                hiveStreamingConnection.commitTransaction();
                rawIn.close();

                Map<String, String> updateAttributes = new HashMap<>();
                updateAttributes.put(HIVE_STREAMING_RECORD_COUNT_ATTR, Long.toString(hiveStreamingConnection.getConnectionStats().getRecordsWritten()));
                updateAttributes.put(ATTR_OUTPUT_TABLES, options.getQualifiedTableName());
                flowFile = session.putAllAttributes(flowFile, updateAttributes);
                session.getProvenanceReporter().send(flowFile, hiveStreamingConnection.getMetastoreUri());
                session.transfer(flowFile, REL_SUCCESS);
            } catch (TransactionError te) {
                if (rollbackOnFailure) {
                    throw new ProcessException(te.getLocalizedMessage(), te);
                } else {
                    throw new ShouldRetryException(te.getLocalizedMessage(), te);
                }
            } catch (RecordReaderFactoryException rrfe) {
                if (rollbackOnFailure) {
                    throw new ProcessException(rrfe);
                } else {
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        } catch (InvalidTable | SerializationError | StreamingIOFailure | IOException e) {
            if (rollbackOnFailure) {
                if (hiveStreamingConnection != null) {
                    abortConnection(hiveStreamingConnection);
                }
                throw new ProcessException(e.getLocalizedMessage(), e);
            } else {
                Map<String, String> updateAttributes = new HashMap<>();
                updateAttributes.put(HIVE_STREAMING_RECORD_COUNT_ATTR, Long.toString(hiveStreamingConnection.getConnectionStats().getRecordsWritten()));
                updateAttributes.put(ATTR_OUTPUT_TABLES, options.getQualifiedTableName());
                flowFile = session.putAllAttributes(flowFile, updateAttributes);
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (DiscontinuedException e) {
            // The input FlowFile processing is discontinued. Keep it in the input queue.
            getLogger().warn("Discontinued processing for {} due to {}", new Object[]{flowFile, e}, e);
            session.transfer(flowFile, Relationship.SELF);
        } catch (ConnectionError ce) {
            // If we can't connect to the metastore, yield the processor
            context.yield();
            throw new ProcessException("A connection to metastore cannot be established", ce);
        } catch (ShouldRetryException e) {
            // This exception is already a result of adjusting an error, so simply transfer the FlowFile to retry. Still need to abort the txn
            getLogger().error(e.getLocalizedMessage(), e);
            if (hiveStreamingConnection != null) {
                abortConnection(hiveStreamingConnection);
            }
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_RETRY);
        } catch (StreamingException se) {
            // Handle all other exceptions. These are often record-based exceptions (since Hive will throw a subclass of the exception caught above)
            Throwable cause = se.getCause();
            if (cause == null) cause = se;
            // This is a failure on the incoming data, rollback on failure if specified; otherwise route to failure after penalizing (and abort txn in any case)
            if (rollbackOnFailure) {
                if (hiveStreamingConnection != null) {
                    abortConnection(hiveStreamingConnection);
                }
                throw new ProcessException(cause.getLocalizedMessage(), cause);
            } else {
                flowFile = session.penalize(flowFile);
                Map<String, String> updateAttributes = new HashMap<>();
                updateAttributes.put(HIVE_STREAMING_RECORD_COUNT_ATTR, Long.toString(hiveStreamingConnection.getConnectionStats().getRecordsWritten()));
                updateAttributes.put(ATTR_OUTPUT_TABLES, options.getQualifiedTableName());
                flowFile = session.putAllAttributes(flowFile, updateAttributes);
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (Throwable t) {
            if (hiveStreamingConnection != null) {
                abortConnection(hiveStreamingConnection);
            }
            throw (t instanceof ProcessException) ? (ProcessException) t : new ProcessException(t);
        } finally {
            closeConnection(hiveStreamingConnection);
            // Restore original class loader, might not be necessary but is good practice since the processor task changed it
            Thread.currentThread().setContextClassLoader(originalClassloader);
        }
    }

    StreamingConnection makeStreamingConnection(HiveOptions options, RecordReader reader) throws StreamingException {
        return HiveStreamingConnection.newBuilder()
                .withDatabase(options.getDatabaseName())
                .withTable(options.getTableName())
                .withStaticPartitionValues(options.getStaticPartitionValues())
                .withHiveConf(options.getHiveConf())
                .withRecordWriter(new HiveRecordWriter(reader, getLogger()))
                .withAgentInfo("NiFi " + this.getClass().getSimpleName() + " [" + this.getIdentifier()
                        + "] thread " + Thread.currentThread().getId() + "[" + Thread.currentThread().getName() + "]")
                .connect();
    }

    @OnStopped
    public void cleanup() {
        validationResourceHolder.set(null); // trigger re-validation of resources

        ComponentLog log = getLogger();

        if (callTimeoutPool != null) {
            callTimeoutPool.shutdown();
            try {
                while (!callTimeoutPool.isTerminated()) {
                    callTimeoutPool.awaitTermination(callTimeout, TimeUnit.MILLISECONDS);
                }
            } catch (Throwable t) {
                log.warn("shutdown interrupted on " + callTimeoutPool, t);
            }
            callTimeoutPool = null;
        }

        ugi = null;
    }

    private void abortAndCloseConnection(StreamingConnection connection) {
        try {
            abortConnection(connection);
            closeConnection(connection);
        } catch (Exception ie) {
            getLogger().warn("unable to close hive connections. ", ie);
        }
    }

    /**
     * Abort current Txn on the connection
     */
    private void abortConnection(StreamingConnection connection) {
        if (connection != null) {
            try {
                connection.abortTransaction();
            } catch (Exception e) {
                getLogger().error("Failed to abort Hive Streaming transaction " + connection + " due to exception ", e);
            }
        }
    }

    /**
     * Close the streaming connection
     */
    private void closeConnection(StreamingConnection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                getLogger().error("Failed to close Hive Streaming connection " + connection + " due to exception ", e);
            }
        }
    }

    private static class ShouldRetryException extends RuntimeException {
        private ShouldRetryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

