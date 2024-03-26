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
package org.apache.nifi.processors.iceberg;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.Tasks;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.iceberg.catalog.IcebergCatalogFactory;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.writer.IcebergTaskWriterFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.ietf.jgss.GSSException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.nifi.processors.iceberg.IcebergUtils.findCause;
import static org.apache.nifi.processors.iceberg.IcebergUtils.getConfigurationFromFiles;
import static org.apache.nifi.processors.iceberg.IcebergUtils.getDynamicProperties;

@Tags({"iceberg", "put", "table", "store", "record", "parse", "orc", "parquet", "avro"})
@CapabilityDescription("This processor uses Iceberg API to parse and load records into Iceberg tables. " +
        "The incoming data sets are parsed with Record Reader Controller Service and ingested into an Iceberg table using the configured catalog service and provided table information. " +
        "The target Iceberg table should already exist and it must have matching schemas with the incoming records, " +
        "which means the Record Reader schema must contain all the Iceberg schema fields, every additional field which is not present in the Iceberg schema will be ignored. " +
        "To avoid 'small file problem' it is recommended pre-appending a MergeRecord processor.")
@DynamicProperty(
        name = "A custom key to add to the snapshot summary. The value must start with 'snapshot-property.' prefix.",
        value = "A custom value to add to the snapshot summary.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Adds an entry with custom-key and corresponding value in the snapshot summary. The key format must be 'snapshot-property.custom-key'.")
@WritesAttributes({
        @WritesAttribute(attribute = "iceberg.record.count", description = "The number of records in the FlowFile.")
})
public class PutIceberg extends AbstractIcebergProcessor {

    public static final String ICEBERG_RECORD_COUNT = "iceberg.record.count";
    public static final String ICEBERG_SNAPSHOT_SUMMARY_PREFIX = "snapshot-property.";
    public static final String ICEBERG_SNAPSHOT_SUMMARY_FLOWFILE_UUID = "nifi-flowfile-uuid";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor CATALOG_NAMESPACE = new PropertyDescriptor.Builder()
            .name("catalog-namespace")
            .displayName("Catalog Namespace")
            .description("The namespace of the catalog.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Table Name")
            .description("The name of the Iceberg table to write to.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("unmatched-column-behavior")
            .displayName("Unmatched Column Behavior")
            .description("If an incoming record does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation.")
            .allowableValues(UnmatchedColumnBehavior.class)
            .defaultValue(UnmatchedColumnBehavior.FAIL_UNMATCHED_COLUMN)
            .required(true)
            .build();

    static final PropertyDescriptor FILE_FORMAT = new PropertyDescriptor.Builder()
            .name("file-format")
            .displayName("File Format")
            .description("File format to use when writing Iceberg data files." +
                    " If not set, then the 'write.format.default' table property will be used, default value is parquet.")
            .allowableValues(new FileFormat[]{FileFormat.AVRO, FileFormat.PARQUET, FileFormat.ORC})
            .build();

    static final PropertyDescriptor MAXIMUM_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("maximum-file-size")
            .displayName("Maximum File Size")
            .description("The maximum size that a file can be, if the file size is exceeded a new file will be generated with the remaining data." +
                    " If not set, then the 'write.target-file-size-bytes' table property will be used, default value is 512 MB.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final PropertyDescriptor NUMBER_OF_COMMIT_RETRIES = new PropertyDescriptor.Builder()
            .name("number-of-commit-retries")
            .displayName("Number of Commit Retries")
            .description("Number of times to retry a commit before failing.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("10")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor MINIMUM_COMMIT_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("minimum-commit-wait-time")
            .displayName("Minimum Commit Wait Time")
            .description("Minimum time to wait before retrying a commit.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("100 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_COMMIT_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("maximum-commit-wait-time")
            .displayName("Maximum Commit Wait Time")
            .description("Maximum time to wait before retrying a commit.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("2 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_COMMIT_DURATION = new PropertyDescriptor.Builder()
            .name("maximum-commit-duration")
            .displayName("Maximum Commit Duration")
            .description("Total retry timeout period for a commit.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the data ingestion was successful.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            RECORD_READER,
            CATALOG,
            CATALOG_NAMESPACE,
            TABLE_NAME,
            UNMATCHED_COLUMN_BEHAVIOR,
            FILE_FORMAT,
            MAXIMUM_FILE_SIZE,
            KERBEROS_USER_SERVICE,
            NUMBER_OF_COMMIT_RETRIES,
            MINIMUM_COMMIT_WAIT_TIME,
            MAXIMUM_COMMIT_WAIT_TIME,
            MAXIMUM_COMMIT_DURATION
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator((subject, input, context) -> {
                    ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(input);
                    if (subject.startsWith(ICEBERG_SNAPSHOT_SUMMARY_PREFIX)) {
                        builder.valid(true);
                    } else {
                        builder.valid(false).explanation("Dynamic property key must begin with '" + ICEBERG_SNAPSHOT_SUMMARY_PREFIX + "'");
                    }
                    return builder.build();
                })
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        boolean catalogServiceEnabled = context.getControllerServiceLookup().isControllerServiceEnabled(catalogService);

        if (catalogServiceEnabled) {
            final boolean kerberosUserServiceIsSet = context.getProperty(KERBEROS_USER_SERVICE).isSet();
            final boolean securityEnabled = SecurityUtil.isSecurityEnabled(getConfigurationFromFiles(catalogService.getConfigFilePaths()));

            if (securityEnabled && !kerberosUserServiceIsSet) {
                problems.add(new ValidationResult.Builder()
                        .subject(KERBEROS_USER_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("'hadoop.security.authentication' is set to 'kerberos' in the hadoop configuration files but no KerberosUserService is configured.")
                        .build());
            }

            if (!securityEnabled && kerberosUserServiceIsSet) {
                problems.add(new ValidationResult.Builder()
                        .subject(KERBEROS_USER_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("KerberosUserService is configured but 'hadoop.security.authentication' is not set to 'kerberos' in the hadoop configuration files.")
                        .build());
            }
        }

        return problems;
    }

    @Override
    public void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final long startNanos = System.nanoTime();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String fileFormat = context.getProperty(FILE_FORMAT).getValue();
        final String maximumFileSize = context.getProperty(MAXIMUM_FILE_SIZE).evaluateAttributeExpressions(flowFile).getValue();

        Table table;

        try {
            table = loadTable(context, flowFile);
        } catch (Exception e) {
            final Optional<GSSException> causeOptional = findCause(e, GSSException.class, gsse -> GSSException.NO_CRED == gsse.getMajor());
            if (causeOptional.isPresent()) {
                getLogger().warn("No valid Kerberos credential found, retrying login", causeOptional.get());
                initKerberosCredentials(context);
                session.rollback();
                context.yield();
            } else {
                getLogger().error("Failed to load table from catalog", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }
            return;
        }

        TaskWriter<org.apache.iceberg.data.Record> taskWriter = null;
        int recordCount = 0;

        try (final InputStream in = session.read(flowFile); final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            final FileFormat format = getFileFormat(table.properties(), fileFormat);
            final IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(table, flowFile.getId(), format, maximumFileSize);
            taskWriter = taskWriterFactory.create();
            final UnmatchedColumnBehavior unmatchedColumnBehavior = context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).asAllowableValue(UnmatchedColumnBehavior.class);

            final IcebergRecordConverter recordConverter = new IcebergRecordConverter(table.schema(), reader.getSchema(), format, unmatchedColumnBehavior, getLogger());

            Record record;
            while ((record = reader.nextRecord()) != null) {
                taskWriter.write(recordConverter.convert(record));
                recordCount++;
            }

            final WriteResult result = taskWriter.complete();
            appendDataFiles(context, flowFile, table, result);
        } catch (Exception e) {
            final Optional<GSSException> causeOptional = findCause(e, GSSException.class, gsse -> GSSException.NO_CRED == gsse.getMajor());
            if (causeOptional.isPresent()) {
                getLogger().warn("No valid Kerberos credential found, retrying login", causeOptional.get());
                initKerberosCredentials(context);
                session.rollback();
                context.yield();
            } else {
                getLogger().error("Exception occurred while writing Iceberg records", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
            }

            try {
                if (taskWriter != null) {
                    abort(taskWriter.dataFiles(), table);
                }
            } catch (Exception ex) {
                getLogger().warn("Failed to abort uncommitted data files", ex);
            }
            return;
        }

        flowFile = session.putAttribute(flowFile, ICEBERG_RECORD_COUNT, String.valueOf(recordCount));
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, table.location(), transferMillis);
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * Loads a table from the catalog service with the provided values from the property context.
     *
     * @param context holds the user provided information for the {@link Catalog} and the {@link Table}
     * @return loaded table
     */
    private Table loadTable(final PropertyContext context, final FlowFile flowFile) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final String catalogNamespace = context.getProperty(CATALOG_NAMESPACE).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final IcebergCatalogFactory catalogFactory = new IcebergCatalogFactory(catalogService);
        final Catalog catalog = catalogFactory.create();

        final Namespace namespace = Namespace.of(catalogNamespace);
        final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        return catalog.loadTable(tableIdentifier);
    }

    /**
     * Appends the pending data files to the given {@link Table}.
     *
     * @param context processor context
     * @param table   table to append
     * @param result  datafiles created by the {@link TaskWriter}
     */
    void appendDataFiles(ProcessContext context, FlowFile flowFile, Table table, WriteResult result) {
        final int numberOfCommitRetries = context.getProperty(NUMBER_OF_COMMIT_RETRIES).evaluateAttributeExpressions(flowFile).asInteger();
        final long minimumCommitWaitTime = context.getProperty(MINIMUM_COMMIT_WAIT_TIME).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumCommitWaitTime = context.getProperty(MAXIMUM_COMMIT_WAIT_TIME).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final long maximumCommitDuration = context.getProperty(MAXIMUM_COMMIT_DURATION).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);

        final AppendFiles appender = table.newAppend();
        Arrays.stream(result.dataFiles()).forEach(appender::appendFile);

        addSnapshotSummaryProperties(context, appender, flowFile);

        Tasks.foreach(appender)
                .exponentialBackoff(minimumCommitWaitTime, maximumCommitWaitTime, maximumCommitDuration, 2.0)
                .retry(numberOfCommitRetries)
                .onlyRetryOn(CommitFailedException.class)
                .run(PendingUpdate::commit);
    }

    /**
     * Adds the FlowFile's uuid and additional entries provided in Dynamic properties to the snapshot summary.
     *
     * @param context  processor context
     * @param appender table appender to set the snapshot summaries
     * @param flowFile the FlowFile to get the uuid from
     */
    private void addSnapshotSummaryProperties(ProcessContext context, AppendFiles appender, FlowFile flowFile) {
        appender.set(ICEBERG_SNAPSHOT_SUMMARY_FLOWFILE_UUID, flowFile.getAttribute(CoreAttributes.UUID.key()));

        for (Map.Entry<String, String> dynamicProperty : getDynamicProperties(context, flowFile).entrySet()) {
            String key = dynamicProperty.getKey().substring(ICEBERG_SNAPSHOT_SUMMARY_PREFIX.length());
            appender.set(key, dynamicProperty.getValue());
        }
    }

    /**
     * Determines the write file format from the requested value and the table configuration.
     *
     * @param tableProperties table properties
     * @param fileFormat      requested file format from the processor
     * @return file format to use
     */
    private FileFormat getFileFormat(Map<String, String> tableProperties, String fileFormat) {
        final String fileFormatName = fileFormat != null ? fileFormat : tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(fileFormatName.toUpperCase(Locale.ENGLISH));
    }

    /**
     * Deletes the completed data files that have not been committed to the table yet.
     *
     * @param dataFiles files created by the task writer
     * @param table     table
     */
    void abort(DataFile[] dataFiles, Table table) {
        Tasks.foreach(dataFiles)
                .retry(3)
                .run(file -> table.io().deleteFile(file.path().toString()));
    }
}
