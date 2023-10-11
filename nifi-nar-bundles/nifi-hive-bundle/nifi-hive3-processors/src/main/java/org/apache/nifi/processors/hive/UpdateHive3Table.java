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

import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.hive.Hive3DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.DiscontinuedException;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import java.util.stream.Collectors;


@Tags({"hive", "metadata", "jdbc", "database", "table"})
@CapabilityDescription("This processor uses a Hive JDBC connection and incoming records to generate any Hive 3.0+ table changes needed to support the incoming records.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hive.table.management.strategy", description = "This attribute is read if the 'Table Management Strategy' property is configured "
                + "to use the value of this attribute. The value of this attribute should correspond (ignoring case) to a valid option of the 'Table Management Strategy' property.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "output.table", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the target table name."),
        @WritesAttribute(attribute = "output.path", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the path on the file system to the table (or partition location if the table is partitioned)."),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer, only if a Record Writer is specified "
                + "and Update Field Names is 'true'."),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the FlowFile, only if a Record Writer is specified and Update Field Names is 'true'.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@RequiresInstanceClassLoading
public class UpdateHive3Table extends AbstractProcessor {

    static final String TEXTFILE = "TEXTFILE";
    static final String SEQUENCEFILE = "SEQUENCEFILE";
    static final String ORC = "ORC";
    static final String PARQUET = "PARQUET";
    static final String AVRO = "AVRO";
    static final String RCFILE = "RCFILE";

    static final AllowableValue TEXTFILE_STORAGE = new AllowableValue(TEXTFILE, TEXTFILE, "Stored as plain text files. TEXTFILE is the default file format, unless the configuration "
            + "parameter hive.default.fileformat has a different setting.");
    static final AllowableValue SEQUENCEFILE_STORAGE = new AllowableValue(SEQUENCEFILE, SEQUENCEFILE, "Stored as compressed Sequence Files.");
    static final AllowableValue ORC_STORAGE = new AllowableValue(ORC, ORC, "Stored as ORC file format. Supports ACID Transactions & Cost-based Optimizer (CBO). "
            + "Stores column-level metadata.");
    static final AllowableValue PARQUET_STORAGE = new AllowableValue(PARQUET, PARQUET, "Stored as Parquet format for the Parquet columnar storage format.");
    static final AllowableValue AVRO_STORAGE = new AllowableValue(AVRO, AVRO, "Stored as Avro format.");
    static final AllowableValue RCFILE_STORAGE = new AllowableValue(RCFILE, RCFILE, "Stored as Record Columnar File format.");

    static final AllowableValue CREATE_IF_NOT_EXISTS = new AllowableValue("Create If Not Exists", "Create If Not Exists",
            "Create a table with the given schema if it does not already exist");
    static final AllowableValue FAIL_IF_NOT_EXISTS = new AllowableValue("Fail If Not Exists", "Fail If Not Exists",
            "If the target does not already exist, log an error and route the flowfile to failure");

    static final String TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE = "hive.table.management.strategy";
    static final AllowableValue MANAGED_TABLE = new AllowableValue("Managed", "Managed",
            "Any tables created by this processor will be managed tables (see Hive documentation for details).");
    static final AllowableValue EXTERNAL_TABLE = new AllowableValue("External", "External",
            "Any tables created by this processor will be external tables located at the `External Table Location` property value.");
    static final AllowableValue ATTRIBUTE_DRIVEN_TABLE = new AllowableValue("Use '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' Attribute",
            "Use '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' Attribute",
            "Inspects the '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' FlowFile attribute to determine the table management strategy. The value "
                    + "of this attribute must be a case-insensitive match to one of the other allowable values (Managed, External, e.g.).");

    static final String ATTR_OUTPUT_TABLE = "output.table";
    static final String ATTR_OUTPUT_PATH = "output.path";

    // Properties
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading incoming flow files. The reader is only used to determine the schema of the records, the actual records will not be processed.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor HIVE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("hive3-dbcp-service")
            .displayName("Hive Database Connection Pooling Service")
            .description("The Hive Controller Service that is used to obtain connection(s) to the Hive database")
            .required(true)
            .identifiesControllerService(Hive3DBCPService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("hive3-table-name")
            .displayName("Table Name")
            .description("The name of the database table to update. If the table does not exist, then it will either be created or an error thrown, depending "
                    + "on the value of the Create Table property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("hive3-create-table")
            .displayName("Create Table Strategy")
            .description("Specifies how to process the target table when it does not exist (create it, fail, e.g.).")
            .required(true)
            .addValidator(Validator.VALID)
            .allowableValues(CREATE_IF_NOT_EXISTS, FAIL_IF_NOT_EXISTS)
            .defaultValue(FAIL_IF_NOT_EXISTS.getValue())
            .build();

    static final PropertyDescriptor TABLE_MANAGEMENT_STRATEGY = new PropertyDescriptor.Builder()
            .name("hive3-create-table-management")
            .displayName("Create Table Management Strategy")
            .description("Specifies (when a table is to be created) whether the table is a managed table or an external table. Note that when External is specified, the "
                    + "'External Table Location' property must be specified. If the '" + TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE + "' value is selected, 'External Table Location' "
                    + "must still be specified, but can contain Expression Language or be set to the empty string, and is ignored when the attribute evaluates to 'Managed'.")
            .required(true)
            .addValidator(Validator.VALID)
            .allowableValues(MANAGED_TABLE, EXTERNAL_TABLE, ATTRIBUTE_DRIVEN_TABLE)
            .defaultValue(MANAGED_TABLE.getValue())
            .dependsOn(CREATE_TABLE, CREATE_IF_NOT_EXISTS)
            .build();

    static final PropertyDescriptor UPDATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("hive3-update-field-names")
            .displayName("Update Field Names")
            .description("This property indicates whether to update the output schema such that the field names are set to the exact column names from the specified "
                    + "table. This should be used if the incoming record field names may not match the table's column names in terms of upper- and lower-case. For example, this property should be "
                    + "set to true if the output FlowFile (and target table storage) is Avro format, as Hive/Impala expects the field names to match the column names exactly.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("hive3-record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing results to a FlowFile. The Record Writer should use Inherit Schema to emulate the inferred schema behavior, i.e. "
                    + "an explicit schema need not be defined in the writer, and will be supplied by the same logic used to infer the schema from the column types. If Create Table Strategy is set "
                    + "'Create If Not Exists', the Record Writer's output format must match the Record Reader's format in order for the data to be placed in the created table location. Note that "
                    + "this property is only used if 'Update Field Names' is set to true and the field names do not all match the column names exactly. If no "
                    + "update is needed for any field names (or 'Update Field Names' is false), the Record Writer is not used and instead the input FlowFile is routed to success or failure "
                    + "without modification.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .dependsOn(UPDATE_FIELD_NAMES, "true")
            .required(true)
            .build();

    static final PropertyDescriptor EXTERNAL_TABLE_LOCATION = new PropertyDescriptor.Builder()
            .name("hive3-external-table-location")
            .displayName("External Table Location")
            .description("Specifies (when an external table is to be created) the file path (in HDFS, e.g.) to store table data.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .dependsOn(TABLE_MANAGEMENT_STRATEGY, EXTERNAL_TABLE, ATTRIBUTE_DRIVEN_TABLE)
            .build();

    static final PropertyDescriptor TABLE_STORAGE_FORMAT = new PropertyDescriptor.Builder()
            .name("hive3-storage-format")
            .displayName("Create Table Storage Format")
            .description("If a table is to be created, the specified storage format will be used.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(TEXTFILE_STORAGE, SEQUENCEFILE_STORAGE, ORC_STORAGE, PARQUET_STORAGE, AVRO_STORAGE, RCFILE_STORAGE)
            .defaultValue(TEXTFILE)
            .dependsOn(CREATE_TABLE, CREATE_IF_NOT_EXISTS)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hive3-query-timeout")
            .displayName("Query Timeout")
            .description("Sets the number of seconds the driver will wait for a query to execute. "
                    + "A value of 0 means no timeout. NOTE: Non-zero values may not be supported by the driver.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor PARTITION_CLAUSE = new PropertyDescriptor.Builder()
            .name("hive3-partition-clause")
            .displayName("Partition Clause")
            .description("Specifies a comma-separated list of attribute names and optional data types corresponding to the partition columns of the target table. Simply put, if the table is "
                    + "partitioned or is to be created with partitions, each partition name should be an attribute on the FlowFile and listed in this property. This assumes all incoming records "
                    + "belong to the same partition and the partition columns are not fields in the record. An example of specifying this field is if PartitionRecord "
                    + "is upstream and two partition columns 'name' (of type string) and 'age' (of type integer) are used, then this property can be set to 'name string, age int'. The data types "
                    + "are optional and if partition(s) are to be created they will default to string type if not specified. For non-string primitive types, specifying the data type for existing "
                    + "partition columns is helpful for interpreting the partition value(s). If the table exists, the data types need not be specified "
                    + "(and are ignored in that case). This property must be set if the table is partitioned, and there must be an attribute for each partition column in the table. "
                    + "The values of the attributes will be used as the partition values, and the resulting output.path attribute value will reflect the location of the partition in the filesystem "
                    + "(for use downstream in processors such as PutHDFS).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing records routed to this relationship after the record has been successfully transmitted to Hive.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile containing records routed to this relationship if the record could not be transmitted to Hive.")
            .build();

    private List<PropertyDescriptor> propertyDescriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(RECORD_READER);
        props.add(HIVE_DBCP_SERVICE);
        props.add(TABLE_NAME);
        props.add(PARTITION_CLAUSE);
        props.add(CREATE_TABLE);
        props.add(TABLE_MANAGEMENT_STRATEGY);
        props.add(EXTERNAL_TABLE_LOCATION);
        props.add(TABLE_STORAGE_FORMAT);
        props.add(UPDATE_FIELD_NAMES);
        props.add(RECORD_WRITER_FACTORY);
        props.add(QUERY_TIMEOUT);

        propertyDescriptors = Collections.unmodifiableList(props);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));
        final boolean recordWriterFactorySet = validationContext.getProperty(RECORD_WRITER_FACTORY).isSet();
        final boolean createIfNotExists = validationContext.getProperty(CREATE_TABLE).getValue().equals(CREATE_IF_NOT_EXISTS.getValue());
        final boolean updateFieldNames = validationContext.getProperty(UPDATE_FIELD_NAMES).asBoolean();

        if (!recordWriterFactorySet && updateFieldNames) {
            validationResults.add(new ValidationResult.Builder().subject(RECORD_WRITER_FACTORY.getDisplayName())
                    .explanation("Record Writer must be set if 'Update Field Names' is true").valid(false).build());
        }
        final String tableManagementStrategy = validationContext.getProperty(TABLE_MANAGEMENT_STRATEGY).getValue();
        final boolean managedTable;
        if (!ATTRIBUTE_DRIVEN_TABLE.getValue().equals(tableManagementStrategy)) {
            managedTable = MANAGED_TABLE.getValue().equals(tableManagementStrategy);
            // Ensure valid configuration for external tables
            if (createIfNotExists && !managedTable && !validationContext.getProperty(EXTERNAL_TABLE_LOCATION).isSet()) {
                validationResults.add(new ValidationResult.Builder().subject(EXTERNAL_TABLE_LOCATION.getDisplayName())
                        .explanation("External Table Location must be set when Table Management Strategy is set to External").valid(false).build());
            }
        }
        return validationResults;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory recordWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String partitionClauseString = context.getProperty(PARTITION_CLAUSE).evaluateAttributeExpressions(flowFile).getValue();
        List<String> partitionClauseElements = null;
        if (!StringUtils.isEmpty(partitionClauseString)) {
            partitionClauseElements = Arrays.stream(partitionClauseString.split(",")).filter(Objects::nonNull).map(String::trim).collect(Collectors.toList());
        }

        final ComponentLog log = getLogger();

        try {
            final RecordReader reader;

            try (final InputStream in = session.read(flowFile)) {
                // if we fail to create the RecordReader then we want to route to failure, so we need to
                // handle this separately from the other IOExceptions which normally route to retry
                try {
                    reader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                } catch (Exception e) {
                    throw new RecordReaderFactoryException("Unable to create RecordReader", e);
                }
            } catch (RecordReaderFactoryException rrfe) {
                log.error(
                        "Failed to create {} for {} - routing to failure",
                        RecordReader.class.getSimpleName(), flowFile,
                        rrfe
                );
                // Since we are wrapping the exceptions above there should always be a cause
                // but it's possible it might not have a message. This handles that by logging
                // the name of the class thrown.
                Throwable c = rrfe.getCause();
                if (c != null) {
                    session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
                } else {
                    session.putAttribute(flowFile, "record.error.message", rrfe.getClass().getCanonicalName() + " Thrown");
                }
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final RecordSchema recordSchema = reader.getSchema();

            final boolean createIfNotExists = context.getProperty(CREATE_TABLE).getValue().equals(CREATE_IF_NOT_EXISTS.getValue());
            final boolean updateFieldNames = context.getProperty(UPDATE_FIELD_NAMES).asBoolean();
            if (recordWriterFactory == null && updateFieldNames) {
                throw new ProcessException("Record Writer must be set if 'Update Field Names' is true");
            }
            final String tableManagementStrategy = context.getProperty(TABLE_MANAGEMENT_STRATEGY).getValue();
            final boolean managedTable;
            if (ATTRIBUTE_DRIVEN_TABLE.getValue().equals(tableManagementStrategy)) {
                String tableManagementStrategyAttribute = flowFile.getAttribute(TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE);
                if (MANAGED_TABLE.getValue().equalsIgnoreCase(tableManagementStrategyAttribute)) {
                    managedTable = true;
                } else if (EXTERNAL_TABLE.getValue().equalsIgnoreCase(tableManagementStrategyAttribute)) {
                    managedTable = false;
                } else {
                    log.error("The '{}' attribute either does not exist or has invalid value: {}. Must be one of (ignoring case): Managed, External. "
                                    + "Routing flowfile to failure",
                            new Object[]{TABLE_MANAGEMENT_STRATEGY_ATTRIBUTE, tableManagementStrategyAttribute});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            } else {
                managedTable = MANAGED_TABLE.getValue().equals(tableManagementStrategy);
            }

            // Ensure valid configuration for external tables
            if (createIfNotExists && !managedTable && !context.getProperty(EXTERNAL_TABLE_LOCATION).isSet()) {
                throw new IOException("External Table Location must be set when Table Management Strategy is set to External");
            }
            final String externalTableLocation = managedTable ? null : context.getProperty(EXTERNAL_TABLE_LOCATION).evaluateAttributeExpressions(flowFile).getValue();
            if (!managedTable && StringUtils.isEmpty(externalTableLocation)) {
                log.error("External Table Location has invalid value: {}. Routing flowfile to failure", new Object[]{externalTableLocation});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final String storageFormat = context.getProperty(TABLE_STORAGE_FORMAT).getValue();
            final Hive3DBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(Hive3DBCPService.class);
            try (final Connection connection = dbcpService.getConnection()) {

                final Map<String,String> attributes = new HashMap<>(flowFile.getAttributes());
                OutputMetadataHolder outputMetadataHolder = checkAndUpdateTableSchema(attributes, connection, recordSchema, tableName, partitionClauseElements,
                        createIfNotExists, externalTableLocation, storageFormat, updateFieldNames);
                if (outputMetadataHolder != null) {
                    // The output schema changed (i.e. field names were updated), so write out the corresponding FlowFile
                    try {
                        final FlowFile inputFlowFile = flowFile;
                        flowFile = session.write(flowFile, (in, out) -> {

                            // if we fail to create the RecordReader then we want to route to failure, so we need to
                            // handle this separately from the other IOExceptions which normally route to retry
                            final RecordReader recordReader;
                            final RecordSetWriter recordSetWriter;
                            try {
                                recordReader = recordReaderFactory.createRecordReader(inputFlowFile, in, getLogger());
                                recordSetWriter = recordWriterFactory.createWriter(getLogger(), outputMetadataHolder.getOutputSchema(), out, attributes);
                            } catch (Exception e) {
                                if(e instanceof IOException) {
                                    throw (IOException) e;
                                }
                                throw new IOException(new RecordReaderFactoryException("Unable to create RecordReader", e));
                            }

                            WriteResult writeResult = updateRecords(recordSchema, outputMetadataHolder, recordReader, recordSetWriter);
                            recordSetWriter.flush();
                            recordSetWriter.close();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), recordSetWriter.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                        });
                    } catch (final Exception e) {
                        getLogger().error("Failed to process {}; will route to failure", new Object[]{flowFile, e});
                        // Since we are wrapping the exceptions above there should always be a cause
                        // but it's possible it might not have a message. This handles that by logging
                        // the name of the class thrown.
                        Throwable c = e.getCause();
                        if (c != null) {
                            session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
                        } else {
                            session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
                        }
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }

                }
                attributes.put(ATTR_OUTPUT_TABLE, tableName);
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().invokeRemoteProcess(flowFile, dbcpService.getConnectionURL());
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException | SQLException e) {
            flowFile = session.putAttribute(flowFile, ATTR_OUTPUT_TABLE, tableName);
            log.error("Exception while processing {} - routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (DiscontinuedException e) {
            // The input FlowFile processing is discontinued. Keep it in the input queue.
            getLogger().warn("Discontinued processing for {} due to {}", flowFile, e, e);
            session.transfer(flowFile, Relationship.SELF);
        } catch (Throwable t) {
            throw (t instanceof ProcessException) ? (ProcessException) t : new ProcessException(t);
        }
    }

    private synchronized OutputMetadataHolder checkAndUpdateTableSchema(Map<String,String> attributes, final Connection conn, final RecordSchema schema,
                                                                      final String tableName, List<String> partitionClause, final boolean createIfNotExists,
                                                                      final String externalTableLocation, final String storageFormat, final boolean updateFieldNames) throws IOException {
        // Read in the current table metadata, compare it to the reader's schema, and
        // add any columns from the schema that are missing in the table
        try (Statement s = conn.createStatement()) {
            // Determine whether the table exists
            ResultSet tables = s.executeQuery("SHOW TABLES");
            List<String> tableNames = new ArrayList<>();
            String hiveTableName;
            while (tables.next() && StringUtils.isNotEmpty(hiveTableName = tables.getString(1))) {
                tableNames.add(hiveTableName);
            }

            List<String> columnsToAdd = new ArrayList<>();
            String outputPath;
            boolean tableCreated = false;
            if (!tableNames.contains(tableName) && createIfNotExists) {
                StringBuilder createTableStatement = new StringBuilder();
                for (RecordField recordField : schema.getFields()) {
                    String recordFieldName = recordField.getFieldName();
                    // The field does not exist in the table, add it
                    columnsToAdd.add("`" + recordFieldName + "` " + NiFiOrcUtils.getHiveTypeFromFieldType(recordField.getDataType(), true));
                    getLogger().debug("Adding column " + recordFieldName + " to table " + tableName);
                }

                // Handle partition clause
                if (partitionClause == null) {
                    partitionClause = Collections.emptyList();
                }
                List<String> validatedPartitionClause = new ArrayList<>(partitionClause.size());
                for (String partition : partitionClause) {
                    String[] partitionInfo = partition.split(" ");
                    if (partitionInfo.length != 2) {
                        validatedPartitionClause.add("`" + partitionInfo[0] + "` string");
                    } else {
                        validatedPartitionClause.add("`" + partitionInfo[0] + "` " + partitionInfo[1]);
                    }
                }

                createTableStatement.append("CREATE ")
                        .append(externalTableLocation == null ? "" : "EXTERNAL ")
                        .append("TABLE IF NOT EXISTS `")
                        .append(tableName)
                        .append("` (")
                        .append(String.join(", ", columnsToAdd))
                        .append(") ")
                        .append(validatedPartitionClause.isEmpty() ? "" : "PARTITIONED BY (" + String.join(", ", validatedPartitionClause) + ") ")
                        .append("STORED AS ")
                        .append(storageFormat)
                        .append(externalTableLocation == null ? "" : " LOCATION '" + externalTableLocation + "'");

                String createTableSql = createTableStatement.toString();

                if (StringUtils.isNotEmpty(createTableSql)) {
                    // Perform the table create
                    getLogger().info("Executing Hive DDL: " + createTableSql);
                    s.execute(createTableSql);
                }

                tableCreated = true;
            }

            // Process the table (columns, partitions, location, etc.)
            List<String> hiveColumns = new ArrayList<>();

            String describeTable = "DESC FORMATTED `" + tableName + "`";
            ResultSet tableInfo = s.executeQuery(describeTable);
            // Result is 3 columns, col_name, data_type, comment. Check the first row for a header and skip if so, otherwise add column name
            tableInfo.next();
            String columnName = tableInfo.getString(1);
            if (StringUtils.isNotEmpty(columnName) && !columnName.startsWith("#")) {
                hiveColumns.add(columnName);
            }
            // If the column was a header, check for a blank line to follow and skip it, otherwise add the column name
            if (columnName.startsWith("#")) {
                tableInfo.next();
                columnName = tableInfo.getString(1);
                if (StringUtils.isNotEmpty(columnName)) {
                    hiveColumns.add(columnName);
                }
            }

            // Collect all column names
            while (tableInfo.next() && StringUtils.isNotEmpty(columnName = tableInfo.getString(1))) {
                hiveColumns.add(columnName);
            }

            // Collect all partition columns
            boolean moreRows = true;
            boolean headerFound = false;
            while (moreRows && !headerFound) {
                String line = tableInfo.getString(1);
                if ("# Partition Information".equals(line)) {
                    headerFound = true;
                } else if ("# Detailed Table Information".equals(line)) {
                    // Not partitioned, exit the loop with headerFound = false
                    break;
                }
                moreRows = tableInfo.next();
            }

            List<String> partitionColumns = new ArrayList<>();
            List<String> partitionColumnsEqualsValueList = new ArrayList<>();
            List<String> partitionColumnsLocationList = new ArrayList<>();
            if (headerFound) {
                // If the table is partitioned, construct the partition=value strings for each partition column
                String partitionColumnName;
                columnName = tableInfo.getString(1);
                if (StringUtils.isNotEmpty(columnName) && !columnName.startsWith("#")) {
                    partitionColumns.add(columnName);
                }
                // If the column was a header, check for a blank line to follow and skip it, otherwise add the column name
                if (columnName.startsWith("#")) {
                    tableInfo.next();
                    columnName = tableInfo.getString(1);
                    if (StringUtils.isNotEmpty(columnName)) {
                        partitionColumns.add(columnName);
                    }
                }
                while (tableInfo.next() && StringUtils.isNotEmpty(partitionColumnName = tableInfo.getString(1))) {
                    partitionColumns.add(partitionColumnName);
                }

                final int partitionColumnsSize = partitionColumns.size();
                final int partitionClauseSize = (partitionClause == null) ? 0 : partitionClause.size();
                if (partitionClauseSize != partitionColumnsSize) {
                    throw new IOException("Found " + partitionColumnsSize + " partition columns but " + partitionClauseSize + " partition values were supplied");
                }

                for (int i = 0; i < partitionClauseSize; i++) {
                    String partitionName = partitionClause.get(i).split(" ")[0];
                    String partitionValue = attributes.get(partitionName);
                    if (StringUtils.isEmpty(partitionValue)) {
                        throw new IOException("No value found for partition value attribute '" + partitionName + "'");
                    }
                    if (!partitionColumns.contains(partitionName)) {
                        throw new IOException("Cannot add partition '" + partitionName + "' to existing table");
                    }
                    partitionColumnsEqualsValueList.add("`" + partitionName + "`='" + partitionValue + "'");
                    // Add unquoted version for the output path
                    partitionColumnsLocationList.add(partitionName + "=" + partitionValue);
                }
            }

            // Get table location
            moreRows = true;
            headerFound = false;
            while (moreRows && !headerFound) {
                String line = tableInfo.getString(1);
                if (line.startsWith("Location:")) {
                    headerFound = true;
                    continue; // Don't do a next() here, need to get the second column value
                }
                moreRows = tableInfo.next();
            }
            String tableLocation = tableInfo.getString(2);

            String alterTableSql;
            // If the table wasn't newly created, alter it accordingly
            if (!tableCreated) {
                StringBuilder alterTableStatement = new StringBuilder();
                // Handle new columns
                for (RecordField recordField : schema.getFields()) {
                    String recordFieldName = recordField.getFieldName().toLowerCase();
                    if (!hiveColumns.contains(recordFieldName) && !partitionColumns.contains(recordFieldName)) {
                        // The field does not exist in the table (and is not a partition column), add it
                        columnsToAdd.add("`" + recordFieldName + "` " + NiFiOrcUtils.getHiveTypeFromFieldType(recordField.getDataType(), true));
                        hiveColumns.add(recordFieldName);
                        getLogger().info("Adding column " + recordFieldName + " to table " + tableName);
                    }
                }

                if (!columnsToAdd.isEmpty()) {
                    alterTableStatement.append("ALTER TABLE `")
                            .append(tableName)
                            .append("` ADD COLUMNS (")
                            .append(String.join(", ", columnsToAdd))
                            .append(")");

                    alterTableSql = alterTableStatement.toString();
                    if (StringUtils.isNotEmpty(alterTableSql)) {
                        // Perform the table update
                        getLogger().info("Executing Hive DDL: " + alterTableSql);
                        s.execute(alterTableSql);
                    }
                }
            }

            outputPath = tableLocation;

            // Handle new partition values
            if (!partitionColumnsEqualsValueList.isEmpty()) {
                alterTableSql = "ALTER TABLE `" +
                        tableName +
                        "` ADD IF NOT EXISTS PARTITION (" +
                        String.join(", ", partitionColumnsEqualsValueList) +
                        ")";
                if (StringUtils.isNotEmpty(alterTableSql)) {
                    // Perform the table update
                    getLogger().info("Executing Hive DDL: " + alterTableSql);
                    s.execute(alterTableSql);
                }
                // Add attribute for HDFS location of the partition values
                outputPath = tableLocation + "/" + String.join("/", partitionColumnsLocationList);
            }

            // If updating field names, return a new RecordSchema, otherwise return null
            OutputMetadataHolder outputMetadataHolder;
            if (updateFieldNames) {
                List<RecordField> inputRecordFields = schema.getFields();
                List<RecordField> outputRecordFields = new ArrayList<>();
                Map<String,String> fieldMap = new HashMap<>();
                boolean needsUpdating = false;

                for (RecordField inputRecordField : inputRecordFields) {
                    final String inputRecordFieldName = inputRecordField.getFieldName();
                    boolean found = false;
                    for (String hiveColumnName : hiveColumns) {
                        if (inputRecordFieldName.equalsIgnoreCase(hiveColumnName)) {
                            // Set a flag if the field name doesn't match the column name exactly. This overall flag will determine whether
                            // the records need updating (if true) or not (if false)
                            if (!inputRecordFieldName.equals(hiveColumnName)) {
                                needsUpdating = true;
                            }
                            fieldMap.put(inputRecordFieldName, hiveColumnName);
                            outputRecordFields.add(new RecordField(hiveColumnName, inputRecordField.getDataType(), inputRecordField.getDefaultValue(), inputRecordField.isNullable()));
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // If the input field wasn't a Hive table column, add it back to the schema as-is
                        fieldMap.put(inputRecordFieldName, inputRecordFieldName);
                    }
                }
                outputMetadataHolder = needsUpdating ? new OutputMetadataHolder(new SimpleRecordSchema(outputRecordFields), fieldMap)
                        : null;
            } else {
                outputMetadataHolder = null;
            }
            attributes.put(ATTR_OUTPUT_PATH, outputPath);
            return outputMetadataHolder;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private synchronized WriteResult updateRecords(final RecordSchema inputRecordSchema, final OutputMetadataHolder outputMetadataHolder,
                                            final RecordReader reader, final RecordSetWriter writer) throws IOException {
        try {
            writer.beginRecordSet();
            Record inputRecord;
            while((inputRecord = reader.nextRecord()) != null) {
                List<RecordField> inputRecordFields = inputRecordSchema.getFields();
                Map<String,Object> outputRecordFields = new HashMap<>(inputRecordFields.size());
                // Copy values from input field name to output field name
                for(Map.Entry<String,String> mapping : outputMetadataHolder.getFieldMap().entrySet()) {
                    outputRecordFields.put(mapping.getValue(), inputRecord.getValue(mapping.getKey()));
                }
                Record outputRecord = new MapRecord(outputMetadataHolder.getOutputSchema(), outputRecordFields);
                writer.write(outputRecord);
            }
            return writer.finishRecordSet();

        } catch (MalformedRecordException mre) {
            throw new IOException("Error reading records: "+mre.getMessage(), mre);
        }
    }

    private static class OutputMetadataHolder {
        private final RecordSchema outputSchema;
        private final Map<String,String> fieldMap;

        public OutputMetadataHolder(RecordSchema outputSchema, Map<String, String> fieldMap) {
            this.outputSchema = outputSchema;
            this.fieldMap = fieldMap;
        }

        public RecordSchema getOutputSchema() {
            return outputSchema;
        }

        public Map<String, String> getFieldMap() {
            return fieldMap;
        }
    }
}
