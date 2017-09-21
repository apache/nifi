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
package org.apache.nifi.processors.standard;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.ErrorTypes;
import org.apache.nifi.processor.util.pattern.ExceptionHandler;
import org.apache.nifi.processor.util.pattern.PartialFunctions;
import org.apache.nifi.processor.util.pattern.Put;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processor.util.pattern.RoutingResult;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;


@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "record", "jdbc", "put", "database", "update", "insert", "delete"})
@CapabilityDescription("The PutDatabaseRecord processor uses a specified RecordReader to input (possibly multiple) records from an incoming flow file. These records are translated to SQL "
        + "statements and executed as a single batch. If any errors occur, the flow file is routed to failure or retry, and if the records are transmitted successfully, the incoming flow file is "
        + "routed to success.  The type of statement executed by the processor is specified via the Statement Type property, which accepts some hard-coded values such as INSERT, UPDATE, and DELETE, "
        + "as well as 'Use statement.type Attribute', which causes the processor to get the statement type from a flow file attribute.  IMPORTANT: If the Statement Type is UPDATE, then the incoming "
        + "records must not alter the value(s) of the primary keys (or user-specified Update Keys). If such records are encountered, the UPDATE statement issued to the database may do nothing "
        + "(if no existing records with the new primary key values are found), or could inadvertently corrupt the existing data (by changing records for which the new values of the primary keys "
        + "exist).")
@ReadsAttribute(attribute = PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, description = "If 'Use statement.type Attribute' is selected for the Statement Type property, the value of this attribute "
        + "will be used to determine the type of statement (INSERT, UPDATE, DELETE, SQL, etc.) to generate and execute.")
@WritesAttribute(attribute = PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR, description = "If an error occurs during processing, the flow file will be routed to failure or retry, and this attribute "
        + "will be populated with the cause of the error.")
public class PutDatabaseRecord extends AbstractSessionFactoryProcessor {

    static final String UPDATE_TYPE = "UPDATE";
    static final String INSERT_TYPE = "INSERT";
    static final String DELETE_TYPE = "DELETE";
    static final String SQL_TYPE = "SQL";   // Not an allowable value in the Statement Type property, must be set by attribute
    static final String USE_ATTR_TYPE = "Use statement.type Attribute";

    static final String STATEMENT_TYPE_ATTRIBUTE = "statement.type";

    static final String PUT_DATABASE_RECORD_ERROR = "putdatabaserecord.error";

    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Any field in the document that cannot be mapped to a column in the database is ignored");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail on Unmatched Fields", "Fail on Unmatched Fields",
            "If the document has any field that cannot be mapped to a column in the database, the FlowFile will be routed to the failure relationship");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  No notification will be logged");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  A warning will be logged");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "A flow will fail if any column in the database that does not have a field in the document.  An error will be logged");

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    protected static Set<Relationship> relationships;

    // Properties
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("put-db-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("put-db-record-statement-type")
            .displayName("Statement Type")
            .description("Specifies the type of SQL Statement to generate. If 'Use statement.type Attribute' is chosen, then the value is taken from the statement.type attribute in the "
                    + "FlowFile. The 'Use statement.type Attribute' option is the only one that allows the 'SQL' statement type. If 'SQL' is specified, the value of the field specified by the "
                    + "'Field Containing SQL' property is expected to be a valid SQL statement on the target database, and will be executed as-is.")
            .required(true)
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, DELETE_TYPE, USE_ATTR_TYPE)
            .build();

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("put-db-record-dcbp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database for sending records.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("put-db-record-table-name")
            .displayName("Table Name")
            .description("The name of the table that the statement should affect.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("put-db-record-translate-field-names")
            .displayName("Translate Field Names")
            .description("If true, the Processor will attempt to translate field names into the appropriate column names for the table specified. "
                    + "If false, the field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("put-db-record-unmatched-field-behavior")
            .displayName("Unmatched Field Behavior")
            .description("If an incoming record has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("put-db-record-unmatched-column-behavior")
            .displayName("Unmatched Column Behavior")
            .description("If an incoming record does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new PropertyDescriptor.Builder()
            .name("put-db-record-update-keys")
            .displayName("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
                    + "In this case, if no Primary Key exists, the conversion to SQL will fail if Unmatched Column Behaviour is set to FAIL. "
                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor FIELD_CONTAINING_SQL = new PropertyDescriptor.Builder()
            .name("put-db-record-field-containing-sql")
            .displayName("Field Containing SQL")
            .description("If the Statement Type is 'SQL' (as set in the statement.type attribute), this field indicates which field in the record(s) contains the SQL statement to execute. The value "
                    + "of the field must be a single SQL statement. If the Statement Type is not 'SQL', this field is ignored.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor QUOTED_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("put-db-record-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTED_TABLE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("put-db-record-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the use of special characters in the table name.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("put-db-record-query-timeout")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL statement "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static List<PropertyDescriptor> propDescriptors;

    private final Map<SchemaKey, TableSchema> schemaCache = new LinkedHashMap<SchemaKey, TableSchema>(100) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<SchemaKey, TableSchema> eldest) {
            return size() >= 100;
        }
    };


    static {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        r.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(RECORD_READER_FACTORY);
        pds.add(STATEMENT_TYPE);
        pds.add(DBCP_SERVICE);
        pds.add(CATALOG_NAME);
        pds.add(SCHEMA_NAME);
        pds.add(TABLE_NAME);
        pds.add(TRANSLATE_FIELD_NAMES);
        pds.add(UNMATCHED_FIELD_BEHAVIOR);
        pds.add(UNMATCHED_COLUMN_BEHAVIOR);
        pds.add(UPDATE_KEYS);
        pds.add(FIELD_CONTAINING_SQL);
        pds.add(QUOTED_IDENTIFIERS);
        pds.add(QUOTED_TABLE_IDENTIFIER);
        pds.add(QUERY_TIMEOUT);
        pds.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);

        propDescriptors = Collections.unmodifiableList(pds);
    }

    private Put<FunctionContext, Connection> process;
    private ExceptionHandler<FunctionContext> exceptionHandler;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    private final PartialFunctions.InitConnection<FunctionContext, Connection> initConnection = (c, s, fc) -> {
        final Connection connection = c.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class).getConnection();
        try {
            fc.originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            String jdbcUrl = "DBCPService";
            try {
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                if (databaseMetaData != null) {
                    jdbcUrl = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            } finally {
                fc.jdbcUrl = jdbcUrl;
            }

        } catch (SQLException e) {
            throw new ProcessException("Failed to disable auto commit due to " + e, e);
        }
        return connection;
    };

    private final Put.PutFlowFile<FunctionContext, Connection> putFlowFile = (context, session, functionContext, conn, flowFile, result) -> {

        exceptionHandler.execute(functionContext, flowFile, inputFlowFile -> {

            // Get the statement type from the attribute if necessary
            final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
            String statementType = statementTypeProperty;
            if (USE_ATTR_TYPE.equals(statementTypeProperty)) {
                statementType = inputFlowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
            }
            if (StringUtils.isEmpty(statementType)) {
                final String msg = format("Statement Type is not specified, FlowFile %s", inputFlowFile);
                throw new IllegalArgumentException(msg);
            }


            try (final InputStream in = session.read(inputFlowFile)) {

                final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY)
                        .asControllerService(RecordReaderFactory.class);
                final RecordReader recordParser = recordParserFactory.createRecordReader(inputFlowFile, in, getLogger());

                if (SQL_TYPE.equalsIgnoreCase(statementType)) {
                    executeSQL(context, session, inputFlowFile, functionContext, result, conn, recordParser);

                } else {
                    final DMLSettings settings = new DMLSettings(context);
                    executeDML(context, session, inputFlowFile, functionContext, result, conn, recordParser, statementType, settings);
                }
            }

        }, (fc, inputFlowFile, r, e) -> {

            getLogger().warn("Failed to process {} due to {}", new Object[]{inputFlowFile, e}, e);

            if (e instanceof BatchUpdateException) {
                try {
                    // Although process session will move forward in order to route the failed FlowFile,
                    // database transaction should be rolled back to avoid partial batch update.
                    conn.rollback();
                } catch (SQLException re) {
                    getLogger().error("Failed to rollback database due to {}, transaction may be incomplete.", new Object[]{re}, re);
                }
            }

            // Embed Exception detail to FlowFile attribute then delegate error handling to default and rollbackOnFailure.
            final FlowFile flowFileWithAttributes = session.putAttribute(inputFlowFile, PUT_DATABASE_RECORD_ERROR, e.getMessage());
            final ExceptionHandler.OnError<FunctionContext, FlowFile> defaultOnError = ExceptionHandler.createOnError(context, session, result, REL_FAILURE, REL_RETRY);
            final ExceptionHandler.OnError<FunctionContext, FlowFile> rollbackOnFailure = RollbackOnFailure.createOnError(defaultOnError);
            rollbackOnFailure.apply(fc, flowFileWithAttributes, r, e);
        });
    };


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        synchronized (this) {
            schemaCache.clear();
        }

        process = new Put<>();

        process.setLogger(getLogger());
        process.initConnection(initConnection);
        process.putFlowFile(putFlowFile);
        process.adjustRoute(RollbackOnFailure.createAdjustRoute(REL_FAILURE, REL_RETRY));

        process.onCompleted((c, s, fc, conn) -> {
            try {
                conn.commit();
            } catch (SQLException e) {
                // Throw ProcessException to rollback process session.
                throw new ProcessException("Failed to commit database connection due to " + e, e);
            }
        });

        process.onFailed((c, s, fc, conn, e) -> {
            try {
                conn.rollback();
            } catch (SQLException re) {
                // Just log the fact that rollback failed.
                // ProcessSession will be rollback by the thrown Exception so don't have to do anything here.
                getLogger().warn("Failed to rollback database connection due to %s", new Object[]{re}, re);
            }
        });

        process.cleanup((c, s, fc, conn) -> {
            // make sure that we try to set the auto commit back to whatever it was.
            if (fc.originalAutoCommit) {
                try {
                    conn.setAutoCommit(true);
                } catch (final SQLException se) {
                    getLogger().warn("Failed to reset autocommit due to {}", new Object[]{se});
                }
            }
        });

        exceptionHandler = new ExceptionHandler<>();
        exceptionHandler.mapException(s -> {

            try {
                if (s == null) {
                    return ErrorTypes.PersistentFailure;
                }
                throw s;

            } catch (IllegalArgumentException
                    |MalformedRecordException
                    |SQLNonTransientException e) {
                return ErrorTypes.InvalidInput;

            } catch (IOException
                    |SQLException e) {
                return ErrorTypes.TemporalFailure;

            } catch (Exception e) {
                return ErrorTypes.UnknownFailure;
            }

        });
        exceptionHandler.adjustError(RollbackOnFailure.createAdjustError(getLogger()));
    }

    private static class FunctionContext extends RollbackOnFailure {
        private final int queryTimeout;
        private boolean originalAutoCommit = false;
        private String jdbcUrl;

        public FunctionContext(boolean rollbackOnFailure, int queryTimeout) {
            super(rollbackOnFailure, true);
            this.queryTimeout = queryTimeout;
        }
    }

    static class DMLSettings {
        private final boolean translateFieldNames;
        private final boolean ignoreUnmappedFields;

        // Is the unmatched column behaviour fail or warning?
        private final boolean failUnmappedColumns;
        private final boolean warningUnmappedColumns;

        // Escape column names?
        private final boolean escapeColumnNames;

        // Quote table name?
        private final boolean quoteTableName;

        private DMLSettings(ProcessContext context) {
            translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());

            failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
            warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            escapeColumnNames = context.getProperty(QUOTED_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTED_TABLE_IDENTIFIER).asBoolean();
        }

    }

    private void executeSQL(ProcessContext context, ProcessSession session,
                            FlowFile flowFile, FunctionContext functionContext, RoutingResult result,
                            Connection con, RecordReader recordParser)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final RecordSchema recordSchema = recordParser.getSchema();

        // Find which field has the SQL statement in it
        final String sqlField = context.getProperty(FIELD_CONTAINING_SQL).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(sqlField)) {
            throw new IllegalArgumentException(format("SQL specified as Statement Type but no Field Containing SQL was found, FlowFile %s", flowFile));
        }

        boolean schemaHasSqlField = recordSchema.getFields().stream().anyMatch((field) -> sqlField.equals(field.getFieldName()));
        if (!schemaHasSqlField) {
            throw new IllegalArgumentException(format("Record schema does not contain Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
        }

        try (Statement s = con.createStatement()) {

            try {
                s.setQueryTimeout(functionContext.queryTimeout); // timeout in seconds
            } catch (SQLException se) {
                // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                if (functionContext.queryTimeout > 0) {
                    throw se;
                }
            }

            Record currentRecord;
            while ((currentRecord = recordParser.nextRecord()) != null) {
                Object sql = currentRecord.getValue(sqlField);
                if (sql == null || StringUtils.isEmpty((String) sql)) {
                    throw new MalformedRecordException(format("Record had no (or null) value for Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
                }

                // Execute the statement as-is
                s.execute((String) sql);
            }
            result.routeTo(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, functionContext.jdbcUrl);
        }
    }

    private void executeDML(ProcessContext context, ProcessSession session, FlowFile flowFile,
                            FunctionContext functionContext, RoutingResult result, Connection con,
                            RecordReader recordParser, String statementType, DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final RecordSchema recordSchema = recordParser.getSchema();
        final ComponentLog log = getLogger();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(flowFile).getValue();
        final SchemaKey schemaKey = new PutDatabaseRecord.SchemaKey(catalog, schemaName, tableName);

        // Ensure the table name has been set, the generated SQL statements (and TableSchema cache) will need it
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(format("Cannot process %s because Table Name is null or empty", flowFile));
        }

        // Always get the primary keys if Update Keys is empty. Otherwise if we have an Insert statement first, the table will be
        // cached but the primary keys will not be retrieved, causing future UPDATE statements to not have primary keys available
        final boolean includePrimaryKeys = updateKeys == null;

        // get the database schema from the cache, if one exists. We do this in a synchronized block, rather than
        // using a ConcurrentMap because the Map that we are using is a LinkedHashMap with a capacity such that if
        // the Map grows beyond this capacity, old elements are evicted. We do this in order to avoid filling the
        // Java Heap if there are a lot of different SQL statements being generated that reference different tables.
        TableSchema tableSchema;
        synchronized (this) {
            tableSchema = schemaCache.get(schemaKey);
            if (tableSchema == null) {
                // No schema exists for this table yet. Query the database to determine the schema and put it into the cache.
                tableSchema = TableSchema.from(con, catalog, schemaName, tableName, settings.translateFieldNames, includePrimaryKeys);
                schemaCache.put(schemaKey, tableSchema);
            }
        }
        if (tableSchema == null) {
            throw new IllegalArgumentException("No table schema specified!");
        }

        // build the fully qualified table name
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            tableNameBuilder.append(catalog).append(".");
        }
        if (schemaName != null) {
            tableNameBuilder.append(schemaName).append(".");
        }
        tableNameBuilder.append(tableName);
        final String fqTableName = tableNameBuilder.toString();

        if (recordSchema == null) {
            throw new IllegalArgumentException("No record schema specified!");
        }

        final SqlAndIncludedColumns sqlHolder;
        if (INSERT_TYPE.equalsIgnoreCase(statementType)) {
            sqlHolder = generateInsert(recordSchema, fqTableName, tableSchema, settings);

        } else if (UPDATE_TYPE.equalsIgnoreCase(statementType)) {
            sqlHolder = generateUpdate(recordSchema, fqTableName, updateKeys, tableSchema, settings);

        } else if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
            sqlHolder = generateDelete(recordSchema, fqTableName, tableSchema, settings);

        } else {
            throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, flowFile));
        }

        try (PreparedStatement ps = con.prepareStatement(sqlHolder.getSql())) {

            final int queryTimeout = functionContext.queryTimeout;
            try {
                ps.setQueryTimeout(queryTimeout); // timeout in seconds
            } catch (SQLException se) {
                // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                if (queryTimeout > 0) {
                    throw se;
                }
            }

            Record currentRecord;
            List<Integer> fieldIndexes = sqlHolder.getFieldIndexes();

            while ((currentRecord = recordParser.nextRecord()) != null) {
                Object[] values = currentRecord.getValues();
                if (values != null) {
                    if (fieldIndexes != null) {
                        for (int i = 0; i < fieldIndexes.size(); i++) {
                            // If DELETE type, insert the object twice because of the null check (see generateDelete for details)
                            if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                                ps.setObject(i * 2 + 1, values[fieldIndexes.get(i)]);
                                ps.setObject(i * 2 + 2, values[fieldIndexes.get(i)]);
                            } else {
                                ps.setObject(i + 1, values[fieldIndexes.get(i)]);
                            }
                        }
                    } else {
                        // If there's no index map, assume all values are included and set them in order
                        for (int i = 0; i < values.length; i++) {
                            // If DELETE type, insert the object twice because of the null check (see generateDelete for details)
                            if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                                ps.setObject(i * 2 + 1, values[i]);
                                ps.setObject(i * 2 + 2, values[i]);
                            } else {
                                ps.setObject(i + 1, values[i]);
                            }
                        }
                    }
                    ps.addBatch();
                }
            }

            log.debug("Executing query {}", new Object[]{sqlHolder});
            ps.executeBatch();
            result.routeTo(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, functionContext.jdbcUrl);

        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

        final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();

        final FunctionContext functionContext = new FunctionContext(rollbackOnFailure, queryTimeout);

        RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), session -> process.onTrigger(context, session, functionContext));

    }

    private Set<String> getNormalizedColumnNames(final RecordSchema schema, final boolean translateFieldNames) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        if (schema != null) {
            schema.getFieldNames().forEach((fieldName) -> normalizedFieldNames.add(normalizeColumnName(fieldName, translateFieldNames)));
        }
        return normalizedFieldNames;
    }

    SqlAndIncludedColumns generateInsert(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new IllegalArgumentException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        if (settings.quoteTableName) {
            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }
        sqlBuilder.append(" (");

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database");
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }
                    includedColumns.add(i);
                }
            }

            // complete the SQL statements by adding ?'s for all of the values to be escaped.
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append(StringUtils.repeat("?", ",", includedColumns.size()));
            sqlBuilder.append(")");

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table");
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    SqlAndIncludedColumns generateUpdate(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLException {

        final Set<String> updateKeyNames;
        if (updateKeys == null) {
            updateKeyNames = tableSchema.getPrimaryKeyColumnNames();
        } else {
            updateKeyNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyNames.add(updateKey.trim());
            }
        }

        if (updateKeyNames.isEmpty()) {
            throw new SQLIntegrityConstraintViolationException("Table '" + tableName + "' does not have a Primary Key and no Update Keys were specified");
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        if (settings.quoteTableName) {
            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        // Create a Set of all normalized Update Key names, and ensure that there is a field in the record
        // for each of the Update Key fields.
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);
        final Set<String> normalizedUpdateNames = new HashSet<>();
        for (final String uk : updateKeyNames) {
            final String normalizedUK = normalizeColumnName(uk, settings.translateFieldNames);
            normalizedUpdateNames.add(normalizedUK);

            if (!normalizedFieldNames.contains(normalizedUK)) {
                String missingColMessage = "Record does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + uk + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" SET ");

            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final String normalizedColName = normalizeColumnName(fieldName, settings.translateFieldNames);
                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null) {
                    if (!settings.ignoreUnmappedFields) {
                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database");
                    } else {
                        continue;
                    }
                }

                // Check if this column is an Update Key. If so, skip it for now. We will come
                // back to it after we finish the SET clause
                if (!normalizedUpdateNames.contains(normalizedColName)) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }

                    sqlBuilder.append(" = ?");
                    includedColumns.add(i);
                }
            }

            // Set the WHERE clause based on the Update Key values
            sqlBuilder.append(" WHERE ");
            AtomicInteger whereFieldCount = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {

                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final String normalizedColName = normalizeColumnName(fieldName, settings.translateFieldNames);
                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc != null) {

                    // Check if this column is a Update Key. If so, add it to the WHERE clause
                    if (normalizedUpdateNames.contains(normalizedColName)) {

                        if (whereFieldCount.getAndIncrement() > 0) {
                            sqlBuilder.append(" AND ");
                        }

                        if (settings.escapeColumnNames) {
                            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                    .append(normalizedColName)
                                    .append(tableSchema.getQuotedIdentifierString());
                        } else {
                            sqlBuilder.append(normalizedColName);
                        }
                        sqlBuilder.append(" = ?");
                        includedColumns.add(i);
                    }
                }
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    SqlAndIncludedColumns generateDelete(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLDataException {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);
        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ");
        if (settings.quoteTableName) {
            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" WHERE ");
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {

                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database");
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(" AND ");
                    }

                    String columnName;
                    if (settings.escapeColumnNames) {
                        columnName = tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString();
                    } else {
                        columnName = desc.getColumnName();
                    }
                    // Need to build a null-safe construct for the WHERE clause, since we are using PreparedStatement and won't know if the values are null. If they are null,
                    // then the filter should be "column IS null" vs "column = null". Since we don't know whether the value is null, we can use the following construct (from NIFI-3742):
                    //   (column = ? OR (column is null AND ? is null))
                    sqlBuilder.append("(");
                    sqlBuilder.append(columnName);
                    sqlBuilder.append(" = ? OR (");
                    sqlBuilder.append(columnName);
                    sqlBuilder.append(" is null AND ? is null))");
                    includedColumns.add(i);

                }
            }

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table");
            }
        }

        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    private static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return colName == null ? null : (translateColumnNames ? colName.toUpperCase().replace("_", "") : colName);
    }

    static class TableSchema {
        private List<String> requiredColumnNames;
        private Set<String> primaryKeyColumnNames;
        private Map<String, ColumnDescription> columns;
        private String quotedIdentifierString;

        private TableSchema(final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames,
                            final Set<String> primaryKeyColumnNames, final String quotedIdentifierString) {
            this.columns = new HashMap<>();
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            this.quotedIdentifierString = quotedIdentifierString;

            this.requiredColumnNames = new ArrayList<>();
            for (final ColumnDescription desc : columnDescriptions) {
                columns.put(normalizeColumnName(desc.columnName, translateColumnNames), desc);
                if (desc.isRequired()) {
                    requiredColumnNames.add(desc.columnName);
                }
            }
        }

        public Map<String, ColumnDescription> getColumns() {
            return columns;
        }

        public List<String> getRequiredColumnNames() {
            return requiredColumnNames;
        }

        public Set<String> getPrimaryKeyColumnNames() {
            return primaryKeyColumnNames;
        }

        public String getQuotedIdentifierString() {
            return quotedIdentifierString;
        }

        public static TableSchema from(final Connection conn, final String catalog, final String schema, final String tableName,
                                       final boolean translateColumnNames, final boolean includePrimaryKeys) throws SQLException {
            final DatabaseMetaData dmd = conn.getMetaData();

            try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
                final List<ColumnDescription> cols = new ArrayList<>();
                while (colrs.next()) {
                    final ColumnDescription col = ColumnDescription.from(colrs);
                    cols.add(col);
                }

                final Set<String> primaryKeyColumns = new HashSet<>();
                if (includePrimaryKeys) {
                    try (final ResultSet pkrs = dmd.getPrimaryKeys(catalog, null, tableName)) {

                        while (pkrs.next()) {
                            final String colName = pkrs.getString("COLUMN_NAME");
                            primaryKeyColumns.add(normalizeColumnName(colName, translateColumnNames));
                        }
                    }
                }

                return new TableSchema(cols, translateColumnNames, primaryKeyColumns, dmd.getIdentifierQuoteString());
            }
        }
    }

    protected static class ColumnDescription {
        private final String columnName;
        private final int dataType;
        private final boolean required;
        private final Integer columnSize;

        public ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.required = required;
            this.columnSize = columnSize;
        }

        public int getDataType() {
            return dataType;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isRequired() {
            return required;
        }

        public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
            final ResultSetMetaData md = resultSet.getMetaData();
            List<String> columns = new ArrayList<>();

            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                columns.add(md.getColumnName(i));
            }

            final String columnName = resultSet.getString("COLUMN_NAME");
            final int dataType = resultSet.getInt("DATA_TYPE");
            final int colSize = resultSet.getInt("COLUMN_SIZE");

            final String nullableValue = resultSet.getString("IS_NULLABLE");
            final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
            final String defaultValue = resultSet.getString("COLUMN_DEF");
            String autoIncrementValue = "NO";

            if (columns.contains("IS_AUTOINCREMENT")) {
                autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
            }

            final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
            final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

            return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize);
        }
    }

    static class SchemaKey {
        private final String catalog;
        private final String schemaName;
        private final String tableName;

        public SchemaKey(final String catalog, final String schemaName, final String tableName) {
            this.catalog = catalog;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int result = catalog != null ? catalog.hashCode() : 0;
            result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
            result = 31 * result + tableName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaKey schemaKey = (SchemaKey) o;

            if (catalog != null ? !catalog.equals(schemaKey.catalog) : schemaKey.catalog != null) return false;
            if (schemaName != null ? !schemaName.equals(schemaKey.schemaName) : schemaKey.schemaName != null) return false;
            return tableName.equals(schemaKey.tableName);
        }
    }

    /**
     * A holder class for a SQL prepared statement and a BitSet indicating which columns are being updated (to determine which values from the record to set on the statement)
     * A value of null for getIncludedColumns indicates that all columns/fields should be included.
     */
    static class SqlAndIncludedColumns {
        String sql;
        List<Integer> fieldIndexes;

        /**
         * Constructor
         *
         * @param sql          The prepared SQL statement (including parameters notated by ? )
         * @param fieldIndexes A List of record indexes. The index of the list is the location of the record field in the SQL prepared statement
         */
        public SqlAndIncludedColumns(String sql, List<Integer> fieldIndexes) {
            this.sql = sql;
            this.fieldIndexes = fieldIndexes;
        }

        public String getSql() {
            return sql;
        }

        public List<Integer> getFieldIndexes() {
            return fieldIndexes;
        }
    }
}
