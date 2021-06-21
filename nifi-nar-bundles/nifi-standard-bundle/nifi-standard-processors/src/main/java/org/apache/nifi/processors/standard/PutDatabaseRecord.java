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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "record", "jdbc", "put", "database", "update", "insert", "delete"})
@CapabilityDescription("The PutDatabaseRecord processor uses a specified RecordReader to input (possibly multiple) records from an incoming flow file. These records are translated to SQL "
        + "statements and executed as a single transaction. If any errors occur, the flow file is routed to failure or retry, and if the records are transmitted successfully, "
        + "the incoming flow file is "
        + "routed to success.  The type of statement executed by the processor is specified via the Statement Type property, which accepts some hard-coded values such as INSERT, UPDATE, and DELETE, "
        + "as well as 'Use statement.type Attribute', which causes the processor to get the statement type from a flow file attribute.  IMPORTANT: If the Statement Type is UPDATE, then the incoming "
        + "records must not alter the value(s) of the primary keys (or user-specified Update Keys). If such records are encountered, the UPDATE statement issued to the database may do nothing "
        + "(if no existing records with the new primary key values are found), or could inadvertently corrupt the existing data (by changing records for which the new values of the primary keys "
        + "exist).")
@ReadsAttribute(attribute = PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, description = "If 'Use statement.type Attribute' is selected for the Statement Type property, the value of this attribute "
        + "will be used to determine the type of statement (INSERT, UPDATE, DELETE, SQL, etc.) to generate and execute.")
@WritesAttribute(attribute = PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR, description = "If an error occurs during processing, the flow file will be routed to failure or retry, and this attribute "
        + "will be populated with the cause of the error.")
public class PutDatabaseRecord extends AbstractProcessor {

    public static final String UPDATE_TYPE = "UPDATE";
    public static final String INSERT_TYPE = "INSERT";
    public static final String DELETE_TYPE = "DELETE";
    public static final String UPSERT_TYPE = "UPSERT";
    public static final String INSERT_IGNORE_TYPE = "INSERT_IGNORE";
    public static final String SQL_TYPE = "SQL";   // Not an allowable value in the Statement Type property, must be set by attribute
    public static final String USE_ATTR_TYPE = "Use statement.type Attribute";
    public static final String USE_RECORD_PATH = "Use Record Path";

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
    static final PropertyDescriptor RECORD_READER_FACTORY = new Builder()
            .name("put-db-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new Builder()
            .name("put-db-record-statement-type")
            .displayName("Statement Type")
            .description("Specifies the type of SQL Statement to generate. "
                    + "Please refer to the database documentation for a description of the behavior of each operation. "
                    + "Please note that some Database Types may not support certain Statement Types. "
                    + "If 'Use statement.type Attribute' is chosen, then the value is taken from the statement.type attribute in the "
                    + "FlowFile. The 'Use statement.type Attribute' option is the only one that allows the 'SQL' statement type. If 'SQL' is specified, the value of the field specified by the "
                    + "'Field Containing SQL' property is expected to be a valid SQL statement on the target database, and will be executed as-is.")
            .required(true)
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, UPSERT_TYPE, INSERT_IGNORE_TYPE, DELETE_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE_RECORD_PATH = new Builder()
        .name("Statement Type Record Path")
        .displayName("Statement Type Record Path")
        .description("Specifies a RecordPath to evaluate against each Record in order to determine the Statement Type. The RecordPath should equate to either INSERT, UPDATE, UPSERT, or DELETE.")
        .required(true)
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(NONE)
        .dependsOn(STATEMENT_TYPE, USE_RECORD_PATH)
        .build();

    static final PropertyDescriptor DATA_RECORD_PATH = new Builder()
        .name("Data Record Path")
        .displayName("Data Record Path")
        .description("If specified, this property denotes a RecordPath that will be evaluated against each incoming Record and the Record that results from evaluating the RecordPath will be sent to" +
            " the database instead of sending the entire incoming Record. If not specified, the entire incoming Record will be published to the database.")
        .required(false)
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(NONE)
        .build();

    static final PropertyDescriptor DBCP_SERVICE = new Builder()
            .name("put-db-record-dcbp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database for sending records.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new Builder()
            .name("put-db-record-catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the catalog name must match the database's catalog name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new Builder()
            .name("put-db-record-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the schema name must match the database's schema name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new Builder()
            .name("put-db-record-table-name")
            .displayName("Table Name")
            .description("The name of the table that the statement should affect. Note that if the database is case-sensitive, the table name must match the database's table name exactly.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new Builder()
            .name("put-db-record-translate-field-names")
            .displayName("Translate Field Names")
            .description("If true, the Processor will attempt to translate field names into the appropriate column names for the table specified. "
                    + "If false, the field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-field-behavior")
            .displayName("Unmatched Field Behavior")
            .description("If an incoming record has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-column-behavior")
            .displayName("Unmatched Column Behavior")
            .description("If an incoming record does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new Builder()
            .name("put-db-record-update-keys")
            .displayName("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
                    + "In this case, if no Primary Key exists, the conversion to SQL will fail if Unmatched Column Behaviour is set to FAIL. "
                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, UPDATE_TYPE, UPSERT_TYPE, SQL_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor FIELD_CONTAINING_SQL = new Builder()
            .name("put-db-record-field-containing-sql")
            .displayName("Field Containing SQL")
            .description("If the Statement Type is 'SQL' (as set in the statement.type attribute), this field indicates which field in the record(s) contains the SQL statement to execute. The value "
                    + "of the field must be a single SQL statement. If the Statement Type is not 'SQL', this field is ignored.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor ALLOW_MULTIPLE_STATEMENTS = new Builder()
            .name("put-db-record-allow-multiple-statements")
            .displayName("Allow Multiple SQL Statements")
            .description("If the Statement Type is 'SQL' (as set in the statement.type attribute), this field indicates whether to split the field value by a semicolon and execute each statement "
                    + "separately. If any statement causes an error, the entire set of statements will be rolled back. If the Statement Type is not 'SQL', this field is ignored.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor QUOTE_IDENTIFIERS = new Builder()
            .name("put-db-record-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTE_TABLE_IDENTIFIER = new Builder()
            .name("put-db-record-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the use of special characters in the table name.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new Builder()
            .name("put-db-record-query-timeout")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL statement "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new Builder()
            .name("table-schema-cache-size")
            .displayName("Table Schema Cache Size")
            .description("Specifies how many Table Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new Builder()
            .name("put-db-record-max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Specifies maximum batch size for INSERT and UPDATE statements. This parameter has no effect for other statements specified in 'Statement Type'."
                            + " Zero means the batch size is not limited.")
            .defaultValue("0")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, INSERT_TYPE, UPDATE_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor DB_TYPE;

    protected static final Map<String, DatabaseAdapter> dbAdapters;
    protected static List<PropertyDescriptor> propDescriptors;
    private Cache<SchemaKey, TableSchema> schemaCache;

    static {
        dbAdapters = new HashMap<>();
        ArrayList<AllowableValue> dbAdapterValues = new ArrayList<>();

        ServiceLoader<DatabaseAdapter> dbAdapterLoader = ServiceLoader.load(DatabaseAdapter.class);
        dbAdapterLoader.forEach(databaseAdapter -> {
            dbAdapters.put(databaseAdapter.getName(), databaseAdapter);
            dbAdapterValues.add(new AllowableValue(databaseAdapter.getName(), databaseAdapter.getName(), databaseAdapter.getDescription()));
        });

        DB_TYPE = new Builder()
            .name("db-type")
            .displayName("Database Type")
            .description("The type/flavor of database, used for generating database-specific code. In many cases the Generic type "
                + "should suffice, but some databases (such as Oracle) require custom SQL clauses. ")
            .allowableValues(dbAdapterValues.toArray(new AllowableValue[0]))
            .defaultValue("Generic")
            .required(false)
            .build();

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        r.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(RECORD_READER_FACTORY);
        pds.add(DB_TYPE);
        pds.add(STATEMENT_TYPE);
        pds.add(STATEMENT_TYPE_RECORD_PATH);
        pds.add(DATA_RECORD_PATH);
        pds.add(DBCP_SERVICE);
        pds.add(CATALOG_NAME);
        pds.add(SCHEMA_NAME);
        pds.add(TABLE_NAME);
        pds.add(TRANSLATE_FIELD_NAMES);
        pds.add(UNMATCHED_FIELD_BEHAVIOR);
        pds.add(UNMATCHED_COLUMN_BEHAVIOR);
        pds.add(UPDATE_KEYS);
        pds.add(FIELD_CONTAINING_SQL);
        pds.add(ALLOW_MULTIPLE_STATEMENTS);
        pds.add(QUOTE_IDENTIFIERS);
        pds.add(QUOTE_TABLE_IDENTIFIER);
        pds.add(QUERY_TIMEOUT);
        pds.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        pds.add(TABLE_SCHEMA_CACHE_SIZE);
        pds.add(MAX_BATCH_SIZE);

        propDescriptors = Collections.unmodifiableList(pds);
    }

    private DatabaseAdapter databaseAdapter;
    private volatile Function<Record, String> recordPathOperationType;
    private volatile RecordPath dataRecordPath;


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
        return new Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        DatabaseAdapter databaseAdapter = dbAdapters.get(validationContext.getProperty(DB_TYPE).getValue());
        String statementType = validationContext.getProperty(STATEMENT_TYPE).getValue();
        if ((UPSERT_TYPE.equals(statementType) && !databaseAdapter.supportsUpsert())
            || (INSERT_IGNORE_TYPE.equals(statementType) && !databaseAdapter.supportsInsertIgnore())) {
            validationResults.add(new ValidationResult.Builder()
                .subject(STATEMENT_TYPE.getDisplayName())
                .valid(false)
                .explanation(databaseAdapter.getName() + " does not support " + statementType)
                .build()
            );
        }

        return validationResults;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        databaseAdapter = dbAdapters.get(context.getProperty(DB_TYPE).getValue());

        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();

        final String statementTypeRecordPathValue = context.getProperty(STATEMENT_TYPE_RECORD_PATH).getValue();
        if (statementTypeRecordPathValue == null) {
            recordPathOperationType = null;
        } else {
            final RecordPath recordPath = RecordPath.compile(statementTypeRecordPathValue);
            recordPathOperationType = new RecordPathStatementType(recordPath);
        }

        final String dataRecordPathValue = context.getProperty(DATA_RECORD_PATH).getValue();
        dataRecordPath = dataRecordPathValue == null ? null : RecordPath.compile(dataRecordPathValue);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final Connection connection = dbcpService.getConnection(flowFile.getAttributes());

        boolean originalAutoCommit = false;
        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            putToDatabase(context, session, flowFile, connection);
            connection.commit();

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
        } catch (final Exception e) {
            // When an Exception is thrown, we want to route to 'retry' if we expect that attempting the same request again
            // might work. Otherwise, route to failure. SQLTransientException is a specific type that indicates that a retry may work.
            final Relationship relationship;
            final Throwable toAnalyze = (e instanceof BatchUpdateException) ? e.getCause() : e;
            if (toAnalyze instanceof SQLTransientException) {
                relationship = REL_RETRY;
                flowFile = session.penalize(flowFile);
            } else {
                relationship = REL_FAILURE;
            }

            getLogger().error("Failed to put Records to database for {}. Routing to {}.", flowFile, relationship, e);

            final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
            if (rollbackOnFailure) {
                session.rollback();
            } else {
                flowFile = session.putAttribute(flowFile, PUT_DATABASE_RECORD_ERROR, e.getMessage());
                session.transfer(flowFile, relationship);
            }

            try {
                connection.rollback();
            } catch (final Exception e1) {
                getLogger().error("Failed to rollback JDBC transaction", e1);
            }
        } finally {
            if (originalAutoCommit) {
                try {
                    connection.setAutoCommit(true);
                } catch (final Exception e) {
                    getLogger().warn("Failed to set auto-commit back to true on connection {} after finishing update", connection);
                }
            }

            try {
                connection.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close database connection", e);
            }
        }
    }


    private void executeSQL(final ProcessContext context, final FlowFile flowFile, final Connection connection, final RecordReader recordReader)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final RecordSchema recordSchema = recordReader.getSchema();

        // Find which field has the SQL statement in it
        final String sqlField = context.getProperty(FIELD_CONTAINING_SQL).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(sqlField)) {
            throw new IllegalArgumentException(format("SQL specified as Statement Type but no Field Containing SQL was found, FlowFile %s", flowFile));
        }

        boolean schemaHasSqlField = recordSchema.getFields().stream().anyMatch((field) -> sqlField.equals(field.getFieldName()));
        if (!schemaHasSqlField) {
            throw new IllegalArgumentException(format("Record schema does not contain Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
        }

        try (final Statement statement = connection.createStatement()) {
            final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            try {
                statement.setQueryTimeout(timeoutMillis); // timeout in seconds
            } catch (SQLException se) {
                // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                if (timeoutMillis > 0) {
                    throw se;
                }
            }

            Record currentRecord;
            while ((currentRecord = recordReader.nextRecord()) != null) {
                final String sql = currentRecord.getAsString(sqlField);
                if (sql == null || StringUtils.isEmpty(sql)) {
                    throw new MalformedRecordException(format("Record had no (or null) value for Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
                }

                // Execute the statement(s) as-is
                if (context.getProperty(ALLOW_MULTIPLE_STATEMENTS).asBoolean()) {
                    final String regex = "(?<!\\\\);";
                    final String[] sqlStatements = (sql).split(regex);
                    for (String sqlStatement : sqlStatements) {
                        statement.execute(sqlStatement);
                    }
                } else {
                    statement.execute(sql);
                }
            }
        }
    }


    private void executeDML(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                            final Connection con, final RecordReader recordReader, final String explicitStatementType, final DMLSettings settings)
        throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final ComponentLog log = getLogger();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(flowFile).getValue();
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
        final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        // Ensure the table name has been set, the generated SQL statements (and TableSchema cache) will need it
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(format("Cannot process %s because Table Name is null or empty", flowFile));
        }

        // Always get the primary keys if Update Keys is empty. Otherwise if we have an Insert statement first, the table will be
        // cached but the primary keys will not be retrieved, causing future UPDATE statements to not have primary keys available
        final boolean includePrimaryKeys = updateKeys == null;

        final SchemaKey schemaKey = new PutDatabaseRecord.SchemaKey(catalog, schemaName, tableName);
        final TableSchema tableSchema = schemaCache.get(schemaKey, key -> {
            try {
                final TableSchema schema = TableSchema.from(con, catalog, schemaName, tableName, settings.translateFieldNames, includePrimaryKeys, log);
                getLogger().debug("Fetched Table Schema {} for table name {}", schema, tableName);
                return schema;
            } catch (SQLException e) {
                throw new ProcessException(e);
            }
        });
        if (tableSchema == null) {
            throw new IllegalArgumentException("No table schema specified!");
        }

        // build the fully qualified table name
        final String fqTableName =  generateTableName(settings, catalog, schemaName, tableName, tableSchema);

        final Map<String, PreparedSqlAndColumns> preparedSql = new HashMap<>();
        int currentBatchSize = 0;
        int batchIndex = 0;
        Record outerRecord;
        PreparedStatement lastPreparedStatement = null;

        try {
            while ((outerRecord = recordReader.nextRecord()) != null) {
                final String statementType;
                if (USE_RECORD_PATH.equalsIgnoreCase(explicitStatementType)) {
                    statementType = recordPathOperationType.apply(outerRecord);
                } else {
                    statementType = explicitStatementType;
                }

                final List<Record> dataRecords = getDataRecords(outerRecord);
                for (final Record currentRecord : dataRecords) {
                    PreparedSqlAndColumns preparedSqlAndColumns = preparedSql.get(statementType);
                    if (preparedSqlAndColumns == null) {
                        final RecordSchema recordSchema = currentRecord.getSchema();

                        final SqlAndIncludedColumns sqlHolder;
                        if (INSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsert(recordSchema, fqTableName, tableSchema, settings);
                        } else if (UPDATE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpdate(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateDelete(recordSchema, fqTableName, tableSchema, settings);
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpsert(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else if (INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsertIgnore(recordSchema, fqTableName, updateKeys, tableSchema, settings);
                        } else {
                            throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, flowFile));
                        }

                        // Create the Prepared Statement
                        final PreparedStatement preparedStatement = con.prepareStatement(sqlHolder.getSql());

                        try {
                            preparedStatement.setQueryTimeout(timeoutMillis); // timeout in seconds
                        } catch (final SQLException se) {
                            // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                            if (timeoutMillis > 0) {
                                throw se;
                            }
                        }

                        preparedSqlAndColumns = new PreparedSqlAndColumns(sqlHolder, preparedStatement);
                        preparedSql.put(statementType, preparedSqlAndColumns);
                    }

                    final PreparedStatement ps = preparedSqlAndColumns.getPreparedStatement();
                    final List<Integer> fieldIndexes = preparedSqlAndColumns.getSqlAndIncludedColumns().getFieldIndexes();
                    final String sql = preparedSqlAndColumns.getSqlAndIncludedColumns().getSql();

                    if (currentBatchSize > 0 && ps != lastPreparedStatement && lastPreparedStatement != null) {
                        batchIndex++;
                        log.debug("Executing query {} because Statement Type changed between Records for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                            sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        lastPreparedStatement.executeBatch();

                        session.adjustCounter("Batches Executed", 1, false);
                        currentBatchSize = 0;
                    }
                    lastPreparedStatement = ps;

                    final Object[] values = currentRecord.getValues();
                    final List<DataType> dataTypes = currentRecord.getSchema().getDataTypes();
                    final RecordSchema recordSchema = currentRecord.getSchema();
                    final Map<String, ColumnDescription> columns = tableSchema.getColumns();

                    int deleteIndex = 0;
                    for (int i = 0; i < fieldIndexes.size(); i++) {
                        final int currentFieldIndex = fieldIndexes.get(i);
                        Object currentValue = values[currentFieldIndex];
                        final DataType dataType = dataTypes.get(currentFieldIndex);
                        final int fieldSqlType = DataTypeUtils.getSQLTypeValue(dataType);
                        final String fieldName = recordSchema.getField(currentFieldIndex).getFieldName();
                        String columnName = normalizeColumnName(fieldName, settings.translateFieldNames);
                        int sqlType;

                        final ColumnDescription column = columns.get(columnName);
                        // 'column' should not be null here as the fieldIndexes should correspond to fields that match table columns, but better to handle just in case
                        if (column == null) {
                            if (!settings.ignoreUnmappedFields) {
                                throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", columns.keySet()));
                            } else {
                                sqlType = fieldSqlType;
                            }
                        } else {
                            sqlType = column.dataType;
                        }

                        // Convert (if necessary) from field data type to column data type
                        if (fieldSqlType != sqlType) {
                            try {
                                DataType targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(sqlType);
                                if (targetDataType != null) {
                                    if (sqlType == Types.BLOB || sqlType == Types.BINARY) {
                                        if (currentValue instanceof Object[]) {
                                            // Convert Object[Byte] arrays to byte[]
                                            Object[] src = (Object[]) currentValue;
                                            if (src.length > 0) {
                                                if (!(src[0] instanceof Byte)) {
                                                    throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY");
                                                }
                                            }
                                            byte[] dest = new byte[src.length];
                                            for (int j = 0; j < src.length; j++) {
                                                dest[j] = (Byte) src[j];
                                            }
                                            currentValue = dest;
                                        } else if (currentValue instanceof String) {
                                            currentValue = ((String) currentValue).getBytes(StandardCharsets.UTF_8);
                                        } else if (currentValue != null && !(currentValue instanceof byte[])) {
                                            throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY");
                                        }
                                    } else {
                                        currentValue = DataTypeUtils.convertType(
                                                currentValue,
                                                targetDataType,
                                                fieldName);
                                    }
                                }
                            } catch (IllegalTypeConversionException itce) {
                                // If the field and column types don't match or the value can't otherwise be converted to the column datatype,
                                // try with the original object and field datatype
                                sqlType = DataTypeUtils.getSQLTypeValue(dataType);
                            }
                        }

                        if (sqlType == Types.DATE && currentValue instanceof Date) {
                            // convert Date from the internal UTC normalized form to local time zone needed by database drivers
                            currentValue = DataTypeUtils.convertDateToLocalTZ((Date) currentValue);
                        }

                        // If DELETE type, insert the object twice if the column is nullable because of the null check (see generateDelete for details)
                        if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            if (column.isNullable()) {
                                setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            }
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            final int timesToAddObjects = databaseAdapter.getTimesToAddColumnObjectsForUpsert();
                            for (int j = 0; j < timesToAddObjects; j++) {
                                setParameter(ps, i + (fieldIndexes.size() * j) + 1, currentValue, fieldSqlType, sqlType);
                            }
                        } else {
                            setParameter(ps, i + 1, currentValue, fieldSqlType, sqlType);
                        }
                    }

                    ps.addBatch();
                    session.adjustCounter(statementType + " updates performed", 1, false);
                    if (++currentBatchSize == maxBatchSize) {
                        batchIndex++;
                        log.debug("Executing query {} because batch reached max size for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                            sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        session.adjustCounter("Batches Executed", 1, false);
                        ps.executeBatch();
                        currentBatchSize = 0;
                    }
                }
            }

            if (currentBatchSize > 0) {
                lastPreparedStatement.executeBatch();
                session.adjustCounter("Batches Executed", 1, false);
            }
        } finally {
            for (final PreparedSqlAndColumns preparedSqlAndColumns : preparedSql.values()) {
                preparedSqlAndColumns.getPreparedStatement().close();
            }
        }
    }

    private void setParameter(PreparedStatement ps, int index, Object value, int fieldSqlType, int sqlType) throws IOException {
        if (sqlType == Types.BLOB) {
            // Convert Byte[] or String (anything that has been converted to byte[]) into BLOB
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement" , e);
                        }
                    } else {
                        throw new IOException("Expected BLOB to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
                    ps.setBlob(index, inputStream);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e.getCause());
                }
            } else {
                try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8))) {
                    ps.setBlob(index, inputStream);
                } catch (IOException | SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e.getCause());
                }
            }
        } else if (sqlType == Types.CLOB) {
            if (value == null) {
                try {
                    ps.setNull(index, Types.CLOB);
                } catch (SQLException e) {
                    throw new IOException("Unable to setNull() on prepared statement", e);
                }
            } else {
                try {
                    Clob clob = ps.getConnection().createClob();
                    clob.setString(1, value.toString());
                    ps.setClob(index, clob);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse data as CLOB/String " + value, e.getCause());
                }
            }
        } else {
            try {
                ps.setObject(index, value, sqlType);
            } catch (SQLException e) {
                throw new IOException("Unable to setObject() with value " + value + " at index " + index + " of type " + sqlType , e);
            }
        }
    }

    private List<Record> getDataRecords(final Record outerRecord) {
        if (dataRecordPath == null) {
            return Collections.singletonList(outerRecord);
        }

        final RecordPathResult result = dataRecordPath.evaluate(outerRecord);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        if (fieldValues.isEmpty()) {
            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
        }

        for (final FieldValue fieldValue : fieldValues) {
            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
            if (fieldType != RecordFieldType.RECORD) {
                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                    " " + fieldType);
            }
        }

        final List<Record> dataRecords = new ArrayList<>(fieldValues.size());
        for (final FieldValue fieldValue : fieldValues) {
            dataRecords.add((Record) fieldValue.getValue());
        }

        return dataRecords;
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }

    private String getStatementType(final ProcessContext context, final FlowFile flowFile) {
        // Get the statement type from the attribute if necessary
        final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
        String statementType = statementTypeProperty;
        if (USE_ATTR_TYPE.equals(statementTypeProperty)) {
            statementType = flowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
        }

        return validateStatementType(statementType, flowFile);
    }

    private String validateStatementType(final String statementType, final FlowFile flowFile) {
        if (statementType == null || statementType.trim().isEmpty()) {
            throw new ProcessException("No Statement Type specified for " + flowFile);
        }

        if (INSERT_TYPE.equalsIgnoreCase(statementType) || UPDATE_TYPE.equalsIgnoreCase(statementType) || DELETE_TYPE.equalsIgnoreCase(statementType)
                || UPSERT_TYPE.equalsIgnoreCase(statementType) || SQL_TYPE.equalsIgnoreCase(statementType) || USE_RECORD_PATH.equalsIgnoreCase(statementType)
                || INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {

            return statementType;
        }

        throw new ProcessException("Invalid Statement Type <" + statementType + "> for " + flowFile);
    }

    private void putToDatabase(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Connection connection) throws Exception {
        final String statementType = getStatementType(context, flowFile);

        try (final InputStream in = session.read(flowFile)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());

            if (SQL_TYPE.equalsIgnoreCase(statementType)) {
                executeSQL(context, flowFile, connection, recordReader);
            } else {
                final DMLSettings settings = new DMLSettings(context);
                executeDML(context, session, flowFile, connection, recordReader, statementType, settings);
            }
        }
    }

    private String generateTableName(final DMLSettings settings, final String catalog, final String schemaName, final String tableName, final TableSchema tableSchema) {
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(catalog)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(catalog);
            }

            tableNameBuilder.append(".");
        }

        if (schemaName != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(schemaName)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(schemaName);
            }

            tableNameBuilder.append(".");
        }

        if (settings.quoteTableName) {
            tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            tableNameBuilder.append(tableName);
        }

        return tableNameBuilder.toString();
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

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(tableName);
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
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
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
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            // complete the SQL statements by adding ?'s for all of the values to be escaped.
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append(StringUtils.repeat("?", ",", includedColumns.size()));
            sqlBuilder.append(")");

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    SqlAndIncludedColumns generateUpsert(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
        throws IllegalArgumentException, SQLException, MalformedRecordException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        Set<String> normalizedKeyColumnNames = normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames, tableSchema.getQuotedIdentifierString());

        List<String> usedColumnNames = new ArrayList<>();
        List<Integer> usedColumnIndices = new ArrayList<>();

        List<String> fieldNames = recordSchema.getFieldNames();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (settings.escapeColumnNames) {
                        usedColumnNames.add(tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString());
                    } else {
                        usedColumnNames.add(desc.getColumnName());
                    }
                    usedColumnIndices.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }
        }

        String sql = databaseAdapter.getUpsertStatement(tableName, usedColumnNames, normalizedKeyColumnNames);

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    SqlAndIncludedColumns generateInsertIgnore(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                               final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException, MalformedRecordException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings);

        Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        Set<String> normalizedKeyColumnNames = normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames, tableSchema.getQuotedIdentifierString());

        List<String> usedColumnNames = new ArrayList<>();
        List<Integer> usedColumnIndices = new ArrayList<>();

        List<String> fieldNames = recordSchema.getFieldNames();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (settings.escapeColumnNames) {
                        usedColumnNames.add(tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString());
                    } else {
                        usedColumnNames.add(desc.getColumnName());
                    }
                    usedColumnIndices.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }
        }

        String sql = databaseAdapter.getInsertIgnoreStatement(tableName, usedColumnNames, normalizedKeyColumnNames);

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    SqlAndIncludedColumns generateUpdate(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, SQLException {


        final Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        final Set<String> normalizedKeyColumnNames = normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames, tableSchema.getQuotedIdentifierString());

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(tableName);

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
                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                    } else {
                        // User is ignoring unmapped fields, but log at debug level just in case
                        getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                        continue;
                    }
                }

                // Check if this column is an Update Key. If so, skip it for now. We will come
                // back to it after we finish the SET clause
                if (!normalizedKeyColumnNames.contains(normalizedColName)) {
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
                    if (normalizedKeyColumnNames.contains(normalizedColName)) {

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
        sqlBuilder.append(tableName);

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
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
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
                    sqlBuilder.append(" = ?");

                    // Only need null check if the column is nullable, otherwise the row wouldn't exist
                    if (desc.isNullable()) {
                        sqlBuilder.append(" OR (");
                        sqlBuilder.append(columnName);
                        sqlBuilder.append(" is null AND ? is null))");
                    } else {
                        sqlBuilder.append(")");
                    }
                    includedColumns.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }

        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    private void checkValuesForRequiredColumns(RecordSchema recordSchema, TableSchema tableSchema, DMLSettings settings) {
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
    }

    private Set<String> getUpdateKeyColumnNames(String tableName, String updateKeys, TableSchema tableSchema) throws SQLIntegrityConstraintViolationException {
        final Set<String> updateKeyColumnNames;

        if (updateKeys == null) {
            updateKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        } else {
            updateKeyColumnNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyColumnNames.add(updateKey.trim());
            }
        }

        if (updateKeyColumnNames.isEmpty()) {
            throw new SQLIntegrityConstraintViolationException("Table '" + tableName + "' not found or does not have a Primary Key and no Update Keys were specified");
        }

        return updateKeyColumnNames;
    }

    private Set<String> normalizeKeyColumnNamesAndCheckForValues(RecordSchema recordSchema, String updateKeys, DMLSettings settings, Set<String> updateKeyColumnNames, final String quoteString)
            throws MalformedRecordException {
        // Create a Set of all normalized Update Key names, and ensure that there is a field in the record
        // for each of the Update Key fields.
        final Set<String> normalizedRecordFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        final Set<String> normalizedKeyColumnNames = new HashSet<>();
        for (final String updateKeyColumnName : updateKeyColumnNames) {
            String normalizedKeyColumnName = normalizeColumnName(updateKeyColumnName, settings.translateFieldNames);

            if (!normalizedRecordFieldNames.contains(normalizedKeyColumnName)) {
                String missingColMessage = "Record does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + updateKeyColumnName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
            // Optionally quote the name before returning
            if (settings.escapeColumnNames) {
                normalizedKeyColumnName = quoteString + normalizedKeyColumnName + quoteString;
            }
            normalizedKeyColumnNames.add(normalizedKeyColumnName);
        }

        return normalizedKeyColumnNames;
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
            this.columns = new LinkedHashMap<>();
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

        public List<ColumnDescription> getColumnsAsList() {
            return new ArrayList<>(columns.values());
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
                                       final boolean translateColumnNames, final boolean includePrimaryKeys, ComponentLog log) throws SQLException {
            final DatabaseMetaData dmd = conn.getMetaData();

            try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
                final List<ColumnDescription> cols = new ArrayList<>();
                while (colrs.next()) {
                    final ColumnDescription col = ColumnDescription.from(colrs);
                    cols.add(col);
                }
                // If no columns are found, check that the table exists
                if (cols.isEmpty()) {
                    try (final ResultSet tblrs = dmd.getTables(catalog, schema, tableName, null)) {
                        List<String> qualifiedNameSegments = new ArrayList<>();
                        if (catalog != null) {
                            qualifiedNameSegments.add(catalog);
                        }
                        if (schema != null) {
                            qualifiedNameSegments.add(schema);
                        }
                        if (tableName != null) {
                            qualifiedNameSegments.add(tableName);
                        }
                        if (!tblrs.next()) {

                            throw new SQLException("Table "
                                    + String.join(".", qualifiedNameSegments)
                                    + " not found, ensure the Catalog, Schema, and/or Table Names match those in the database exactly");
                        } else {
                            log.warn("Table "
                                    + String.join(".", qualifiedNameSegments)
                                    + " found but no columns were found, if this is not expected then check the user permissions for getting table metadata from the database");
                        }
                    }
                }

                final Set<String> primaryKeyColumns = new HashSet<>();
                if (includePrimaryKeys) {
                    try (final ResultSet pkrs = dmd.getPrimaryKeys(catalog, schema, tableName)) {

                        while (pkrs.next()) {
                            final String colName = pkrs.getString("COLUMN_NAME");
                            primaryKeyColumns.add(normalizeColumnName(colName, translateColumnNames));
                        }
                    }
                }

                return new TableSchema(cols, translateColumnNames, primaryKeyColumns, dmd.getIdentifierQuoteString());
            }
        }

        @Override
        public String toString() {
            return "TableSchema[columns=" + columns.values() + "]";
        }
    }

    protected static class ColumnDescription {
        private final String columnName;
        private final int dataType;
        private final boolean required;
        private final Integer columnSize;
        private final boolean nullable;

        public ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize, final boolean nullable) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.required = required;
            this.columnSize = columnSize;
            this.nullable = nullable;
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

        public boolean isNullable() {
            return nullable;
        }

        public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
            final ResultSetMetaData md = resultSet.getMetaData();
            List<String> columns = new ArrayList<>();

            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                columns.add(md.getColumnName(i));
            }
            // COLUMN_DEF must be read first to work around Oracle bug, see NIFI-4279 for details
            final String defaultValue = resultSet.getString("COLUMN_DEF");
            final String columnName = resultSet.getString("COLUMN_NAME");
            final int dataType = resultSet.getInt("DATA_TYPE");
            final int colSize = resultSet.getInt("COLUMN_SIZE");

            final String nullableValue = resultSet.getString("IS_NULLABLE");
            final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
            String autoIncrementValue = "NO";

            if (columns.contains("IS_AUTOINCREMENT")) {
                autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
            }

            final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
            final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

            return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize, isNullable);
        }

        @Override
        public String toString() {
            return "Column[name=" + columnName + ", dataType=" + dataType + ", required=" + required + ", columnSize=" + columnSize + "]";
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
        private final String sql;
        private final List<Integer> fieldIndexes;

        /**
         * Constructor
         *
         * @param sql          The prepared SQL statement (including parameters notated by ? )
         * @param fieldIndexes A List of record indexes. The index of the list is the location of the record field in the SQL prepared statement
         */
        public SqlAndIncludedColumns(final String sql, final List<Integer> fieldIndexes) {
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

    static class PreparedSqlAndColumns {
        private final SqlAndIncludedColumns sqlAndIncludedColumns;
        private final PreparedStatement preparedStatement;

        public PreparedSqlAndColumns(final SqlAndIncludedColumns sqlAndIncludedColumns, final PreparedStatement preparedStatement) {
            this.sqlAndIncludedColumns = sqlAndIncludedColumns;
            this.preparedStatement = preparedStatement;
        }

        public SqlAndIncludedColumns getSqlAndIncludedColumns() {
            return sqlAndIncludedColumns;
        }

        public PreparedStatement getPreparedStatement() {
            return preparedStatement;
        }
    }

    private static class RecordPathStatementType implements Function<Record, String> {
        private final RecordPath recordPath;

        public RecordPathStatementType(final RecordPath recordPath) {
            this.recordPath = recordPath;
        }

        @Override
        public String apply(final Record record) {
            final RecordPathResult recordPathResult = recordPath.evaluate(record);
            final List<FieldValue> resultList = recordPathResult.getSelectedFields().distinct().collect(Collectors.toList());
            if (resultList.isEmpty()) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record but got no results");
            }

            if (resultList.size() > 1) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record and received multiple distinct results (" + resultList + ")");
            }

            final String resultValue = String.valueOf(resultList.get(0).getValue()).toUpperCase();
            switch (resultValue) {
                case INSERT_TYPE:
                case UPDATE_TYPE:
                case DELETE_TYPE:
                case UPSERT_TYPE:
                    return resultValue;
            }

            throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record to determine Statement Type but found invalid value: " + resultValue);
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

            escapeColumnNames = context.getProperty(QUOTE_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTE_TABLE_IDENTIFIER).asBoolean();
        }
    }

}
