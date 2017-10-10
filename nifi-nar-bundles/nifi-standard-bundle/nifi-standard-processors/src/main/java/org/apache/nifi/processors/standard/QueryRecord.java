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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.queryrecord.FlowFileTable;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.StopWatch;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"sql", "query", "calcite", "route", "record", "transform", "select", "update", "modify", "etl", "filter", "record", "csv", "json", "logs", "text", "avro", "aggregate"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates one or more SQL queries against the contents of a FlowFile. The result of the "
    + "SQL query then becomes the content of the output FlowFile. This can be used, for example, "
    + "for field-specific filtering, transformation, and row-level filtering. "
    + "Columns can be renamed, simple calculations and aggregations performed, etc. "
    + "The Processor is configured with a Record Reader Controller Service and a Record Writer service so as to allow flexibility in incoming and outgoing data formats. "
    + "The Processor must be configured with at least one user-defined property. The name of the Property "
    + "is the Relationship to route data to, and the value of the Property is a SQL SELECT statement that is used to specify how input data should be transformed/filtered. "
    + "The SQL statement must be valid ANSI SQL and is powered by Apache Calcite. "
    + "If the transformation fails, the original FlowFile is routed to the 'failure' relationship. Otherwise, the data selected will be routed to the associated "
    + "relationship. If the Record Writer chooses to inherit the schema from the Record, it is important to note that the schema that is inherited will be from the "
    + "ResultSet, rather than the input Record. This allows a single instance of the QueryRecord processor to have multiple queries, each of which returns a different "
    + "set of columns and aggregations. As a result, though, the schema that is derived will have no schema name, so it is important that the configured Record Writer not attempt "
    + "to write the Schema Name as an attribute if inheriting the Schema from the Record. See the Processor Usage documentation for more information.")
@DynamicRelationship(name="<Property Name>", description="Each user-defined property defines a new Relationship for this Processor.")
@DynamicProperty(name = "The name of the relationship to route data to", value="A SQL SELECT statement that is used to determine what data should be routed to this "
        + "relationship.", supportsExpressionLanguage=true, description="Each user-defined property specifies a SQL SELECT statement to run over the data, with the data "
        + "that is selected being routed to the relationship whose name is the property name")
public class QueryRecord extends AbstractProcessor {
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing results to a FlowFile")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
        .name("include-zero-record-flowfiles")
        .displayName("Include Zero Record FlowFiles")
        .description("When running the SQL statement against an incoming FlowFile, if the result has no data, "
            + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    static final PropertyDescriptor CACHE_SCHEMA = new PropertyDescriptor.Builder()
        .name("cache-schema")
        .displayName("Cache Schema")
        .description("Parsing the SQL query and deriving the FlowFile's schema is relatively expensive. If this value is set to true, "
            + "the Processor will cache these values so that the Processor is much more efficient and much faster. However, if this is done, "
            + "then the schema that is derived for the first FlowFile processed must apply to all FlowFiles. If all FlowFiles will not have the exact "
            + "same schema, or if the SQL SELECT statement uses the Expression Language, this value should be set to false.")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original FlowFile is routed to this relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile fails processing for any reason (for example, the SQL "
            + "statement contains columns not present in input data), the original FlowFile it will "
            + "be routed to this relationship")
        .build();

    private List<PropertyDescriptor> properties;
    private final Set<Relationship> relationships = Collections.synchronizedSet(new HashSet<>());

    private final Map<String, BlockingQueue<CachedStatement>> statementQueues = new HashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        try {
            DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        } catch (final SQLException e) {
            throw new ProcessException("Failed to load Calcite JDBC Driver", e);
        }

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER_FACTORY);
        properties.add(RECORD_WRITER_FACTORY);
        properties.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        properties.add(CACHE_SCHEMA);
        this.properties = Collections.unmodifiableList(properties);

        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        final Relationship relationship = new Relationship.Builder()
            .name(descriptor.getName())
            .description("User-defined relationship that specifies where data that matches the specified SQL query should be routed")
            .build();

        if (newValue == null) {
            relationships.remove(relationship);
        } else {
            relationships.add(relationship);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean cache = validationContext.getProperty(CACHE_SCHEMA).asBoolean();
        if (cache) {
            for (final PropertyDescriptor descriptor : validationContext.getProperties().keySet()) {
                if (descriptor.isDynamic() && validationContext.isExpressionLanguagePresent(validationContext.getProperty(descriptor).getValue())) {
                    return Collections.singleton(new ValidationResult.Builder()
                        .subject("Cache Schema")
                        .input("true")
                        .valid(false)
                        .explanation("Cannot have 'Cache Schema' property set to true if any SQL statement makes use of the Expression Language")
                        .build());
                }
            }
        }

        return Collections.emptyList();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("SQL select statement specifies how data should be filtered/transformed. "
                + "SQL SELECT should select from the FLOWFILE table")
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(true)
            .addValidator(new SqlValidator())
            .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);

        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        final Map<FlowFile, Relationship> transformedFlowFiles = new HashMap<>();
        final Set<FlowFile> createdFlowFiles = new HashSet<>();

        // Determine the schema for writing the data
        final Map<String, String> originalAttributes = original.getAttributes();
        int recordsRead = 0;

        try {
            for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic()) {
                    continue;
                }

                final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();

                // We have to fork a child because we may need to read the input FlowFile more than once,
                // and we cannot call session.read() on the original FlowFile while we are within a write
                // callback for the original FlowFile.
                FlowFile transformed = session.create(original);
                boolean flowFileRemoved = false;

                try {
                    final String sql = context.getProperty(descriptor).evaluateAttributeExpressions(original).getValue();
                    final AtomicReference<WriteResult> writeResultRef = new AtomicReference<>();
                    final QueryResult queryResult;
                    if (context.getProperty(CACHE_SCHEMA).asBoolean()) {
                        queryResult = queryWithCache(session, original, sql, context, recordReaderFactory);
                    } else {
                        queryResult = query(session, original, sql, context, recordReaderFactory);
                    }

                    final AtomicReference<String> mimeTypeRef = new AtomicReference<>();
                    try {
                        final ResultSet rs = queryResult.getResultSet();
                        transformed = session.write(transformed, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                final ResultSetRecordSet recordSet;
                                final RecordSchema writeSchema;

                                try {
                                    recordSet = new ResultSetRecordSet(rs);
                                    final RecordSchema resultSetSchema = recordSet.getSchema();
                                    writeSchema = recordSetWriterFactory.getSchema(originalAttributes, resultSetSchema);
                                } catch (final SQLException | SchemaNotFoundException e) {
                                    throw new ProcessException(e);
                                }

                                try (final RecordSetWriter resultSetWriter = recordSetWriterFactory.createWriter(getLogger(), writeSchema, out)) {
                                    writeResultRef.set(resultSetWriter.write(recordSet));
                                    mimeTypeRef.set(resultSetWriter.getMimeType());
                                } catch (final Exception e) {
                                    throw new IOException(e);
                                }
                            }
                        });
                    } finally {
                        closeQuietly(queryResult);
                    }

                    recordsRead = Math.max(recordsRead, queryResult.getRecordsRead());
                    final WriteResult result = writeResultRef.get();
                    if (result.getRecordCount() == 0 && !context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean()) {
                        session.remove(transformed);
                        flowFileRemoved = true;
                        transformedFlowFiles.remove(transformed);
                        getLogger().info("Transformed {} but the result contained no data so will not pass on a FlowFile", new Object[] {original});
                    } else {
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        if (result.getAttributes() != null) {
                            attributesToAdd.putAll(result.getAttributes());
                        }

                        attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());
                        attributesToAdd.put("record.count", String.valueOf(result.getRecordCount()));
                        transformed = session.putAllAttributes(transformed, attributesToAdd);
                        transformedFlowFiles.put(transformed, relationship);

                        session.adjustCounter("Records Written", result.getRecordCount(), false);
                    }
                } finally {
                    // Ensure that we have the FlowFile in the set in case we throw any Exception
                    if (!flowFileRemoved) {
                        createdFlowFiles.add(transformed);
                    }
                }
            }

            final long elapsedMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            if (transformedFlowFiles.size() > 0) {
                session.getProvenanceReporter().fork(original, transformedFlowFiles.keySet(), elapsedMillis);

                for (final Map.Entry<FlowFile, Relationship> entry : transformedFlowFiles.entrySet()) {
                    final FlowFile transformed = entry.getKey();
                    final Relationship relationship = entry.getValue();

                    session.getProvenanceReporter().route(transformed, relationship);
                    session.transfer(transformed, relationship);
                }
            }

            getLogger().info("Successfully queried {} in {} millis", new Object[] {original, elapsedMillis});
            session.transfer(original, REL_ORIGINAL);
        } catch (final SQLException e) {
            getLogger().error("Unable to query {} due to {}", new Object[] {original, e.getCause() == null ? e : e.getCause()});
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("Unable to query {} due to {}", new Object[] {original, e});
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        }

        session.adjustCounter("Records Read", recordsRead, false);
    }


    private synchronized CachedStatement getStatement(final String sql, final Supplier<CalciteConnection> connectionSupplier, final ProcessSession session,
        final FlowFile flowFile, final RecordReaderFactory recordReaderFactory) throws SQLException {

        final BlockingQueue<CachedStatement> statementQueue = statementQueues.get(sql);
        if (statementQueue == null) {
            return buildCachedStatement(sql, connectionSupplier, session, flowFile, recordReaderFactory);
        }

        final CachedStatement cachedStmt = statementQueue.poll();
        if (cachedStmt != null) {
            return cachedStmt;
        }

        return buildCachedStatement(sql, connectionSupplier, session, flowFile, recordReaderFactory);
    }

    private CachedStatement buildCachedStatement(final String sql, final Supplier<CalciteConnection> connectionSupplier, final ProcessSession session,
        final FlowFile flowFile, final RecordReaderFactory recordReaderFactory) throws SQLException {

        final CalciteConnection connection = connectionSupplier.get();
        final SchemaPlus rootSchema = connection.getRootSchema();

        final FlowFileTable<?, ?> flowFileTable = new FlowFileTable<>(session, flowFile, recordReaderFactory, getLogger());
        rootSchema.add("FLOWFILE", flowFileTable);
        rootSchema.setCacheEnabled(false);

        final PreparedStatement stmt = connection.prepareStatement(sql);
        return new CachedStatement(stmt, flowFileTable, connection);
    }

    @OnStopped
    public synchronized void cleanup() {
        for (final BlockingQueue<CachedStatement> statementQueue : statementQueues.values()) {
            CachedStatement stmt;
            while ((stmt = statementQueue.poll()) != null) {
                closeQuietly(stmt.getStatement(), stmt.getConnection());
            }
        }

        statementQueues.clear();
    }

    @OnScheduled
    public synchronized void setupQueues(final ProcessContext context) {
        // Create a Queue of PreparedStatements for each property that is user-defined. This allows us to easily poll the
        // queue and add as necessary, knowing that the queue already exists.
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }

            final String sql = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
            final BlockingQueue<CachedStatement> queue = new LinkedBlockingQueue<>(context.getMaxConcurrentTasks());
            statementQueues.put(sql, queue);
        }
    }

    protected QueryResult queryWithCache(final ProcessSession session, final FlowFile flowFile, final String sql, final ProcessContext context,
        final RecordReaderFactory recordParserFactory) throws SQLException {

        final Supplier<CalciteConnection> connectionSupplier = () -> {
            final Properties properties = new Properties();
            properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

            try {
                final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
                final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
                return calciteConnection;
            } catch (final Exception e) {
                throw new ProcessException(e);
            }
        };

        final CachedStatement cachedStatement = getStatement(sql, connectionSupplier, session, flowFile, recordParserFactory);
        final PreparedStatement stmt = cachedStatement.getStatement();
        final FlowFileTable<?, ?> table = cachedStatement.getTable();
        table.setFlowFile(session, flowFile);

        final ResultSet rs = stmt.executeQuery();

        return new QueryResult() {
            @Override
            public void close() throws IOException {
                final BlockingQueue<CachedStatement> statementQueue = statementQueues.get(sql);
                if (statementQueue == null || !statementQueue.offer(cachedStatement)) {
                    try {
                        cachedStatement.getConnection().close();
                    } catch (SQLException e) {
                        throw new IOException("Failed to close statement", e);
                    }
                }
            }

            @Override
            public ResultSet getResultSet() {
                return rs;
            }

            @Override
            public int getRecordsRead() {
                return table.getRecordsRead();
            }

        };
    }

    protected QueryResult query(final ProcessSession session, final FlowFile flowFile, final String sql, final ProcessContext context,
        final RecordReaderFactory recordParserFactory) throws SQLException {

        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

        Connection connection = null;
        ResultSet resultSet = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection("jdbc:calcite:", properties);
            final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            final SchemaPlus rootSchema = calciteConnection.getRootSchema();

            final FlowFileTable<?, ?> flowFileTable = new FlowFileTable<>(session, flowFile, recordParserFactory, getLogger());
            rootSchema.add("FLOWFILE", flowFileTable);
            rootSchema.setCacheEnabled(false);

            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

            final ResultSet rs = resultSet;
            final Statement stmt = statement;
            final Connection conn = connection;
            return new QueryResult() {
                @Override
                public void close() throws IOException {
                    closeQuietly(rs, stmt, conn);
                }

                @Override
                public ResultSet getResultSet() {
                    return rs;
                }

                @Override
                public int getRecordsRead() {
                    return flowFileTable.getRecordsRead();
                }
            };
        } catch (final Exception e) {
            closeQuietly(resultSet, statement, connection);
            throw e;
        }
    }

    private void closeQuietly(final AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }

        for (final AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close SQL resource", e);
            }
        }
    }

    private static class SqlValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(true)
                    .explanation("Expression Language Present")
                    .build();
            }

            final String substituted = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();

            final Config config = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .build();

            final SqlParser parser = SqlParser.create(substituted, config);
            try {
                parser.parseStmt();
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .build();
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Not a valid SQL Statement: " + e.getMessage())
                    .build();
            }
        }
    }

    private static interface QueryResult extends Closeable {
        ResultSet getResultSet();

        int getRecordsRead();
    }

    private static class CachedStatement {
        private final FlowFileTable<?, ?> table;
        private final PreparedStatement statement;
        private final Connection connection;

        public CachedStatement(final PreparedStatement statement, final FlowFileTable<?, ?> table, final Connection connection) {
            this.statement = statement;
            this.table = table;
            this.connection = connection;
        }

        public FlowFileTable<?, ?> getTable() {
            return table;
        }

        public PreparedStatement getStatement() {
            return statement;
        }

        public Connection getConnection() {
            return connection;
        }
    }
}
