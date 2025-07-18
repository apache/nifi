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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.sql.ExecuteSQLConfiguration;
import org.apache.nifi.processors.standard.sql.ExecuteSQLFetchSession;
import org.apache.nifi.processors.standard.sql.ResultSetFragments;
import org.apache.nifi.processors.standard.sql.SqlWriter;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public abstract class AbstractExecuteSQL extends AbstractSessionFactoryProcessor {

    public static final String RESULT_ROW_COUNT = "executesql.row.count";
    public static final String RESULT_QUERY_DURATION = "executesql.query.duration";
    public static final String RESULT_QUERY_EXECUTION_TIME = "executesql.query.executiontime";
    public static final String RESULT_QUERY_FETCH_TIME = "executesql.query.fetchtime";
    public static final String RESULTSET_INDEX = "executesql.resultset.index";
    public static final String END_OF_RESULTSET_FLAG = "executesql.end.of.resultset";
    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";
    public static final String INPUT_FLOWFILE_UUID = "input.flowfile.uuid";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();
    protected Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SQL_PRE_QUERY = new PropertyDescriptor.Builder()
            .name("sql-pre-query")
            .displayName("SQL Pre-Query")
            .description("A semicolon-delimited list of queries executed before the main SQL query is executed. " +
                    "For example, set session properties before main query. " +
                    "It's possible to include semicolons in the statements themselves by escaping them with a backslash ('\\;'). " +
                    "Results/outputs from these queries will be suppressed if there are no errors.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
            .name("SQL Query")
            .description("The SQL query to execute. The query can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                    + "to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression "
                    + "Language is not evaluated for flow file contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SQL_POST_QUERY = new PropertyDescriptor.Builder()
            .name("sql-post-query")
            .displayName("SQL Post-Query")
            .description("A semicolon-delimited list of queries executed after the main SQL query is executed. " +
                    "Example like setting session properties after main query. " +
                    "It's possible to include semicolons in the statements themselves by escaping them with a backslash ('\\;'). " +
                    "Results/outputs from these queries will be suppressed if there are no errors.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("esql-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The fragment.count attribute will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-fetch-size")
            .displayName("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the database driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("esql-auto-commit")
            .displayName("Set Auto Commit")
            .description("Enables or disables the auto commit functionality of the DB connection. Default value is 'true'. " +
                    "The default value can be used with most of the JDBC drivers and this functionality doesn't have any impact in most of the cases " +
                    "since this processor is used to read data. " +
                    "However, for some JDBC drivers such as PostgreSQL driver, it is required to disable the auto committing functionality " +
                    "to limit the number of result rows fetching at a time. " +
                    "When auto commit is enabled, postgreSQL driver loads whole result set to memory at once. " +
                    "This could lead for a large amount of memory usage when executing queries which fetch large data sets. " +
                    "More Details of this behaviour in PostgreSQL driver can be found in https://jdbc.postgresql.org//documentation/head/query.html. ")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor CONTENT_OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Content Output Strategy")
            .description("""
                    Specifies the strategy for writing FlowFile content when processing input FlowFiles.
                    The strategy applies when handling queries that do not produce results.
                    """)
            .allowableValues(ContentOutputStrategy.class)
            .defaultValue(ContentOutputStrategy.EMPTY)
            .required(true)
            .build();

    protected List<PropertyDescriptor> propDescriptors;

    protected DBCPService dbcpService;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("SQL select query", SQL_QUERY.getName());
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(SQL_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session, sessionFactory);
            session.commitAsync();
        } catch (final Throwable t) {
            session.rollback(true);
            throw t;
        }
    }

    protected void onTrigger(final ProcessContext context, final ProcessSession session, final ProcessSessionFactory sessionFactory) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ExecuteSQLConfiguration config = new ExecuteSQLConfiguration(context, session, fileToProcess, this::getQueries);
        final ResultSetFragments resultSetFragments = new ResultSetFragments(session, config, fileToProcess, sessionFactory);

        final ComponentLog logger = getLogger();

        final SqlWriter sqlWriter = configureSqlWriter(session, context, fileToProcess);
        final Runnable fetchSession = new ExecuteSQLFetchSession(context, session, config, dbcpService, logger, sqlWriter, resultSetFragments);
        fetchSession.run();
    }

    /*
     * Extract list of queries from config property
     */
    protected List<String> getQueries(final String value) {
        if (value == null || value.isEmpty() || value.isBlank()) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split("(?<!\\\\);")) {
            query = query.replaceAll("\\\\;", ";");
            if (!query.isBlank()) {
                queries.add(query.trim());
            }
        }
        return queries;
    }

    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess);

    public enum ContentOutputStrategy implements DescribedValue {
        EMPTY(
            "Empty",
            "Overwrite the input FlowFile content with an empty result set"
        ),
        ORIGINAL(
            "Original",
            "Retain the input FlowFile content without changes"
        );

        private final String displayName;
        private final String description;

        ContentOutputStrategy(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return this.displayName;
        }

        @Override
        public String getDescription() {
            return this.description;
        }
    }
}
