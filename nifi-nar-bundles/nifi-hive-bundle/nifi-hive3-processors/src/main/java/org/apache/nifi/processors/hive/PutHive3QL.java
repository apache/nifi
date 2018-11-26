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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.Hive3DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.ErrorTypes;
import org.apache.nifi.processor.util.pattern.ExceptionHandler;
import org.apache.nifi.processor.util.pattern.ExceptionHandler.OnError;
import org.apache.nifi.processor.util.pattern.PartialFunctions.FetchFlowFiles;
import org.apache.nifi.processor.util.pattern.PartialFunctions.InitConnection;
import org.apache.nifi.processor.util.pattern.Put;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processor.util.pattern.RoutingResult;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@SeeAlso(SelectHive3QL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "hive", "put", "database", "update", "insert"})
@CapabilityDescription("Executes a HiveQL DDL/DML command (UPDATE, INSERT, e.g.). The content of an incoming FlowFile is expected to be the HiveQL command "
        + "to execute. The HiveQL command may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention hiveql.args.N.type and hiveql.args.N.value, where N is a positive integer. The hiveql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "hiveql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized HiveQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "hiveql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized HiveQL statements. The value of the Parameters are specified as "
                + "hiveql.args.1.value, hiveql.args.2.value, hiveql.args.3.value, and so on. The type of the hiveql.args.1.value Parameter is specified by the hiveql.args.1.type attribute.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "query.input.tables", description = "This attribute is written on the flow files routed to the 'success' relationships, "
                + "and contains input table names (if any) in comma delimited 'databaseName.tableName' format."),
        @WritesAttribute(attribute = "query.output.tables", description = "This attribute is written on the flow files routed to the 'success' relationships, "
                + "and contains the target table names in 'databaseName.tableName' format.")
})
public class PutHive3QL extends AbstractHive3QLProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("hive-batch-size")
            .displayName("Batch Size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor STATEMENT_DELIMITER = new PropertyDescriptor.Builder()
            .name("statement-delimiter")
            .displayName("Statement Delimiter")
            .description("Statement Delimiter used to separate SQL statements in a multiple statement script")
            .required(true)
            .defaultValue(";")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();


    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(HIVE_DBCP_SERVICE);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(QUERY_TIMEOUT);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(STATEMENT_DELIMITER);
        _propertyDescriptors.add(RollbackOnFailure.ROLLBACK_ON_FAILURE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private Put<FunctionContext, Connection> process;
    private ExceptionHandler<FunctionContext> exceptionHandler;

    @OnScheduled
    public void constructProcess() {
        exceptionHandler = new ExceptionHandler<>();
        exceptionHandler.mapException(e -> {
            if (e instanceof SQLNonTransientException) {
                return ErrorTypes.InvalidInput;
            } else if (e instanceof SQLException) {
                // Use the SQLException's vendor code for guidance -- see Hive's ErrorMsg class for details on error codes
                int errorCode = ((SQLException) e).getErrorCode();
                getLogger().debug("Error occurred during Hive operation, Hive returned error code {}", new Object[]{errorCode});
                if (errorCode >= 10000 && errorCode < 20000) {
                    return ErrorTypes.InvalidInput;
                } else if (errorCode >= 20000 && errorCode < 30000) {
                    return ErrorTypes.InvalidInput;
                } else if (errorCode >= 30000 && errorCode < 40000) {
                    return ErrorTypes.TemporalInputFailure;
                } else if (errorCode >= 40000 && errorCode < 50000) {
                    // These are unknown errors (to include some parse errors), but rather than generating an UnknownFailure which causes
                    // a ProcessException, we'll route to failure via an InvalidInput error type.
                    return ErrorTypes.InvalidInput;
                } else {
                    // Default unknown errors to TemporalFailure (as they were implemented originally), so they can be routed to failure
                    // or rolled back depending on the user's setting of Rollback On Failure.
                    return ErrorTypes.TemporalFailure;
                }
            } else {
                return ErrorTypes.UnknownFailure;
            }
        });
        exceptionHandler.adjustError(RollbackOnFailure.createAdjustError(getLogger()));

        process = new Put<>();
        process.setLogger(getLogger());
        process.initConnection(initConnection);
        process.fetchFlowFiles(fetchFlowFiles);
        process.putFlowFile(putFlowFile);
        process.adjustRoute(RollbackOnFailure.createAdjustRoute(REL_FAILURE, REL_RETRY));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private class FunctionContext extends RollbackOnFailure {
        final Charset charset;
        final String statementDelimiter;
        final long startNanos = System.nanoTime();

        String connectionUrl;


        private FunctionContext(boolean rollbackOnFailure, Charset charset, String statementDelimiter) {
            super(rollbackOnFailure, false);
            this.charset = charset;
            this.statementDelimiter = statementDelimiter;
        }
    }

    private InitConnection<FunctionContext, Connection> initConnection = (context, session, fc, ff) -> {
        final Hive3DBCPService dbcpService = context.getProperty(HIVE_DBCP_SERVICE).asControllerService(Hive3DBCPService.class);
        final Connection connection = dbcpService.getConnection();
        fc.connectionUrl = dbcpService.getConnectionURL();
        return connection;
    };

    private FetchFlowFiles<FunctionContext> fetchFlowFiles = (context, session, functionContext, result) -> {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        return session.get(batchSize);
    };

    private Put.PutFlowFile<FunctionContext, Connection> putFlowFile = (context, session, fc, conn, flowFile, result) -> {
        final String script = getHiveQL(session, flowFile, fc.charset);
        String regex = "(?<!\\\\)" + Pattern.quote(fc.statementDelimiter);

        String[] hiveQLs = script.split(regex);

        final Set<TableName> tableNames = new HashSet<>();
        exceptionHandler.execute(fc, flowFile, input -> {
            int loc = 1;
            for (String hiveQLStr: hiveQLs) {
                getLogger().debug("HiveQL: {}", new Object[]{hiveQLStr});

                final String hiveQL = hiveQLStr.trim();
                if (!StringUtils.isEmpty(hiveQL)) {
                    final PreparedStatement stmt = conn.prepareStatement(hiveQL);

                    // Get ParameterMetadata
                    // Hive JDBC Doesn't support this yet:
                    // ParameterMetaData pmd = stmt.getParameterMetaData();
                    // int paramCount = pmd.getParameterCount();
                    int paramCount = StringUtils.countMatches(hiveQL, "?");

                    if (paramCount > 0) {
                        loc = setParameters(loc, stmt, paramCount, flowFile.getAttributes());
                    }

                    // Parse hiveQL and extract input/output tables
                    try {
                        tableNames.addAll(findTableNames(hiveQL));
                    } catch (Exception e) {
                        // If failed to parse the query, just log a warning message, but continue.
                        getLogger().warn("Failed to parse hiveQL: {} due to {}", new Object[]{hiveQL, e}, e);
                    }

                    stmt.setQueryTimeout(context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(flowFile).asInteger());

                    // Execute the statement
                    stmt.execute();
                    fc.proceed();
                }
            }

            // Emit a Provenance SEND event
            final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fc.startNanos);

            final FlowFile updatedFlowFile = session.putAllAttributes(flowFile, toQueryTableAttributes(tableNames));
            session.getProvenanceReporter().send(updatedFlowFile, fc.connectionUrl, transmissionMillis, true);
            result.routeTo(flowFile, REL_SUCCESS);

        }, onFlowFileError(context, session, result));

    };

    private OnError<FunctionContext, FlowFile> onFlowFileError(final ProcessContext context, final ProcessSession session, final RoutingResult result) {
        OnError<FunctionContext, FlowFile> onFlowFileError = ExceptionHandler.createOnError(context, session, result, REL_FAILURE, REL_RETRY);
        onFlowFileError = onFlowFileError.andThen((c, i, r, e) -> {
            switch (r.destination()) {
                case Failure:
                    getLogger().error("Failed to update Hive for {} due to {}; routing to failure", new Object[] {i, e}, e);
                    break;
                case Retry:
                    getLogger().error("Failed to update Hive for {} due to {}; it is possible that retrying the operation will succeed, so routing to retry",
                            new Object[] {i, e}, e);
                    break;
            }
        });
        return RollbackOnFailure.createOnError(onFlowFileError);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final Boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final String statementDelimiter = context.getProperty(STATEMENT_DELIMITER).getValue();
        final FunctionContext functionContext = new FunctionContext(rollbackOnFailure, charset, statementDelimiter);
        RollbackOnFailure.onTrigger(context, sessionFactory, functionContext, getLogger(), session -> process.onTrigger(context, session, functionContext));
    }
}