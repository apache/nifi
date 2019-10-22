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
package org.apache.nifi.reporting.sql;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.StopWatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Tags({"status", "connection", "processor", "jvm", "metrics", "history", "bulletin", "prediction", "process", "group", "record", "sql"})
@CapabilityDescription("Publishes NiFi status information based on the results of a user-specified SQL query. The query may make use of the CONNECTION_STATUS, PROCESSOR_STATUS, "
        + "BULLETINS, PROCESS_GROUP_STATUS, JVM_METRICS, or CONNECTION_STATUS_PREDICTIONS tables, and can use any functions or capabilities provided by Apache Calcite. Note that the "
        + "CONNECTION_STATUS_PREDICTIONS table is not available for querying if analytics are not enabled (see the nifi.analytics.predict.enabled property in nifi.properties). Attempting a "
        + "query on the table when the capability is disabled will cause an error.")
public class QueryNiFiReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor RECORD_SINK = new PropertyDescriptor.Builder()
            .name("sql-reporting-record-sink")
            .displayName("Record Destination Service")
            .description("Specifies the Controller Service to use for writing out the query result records to some destination.")
            .identifiesControllerService(RecordSinkService.class)
            .required(true)
            .build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("sql-reporting-query")
            .displayName("SQL Query")
            .description("SQL SELECT statement specifies which tables to query and how data should be filtered/transformed. "
                    + "SQL SELECT can select from the CONNECTION_STATUS, PROCESSOR_STATUS, BULLETINS, PROCESS_GROUP_STATUS, JVM_METRICS, or CONNECTION_STATUS_PREDICTIONS tables. Note that the "
                    + "CONNECTION_STATUS_PREDICTIONS table is not available for querying if analytics are not enabled).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new SqlValidator())
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_RESULTS = new PropertyDescriptor.Builder()
            .name("sql-reporting-include-zero-record-results")
            .displayName("Include Zero Record Results")
            .description("When running the SQL statement, if the result has no data, this property specifies whether or not the empty result set will be transmitted.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;
    private MetricsQueryService metricsQueryService;

    @Override
    protected void init(final ReportingInitializationContext config) {
        metricsQueryService = new MetricsSqlQueryService(getLogger());
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(QUERY);
        properties.add(RECORD_SINK);
        properties.add(INCLUDE_ZERO_RECORD_RESULTS);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final StopWatch stopWatch = new StopWatch(true);
        try {
            final RecordSinkService recordSinkService = context.getProperty(RECORD_SINK).asControllerService(RecordSinkService.class);
            final String sql = context.getProperty(QUERY).evaluateAttributeExpressions().getValue();
            final QueryResult queryResult = metricsQueryService.query(context, sql);
            final ResultSetRecordSet recordSet;

            try {
                getLogger().debug("Executing query: {}", new Object[]{sql});
                recordSet = metricsQueryService.getResultSetRecordSet(queryResult);
            } catch (final Exception e) {
                getLogger().error("Error creating record set from query results due to {}", new Object[]{e.getMessage()}, e);
                return;
            }

            try {
                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());
                recordSinkService.sendData(recordSet, attributes, context.getProperty(INCLUDE_ZERO_RECORD_RESULTS).asBoolean());
            } catch (Exception e) {
                getLogger().error("Error during transmission of query results due to {}", new Object[]{e.getMessage()}, e);
                return;
            } finally {
                metricsQueryService.closeQuietly(queryResult);
            }
            final long elapsedMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            getLogger().debug("Successfully queried and sent in {} millis", new Object[]{elapsedMillis});
        } catch (Exception e) {
            getLogger().error("Error processing the query due to {}", new Object[]{e.getMessage()}, e);
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

            final SqlParser.Config config = SqlParser.configBuilder()
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

}
