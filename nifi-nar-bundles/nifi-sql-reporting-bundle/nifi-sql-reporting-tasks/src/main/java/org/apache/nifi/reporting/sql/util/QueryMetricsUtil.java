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
package org.apache.nifi.reporting.sql.util;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.rules.PropertyContextActionHandler;
import org.apache.nifi.rules.engine.RulesEngineService;


public class QueryMetricsUtil {

    public static final PropertyDescriptor RECORD_SINK = new PropertyDescriptor.Builder()
            .name("sql-reporting-record-sink")
            .displayName("Record Destination Service")
            .description("Specifies the Controller Service to use for writing out the query result records to some destination.")
            .identifiesControllerService(RecordSinkService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("sql-reporting-query")
            .displayName("SQL Query")
            .description("SQL SELECT statement specifies which tables to query and how data should be filtered/transformed. "
                    + "SQL SELECT can select from the CONNECTION_STATUS, PROCESSOR_STATUS, BULLETINS, PROCESS_GROUP_STATUS, JVM_METRICS, CONNECTION_STATUS_PREDICTIONS, or PROVENANCE tables. "
                    + "Note that the CONNECTION_STATUS_PREDICTIONS table is not available for querying if analytics are not enabled).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(new SqlValidator())
            .build();

    public static final PropertyDescriptor INCLUDE_ZERO_RECORD_RESULTS = new PropertyDescriptor.Builder()
            .name("sql-reporting-include-zero-record-results")
            .displayName("Include Zero Record Results")
            .description("When running the SQL statement, if the result has no data, this property specifies whether or not the empty result set will be transmitted.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor RULES_ENGINE = new PropertyDescriptor.Builder()
            .name("rules-engine-service")
            .displayName("Rules Engine Service")
            .description("Specifies the Controller Service to use for applying rules to metrics.")
            .identifiesControllerService(RulesEngineService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor ACTION_HANDLER = new PropertyDescriptor.Builder()
            .name("action-handler")
            .displayName("Event Action Handler")
            .description("Handler that will execute the defined action returned from rules engine (if Action type is supported by the handler)")
            .identifiesControllerService(PropertyContextActionHandler.class)
            .required(true)
            .build();

    public static class SqlValidator implements Validator {
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
