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

package org.apache.nifi.processors.standard.sql;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public interface ExecuteSQLCommonProperties {
    PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    PropertyDescriptor SQL_PRE_QUERY = new PropertyDescriptor.Builder()
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
    PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
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
    PropertyDescriptor SQL_POST_QUERY = new PropertyDescriptor.Builder()
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
    PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();
    PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("esql-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
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
    PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-fetch-size")
            .displayName("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the database driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
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
    PropertyDescriptor CONTENT_OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Content Output Strategy")
            .description("""
                    Specifies the strategy for writing FlowFile content when processing input FlowFiles.
                    The strategy applies when handling queries that do not produce results.
                    """)
            .allowableValues(ContentOutputStrategy.class)
            .defaultValue(ContentOutputStrategy.EMPTY)
            .required(true)
            .build();
}
