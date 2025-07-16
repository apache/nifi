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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ExecuteSQLConfiguration {
    private final int queryTimeout;

    private final Integer maxRowsPerFlowFile;
    private final int outputBatchSize;
    private final Integer fetchSize;

    private final List<String> preQueries;
    private final List<String> postQueries;

    private final boolean isOutputBatchSizeSet;
    private final boolean isMaxRowsPerFlowFileSet;

    private final String selectQuery;
    private final Map<String, String> connectionAttributes;

    public ExecuteSQLConfiguration(ProcessContext context, ProcessSession session, FlowFile fileToProcess, Function<String, List<String>> getQueries) {
        queryTimeout = context.getProperty(ExecuteSQLCommonProperties.QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
        maxRowsPerFlowFile = context.getProperty(ExecuteSQLCommonProperties.MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer outputBatchSizeField = context.getProperty(ExecuteSQLCommonProperties.OUTPUT_BATCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        fetchSize = context.getProperty(ExecuteSQLCommonProperties.FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();

        preQueries = getQueries.apply(context.getProperty(ExecuteSQLCommonProperties.SQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        postQueries = getQueries.apply(context.getProperty(ExecuteSQLCommonProperties.SQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        isOutputBatchSizeSet = outputBatchSize > 0;
        isMaxRowsPerFlowFileSet = maxRowsPerFlowFile > 0;

        selectQuery = readSelectQuery(context, session, fileToProcess);

        connectionAttributes = fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes();
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public Integer getMaxRowsPerFlowFile() {
        return maxRowsPerFlowFile;
    }

    public int getOutputBatchSize() {
        return outputBatchSize;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public List<String> getPreQueries() {
        return preQueries;
    }

    public List<String> getPostQueries() {
        return postQueries;
    }

    public boolean isOutputBatchSizeSet() {
        return isOutputBatchSizeSet;
    }

    public boolean isMaxRowsPerFlowFileSet() {
        return isMaxRowsPerFlowFileSet;
    }

    public String getSelectQuery() {
        return selectQuery;
    }

    public Map<String, String> getConnectionAttributes() {
        return connectionAttributes;
    }

    private String readSelectQuery(ProcessContext context, ProcessSession session, FlowFile fileToProcess) {
        String selectQuery;
        if (context.getProperty(ExecuteSQLCommonProperties.SQL_QUERY).isSet()) {
            selectQuery = context.getProperty(ExecuteSQLCommonProperties.SQL_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, Charset.defaultCharset())));
            selectQuery = queryContents.toString();
        }
        return selectQuery;
    }
}
