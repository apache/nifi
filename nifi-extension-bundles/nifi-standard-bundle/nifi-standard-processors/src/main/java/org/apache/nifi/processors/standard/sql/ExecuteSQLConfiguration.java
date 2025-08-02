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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.AbstractExecuteSQL;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ExecuteSQLConfiguration {
    private final int queryTimeout;
    private final Integer maxRowsPerFlowFile;
    private final int outputBatchSize;
    private final Integer fetchSize;
    private final Map<String, String> inputFileAttributes;

    public ExecuteSQLConfiguration(ProcessContext context, FlowFile fileToProcess) {
        queryTimeout = context.getProperty(AbstractExecuteSQL.QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
        maxRowsPerFlowFile = context.getProperty(AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer outputBatchSizeField = context.getProperty(AbstractExecuteSQL.OUTPUT_BATCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        fetchSize = context.getProperty(AbstractExecuteSQL.FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        inputFileAttributes = fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes();
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

    public Map<String, String> getInputFileAttributes() {
        return inputFileAttributes;
    }

    public boolean isOutputBatchSizeSet() {
        return outputBatchSize > 0;
    }

    public boolean isMaxRowsPerFlowFileSet() {
        return maxRowsPerFlowFile > 0;
    }

    public boolean isFetchSizeSet() {
        return fetchSize != null && fetchSize > 0;
    }
}
