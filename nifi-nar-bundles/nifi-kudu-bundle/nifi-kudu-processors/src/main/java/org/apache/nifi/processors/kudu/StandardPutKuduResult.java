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
package org.apache.nifi.processors.kudu;

import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowError;
import org.apache.nifi.flowfile.FlowFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StandardPutKuduResult extends PutKuduResult {
    private final Map<Operation, FlowFile> operationFlowFileMap;
    private final List<RowError> pendingRowErrors;
    private final Map<FlowFile, List<RowError>> flowFileRowErrorsMap;

    public StandardPutKuduResult() {
        super();
        this.operationFlowFileMap = new HashMap<>();
        this.pendingRowErrors = new ArrayList<>();
        this.flowFileRowErrorsMap = new HashMap<>();
    }

    @Override
    public void recordOperation(final Operation operation) {
        operationFlowFileMap.put(operation, flowFile);
    }

    @Override
    public void addError(final RowError rowError) {
        // When this class is used to store results from processing FlowFiles, the FlushMode
        // is set to AUTO_FLUSH_BACKGROUND or MANUAL_FLUSH. In either case, we won't know which
        // FlowFile/Record we are currently processing as the RowErrors are obtained from the KuduSession
        // post-processing of the FlowFile/Record
        this.pendingRowErrors.add(rowError);
    }

    @Override
    public void resolveFlowFileToRowErrorAssociations() {
        flowFileRowErrorsMap.putAll(pendingRowErrors.stream()
                .filter(e -> operationFlowFileMap.get(e.getOperation()) != null)
                .collect(
                        Collectors.groupingBy(e -> operationFlowFileMap.get(e.getOperation()))
                )
        );

        pendingRowErrors.clear();
    }

    @Override
    public boolean hasRowErrorsOrFailures() {
        if (!flowFileFailures.isEmpty()) {
            return true;
        }

        return flowFileRowErrorsMap.entrySet()
                .stream()
                .anyMatch(entry -> !entry.getValue().isEmpty());
    }

    @Override
    public List<RowError> getRowErrorsForFlowFile(final FlowFile flowFile) {
        return flowFileRowErrorsMap.getOrDefault(flowFile, Collections.EMPTY_LIST);
    }
}
