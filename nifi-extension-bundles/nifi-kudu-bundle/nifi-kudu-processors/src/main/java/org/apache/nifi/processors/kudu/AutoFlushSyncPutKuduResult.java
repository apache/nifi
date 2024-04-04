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

public class AutoFlushSyncPutKuduResult extends PutKuduResult {
    private final Map<FlowFile, List<RowError>> flowFileRowErrorsMap;

    public AutoFlushSyncPutKuduResult() {
        super();
        this.flowFileRowErrorsMap = new HashMap<>();
    }

    @Override
    public void recordOperation(final Operation operation) {
        // this should be a no-op because we don't need to record Operation's origins
        // for buffered flush when using AUTO_FLUSH_SYNC
        return;
    }

    @Override
    public void addError(final RowError rowError) {
        final List<RowError> rowErrors = flowFileRowErrorsMap.getOrDefault(flowFile, new ArrayList<>());
        rowErrors.add(rowError);
        flowFileRowErrorsMap.put(flowFile, rowErrors);
    }

    @Override
    public void addErrors(final List<RowError> rowErrors) {
        // This is a no-op because we would never be in a situation where we'd have to add a collection of RowError
        // using this Flush Mode. Since we do not keep Operation to FlowFile mapping, it will also be impossible to resolve
        // RowErrors to the FlowFile that caused them, hence this method should never be implemented for AUTO_FLUSH_SYNC
        return;
    }

    @Override
    public boolean hasRowErrorsOrFailures() {
        if (!flowFileFailures.isEmpty()) {
            return true;
        }

        for (final Map.Entry<FlowFile, List<RowError>> entry : flowFileRowErrorsMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public List<RowError> getRowErrorsForFlowFile(final FlowFile flowFile) {
        return flowFileRowErrorsMap.getOrDefault(flowFile, Collections.EMPTY_LIST);
    }
}
