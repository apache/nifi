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
import org.apache.nifi.serialization.record.Record;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class PutKuduResult {
    protected FlowFile flowFile;
    protected final Map<FlowFile, Object> flowFileFailures;
    private final Set<FlowFile> processedFlowFiles;
    private final Map<FlowFile, Integer> processedRecords;

    public PutKuduResult() {
        this.flowFile = null;

        this.flowFileFailures = new HashMap<>();
        this.processedFlowFiles = new HashSet<>();
        this.processedRecords = new HashMap<>();
    }

    public void setFlowFile(final FlowFile flowFile) {
        this.flowFile = flowFile;
        processedFlowFiles.add(flowFile);
    }

    public Set<FlowFile> getProcessedFlowFiles() {
        return this.processedFlowFiles;
    }

    public int getProcessedRecordsForFlowFile(final FlowFile flowFile) {
        return this.processedRecords.getOrDefault(flowFile, 0);
    }

    /**
     * Increments the number of {@link Record}s that has been successfully processed for this {@link FlowFile}
     */
    public void incrementProcessedRecordsForFlowFile() {
        final int newCount = this.processedRecords.getOrDefault(flowFile, 0) + 1;
        this.processedRecords.put(flowFile, newCount);
    }

    /**
     * Records an {@link Operation} being processed for a specific {@link FlowFile}
     * @param operation the {@link Operation} to record
     */
    public abstract void recordOperation(final Operation operation);

    /**
     * Records a {@link RowError} for the particular {@link FlowFile} that's being processed
     * @param rowError the {@link RowError} to add
     */
    public abstract void addError(final RowError rowError);

    /**
     * Records a {@link List} of {@link RowError}s for the particular {@link FlowFile} that's being processed
     * @param rowErrors the {@link List} of {@link RowError}s to add
     */
    public void addErrors(final List<RowError> rowErrors) {
        for (final RowError rowError : rowErrors) {
            addError(rowError);
        }
    }

    /**
     * Records a failure (an {@link Exception} or a {@link RowError}) for the particular {@link FlowFile} that's being processed.
     * A failure is defined as anything that stops the processing of the records in a {@link FlowFile}
     * @param failure the {@link Exception} or {@link RowError} to add
     */
    public void addFailure(final Object failure) {
        if (flowFileFailures.containsKey(flowFile)) {
            throw new IllegalStateException("A failure has already previously occurred while processing FlowFile.");
        }
        flowFileFailures.put(flowFile, failure);
    }


    /**
     * Resolves the associations between {@link FlowFile} and the {@link RowError}s that occurred
     * while processing them. This is only applicable in batch sesssion flushes, namely when
     * using the {@code SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND} and
     * {@code SessionConfiguration.FlushMode.MANUAL_FLUSH} flush modes. Otherwise, this
     * function should be a no-op. This function should only be called once finished with processing
     * all {@link FlowFile}s in a batch.
     */
    public void resolveFlowFileToRowErrorAssociations() {
        return;
    }

    /**
     * Checks whether there was a failure (i.e. either an {@link Exception} or {@link RowError} that happened during processing)
     * @return {@code true} if there was a {@link Exception} or a {@link RowError} that happened during processing, {@code false} otherwise
     */
    public abstract boolean hasRowErrorsOrFailures();

    /**
     * Checks whether the {@link FlowFile} was processed successfully (i.e. no {@link Exception}s or
     * {@link RowError}s occurred while processing the {@link FlowFile}).
     *
     * @param flowFile {@link FlowFile} to check
     * @return {@code true} if the processing the {@link FlowFile} did not incur any exceptions, {@code false} otherwise
     */
    public boolean isFlowFileProcessedSuccessfully(final FlowFile flowFile) {
        return !flowFileFailures.containsKey(flowFile);
    }

    /**
     * Returns the failure ({@link Exception} or {@link RowError}) that occurred while processing the {@link FlowFile}
     * @param flowFile the {@link FlowFile} to check
     * @return the {@link Exception} or {@link RowError} if one occurred while processing the given {@link FlowFile} or {@code null}
     */
    public Object getFailureForFlowFile(final FlowFile flowFile) {
        return flowFileFailures.get(flowFile);
    }

    /**
     * Retrieves the {@link RowError}s that have occurred when processing a {@link FlowFile}
     * @param flowFile the {@link FlowFile} to retrieve the {@link RowError}s of
     * @return a {@link List} of {@link RowError}s for the {@link FlowFile} or an {@code Collections.EMPTY_LIST} if no errors
     */
    public abstract List<RowError> getRowErrorsForFlowFile(final FlowFile flowFile);
}