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
package org.apache.nifi.processors.aws.kinesis.stream.record;

import org.apache.nifi.processors.aws.kinesis.property.SchemaDifferenceHandlingStrategy;
import org.apache.nifi.processors.aws.kinesis.stream.record.AbstractKinesisRecordProcessor.BatchProcessingContext;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorRecord.FlowFileCompletionException;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorRecord.FlowFileState;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * StateHandlerStrategy is responsible for managing the state of FlowFiles created for Records processed in a single batch. In general this class decides what should happen when a new RecordSchema
 * is encountered — whether previous State should be completed and new state created or only a new state should be created.
 */
class StateHandlerStrategy {

    private final SchemaDifferenceHandlingStrategy strategy;
    private final StateInitializerAction stateInitializerAction;
    private final StateFinalizerAction stateFinalizerAction;
    private final Map<RecordSchema, FlowFileState> activeStateMap = new HashMap<>();

    StateHandlerStrategy(final SchemaDifferenceHandlingStrategy strategy, final StateInitializerAction stateInitializerAction, final StateFinalizerAction stateFinalizerAction) {
        this.strategy = strategy;
        this.stateInitializerAction = stateInitializerAction;
        this.stateFinalizerAction = stateFinalizerAction;
    }

    FlowFileState getOrCreate(final Record record, final BatchProcessingContext flowFileContext) throws FlowFileCompletionException, IOException, SchemaNotFoundException {
        final FlowFileState previousState = activeStateMap.get(record.getSchema());
        if (previousState != null) {
            return previousState;
        }
        if (strategy == SchemaDifferenceHandlingStrategy.CREATE_FLOW_FILE) {
            // for create flow file strategy we need to complete the possible previous state before creating a new one
            completeAllAvailableFlowFileStates(flowFileContext);
        }
        return create(record, flowFileContext);
    }

    private void completeAllAvailableFlowFileStates(BatchProcessingContext flowFileContext) throws FlowFileCompletionException {
        FlowFileState previousStateForDifferentSchema;
        while ((previousStateForDifferentSchema = pop()) != null) {
            stateFinalizerAction.complete(previousStateForDifferentSchema, flowFileContext);
        }
    }

    FlowFileState create(final Record record, final BatchProcessingContext flowFileContext) throws IOException, SchemaNotFoundException {
        final FlowFileState previousState = activeStateMap.get(record.getSchema());
        if (previousState != null) {
            throw new IllegalStateException(
                "FlowFile state already exists for schema: " + record.getSchema() + ". This should not happen in a batch processing context."
            );
        }
        if (strategy == SchemaDifferenceHandlingStrategy.CREATE_FLOW_FILE && !activeStateMap.isEmpty()) {
            throw new IllegalStateException(
                "An uncompleted FlowFileState found while using SchemaDifferenceHandlingStrategy: " +
                        SchemaDifferenceHandlingStrategy.CREATE_FLOW_FILE + ". Cannot create a new state until previous is completed or dropped."
            );
        }
        final FlowFileState newState = stateInitializerAction.init(record, flowFileContext);
        activeStateMap.put(record.getSchema(), newState);
        return newState;
    }

    FlowFileState pop() {
        final Iterator<Map.Entry<RecordSchema, FlowFileState>> iterator = activeStateMap.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return activeStateMap.remove(iterator.next().getKey());
    }

    void drop(final RecordSchema recordSchema) {
        activeStateMap.remove(recordSchema);
    }

    interface StateInitializerAction {
        FlowFileState init(Record record, BatchProcessingContext flowFileContext) throws IOException, SchemaNotFoundException;
    }

    interface StateFinalizerAction {
        void complete(FlowFileState flowFile, BatchProcessingContext flowFileContext) throws FlowFileCompletionException;
    }
}
