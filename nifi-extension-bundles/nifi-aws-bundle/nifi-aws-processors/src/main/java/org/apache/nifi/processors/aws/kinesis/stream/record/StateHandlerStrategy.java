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
 *
 * @param <State> the state type that is being managed
 * @param <ActionContext> the context type that provides additional information for state initialization and finalization
 * @param <FinalizeException> the type of exception that can be thrown during state finalization
 */
class StateHandlerStrategy<State, ActionContext, FinalizeException extends Throwable> {

    private final SchemaDifferenceHandlingStrategy strategy;
    private final StateInitializerAction<State, ActionContext> stateInitializerAction;
    private final StateFinalizerAction<State, ActionContext, FinalizeException> stateFinalizerAction;
    private final Map<RecordSchema, State> activeStateMap = new HashMap<>();

    StateHandlerStrategy(final SchemaDifferenceHandlingStrategy strategy, final StateInitializerAction<State, ActionContext> stateInitializerAction, final StateFinalizerAction<State, ActionContext, FinalizeException> stateFinalizerAction) {
        this.strategy = strategy;
        this.stateInitializerAction = stateInitializerAction;
        this.stateFinalizerAction = stateFinalizerAction;
    }

    /**
     *
     * @param record
     * @param flowFileContext
     * @return
     * @throws FinalizeException
     * @throws IOException
     * @throws SchemaNotFoundException
     */
    public State getOrCreate(final Record record, final ActionContext flowFileContext) throws FinalizeException, IOException, SchemaNotFoundException {
        return switch (strategy) {
            case ROLL_FLOW_FILES -> getOrFinalizeAndCreateNewState(record, flowFileContext);
            case GROUP_FLOW_FILES -> getOrCreateNewState(record, flowFileContext);
        };
    }

    private State getOrFinalizeAndCreateNewState(final Record record, final ActionContext flowFileContext) throws FinalizeException, IOException, SchemaNotFoundException {
        final State previousState = activeStateMap.get(record.getSchema());
        if (previousState != null) {
            return previousState;
        }
        final State previousStateForDifferentSchema = pop();
        if (previousStateForDifferentSchema != null) {
            stateFinalizerAction.complete(previousStateForDifferentSchema, flowFileContext);
        }
        final State newState = stateInitializerAction.init(record, flowFileContext);
        activeStateMap.put(record.getSchema(), newState);
        return newState;
    }

    private State getOrCreateNewState(final Record record, final ActionContext flowFileContext) throws IOException, SchemaNotFoundException {
        final State previousState = activeStateMap.get(record.getSchema());
        if (previousState != null) {
            return previousState;
        }
        final State newState = stateInitializerAction.init(record, flowFileContext);
        activeStateMap.put(record.getSchema(), newState);
        return newState;
    }

    public State create(final Record record, final ActionContext flowFileContext) throws IOException, SchemaNotFoundException {
        final State previousState = activeStateMap.get(record.getSchema());
        if (previousState != null) {
            throw new IllegalStateException(
                "FlowFile state already exists for schema: " + record.getSchema() + ". This should not happen in a batch processing context."
            );
        }
        final State newState = stateInitializerAction.init(record, flowFileContext);
        activeStateMap.put(record.getSchema(), newState);
        return newState;
    }

    public State pop() {
        final Iterator<Map.Entry<RecordSchema, State>> iterator = activeStateMap.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return activeStateMap.remove(iterator.next().getKey());
    }

    public void drop(final RecordSchema recordSchema) {
        activeStateMap.remove(recordSchema);
    }

    public interface StateInitializerAction<State, ActionContext> {
        State init(Record record, ActionContext flowFileContext) throws IOException, SchemaNotFoundException;
    }

    public interface StateFinalizerAction<State, ActionContext, CompletionException extends Throwable> {
        void complete(State flowFile, ActionContext flowFileContext) throws CompletionException;
    }
}
