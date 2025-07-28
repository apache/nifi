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
package org.apache.nifi.processors.aws.kinesis.stream.record.statehandlerstrategy;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;

public class RollStateHandlerStrategy<State, ActionContext, CompletionException extends Throwable> extends StateHandlerStrategy<State, ActionContext, CompletionException> {

    public RollStateHandlerStrategy(
            final StateInitializerAction<State, ActionContext> stateInitializerAction,
            final StateFinalizerAction<State, ActionContext, CompletionException> stateFinalizerAction) {
        super(stateInitializerAction, stateFinalizerAction);
    }

    @Override
    public State getOrCreate(final Record record, final ActionContext flowFileContext) throws CompletionException, IOException, SchemaNotFoundException {
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
}
