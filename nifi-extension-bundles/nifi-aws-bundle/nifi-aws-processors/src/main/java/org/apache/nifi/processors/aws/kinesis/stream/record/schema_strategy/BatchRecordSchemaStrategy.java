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
package org.apache.nifi.processors.aws.kinesis.stream.record.schema_strategy;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class BatchRecordSchemaStrategy<State, ActionContext, CompletionException extends Throwable> {

    protected final FlowFileInitializer<State, ActionContext> flowFileInitializer;
    protected final FlowFileCompleter<State, ActionContext, CompletionException> flowFileCompleter;
    protected final Map<RecordSchema, State> flowFileStateMap = new HashMap<>();

    public BatchRecordSchemaStrategy(final FlowFileInitializer<State, ActionContext> flowFileInitializer, final FlowFileCompleter<State, ActionContext, CompletionException> flowFileCompleter) {
        this.flowFileInitializer = flowFileInitializer;
        this.flowFileCompleter = flowFileCompleter;
    }

    abstract public State getOrCreate(final Record record, final ActionContext flowFileContext) throws CompletionException, IOException, SchemaNotFoundException;

    public State create(final Record record, final ActionContext flowFileContext) throws IOException, SchemaNotFoundException {
        final State previousState = flowFileStateMap.get(record.getSchema());
        if (previousState != null) {
            throw new IllegalStateException(
                "FlowFile state already exists for schema: " + record.getSchema() + ". This should not happen in a batch processing context."
            );
        }
        final State newState = flowFileInitializer.init(record, flowFileContext);
        flowFileStateMap.put(record.getSchema(), newState);
        return newState;
    }

    public State pop() {
        final Iterator<Map.Entry<RecordSchema, State>> iterator = flowFileStateMap.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return flowFileStateMap.remove(iterator.next().getKey());
    }

    public void drop(final RecordSchema recordSchema) {
        flowFileStateMap.remove(recordSchema);
    }

    public interface FlowFileInitializer<State, ActionContext> {
        State init(Record record, ActionContext flowFileContext) throws IOException, SchemaNotFoundException;
    }

    public interface FlowFileCompleter<State, ActionContext, CompletionException extends Throwable> {
        void complete(State flowFile, ActionContext flowFileContext) throws CompletionException;
    }
}
