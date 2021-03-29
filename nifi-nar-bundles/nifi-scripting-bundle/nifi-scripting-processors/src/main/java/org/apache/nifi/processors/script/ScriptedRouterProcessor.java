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
package org.apache.nifi.processors.script;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@EventDriven
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.EXECUTE_CODE,
                explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
public abstract class ScriptedRouterProcessor<T> extends ScriptedProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("The Record Reader to use parsing the incoming FlowFile into Records")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("The Record Writer to use for serializing Records after they have been transformed")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final PropertyDescriptor LANGUAGE = new PropertyDescriptor.Builder()
            .name("Script Engine")
            .displayName("Script Language")
            .description("The Language to use for the script")
            .allowableValues(SCRIPT_OPTIONS)
            .defaultValue("Groovy")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = Arrays.asList(
            RECORD_READER,
            RECORD_WRITER,
            LANGUAGE,
            ScriptingComponentUtils.SCRIPT_BODY,
            ScriptingComponentUtils.SCRIPT_FILE,
            ScriptingComponentUtils.MODULES);

    private final Class<T> scriptResultType;

    /**
     * @param scriptResultType Defines the expected result type of the user-provided script.
     */
    protected ScriptedRouterProcessor(final Class<T> scriptResultType) {
        this.scriptResultType = scriptResultType;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ScriptRunner scriptRunner = pollScriptEngine();
        if (scriptRunner == null) {
            // This shouldn't happen. But just in case.
            session.rollback();
            return;
        }

        boolean success = false;

        try {
            final ScriptEvaluator evaluator;

            try {
                final ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
                evaluator = createEvaluator(scriptEngine, flowFile);
            } catch (final ScriptException se) {
                getLogger().error("Failed to initialize script engine", se);
                session.transfer(flowFile, getFailedRelationship());
                return;
            }

            success = route(context, session, flowFile, evaluator);
        } finally {
            offerScriptEngine(scriptRunner);
        }

        session.transfer(flowFile, success ? getOriginalRelationship() : getFailedRelationship());
    }

    private boolean route(
        final ProcessContext context,
        final ProcessSession session,
        final FlowFile incomingFlowFile,
        final ScriptEvaluator evaluator
    ) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final Map<String, String> originalAttributes = incomingFlowFile.getAttributes();

        try {
            session.read(incomingFlowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (
                        final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, incomingFlowFile.getSize(), getLogger())
                    ) {
                        final RecordSchema schema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                        final RecordSet recordSet = reader.createRecordSet();
                        final PushBackRecordSet pushBackSet = new PushBackRecordSet(recordSet);
                        final Map<Relationship, List<Record>> outgoingRecords = new HashMap<>();
                        int index = 0;

                        // Reading in records and evaluate script
                        while (pushBackSet.isAnotherRecord()) {
                            final Record record = pushBackSet.next();
                            final Object evaluatedValue = evaluator.evaluate(record, index++);

                            if (evaluatedValue != null && scriptResultType.isInstance(evaluatedValue)) {
                                final Optional<Relationship> outgoingRelationship = resolveRelationship(scriptResultType.cast(evaluatedValue));

                                if (outgoingRelationship.isPresent()) {
                                    if (!outgoingRecords.containsKey(outgoingRelationship.get())) {
                                        outgoingRecords.put(outgoingRelationship.get(), new LinkedList<>());
                                    }

                                    outgoingRecords.get(outgoingRelationship.get()).add(record);
                                } else {
                                    getLogger().debug("Record with evaluated value {} has no outgoing relationship determined", String.valueOf(evaluatedValue));
                                }
                            } else {
                                throw new ProcessException("Script result is not applicable: " + String.valueOf(evaluatedValue));
                            }
                        }

                        // Creating and sending outgoing flow files
                        for (final Map.Entry<Relationship, List<Record>> entry : outgoingRecords.entrySet()) {
                            final Relationship relationship = entry.getKey();
                            final List<Record> records = entry.getValue();

                            if (records.isEmpty()) {
                                getLogger().debug("No outgoing records for relationship {}", relationship.getName());
                                continue;
                            }

                            FlowFile outgoingFlowFile = session.create(incomingFlowFile);
                            final Map<String, String> attributesToAdd = new HashMap<>();

                            try (
                                final OutputStream out = session.write(outgoingFlowFile);
                                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, outgoingFlowFile);
                            ) {
                                attributesToAdd.put("mime.type", writer.getMimeType());

                                if (records.size() == 1) {
                                    final WriteResult writeResult = writer.write(records.get(0));
                                    attributesToAdd.putAll(writeResult.getAttributes());
                                } else {
                                    final WriteResult writeResult = writer.write(new ListRecordSet(schema, records));
                                    attributesToAdd.putAll(writeResult.getAttributes());
                                }
                            }

                            session.putAllAttributes(outgoingFlowFile, attributesToAdd);
                            session.transfer(outgoingFlowFile, relationship);
                        }
                    } catch (final ScriptException | SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Failed to parse incoming FlowFile", e);
                    }
                }
            });

            return true;
        } catch (final Exception e) {
            getLogger().error("Error during routing records", e);
            return false;
        }
    }

    /**
     * @return Returns with the relationship used for route the incoming FlowFile in case of successful processing.
     */
    protected abstract Relationship getOriginalRelationship();

    /**
     * @return Returns with the relationship used for route the incoming FlowFile in case of unsuccessful processing.
     */
    protected abstract Relationship getFailedRelationship();

    /**
     * Returns a relationship based on the script's result value. As the script uses a given record as input, this helps
     * to dissolve the result value for the routing.
     *
     * @param scriptResult The value returned by the script.
     *
     * @return Returns with a relationship if there is one to determine based on the value. If it is not possible to determine
     * an {code Optional#empty} is expected. Records with empty relationship will not be routed into any relationship (expect for
     * the original or failed).
     */
    protected abstract Optional<Relationship> resolveRelationship(final T scriptResult);
}
