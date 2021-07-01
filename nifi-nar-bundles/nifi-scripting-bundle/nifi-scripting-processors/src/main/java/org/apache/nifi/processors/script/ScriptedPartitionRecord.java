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
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

@EventDriven
@SideEffectFree
@Tags({"record", "partition", "script", "groovy", "jython", "python", "segment", "split", "group", "organize"})
@CapabilityDescription("Receives Record-oriented data (i.e., data that can be read by the configured Record Reader) and evaluates the user provided script against "
        + "each record in the incoming flow file. Each record is then grouped with other records sharing the same partition and a FlowFile is created for each groups of records. " +
        "Two records shares the same partition if the evaluation of the script results the same return value for both. Those will be considered as part of the same partition.")
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.EXECUTE_CODE,
                explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "partition", description = "The partition of the outgoing flow file."),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records within the flow file."),
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer."),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the partitioned FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of partitioned FlowFiles generated from the parent FlowFile")
})
@SeeAlso(classNames = {
        "org.apache.nifi.processors.script.ScriptedTransformRecord",
        "org.apache.nifi.processors.script.ScriptedRouteRecord",
        "org.apache.nifi.processors.script.ScriptedValidateRecord",
        "org.apache.nifi.processors.script.ScriptedFilterRecord"
})
public class ScriptedPartitionRecord extends ScriptedProcessor {

    static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully partitioned will be routed to this relationship")
            .build();
    static final Relationship RELATIONSHIP_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Once all records in an incoming FlowFile have been partitioned, the original FlowFile is routed to this relationship.")
            .build();
    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be partitioned from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    private static Set<Relationship> RELATIONSHIPS = new HashSet<>();

    static {
        RELATIONSHIPS.add(RELATIONSHIP_ORIGINAL);
        RELATIONSHIPS.add(RELATIONSHIP_SUCCESS);
        RELATIONSHIPS.add(RELATIONSHIP_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
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

        final ScriptRunner scriptRunner = pollScriptRunner();
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
                session.transfer(flowFile, RELATIONSHIP_FAILURE);
                return;
            }

            success = partition(context, session, flowFile, evaluator);
        } finally {
            offerScriptRunner(scriptRunner);
        }

        session.transfer(flowFile, success ? RELATIONSHIP_ORIGINAL : RELATIONSHIP_FAILURE);
    }

    private boolean partition(
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
                        final Map<String, RecordBatchingProcessorFlowFileBuilder> recordSetFlowFileBuilders = new HashMap<>();
                        final BiFunction<FlowFile, OutputStream, RecordSetWriter> recordSetWriterFactory = (outgoingFlowFile, out) -> {
                            try {
                                return writerFactory.createWriter(getLogger(), schema, out, outgoingFlowFile);
                            } catch (final IOException | SchemaNotFoundException e) {
                                throw new ProcessException("Could not create RecordSetWriter", e);
                            }
                        };

                        int index = 0;

                        // Reading in records and evaluate script
                        while (pushBackSet.isAnotherRecord()) {
                            final Record record = pushBackSet.next();
                            final Object evaluatedValue = evaluator.evaluate(record, index++);
                            getLogger().debug("Evaluated scripted against {} (index {}), producing result of {}", record, index - 1, evaluatedValue);

                            if (evaluatedValue != null && evaluatedValue instanceof String) {
                                final String partition = (String) evaluatedValue;

                                if (!recordSetFlowFileBuilders.containsKey(partition)) {
                                    recordSetFlowFileBuilders.put(partition, new RecordBatchingProcessorFlowFileBuilder(incomingFlowFile, session, recordSetWriterFactory));
                                }

                                final int recordCount = recordSetFlowFileBuilders.get(partition).addRecord(record);
                                session.adjustCounter("Record Processed", recordCount, false);

                            } else {
                                throw new ProcessException("Script returned a value of " + evaluatedValue
                                        + " but this Processor requires that the object returned by an instance of String");
                            }
                        }

                        // Sending outgoing flow files
                        int fragmentIndex = 1;

                        for (final Map.Entry<String, RecordBatchingProcessorFlowFileBuilder> entry : recordSetFlowFileBuilders.entrySet()) {
                            final String partitionName = entry.getKey();
                            final RecordBatchingProcessorFlowFileBuilder builder = entry.getValue();

                            FlowFile outgoingFlowFile = builder.build();
                            outgoingFlowFile = session.putAttribute(outgoingFlowFile, "partition", partitionName);
                            outgoingFlowFile = session.putAttribute(outgoingFlowFile, "fragment.index", String.valueOf(fragmentIndex));
                            outgoingFlowFile = session.putAttribute(outgoingFlowFile, "fragment.count", String.valueOf(recordSetFlowFileBuilders.size()));
                            session.transfer(outgoingFlowFile, RELATIONSHIP_SUCCESS);
                            fragmentIndex++;
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
}
