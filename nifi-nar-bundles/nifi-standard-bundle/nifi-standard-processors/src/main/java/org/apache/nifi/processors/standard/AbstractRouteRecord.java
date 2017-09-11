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

package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
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
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

public abstract class AbstractRouteRecord<T> extends AbstractProcessor {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship")
        .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Once a FlowFile has been processed and any derivative FlowFiles have been transferred, the original FlowFile will be transferred to this relationship.")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        if (isRouteOriginal()) {
            relationships.add(REL_ORIGINAL);
        }

        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final T flowFileContext;
        try {
            flowFileContext = getFlowFileContext(flowFile, context);
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; routing to failure", new Object[] {flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);


        final AtomicInteger numRecords = new AtomicInteger(0);
        final Map<Relationship, Tuple<FlowFile, RecordSetWriter>> writers = new HashMap<>();
        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = original.getAttributes();
        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, getLogger())) {

                        final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            final Set<Relationship> relationships = route(record, writeSchema, original, context, flowFileContext);
                            numRecords.incrementAndGet();

                            for (final Relationship relationship : relationships) {
                                final RecordSetWriter recordSetWriter;
                                Tuple<FlowFile, RecordSetWriter> tuple = writers.get(relationship);
                                if (tuple == null) {
                                    FlowFile outFlowFile = session.create(original);
                                    final OutputStream out = session.write(outFlowFile);
                                    recordSetWriter = writerFactory.createWriter(getLogger(), writeSchema, out);
                                    recordSetWriter.beginRecordSet();

                                    tuple = new Tuple<>(outFlowFile, recordSetWriter);
                                    writers.put(relationship, tuple);
                                } else {
                                    recordSetWriter = tuple.getValue();
                                }

                                recordSetWriter.write(record);
                            }
                        }
                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }
            });

            for (final Map.Entry<Relationship, Tuple<FlowFile, RecordSetWriter>> entry : writers.entrySet()) {
                final Relationship relationship = entry.getKey();
                final Tuple<FlowFile, RecordSetWriter> tuple = entry.getValue();
                final RecordSetWriter writer = tuple.getValue();
                FlowFile childFlowFile = tuple.getKey();

                final WriteResult writeResult = writer.finishRecordSet();

                try {
                    writer.close();
                } catch (final IOException ioe) {
                    getLogger().warn("Failed to close Writer for {}", new Object[] {childFlowFile});
                }

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());

                childFlowFile = session.putAllAttributes(childFlowFile, attributes);
                session.transfer(childFlowFile, relationship);
                session.adjustCounter("Records Processed", writeResult.getRecordCount(), false);
                session.adjustCounter("Records Routed to " + relationship.getName(), writeResult.getRecordCount(), false);

                session.getProvenanceReporter().route(childFlowFile, relationship);
            }
        } catch (final Exception e) {
            getLogger().error("Failed to process {}", new Object[] {flowFile, e});

            for (final Tuple<FlowFile, RecordSetWriter> tuple : writers.values()) {
                try {
                    tuple.getValue().close();
                } catch (final Exception e1) {
                    getLogger().warn("Failed to close Writer for {}; some resources may not be cleaned up appropriately", new Object[] {tuple.getKey()});
                }

                session.remove(tuple.getKey());
            }

            session.transfer(flowFile, REL_FAILURE);
            return;
        } finally {
            for (final Tuple<FlowFile, RecordSetWriter> tuple : writers.values()) {
                final RecordSetWriter writer = tuple.getValue();
                try {
                    writer.close();
                } catch (final Exception e) {
                    getLogger().warn("Failed to close Record Writer for {}; some resources may not be properly cleaned up", new Object[] {tuple.getKey(), e});
                }
            }
        }

        if (isRouteOriginal()) {
            flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(numRecords));
            session.transfer(flowFile, REL_ORIGINAL);
        } else {
            session.remove(flowFile);
        }

        getLogger().info("Successfully processed {}, creating {} derivative FlowFiles and processing {} records", new Object[] {flowFile, writers.size(), numRecords});
    }

    protected abstract Set<Relationship> route(Record record, RecordSchema writeSchema, FlowFile flowFile, ProcessContext context, T flowFileContext);

    protected abstract boolean isRouteOriginal();

    protected abstract T getFlowFileContext(FlowFile flowFile, ProcessContext context);
}
