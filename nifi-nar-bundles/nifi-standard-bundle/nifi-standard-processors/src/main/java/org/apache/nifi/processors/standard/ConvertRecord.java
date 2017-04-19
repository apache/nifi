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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.RowRecordReaderFactory;
import org.apache.nifi.serialization.WriteResult;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"convert", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile")
})
@CapabilityDescription("Converts records from one data format to another using configured Record Reader and Record Write Controller Services. "
    + "The Reader and Writer must be configured with \"matching\" schemas. By this, we mean the schemas must have the same field names. The types of the fields "
    + "do not have to be the same if a field value can be coerced from one type to another. For instance, if the input schema has a field named \"balance\" of type double, "
    + "the output schema can have a field named \"balance\" with a type of string, double, or float. If any field is present in the input that is not present in the output, "
    + "the field will be left out of the output. If any field is specified in the output schema but is not present in the input data/schema, then the field will not be "
    + "present in the output or will have a null value, depending on the writer.")
public class ConvertRecord extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RowRecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that are successfully transformed will be routed to this relationship")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship")
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
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RowRecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RowRecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RecordSetWriter writer = writerFactory.createWriter(getLogger());

        final AtomicReference<WriteResult> writeResultRef = new AtomicReference<>();

        final FlowFile original = flowFile;
        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {

                        final WriteResult writeResult = writer.write(reader.createRecordSet(), out);
                        writeResultRef.set(writeResult);

                    } catch (final MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }
            });
        } catch (final ProcessException e) {
            getLogger().error("Failed to convert {}", new Object[] {flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final WriteResult writeResult = writeResultRef.get();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
        attributes.putAll(writeResult.getAttributes());

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
        session.adjustCounter("Records Converted", writeResult.getRecordCount(), false);
        getLogger().info("Successfully converted {} records for {}", new Object[] {writeResult.getRecordCount(), flowFile});
    }

}
