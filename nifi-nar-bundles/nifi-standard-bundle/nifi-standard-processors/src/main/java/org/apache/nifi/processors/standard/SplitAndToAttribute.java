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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SideEffectFree
@CapabilityDescription("Read JSON or Avro to Attribute.")
@Tags({"JSON","Avro", "Split","Attribute"})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer for the FlowFiles routed to the 'splits' Relationship."),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile. This is added to FlowFiles that are routed to the 'splits' Relationship."),
        @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
public class SplitAndToAttribute extends AbstractProcessor {

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();
    // Properties
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        final List<PropertyDescriptor> props=new ArrayList<>();
        props.add(RECORD_READER);
        props.add(RECORD_WRITER);
        return props;
    }

    //relation
    static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("The individual 'segments' of the original FlowFile will be routed to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that are created are routed to this relationship")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successfully splitting an input FlowFile, the original FlowFile will be sent to this relationship.")
            .build();
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_FAILURE);
        relationships.add(REL_SPLITS);
        relationships.add(REL_ORIGINAL);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final List<FlowFile> splits = new ArrayList<>();
        final Map<String, String> originalAttributes = original.getAttributes();
        final String fragmentId = UUID.randomUUID().toString();
        try {
            session.read(original, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        final RecordSchema schema = writerFactory.getSchema(originalAttributes, reader.getSchema());

                        final RecordSet recordSet = reader.createRecordSet();
                        final PushBackRecordSet pushbackSet = new PushBackRecordSet(recordSet);

                        int fragmentIndex = 0;
                        while (pushbackSet.isAnotherRecord()) {
                            FlowFile split = session.create(original);

                            try {
                                final Map<String, String> attributes = new HashMap<>();
                                final WriteResult writeResult;

                                try (final OutputStream out = session.write(split);
                                     final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, split)) {
                                    final Record record = pushbackSet.next();
                                    writeResult = writer.write(record);

                                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                    attributes.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                                    attributes.put(FRAGMENT_ID, fragmentId);
                                    attributes.put(SEGMENT_ORIGINAL_FILENAME, original.getAttribute(CoreAttributes.FILENAME.key()));
                                    attributes.putAll(writeResult.getAttributes());

                                    for (RecordField recordField : schema.getFields()) {
                                        final String fieldName = recordField.getFieldName();

                                        Object value = record.getValue(recordField);
                                        System.out.println(fieldName + value);
                                        if (value != null) {
                                            attributes.put(fieldName, value.toString());
                                        } else {
                                        }
                                    }

                                    session.adjustCounter("Records Split", writeResult.getRecordCount(), false);
                                }

                                split = session.putAllAttributes(split, attributes);
                            } finally {
                                splits.add(split);
                            }
                            fragmentIndex++;
                        }
                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Failed to parse incoming data", e);
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to split {}", new Object[] {original, pe});
            session.remove(splits);
            session.transfer(original, REL_FAILURE);
            return;
        }

        final FlowFile originalFlowFile = FragmentAttributes.copyAttributesToOriginal(session, original, fragmentId, splits.size());
        session.transfer(originalFlowFile, REL_ORIGINAL);
        // Add the fragment count to each split
        for(FlowFile split : splits) {
            session.putAttribute(split, FRAGMENT_COUNT, String.valueOf(splits.size()));
        }
        session.transfer(splits, REL_SPLITS);
        getLogger().info("Successfully split {} into {} FlowFiles", new Object[] {original, splits.size()});
    }
}
