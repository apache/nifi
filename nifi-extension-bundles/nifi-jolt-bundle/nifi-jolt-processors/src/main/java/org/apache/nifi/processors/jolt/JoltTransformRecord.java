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
package org.apache.nifi.processors.jolt;

import com.bazaarvoice.jolt.ContextualTransform;
import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.Transform;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SideEffectFree
@SupportsBatching
@Tags({"record", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr", "cardinality", "sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records in an outgoing FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that the configured Record Writer indicates is appropriate"),
})
@CapabilityDescription("Applies a JOLT specification to each record in the FlowFile payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
@RequiresInstanceClassLoading
public class JoltTransformRecord extends AbstractJoltTransform {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor SCHEMA_WRITING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Schema Writing Strategy")
            .description("Specifies how the processor should handle records that result in different schemas after transformation.")
            .allowableValues(JoltTransformWritingStrategy.class)
            .defaultValue(JoltTransformWritingStrategy.USE_FIRST_SCHEMA)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile records cannot be parsed), it will be routed to this relationship")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was transformed. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    SCHEMA_WRITING_STRATEGY,
                    RECORD_READER,
                    RECORD_WRITER
            )
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final String strategy = context.getProperty(SCHEMA_WRITING_STRATEGY).getValue();

        if (strategy.equals(JoltTransformWritingStrategy.PARTITION_BY_SCHEMA.getValue())) {
            processPartitioned(context, session);
        } else {
            processUniform(context, session);
        }
    }

    private void processPartitioned(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        // Maps to track resources per Schema
        final Map<RecordSchema, FlowFile> flowFileMap = new HashMap<>();
        final Map<RecordSchema, OutputStream> streamMap = new HashMap<>();
        final Map<RecordSchema, RecordSetWriter> writerMap = new HashMap<>();
        final Map<RecordSchema, Integer> recordCounts = new HashMap<>();
        final Map<RecordSchema, WriteResult> writeResults = new HashMap<>();

        boolean error = false;

        try (final InputStream in = session.read(original); final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {

            final JoltTransform transform = getTransform(context, original);
            Record currentRecord;

            while ((currentRecord = reader.nextRecord()) != null) {
                final List<Record> transformedRecords = transform(currentRecord, transform);

                if (transformedRecords.isEmpty()) {
                    continue;
                }

                for (Record transformedRecord : transformedRecords) {
                    if (transformedRecord == null) {
                        continue;
                    }

                    final RecordSchema recordSchema = transformedRecord.getSchema();
                    final RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), recordSchema);

                    RecordSetWriter writer = writerMap.get(writeSchema);

                    if (writer == null) {
                        FlowFile newFlowFile = session.create(original);
                        OutputStream out = session.write(newFlowFile);
                        writer = writerFactory.createWriter(getLogger(), writeSchema, out, newFlowFile);

                        writer.beginRecordSet();

                        flowFileMap.put(writeSchema, newFlowFile);
                        streamMap.put(writeSchema, out);
                        writerMap.put(writeSchema, writer);
                        recordCounts.put(writeSchema, 0);
                    }

                    writer.write(transformedRecord);
                    recordCounts.put(writeSchema, recordCounts.get(writeSchema) + 1);
                }
            }
        } catch (final Exception e) {
            error = true;
            logger.error("Transform failed for {}", original, e);
        } finally {
            // Clean up resources
            for (Map.Entry<RecordSchema, RecordSetWriter> entry : writerMap.entrySet()) {
                try {
                    final RecordSetWriter writer = entry.getValue();
                    writeResults.put(entry.getKey(), writer.finishRecordSet());
                    writer.close();
                } catch (Exception e) {
                    getLogger().warn("Failed to close Writer", e);
                }
            }
            for (OutputStream out : streamMap.values()) {
                try {
                    out.close();
                } catch (Exception e) {
                    getLogger().warn("Failed to close OutputStream", e);
                }
            }
        }

        if (error) {
            for (FlowFile flowFile : flowFileMap.values()) {
                session.remove(flowFile);
            }
            session.transfer(original, REL_FAILURE);
        } else {
            if (writerMap.isEmpty()) {
                logger.info("{} had no Records to transform (all filtered)", original);
            } else {
                final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
                for (Map.Entry<RecordSchema, RecordSetWriter> entry : writerMap.entrySet()) {
                    RecordSchema schemaKey = entry.getKey();
                    RecordSetWriter writer = entry.getValue();
                    FlowFile flowFile = flowFileMap.get(schemaKey);
                    int count = recordCounts.get(schemaKey);
                    WriteResult writeResult = writeResults.get(schemaKey);

                    Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                    attributes.put("record.count", String.valueOf(count));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());

                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.getProvenanceReporter().modifyContent(flowFile, "Modified With " + transformType, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                    session.transfer(flowFile, REL_SUCCESS);
                }
            }
            session.transfer(original, REL_ORIGINAL);
        }
    }

    private void processUniform(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        FlowFile transformed = null;

        try (final InputStream in = session.read(original); final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {

            final JoltTransform transform = getTransform(context, original);

            // We need to find the first VALID record to determine the schema for the writer
            Record firstValidRecord = null;
            List<Record> firstValidBatch = null;
            Record currentRecord;

            while ((currentRecord = reader.nextRecord()) != null) {
                List<Record> transformedRecords = transform(currentRecord, transform);
                if (transformedRecords != null && !transformedRecords.isEmpty() && transformedRecords.getFirst() != null) {
                    firstValidBatch = transformedRecords;
                    firstValidRecord = transformedRecords.getFirst();
                    break;
                }
            }

            transformed = session.create(original);
            final WriteResult writeResult;
            final Map<String, String> attributes = new HashMap<>();

            if (firstValidRecord == null) {
                // UPDATED LOGIC:
                // All records were filtered out (or input was empty).
                // The test expects 0 output files. We must remove the FlowFile we created
                // and ensure 'transformed' is null so it isn't transferred to SUCCESS later.
                session.remove(transformed);
                transformed = null;
                logger.info("{} had no Records to transform", original);
            } else {
                // We have at least one valid record, initialize writer with its schema
                final RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), firstValidRecord.getSchema());

                try (final OutputStream out = session.write(transformed); final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, transformed)) {

                    writer.beginRecordSet();

                    // Write the first batch found
                    for (Record r : firstValidBatch) {
                        if (r != null) writer.write(r);
                    }

                    // Write the rest
                    while ((currentRecord = reader.nextRecord()) != null) {
                        final List<Record> transformedRecords = transform(currentRecord, transform);
                        if (transformedRecords != null && !transformedRecords.isEmpty()) {
                            for (Record r : transformedRecords) {
                                if (r != null) writer.write(r);
                            }
                        }
                    }

                    writeResult = writer.finishRecordSet();
                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    attributes.putAll(writeResult.getAttributes());
                }

                final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
                session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transformType, stopWatch.getElapsed(TimeUnit.MILLISECONDS));

                // Only apply attributes if we actually wrote something
                transformed = session.putAllAttributes(transformed, attributes);
            }

        } catch (final Exception e) {
            logger.error("Transform failed for {}", original, e);
            session.transfer(original, REL_FAILURE);
            if (transformed != null) {
                session.remove(transformed);
            }
            return;
        }

        if (transformed != null) {
            session.transfer(transformed, REL_SUCCESS);
        }
        session.transfer(original, REL_ORIGINAL);
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("jolt-record-record-reader", RECORD_READER.getName());
        config.renameProperty("jolt-record-record-writer", RECORD_WRITER.getName());
    }

    private List<Record> transform(final Record record, final JoltTransform transform) {
        Map<String, Object> recordMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
        // JOLT expects arrays to be of type List where our Record code uses Object[].
        // Make another pass of the transformed objects to change Object[] to List.
        recordMap = (Map<String, Object>) normalizeJoltObjects(recordMap);
        final Object transformedObject = transform(transform, recordMap);

        // JOLT expects arrays to be of type List where our Record code uses Object[].
        // Make another pass of the transformed objects to change List to Object[].
        final Object normalizedRecordValues = normalizeRecordObjects(transformedObject);
        final List<Record> recordList = new ArrayList<>();

        if (normalizedRecordValues == null) {
            return recordList;
        }

        // If the top-level object is an array, return a list of the records inside.
        // Otherwise return a singleton list with the single transformed record
        if (normalizedRecordValues instanceof Object[]) {
            for (Object o : (Object[]) normalizedRecordValues) {
                if (o != null) {
                    recordList.add(DataTypeUtils.toRecord(o, "r"));
                }
            }
        } else {
            recordList.add(DataTypeUtils.toRecord(normalizedRecordValues, "r"));
        }
        return recordList;
    }

    protected static Object transform(JoltTransform joltTransform, Object input) {
        return joltTransform instanceof ContextualTransform
                ?
                ((ContextualTransform) joltTransform).transform(input, Collections.emptyMap()) : ((Transform) joltTransform).transform(input);
    }

    /**
     * Recursively replace List objects with Object[].
     * JOLT expects arrays to be of type List where our Record code uses Object[].
     *
     * @param o The object to normalize with respect to JOLT
     */
    @SuppressWarnings("unchecked")
    protected static Object normalizeJoltObjects(final Object o) {
        if (o instanceof Map) {
            Map<String, Object> m = ((Map<String, Object>) o);
            m.forEach((k, v) -> m.put(k, normalizeJoltObjects(v)));
            return m;
        } else if (o instanceof Object[]) {
            return Arrays.stream(((Object[]) o)).map(JoltTransformRecord::normalizeJoltObjects).collect(Collectors.toList());
        } else if (o instanceof Collection) {
            Collection<?> c = (Collection<?>) o;
            return c.stream().map(JoltTransformRecord::normalizeJoltObjects).collect(Collectors.toList());
        } else {
            return o;
        }
    }

    @SuppressWarnings("unchecked")
    protected static Object normalizeRecordObjects(final Object o) {
        if (o instanceof Map) {
            Map<String, Object> m = ((Map<String, Object>) o);
            m.forEach((k, v) -> m.put(k, normalizeRecordObjects(v)));
            return m;
        } else if (o instanceof List) {
            final List<Object> objectList = (List<Object>) o;
            final Object[] objectArray = new Object[objectList.size()];
            for (int i = 0; i < objectArray.length; i++) {
                objectArray[i] = normalizeRecordObjects(objectList.get(i));
            }
            return objectArray;
        } else if (o instanceof Collection) {
            Collection<?> c = (Collection<?>) o;
            final List<Object> objectList = new ArrayList<>();
            for (Object obj : c) {
                objectList.add(normalizeRecordObjects(obj));
            }
            return objectList;
        } else {
            return o;
        }
    }
}