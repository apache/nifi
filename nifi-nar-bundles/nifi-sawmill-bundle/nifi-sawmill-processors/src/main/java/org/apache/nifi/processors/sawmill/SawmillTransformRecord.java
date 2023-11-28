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
package org.apache.nifi.processors.sawmill;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ExecutionResult;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SideEffectFree
@SupportsBatching
@Tags({"record", "sawmill", "transform", "grok", "tag"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records in an outgoing FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that the configured Record Writer indicates is appropriate"),
})
@CapabilityDescription("Applies a Sawmill transformation to each record in the FlowFile payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
@RequiresInstanceClassLoading
@SeeAlso({SawmillTransformJSON.class})
public class SawmillTransformRecord extends BaseSawmillProcessor {

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

    static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Transform Cache Size")
            .description("Compiling a Sawmill Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
                    + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
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

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;

    /*
     * It is a cache for transform objects. It keep values indexed by jolt specification string.
     * For some cases the key could be empty. It means that it represents default transform (e.g. for custom transform
     * when there is no sawmill-record-spec specified).
     */
    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(RECORD_READER);
        _properties.add(RECORD_WRITER);
        _properties.add(SAWMILL_TRANSFORM);
        _properties.add(GEO_DATABASE_FILE);
        _properties.add(TRANSFORM_CACHE_SIZE);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return super.customValidate(validationContext);
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final RecordSchema schema;
        FlowFile transformed = null;

        try (final InputStream in = session.read(original);
             final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger());
             final PipelineExecutor pipelineExecutor = new PipelineExecutor()) {
            schema = writerFactory.getSchema(original.getAttributes(), reader.getSchema());

            final Map<String, String> attributes = new HashMap<>();
            final WriteResult writeResult;
            transformed = session.create(original);

            // We want to transform the first record before creating the Record Writer. We do this because the Record will likely end up with a different structure
            // and therefore a difference Schema after being transformed. As a result, we want to transform the Record and then provide the transformed schema to the
            // Record Writer so that if the Record Writer chooses to inherit the Record Schema from the Record itself, it will inherit the transformed schema, not the
            // schema determined by the Record Reader.
            final Record firstRecord = reader.nextRecord();
            if (firstRecord == null) {
                try (final OutputStream out = session.write(transformed);
                     final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, transformed)) {

                    writer.beginRecordSet();
                    writeResult = writer.finishRecordSet();

                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    attributes.putAll(writeResult.getAttributes());
                }

                transformed = session.putAllAttributes(transformed, attributes);
                logger.info("{} had no Records to transform", original);
            } else {
                final PropertyValue transformProperty = context.getProperty(SAWMILL_TRANSFORM);
                final String transform = readTransform(transformProperty, original);
                final Pipeline sawmillPipelineDefinition = transformCache.get(transform, currString -> getSawmillPipelineDefinition(transform));
                final List<Record> transformedFirstRecords = transform(firstRecord, pipelineExecutor, sawmillPipelineDefinition);

                final RecordSchema newSchema;
                final Record transformedFirstRecord;
                if (transformedFirstRecords.isEmpty()) {
                    logger.debug("Transformation produced no output, if this is not expected it indicates an error with the transformation");
                    newSchema = firstRecord.getSchema();
                    transformedFirstRecord = null;
                } else {
                    transformedFirstRecord = transformedFirstRecords.get(0);
                    if (transformedFirstRecord == null) {
                        throw new ProcessException("Error transforming the first record");
                    }
                    newSchema = transformedFirstRecord.getSchema();
                }

                final RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), newSchema);

                // TODO: Is it possible that two Records with the same input schema could have different schemas after transformation?
                // If so, then we need to avoid this pattern of writing all Records from the input FlowFile to the same output FlowFile
                // and instead use a Map<RecordSchema, RecordSetWriter>. This way, even if many different output schemas are possible,
                // the output FlowFiles will each only contain records that have the same schema.
                try (final OutputStream out = session.write(transformed);
                     final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, transformed)) {

                    writer.beginRecordSet();

                    if (transformedFirstRecord != null) {
                        writer.write(transformedFirstRecord);
                    }
                    Record record;
                    // If multiple output records were generated, write them out
                    for (int i = 1; i < transformedFirstRecords.size(); i++) {
                        record = transformedFirstRecords.get(i);
                        if (record == null) {
                            throw new ProcessException("Error transforming the first record");
                        }
                        writer.write(record);
                    }

                    while ((record = reader.nextRecord()) != null) {
                        final List<Record> transformedRecords = transform(record, pipelineExecutor, sawmillPipelineDefinition);
                        for (Record transformedRecord : transformedRecords) {
                            writer.write(transformedRecord);
                        }
                    }

                    writeResult = writer.finishRecordSet();

                    try {
                        writer.close();
                    } catch (final IOException ioe) {
                        getLogger().warn("Failed to close Writer for {}", transformed);
                    }

                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    attributes.putAll(writeResult.getAttributes());
                }

                transformed = session.putAllAttributes(transformed, attributes);
                session.getProvenanceReporter().modifyContent(transformed, "Modified With Sawmill", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                logger.debug("Transform completed {}", original);
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

    private List<Record> transform(final Record record, final PipelineExecutor pipelineExecutor, final Pipeline sawmillPipelineDefinition) {
        Map<String, Object> recordMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
        Doc doc = new Doc(recordMap);
        ExecutionResult executionResult = pipelineExecutor.execute(sawmillPipelineDefinition, doc);
        final Map<String, Object> transformedObject;
        if (executionResult.isSucceeded()) {
            transformedObject = doc.getSource();
        } else {
            getLogger().warn("Sawmill Transform failed or resulted in no data: Error = {}",
                    executionResult.getError().orElse(new ExecutionResult.Error("unknown", "unknown", Optional.empty())));
            transformedObject = null;
        }

        // JOLT expects arrays to be of type List where our Record code uses Object[].
        // Make another pass of the transformed objects to change List to Object[].
        final Object normalizedRecordValues = normalizeRecordObjects(transformedObject);
        final List<Record> recordList = new ArrayList<>();

        if (normalizedRecordValues == null) {
            return recordList;
        }

        // If the top-level object is an array, return a list of the records inside. Otherwise return a singleton list with the single transformed record
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

    @OnScheduled
    public void setup(final ProcessContext context) {
        super.onScheduled(context);
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
