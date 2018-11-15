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
package org.apache.nifi.processors.jolt.record;

import com.bazaarvoice.jolt.ContextualTransform;
import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Transform;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.jolt.record.util.TransformFactory;
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
import org.apache.nifi.util.StringUtils;

import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"record", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr", "cardinality", "sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records in an outgoing FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that the configured Record Writer indicates is appropriate"),
})
@CapabilityDescription("Applies a list of Jolt specifications to the FlowFile payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class JoltTransformRecord extends AbstractProcessor {

    static final AllowableValue SHIFTR
            = new AllowableValue("jolt-transform-shift", "Shift", "Shift input data to create the output.");
    static final AllowableValue CHAINR
            = new AllowableValue("jolt-transform-chain", "Chain", "Execute list of Jolt transformations.");
    static final AllowableValue DEFAULTR
            = new AllowableValue("jolt-transform-default", "Default", " Apply default values to the output.");
    static final AllowableValue REMOVR
            = new AllowableValue("jolt-transform-remove", "Remove", " Remove values from input data to create the output.");
    static final AllowableValue CARDINALITY
            = new AllowableValue("jolt-transform-card", "Cardinality", "Change the cardinality of input elements to create the output.");
    static final AllowableValue SORTR
            = new AllowableValue("jolt-transform-sort", "Sort", "Sort input field name values alphabetically. Any specification set is ignored.");
    static final AllowableValue CUSTOMR
            = new AllowableValue("jolt-transform-custom", "Custom", "Custom Transformation. Requires Custom Transformation Class Name");
    static final AllowableValue MODIFIER_DEFAULTR
            = new AllowableValue("jolt-transform-modify-default", "Modify - Default", "Writes when field name is missing or value is null");
    static final AllowableValue MODIFIER_OVERWRITER
            = new AllowableValue("jolt-transform-modify-overwrite", "Modify - Overwrite", " Always overwrite value");
    static final AllowableValue MODIFIER_DEFINER
            = new AllowableValue("jolt-transform-modify-define", "Modify - Define", "Writes when key is missing");

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("jolt-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("jolt-record-record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jolt-record-transform")
            .displayName("Jolt Transformation DSL")
            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(CARDINALITY, CHAINR, DEFAULTR, MODIFIER_DEFAULTR, MODIFIER_DEFINER, MODIFIER_OVERWRITER, REMOVR, SHIFTR, SORTR, CUSTOMR)
            .defaultValue(CHAINR.getValue())
            .build();

    static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor.Builder()
            .name("jolt-record-spec")
            .displayName("Jolt Specification")
            .description("Jolt Specification for transform of record data. This value is ignored if the Jolt Sort Transformation is selected.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor CUSTOM_CLASS = new PropertyDescriptor.Builder()
            .name("jolt-record-custom-class")
            .displayName("Custom Transformation Class Name")
            .description("Fully Qualified Class Name for Custom Transformation")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("jolt-record-custom-modules")
            .displayName("Custom Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules containing custom transformations (that are not included on NiFi's classpath).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamicallyModifiesClasspath(true)
            .build();

    static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("jolt-record-transform-cache-size")
            .displayName("Transform Cache Size")
            .description("Compiling a Jolt Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
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
    private final static String DEFAULT_CHARSET = "UTF-8";

    /**
     * It is a cache for transform objects. It keep values indexed by jolt specification string.
     * For some cases the key could be empty. It means that it represents default transform (e.g. for custom transform
     * when there is no jolt-record-spec specified).
     */
    private LoadingCache<Optional<String>, JoltTransform> transformCache;

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(RECORD_READER);
        _properties.add(RECORD_WRITER);
        _properties.add(JOLT_TRANSFORM);
        _properties.add(CUSTOM_CLASS);
        _properties.add(MODULES);
        _properties.add(JOLT_SPEC);
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
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final String transform = validationContext.getProperty(JOLT_TRANSFORM).getValue();
        final String customTransform = validationContext.getProperty(CUSTOM_CLASS).getValue();

        if (!validationContext.getProperty(JOLT_SPEC).isSet() || StringUtils.isEmpty(validationContext.getProperty(JOLT_SPEC).getValue())) {
            if (!SORTR.getValue().equals(transform)) {
                final String message = "A specification is required for this transformation";
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        } else {
            try {
                final String specValue = validationContext.getProperty(JOLT_SPEC).getValue();

                if (validationContext.isExpressionLanguagePresent(specValue)) {
                    final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(specValue, true);
                    if (!StringUtils.isEmpty(invalidExpressionMsg)) {
                        results.add(new ValidationResult.Builder().valid(false)
                                .subject(JOLT_SPEC.getDisplayName())
                                .explanation("Invalid Expression Language: " + invalidExpressionMsg)
                                .build());
                    }
                } else {
                    //for validation we want to be able to ensure the spec is syntactically correct and not try to resolve variables since they may not exist yet
                    Object specJson = SORTR.getValue().equals(transform) ? null : JsonUtils.jsonToObject(specValue.replaceAll("\\$\\{", "\\\\\\\\\\$\\{"), DEFAULT_CHARSET);

                    if (CUSTOMR.getValue().equals(transform)) {
                        if (StringUtils.isEmpty(customTransform)) {
                            final String customMessage = "A custom transformation class should be provided. ";
                            results.add(new ValidationResult.Builder().valid(false)
                                    .explanation(customMessage)
                                    .build());
                        } else {
                            TransformFactory.getCustomTransform(Thread.currentThread().getContextClassLoader(), customTransform, specJson);
                        }
                    } else {
                        TransformFactory.getTransform(Thread.currentThread().getContextClassLoader(), transform, specJson);
                    }
                }
            } catch (final Exception e) {
                getLogger().info("Processor is not valid - " + e.toString());
                String message = "Specification not valid for the selected transformation.";
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        }

        return results;
    }

    @SuppressWarnings("unchecked")
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
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try (final InputStream in = session.read(original);
             final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {
            schema = writerFactory.getSchema(original.getAttributes(), reader.getSchema());
            Record record;

            FlowFile transformed = session.create(original);
            final Map<String, String> attributes = new HashMap<>();
            final WriteResult writeResult;
            try (final OutputStream out = session.write(transformed);
                 final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out)) {

                final JoltTransform transform = getTransform(context, original);
                writer.beginRecordSet();
                while ((record = reader.nextRecord()) != null) {
                    Map<String, Object> recordMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
                    // JOLT expects arrays to be of type List where our Record code uses Object[].
                    // Make another pass of the transformed objects to change Object[] to List.
                    recordMap = (Map<String, Object>) normalizeJoltObjects(recordMap);
                    Object transformedObject = transform(transform, recordMap);
                    // JOLT expects arrays to be of type List where our Record code uses Object[].
                    // Make another pass of the transformed objects to change List to Object[].
                    Record r = DataTypeUtils.toRecord(normalizeRecordObjects(transformedObject), schema, "r");
                    writer.write(r);
                }
                writeResult = writer.finishRecordSet();

                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());
            } catch (Exception e) {
                logger.error("Unable to write transformed records {} due to {}", new Object[]{original, e.toString(), e});
                session.remove(transformed);
                session.transfer(original, REL_FAILURE);
                return;
            }

            final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
            transformed = session.putAllAttributes(transformed, attributes);
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transformType, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(original, REL_ORIGINAL);
            logger.debug("Transformed {}", new Object[]{original});


        } catch (final Exception ex) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, ex.toString(), ex});
            session.transfer(original, REL_FAILURE);
            return;
        }
    }

    private JoltTransform getTransform(final ProcessContext context, final FlowFile flowFile) {
        final Optional<String> specString;
        if (context.getProperty(JOLT_SPEC).isSet()) {
            specString = Optional.of(context.getProperty(JOLT_SPEC).evaluateAttributeExpressions(flowFile).getValue());
        } else {
            specString = Optional.empty();
        }

        return transformCache.get(specString);
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build(specString -> createTransform(context, specString.orElse(null)));
    }

    private JoltTransform createTransform(final ProcessContext context, final String specString) throws Exception {
        final Object specJson;
        if (context.getProperty(JOLT_SPEC).isSet() && !SORTR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            specJson = JsonUtils.jsonToObject(specString, DEFAULT_CHARSET);
        } else {
            specJson = null;
        }

        if (CUSTOMR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            return TransformFactory.getCustomTransform(Thread.currentThread().getContextClassLoader(), context.getProperty(CUSTOM_CLASS).getValue(), specJson);
        } else {
            return TransformFactory.getTransform(Thread.currentThread().getContextClassLoader(), context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
        }
    }

    protected FilenameFilter getJarFilenameFilter() {
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }

    protected static Object transform(JoltTransform joltTransform, Object input) {
        return joltTransform instanceof ContextualTransform
                ? ((ContextualTransform) joltTransform).transform(input, Collections.emptyMap()) : ((Transform) joltTransform).transform(input);
    }

    /**
     * Recursively replace List objects with Object[]. JOLT expects arrays to be of type List where our Record code uses Object[].
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
            Collection c = (Collection) o;
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
            return ((List<Object>) o).stream().map(JoltTransformRecord::normalizeRecordObjects).toArray(Object[]::new);
        } else if (o instanceof Collection) {
            Collection c = (Collection) o;
            return c.stream().map(JoltTransformRecord::normalizeRecordObjects).collect(Collectors.toList());
        } else {
            return o;
        }
    }

}
