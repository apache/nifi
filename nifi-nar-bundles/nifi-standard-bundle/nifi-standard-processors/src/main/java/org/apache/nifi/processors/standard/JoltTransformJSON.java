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

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.jolt.TransformFactory;
import org.apache.nifi.processors.standard.util.jolt.TransformUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr","cardinality","sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type",description = "Always set to application/json")
@CapabilityDescription("Applies a list of Jolt specifications to the flowfile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the JSON transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class JoltTransformJSON extends AbstractProcessor {

    public static final AllowableValue SHIFTR = new AllowableValue("jolt-transform-shift", "Shift", "Shift input JSON/data to create the output JSON.");
    public static final AllowableValue CHAINR = new AllowableValue("jolt-transform-chain", "Chain", "Execute list of Jolt transformations.");
    public static final AllowableValue DEFAULTR = new AllowableValue("jolt-transform-default", "Default", " Apply default values to the output JSON.");
    public static final AllowableValue REMOVR = new AllowableValue("jolt-transform-remove", "Remove", " Remove values from input data to create the output JSON.");
    public static final AllowableValue CARDINALITY = new AllowableValue("jolt-transform-card", "Cardinality", "Change the cardinality of input elements to create the output JSON.");
    public static final AllowableValue SORTR = new AllowableValue("jolt-transform-sort", "Sort", "Sort input json key values alphabetically. Any specification set is ignored.");
    public static final AllowableValue CUSTOMR = new AllowableValue("jolt-transform-custom", "Custom", "Custom Transformation. Requires Custom Transformation Class Name");
    public static final AllowableValue MODIFIER_DEFAULTR = new AllowableValue("jolt-transform-modify-default", "Modify - Default", "Writes when key is missing or value is null");
    public static final AllowableValue MODIFIER_OVERWRITER = new AllowableValue("jolt-transform-modify-overwrite", "Modify - Overwrite", " Always overwrite value");
    public static final AllowableValue MODIFIER_DEFINER = new AllowableValue("jolt-transform-modify-define", "Modify - Define", "Writes when key is missing");

    public static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jolt-transform")
            .displayName("Jolt Transformation DSL")
            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(CARDINALITY, CHAINR, DEFAULTR, MODIFIER_DEFAULTR, MODIFIER_DEFINER, MODIFIER_OVERWRITER, REMOVR, SHIFTR, SORTR, CUSTOMR)
            .defaultValue(CHAINR.getValue())
            .build();

    public static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor.Builder()
            .name("jolt-spec")
            .displayName("Jolt Specification")
            .description("Jolt Specification for transform of JSON data. This value is ignored if the Jolt Sort Transformation is selected.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor CUSTOM_CLASS = new PropertyDescriptor.Builder()
            .name("jolt-custom-class")
            .displayName("Custom Transformation Class Name")
            .description("Fully Qualified Class Name for Custom Transformation")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("jolt-custom-modules")
            .displayName("Custom Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules containing custom transformations (that are not included on NiFi's classpath).")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Transform Cache Size")
            .description("Compiling a Jolt Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
                + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON), it will be routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> properties;
    private final static Set<Relationship> relationships;
    private volatile ClassLoader customClassLoader;
    private final static String DEFAULT_CHARSET = "UTF-8";

    // Cache is guarded by synchronizing on 'this'.
    private volatile int maxTransformsToCache = 10;
    private final Map<String, JoltTransform> transformCache = new LinkedHashMap<String, JoltTransform>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, JoltTransform> eldest) {
            final boolean evict = size() > maxTransformsToCache;
            if (evict) {
                getLogger().debug("Removing Jolt Transform from cache because cache is full");
            }
            return evict;
        }
    };

    static {
        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(JOLT_TRANSFORM);
        _properties.add(CUSTOM_CLASS);
        _properties.add(MODULES);
        _properties.add(JOLT_SPEC);
        _properties.add(TRANSFORM_CACHE_SIZE);
        properties = Collections.unmodifiableList(_properties);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
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
        final String modulePath = validationContext.getProperty(MODULES).isSet()? validationContext.getProperty(MODULES).getValue() : null;

        if(!validationContext.getProperty(JOLT_SPEC).isSet() || StringUtils.isEmpty(validationContext.getProperty(JOLT_SPEC).getValue())){
            if(!SORTR.getValue().equals(transform)) {
                final String message = "A specification is required for this transformation";
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        } else {
            final ClassLoader customClassLoader;

            try {
                if (modulePath != null) {
                    customClassLoader = ClassLoaderUtils.getCustomClassLoader(modulePath, this.getClass().getClassLoader(), getJarFilenameFilter());
                } else {
                    customClassLoader =  this.getClass().getClassLoader();
                }

                final String specValue =  validationContext.getProperty(JOLT_SPEC).getValue();
                final String invalidExpressionMsg = validationContext.newExpressionLanguageCompiler().validateExpression(specValue,true);

                if (validationContext.isExpressionLanguagePresent(specValue) && invalidExpressionMsg != null) {
                    final String customMessage = "The expression language used withing this specification is invalid";
                    results.add(new ValidationResult.Builder().valid(false)
                            .explanation(customMessage)
                            .build());
                } else {
                    //for validation we want to be able to ensure the spec is syntactically correct and not try to resolve variables since they may not exist yet
                    Object specJson = SORTR.getValue().equals(transform) ? null : JsonUtils.jsonToObject(specValue.replaceAll("\\$\\{","\\\\\\\\\\$\\{"), DEFAULT_CHARSET);

                    if (CUSTOMR.getValue().equals(transform)) {
                        if (StringUtils.isEmpty(customTransform)) {
                            final String customMessage = "A custom transformation class should be provided. ";
                            results.add(new ValidationResult.Builder().valid(false)
                                    .explanation(customMessage)
                                    .build());
                        } else {
                            TransformFactory.getCustomTransform(customClassLoader, customTransform, specJson);
                        }
                    } else {
                        TransformFactory.getTransform(customClassLoader, transform, specJson);
                    }
                }
            } catch (final Exception e) {
                getLogger().info("Processor is not valid - " + e.toString());
                String message = "Specification not valid for the selected transformation." ;
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        final Object inputJson;
        try (final InputStream in = session.read(original)) {
            inputJson = JsonUtils.jsonToObject(in);
        } catch (final Exception e) {
            logger.error("Failed to transform {}; routing to failure", new Object[] {original, e});
            session.transfer(original, REL_FAILURE);
            return;
        }

        final String jsonString;
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final JoltTransform transform = getTransform(context, original);
            if (customClassLoader != null) {
                Thread.currentThread().setContextClassLoader(customClassLoader);
            }

            final Object transformedJson = TransformUtils.transform(transform,inputJson);
            jsonString = JsonUtils.toJsonString(transformedJson);
        } catch (final Exception ex) {
            logger.error("Unable to transform {} due to {}", new Object[] {original, ex.toString(), ex});
            session.transfer(original, REL_FAILURE);
            return;
        } finally {
            if (customClassLoader != null && originalContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalContextClassLoader);
            }
        }

        FlowFile transformed = session.write(original, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(jsonString.getBytes(DEFAULT_CHARSET));
            }
        });

        final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
        transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(transformed, REL_SUCCESS);
        session.getProvenanceReporter().modifyContent(transformed,"Modified With " + transformType ,stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        logger.info("Transformed {}", new Object[]{original});
    }

    private JoltTransform getTransform(final ProcessContext context, final FlowFile flowFile) throws Exception {
        final String specString;
        if (context.getProperty(JOLT_SPEC).isSet() && !StringUtils.isEmpty(context.getProperty(JOLT_SPEC).getValue())) {
            specString = context.getProperty(JOLT_SPEC).evaluateAttributeExpressions(flowFile).getValue();
        } else {
            specString = null;
        }

        // Get the transform from our cache, if it exists.
        JoltTransform transform = null;
        synchronized (this) {
            transform = transformCache.get(specString);
        }

        if (transform != null) {
            return transform;
        }

        // If no transform for our spec, create the transform.
        final Object specJson;
        if (context.getProperty(JOLT_SPEC).isSet() && !SORTR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            specJson = JsonUtils.jsonToObject(specString, DEFAULT_CHARSET);
        } else {
            specJson = null;
        }

        if (CUSTOMR.getValue().equals(context.getProperty(JOLT_TRANSFORM).getValue())) {
            transform = TransformFactory.getCustomTransform(customClassLoader, context.getProperty(CUSTOM_CLASS).getValue(), specJson);
        } else {
            transform = TransformFactory.getTransform(customClassLoader, context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
        }

        // Check again for the transform in our cache, since it's possible that another thread has
        // already populated it. If absent from the cache, populate the cache. Otherwise, use the
        // value from the cache.
        synchronized (this) {
            final JoltTransform existingTransform = transformCache.get(specString);
            if (existingTransform == null) {
                transformCache.put(specString, transform);
            } else {
                transform = existingTransform;
            }
        }

        return transform;
    }

    @OnScheduled
    public synchronized void setup(final ProcessContext context) {
        transformCache.clear();
        maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();

        try {
            if (context.getProperty(MODULES).isSet()) {
                customClassLoader = ClassLoaderUtils.getCustomClassLoader(context.getProperty(MODULES).getValue(), this.getClass().getClassLoader(), getJarFilenameFilter());
            } else {
                customClassLoader = this.getClass().getClassLoader();
            }
        } catch (final Exception ex) {
            getLogger().error("Unable to setup processor", ex);
        }
    }

    protected FilenameFilter getJarFilenameFilter(){
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }

}
