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

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DynamicRelationships;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "evaluate", "JsonPath"})
@CapabilityDescription("Evaluates one or more JsonPath expressions against the content of a FlowFile. "
        + "The results of those expressions are assigned to FlowFile Attributes or are written to the content of the FlowFile itself, "
        + "depending on configuration of the Processor. "
        + "JsonPaths are entered by adding user-defined properties; the name of the property maps to the Attribute Name "
        + "into which the result will be placed (if the Destination is flowfile-attribute; otherwise, the property name is ignored). "
        + "The value of the property must be a valid JsonPath expression. "
        + "If the JsonPath evaluates to a JSON array or JSON object and the Return Type is set to 'scalar' the FlowFile will be unmodified and will be routed to failure. "
        + "A Return Type of JSON can return scalar values if the provided JsonPath evaluates to the specified value and will be routed as a match."
        + "If Destination is 'flowfile-content' and the JsonPath does not evaluate to a defined path, the FlowFile will be routed to 'unmatched' without having its contents modified. "
        + "If Destination is flowfile-attribute and the expression matches nothing, attributes will be created with "
        + "empty strings as the value, and the FlowFile will always be routed to 'matched.'")
@DynamicProperty(name="A FlowFile attribute(if <Destination> is set to 'flowfile-attribute')", description="JsonPath expression")
public class EvaluateJsonPath extends AbstractJsonPathProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_JSON = "json";
    public static final String RETURN_TYPE_SCALAR = "scalar";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the JsonPath evaluation are written to the FlowFile content or a FlowFile attribute; if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one JsonPath may be specified, and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type")
            .description("Indicates the desired return type of the JSON Path expressions.  Selecting 'auto-detect' will set the return type to 'json' for a Destination of 'flowfile-content', and 'string' for a Destination of 'flowfile-attribute'.")
            .required(true)
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_JSON, RETURN_TYPE_SCALAR)
            .defaultValue(RETURN_TYPE_AUTO)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder().name("matched").description("FlowFiles are routed to this relationship when the JsonPath is successfully evaluated and the FlowFile is modified as a result").build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder().name("unmatched").description("FlowFiles are routed to this relationship when the JsonPath does not match the content of the FlowFile and the Destination is set to flowfile-content").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles are routed to this relationship when the JsonPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid JSON").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final ConcurrentMap<String, JsonPath> cachedJsonPathMap = new ConcurrentHashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(RETURN_TYPE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int jsonPathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    jsonPathCount++;
                }
            }

            if (jsonPathCount != 1) {
                results.add(new ValidationResult.Builder().subject("JsonPaths").valid(false).explanation("Exactly one JsonPath must be set if using destination of " + DESTINATION_CONTENT).build());
            }
        }

        return results;
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
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(new JsonPathValidator() {
                    @Override
                    public void cacheComputedValue(String subject, String input, JsonPath computedJsonPath) {
                        cachedJsonPathMap.put(input, computedJsonPath);
                    }

                    @Override
                    public boolean isStale(String subject, String input) {
                        return cachedJsonPathMap.get(input) == null;
                    }
                })
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.isDynamic()) {
            if (!StringUtils.equals(oldValue, newValue)) {
                if (oldValue != null) {
                    cachedJsonPathMap.remove(oldValue);
                }
            }
        }
    }

    /**
     * Provides cleanup of the map for any JsonPath values that may have been created.  This will remove common values
     * shared between multiple instances, but will be regenerated when the next validation cycle occurs as a result of
     * isStale()
     */
    @OnRemoved
    public void onRemoved(ProcessContext processContext) {
        for (PropertyDescriptor propertyDescriptor : getPropertyDescriptors()) {
            if (propertyDescriptor.isDynamic()) {
                cachedJsonPathMap.remove(processContext.getProperty(propertyDescriptor).getValue());
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();

        /* Build the JsonPath expressions from attributes */
        final Map<String, JsonPath> attributeToJsonPathMap = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : processContext.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            final JsonPath jsonPath = JsonPath.compile(entry.getValue());
            attributeToJsonPathMap.put(entry.getKey().getName(), jsonPath);
        }

        final String destination = processContext.getProperty(DESTINATION).getValue();
        String returnType = processContext.getProperty(RETURN_TYPE).getValue();
        if (returnType.equals(RETURN_TYPE_AUTO)) {
            returnType = destination.equals(DESTINATION_CONTENT) ? RETURN_TYPE_JSON : RETURN_TYPE_SCALAR;
        }

        DocumentContext documentContext = null;
        try {
            documentContext = validateAndEstablishJsonContext(processSession, flowFile);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile});
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> jsonPathResults = new HashMap<>();

        for (final Map.Entry<String, JsonPath> attributeJsonPathEntry : attributeToJsonPathMap.entrySet()) {

            String jsonPathAttrKey = attributeJsonPathEntry.getKey();
            JsonPath jsonPathExp = attributeJsonPathEntry.getValue();

            final ObjectHolder<Object> resultHolder = new ObjectHolder<>(null);
            try {
                Object result = documentContext.read(jsonPathExp);
                if (returnType.equals(RETURN_TYPE_SCALAR) && !isJsonScalar(result)) {
                    logger.error("Unable to return a scalar value for the expression {} for FlowFile {}. Evaluated value was {}. Transferring to {}.",
                            new Object[]{jsonPathExp.getPath(), flowFile.getId(), result.toString(), REL_FAILURE.getName()});
                    processSession.transfer(flowFile, REL_FAILURE);
                    return;
                }
                resultHolder.set(result);
            } catch (PathNotFoundException e) {
                logger.warn("FlowFile {} could not find path {} for attribute key {}.", new Object[]{flowFile.getId(), jsonPathExp.getPath(), jsonPathAttrKey}, e);
                if (destination.equals(DESTINATION_ATTRIBUTE)) {
                    jsonPathResults.put(jsonPathAttrKey, StringUtils.EMPTY);
                    continue;
                } else {
                    processSession.transfer(flowFile, REL_NO_MATCH);
                    return;
                }
            }

            final String resultRepresentation = getResultRepresentation(resultHolder.get());
            switch (destination) {
                case DESTINATION_ATTRIBUTE:
                    jsonPathResults.put(jsonPathAttrKey, resultRepresentation);
                case DESTINATION_CONTENT:
                    flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                outputStream.write(resultRepresentation.getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    });
                    processSession.getProvenanceReporter().modifyContent(flowFile,
                            "Replaced content with result of expression " + jsonPathExp.getPath());
                    break;
            }
        }
        flowFile = processSession.putAllAttributes(flowFile, jsonPathResults);
        processSession.transfer(flowFile, REL_MATCH);
    }
}