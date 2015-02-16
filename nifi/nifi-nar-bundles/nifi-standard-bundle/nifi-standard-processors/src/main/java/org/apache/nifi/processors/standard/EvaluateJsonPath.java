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

import com.jayway.jsonpath.*;
import com.jayway.jsonpath.spi.json.JsonProvider;
import net.minidev.json.JSONValue;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.util.BooleanHolder;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "evaluate", "JsonPath"})
@CapabilityDescription("")
public class EvaluateJsonPath extends AbstractProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_JSON = "json";
    public static final String RETURN_TYPE_STRING = "string";

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
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_AUTO, RETURN_TYPE_STRING)
            .defaultValue(RETURN_TYPE_AUTO)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder().name("matched").description("FlowFiles are routed to this relationship when the JsonPath is successfully evaluated and the FlowFile is modified as a result").build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder().name("unmatched").description("FlowFiles are routed to this relationship when the JsonPath does not match the content of the FlowFile and the Destination is set to flowfile-content").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles are routed to this relationship when the JsonPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid JSON").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private static final JsonProvider JSON_PROVIDER = Configuration.defaultConfiguration().jsonProvider();


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
                .addValidator(new JsonPathValidator())
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

        List<FlowFile> flowFiles = processSession.get(50);
        if (flowFiles.isEmpty()) {
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

        flowFileLoop:
        for (FlowFile flowFile : flowFiles) {
            // Validate the JSON document before attempting processing
            final BooleanHolder validJsonHolder = new BooleanHolder(false);
            processSession.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try (InputStreamReader inputStreamReader = new InputStreamReader(in)) {
                        /*
                         * JSONValue#isValidJson is permissive to the degree of the Smart JSON definition.
                         * Accordingly, a strict JSON approach is preferred in determining whether or not a document is valid.
                         */
                        boolean validJson = JSONValue.isValidJsonStrict(inputStreamReader);
                        validJsonHolder.set(validJson);
                    }
                }
            });

            if (!validJsonHolder.get()) {
                logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile.getId()});
                processSession.transfer(flowFile, REL_FAILURE);
                continue flowFileLoop;
            }

            // Parse the document once into an associated context to support multiple path evaluations if specified
            final ObjectHolder<DocumentContext> contextHolder = new ObjectHolder<>(null);
            processSession.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in)) {
                        DocumentContext ctx = JsonPath.parse(in);
                        contextHolder.set(ctx);
                    }
                }
            });

            final DocumentContext documentContext = contextHolder.get();

            final Map<String, String> jsonPathResults = new HashMap<>();

            // Iterate through all JsonPath entries specified
            for (final Map.Entry<String, JsonPath> attributeJsonPathEntry : attributeToJsonPathMap.entrySet()) {

                String jsonPathAttrKey = attributeJsonPathEntry.getKey();
                JsonPath jsonPathExp = attributeJsonPathEntry.getValue();
                final String evalResult = evaluatePathForContext(jsonPathExp, documentContext);

                try {
                    switch (destination) {
                        case DESTINATION_ATTRIBUTE:
                            jsonPathResults.put(jsonPathAttrKey, evalResult);
                            break;
                        case DESTINATION_CONTENT:
                            flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                                @Override
                                public void process(final OutputStream out) throws IOException {
                                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                                        outputStream.write(evalResult.getBytes(StandardCharsets.UTF_8));
                                    }
                                }
                            });
                            break;
                    }
                } catch (PathNotFoundException e) {
                    logger.error("FlowFile {} could not find path {} for attribute key {}.", new Object[]{flowFile.getId(), jsonPathExp.getPath(), jsonPathAttrKey}, e);
                    jsonPathResults.put(jsonPathAttrKey, "");
                }
            }
            flowFile = processSession.putAllAttributes(flowFile, jsonPathResults);
            processSession.transfer(flowFile, REL_MATCH);
        }
    }

    private static String evaluatePathForContext(JsonPath path, ReadContext readCtx) {
        Object pathResult = readCtx.read(path);
        /*
         *  A given path could be a JSON object or a single value, if a sole value, treat as a String; otherwise, return the
         *  representative JSON.
         */
        if (pathResult instanceof String) {
            return pathResult.toString();
        }
        return JSON_PROVIDER.toJson(pathResult);
    }

    private static class JsonPathValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String error = null;
            try {
                JsonPath compile = JsonPath.compile(input);
            } catch (InvalidPathException ipe) {
                error = ipe.toString();
            }
            return new ValidationResult.Builder().valid(error == null).explanation(error).build();
        }
    }

}
