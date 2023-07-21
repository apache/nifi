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
package org.apache.nifi.processors.jslt;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.jslt.JSLTTransformJSON.TransformStrategy.APPLY_TRANSFORM_TO_EACH_OBJECT;
import static org.apache.nifi.processors.jslt.JSLTTransformJSON.TransformStrategy.APPLY_TRANSFORM_TO_ENTIRE_FLOWFILE;

@SideEffectFree
@SupportsBatching
@Tags({"json", "jslt", "transform"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@WritesAttribute(attribute = "mime.type", description = "Always set to application/json")
@CapabilityDescription("Applies a JSLT transformation to the FlowFile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the JSLT transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class JSLTTransformJSON extends AbstractProcessor {

    public static final PropertyDescriptor JSLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jslt-transform-transformation")
            .displayName("JSLT Transformation")
            .description("JSLT Transformation for transform of JSON data. Any NiFi Expression Language present will be evaluated first to get the final transform to be applied. " +
                    "The JSLT Tutorial provides an overview of supported expressions: https://github.com/schibsted/jslt/blob/master/tutorial.md")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT, ResourceType.FILE)
            .required(true)
            .build();

    public static final PropertyDescriptor TRANSFORM_STRATEGY = new PropertyDescriptor.Builder()
            .name("jslt-transform-transform-strategy")
            .displayName("Transformation Strategy")
            .description("Whether to apply the JSLT transformation to the entire FlowFile contents or each JSON object in the root-level array")
            .required(true)
            .allowableValues(TransformStrategy.class)
            .defaultValue(APPLY_TRANSFORM_TO_EACH_OBJECT.getValue())
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("jslt-transform-pretty_print")
            .displayName("Pretty Print")
            .description("Apply pretty-print formatting to the output of the JSLT transform")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("jslt-transform-cache-size")
            .displayName("Transform Cache Size")
            .description("Compiling a JSLT Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
                    + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
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

    private static final List<PropertyDescriptor> descriptors;
    private static final Set<Relationship> relationships;
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper();

    static {
        descriptors = Collections.unmodifiableList(
                Arrays.asList(
                        JSLT_TRANSFORM,
                        TRANSFORM_STRATEGY,
                        PRETTY_PRINT,
                        TRANSFORM_CACHE_SIZE
                )
        );

        relationships = Collections.unmodifiableSet(new LinkedHashSet<>(
                Arrays.asList(
                        REL_SUCCESS,
                        REL_FAILURE
                )
        ));
    }

    private Cache<String, Expression> transformCache;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final ValidationResult.Builder transformBuilder = new ValidationResult.Builder().subject(JSLT_TRANSFORM.getDisplayName());

        final PropertyValue transformProperty = validationContext.getProperty(JSLT_TRANSFORM);
        if (transformProperty.isExpressionLanguagePresent()) {
            transformBuilder.valid(true);
        } else {
            try {
                final String transform = readTransform(transformProperty);
                Parser.compileString(transform);
                transformBuilder.valid(true);
            } catch (final RuntimeException e) {
                final String explanation = String.format("JSLT Transform not valid: %s", e.getMessage());
                transformBuilder.valid(false).explanation(explanation);
            }
        }

        results.add(transformBuilder.build());
        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build();
        // Precompile the transform if it hasn't been done already (and if there is no Expression Language present)
        final PropertyValue transformProperty = context.getProperty(JSLT_TRANSFORM);
        if (!transformProperty.isExpressionLanguagePresent()) {
            try {
                final String transform = readTransform(transformProperty);
                transformCache.put(transform, Parser.compileString(transform));
            } catch (final RuntimeException e) {
                throw new ProcessException("JSLT Transform compilation failed", e);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final TransformStrategy transformStrategy = TransformStrategy.valueOf(context.getProperty(TRANSFORM_STRATEGY).getValue());
        final StopWatch stopWatch = new StopWatch(true);

        final PropertyValue transformProperty = context.getProperty(JSLT_TRANSFORM);
        FlowFile transformed;
        final JsonFactory jsonFactory = new JsonFactory();

        try {
            final String transform = readTransform(transformProperty, original);
            final Expression jsltExpression = transformCache.get(transform, currString -> Parser.compileString(transform));
            final boolean prettyPrint = context.getProperty(PRETTY_PRINT).asBoolean();

            transformed = session.write(original, (inputStream, outputStream) -> {
                boolean topLevelArray = false;
                JsonParser jsonParser;
                JsonNode firstJsonNode;
                if (APPLY_TRANSFORM_TO_EACH_OBJECT.equals(transformStrategy)) {
                    jsonParser = jsonFactory.createParser(inputStream);
                    jsonParser.setCodec(jsonObjectMapper);

                    JsonToken token = jsonParser.nextToken();
                    if (token == JsonToken.START_ARRAY) {
                        token = jsonParser.nextToken(); // advance to START_OBJECT token
                        topLevelArray = true;
                    }
                    if (token == JsonToken.START_OBJECT) { // could be END_ARRAY also
                        firstJsonNode = jsonParser.readValueAsTree();
                    } else {
                        firstJsonNode = null;
                    }
                } else {
                    firstJsonNode = readJson(inputStream);
                    jsonParser = null; // This will not be used when applying the transform to the entire FlowFile
                }

                final ObjectWriter writer = prettyPrint ? jsonObjectMapper.writerWithDefaultPrettyPrinter() : jsonObjectMapper.writer();
                final JsonGenerator jsonGenerator = writer.createGenerator(outputStream);

                Object outputObject;
                JsonNode nextNode;

                if (topLevelArray) {
                    jsonGenerator.writeStartArray();
                }
                nextNode = firstJsonNode;
                do {
                    final JsonNode transformedJson = jsltExpression.apply(nextNode);
                    if (transformedJson == null || transformedJson.isNull()) {
                        getLogger().warn("JSLT Transform resulted in no data {}", original);
                        outputObject = null;
                    } else {
                        outputObject = transformedJson;
                    }
                    if (outputObject != null) {
                        jsonGenerator.writeObject(outputObject);
                    }
                } while ((nextNode = getNextJsonNode(transformStrategy, jsonParser)) != null);
                if (topLevelArray) {
                    jsonGenerator.writeEndArray();
                }
                jsonGenerator.flush();
            });

            transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transform, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            stopWatch.stop();
            getLogger().debug("JSLT Transform completed {}", original);
        } catch (final Exception e) {
            getLogger().error("JSLT Transform failed {}", original, e);
            session.transfer(original, REL_FAILURE);
        }
    }

    @OnStopped
    @OnShutdown
    public void onStopped() {
        transformCache.cleanUp();
    }

    private JsonNode readJson(final InputStream in) throws IOException {
        try {
            return jsonObjectMapper.readTree(in);
        } catch (final JsonParseException e) {
            throw new IOException("Could not parse data as JSON", e);
        }
    }

    private String readTransform(final PropertyValue propertyValue, final FlowFile flowFile) {
        final String transform;

        if (propertyValue.isExpressionLanguagePresent()) {
            transform = propertyValue.evaluateAttributeExpressions(flowFile).getValue();
        } else {
            transform = readTransform(propertyValue);
        }

        return transform;
    }

    private String readTransform(final PropertyValue propertyValue) {
        final ResourceReference resourceReference = propertyValue.asResource();
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(resourceReference.read()))) {
            return reader.lines().collect(Collectors.joining());
        } catch (final IOException e) {
            throw new UncheckedIOException("Read JSLT Transform failed", e);
        }
    }


    protected JsonNode getNextJsonNode(final TransformStrategy transformStrategy, final JsonParser jsonParser) throws IOException {

        if (APPLY_TRANSFORM_TO_ENTIRE_FLOWFILE.equals(transformStrategy)) {
            return null;
        }
        return getJsonNode(jsonParser);
    }

    private JsonNode getJsonNode(JsonParser jsonParser) throws IOException {
        while (true) {
            final JsonToken token = jsonParser.nextToken();
            if (token == null) {
                return null;
            }

            switch (token) {
                case START_ARRAY:
                case END_ARRAY:
                case END_OBJECT:
                    break;
                case START_OBJECT:
                    return jsonParser.readValueAsTree();
                default:
                    throw new IOException("Expected to get a JSON Object but got a token of type " + token.name());
            }
        }
    }

    /**
     * Enumeration of supported Output Strategies
     */
    enum TransformStrategy implements DescribedValue {
        APPLY_TRANSFORM_TO_ENTIRE_FLOWFILE("APPLY_TRANSFORM_TO_ENTIRE_FLOWFILE", "Entire FlowFile", "Entire FlowFile"),
        APPLY_TRANSFORM_TO_EACH_OBJECT("APPLY_TRANSFORM_TO_EACH_OBJECT", "Each JSON Object", "Each JSON Object");

        private final String value;

        private final String displayName;

        private final String description;

        TransformStrategy(final String value, final String displayName, final String description) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}