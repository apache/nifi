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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.JsltException;
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
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
            .description("JSLT Transformation for transform of JSON data. Any NiFi Expression Language present will be evaluated first to get the final transform to be applied.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
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

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper();

    /**
     * A cache for transform objects. It keeps values indexed by JSLT specification string.
     */
    private Cache<String, Expression> transformCache;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(JSLT_TRANSFORM);
        descriptors.add(PRETTY_PRINT);
        descriptors.add(TRANSFORM_CACHE_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        // If no EL present, pre-compile the script (and report any errors as to mark the processor invalid)
        if (!validationContext.getProperty(JSLT_TRANSFORM).isExpressionLanguagePresent()) {
            final String transform = validationContext.getProperty(JSLT_TRANSFORM).getValue();
            try {
                Parser.compileString(transform);
            } catch (JsltException je) {
                results.add(new ValidationResult.Builder().subject(JSLT_TRANSFORM.getDisplayName()).valid(false).explanation("error in transform: " + je.getMessage()).build());
            }
        } else {
            // Expression Language is present, we won't know if the transform is valid until the EL is evaluated
            results.add(new ValidationResult.Builder().subject(JSLT_TRANSFORM.getDisplayName()).valid(true).build());
        }
        return results;

    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build();
        // Precompile the transform if it hasn't been done already (and if there is no Expression Language present)
        if (!context.getProperty(JSLT_TRANSFORM).isExpressionLanguagePresent()) {
            final String transform = context.getProperty(JSLT_TRANSFORM).getValue();
            try {
                transformCache.put(transform, Parser.compileString(transform));
            } catch (JsltException je) {
                throw new ProcessException("Error compiling JSLT transform: " + je.getMessage(), je);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        JsonNode firstJsonNode;
        try (final InputStream in = session.read(original)) {
            firstJsonNode = readJson(in);
        } catch (final Exception e) {
            logger.error("Failed to transform {}; routing to failure", original, e);
            session.transfer(original, REL_FAILURE);
            return;
        }

        try {
            final String transform = context.getProperty(JSLT_TRANSFORM).evaluateAttributeExpressions(original).getValue();
            Expression jsltExpression = transformCache.get(transform, currString -> Parser.compileString(transform));

            final JsonNode transformedJson = jsltExpression.apply(firstJsonNode);
            final ObjectWriter writer = context.getProperty(PRETTY_PRINT).asBoolean() ? jsonObjectMapper.writerWithDefaultPrettyPrinter() : jsonObjectMapper.writer();
            final Object outputObject;
            if (transformedJson == null || transformedJson.isNull()) {
                logger.warn("JSLT transform resulted in no data");
                outputObject = null;
            } else {
                outputObject = transformedJson;
            }
            FlowFile transformed = session.write(original, out -> {
                if (outputObject != null) {
                    writer.writeValue(out, outputObject);
                }
            });
            transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transform, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.debug("Transformed {}", original);
        } catch (final Exception ex) {
            logger.error("JSLT Transform failed {}", original, ex);
            session.transfer(original, REL_FAILURE);
        }
    }

    @OnStopped
    @OnShutdown
    public void onStopped(ProcessContext context) {
        transformCache.cleanUp();
    }

    private JsonNode readJson(final InputStream in) throws IOException {
        try {
            return jsonObjectMapper.readTree(in);
        } catch (final JsonParseException e) {
            throw new IOException("Could not parse data as JSON", e);
        }
    }
}
