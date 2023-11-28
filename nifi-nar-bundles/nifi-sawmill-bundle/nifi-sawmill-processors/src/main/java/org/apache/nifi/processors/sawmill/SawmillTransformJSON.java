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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ExecutionResult;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SideEffectFree
@SupportsBatching
@Tags({"json", "sawmill", "transform", "grok", "tag"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@WritesAttribute(attribute = "mime.type", description = "Always set to application/json")
@CapabilityDescription("Applies a Sawmill transformation to the FlowFile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship. Note that the input is expected to be a top-level "
        + "JSON object not an array. If the input is an array use SawmillTransformRecord instead, and the transformation will be "
        + "applied to each object. It is not currently possible to apply a Sawmill transformation to an entire top-level array.")
public class SawmillTransformJSON extends BaseSawmillProcessor {

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("sawmill-transform-pretty_print")
            .displayName("Pretty Print")
            .description("Apply pretty-print formatting to the output of the Sawmill transform")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
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

    static {
        descriptors = Collections.unmodifiableList(
                Arrays.asList(
                        SAWMILL_TRANSFORM,
                        GEO_DATABASE_FILE,
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
        return super.customValidate(validationContext);
    }

    private ValidationResult validateSawmill(PropertyDescriptor property, PropertyValue value) {
        final ValidationResult.Builder builder = new ValidationResult.Builder().subject(property.getDisplayName());
        try {
            final String transform = readTransform(value);
            getSawmillPipelineDefinition(transform);
            builder.valid(true);
        } catch (final IOException | RuntimeException e) {
            final String explanation = String.format("%s not valid: %s", property.getDisplayName(), e.getMessage());
            builder.valid(false).explanation(explanation);
        }
        return builder.build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);

        final PropertyValue transformProperty = context.getProperty(SAWMILL_TRANSFORM);
        FlowFile transformed;

        try (final PipelineExecutor pipelineExecutor = new PipelineExecutor()) {
            final String transform = readTransform(transformProperty, original);
            final Pipeline sawmillPipelineDefinition = transformCache.get(transform, currString -> getSawmillPipelineDefinition(transform));

            final boolean prettyPrint = context.getProperty(PRETTY_PRINT).asBoolean();

            transformed = session.write(original, (inputStream, outputStream) -> {
                Map<String, Object> jsonNode = jsonObjectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {
                });
                final ObjectWriter writer = prettyPrint ? jsonObjectMapper.writerWithDefaultPrettyPrinter() : jsonObjectMapper.writer();
                final JsonGenerator jsonGenerator = writer.createGenerator(outputStream);

                Doc doc = new Doc(jsonNode);
                ExecutionResult executionResult = pipelineExecutor.execute(sawmillPipelineDefinition, doc);
                if (executionResult.isSucceeded()) {
                    jsonGenerator.writeObject(doc.getSource());
                } else {
                    getLogger().warn("Sawmill Transform failed or resulted in no data: Error = {}",
                            executionResult.getError().orElse(new ExecutionResult.Error("unknown", "unknown", Optional.empty())));
                }

                jsonGenerator.flush();
            });

            transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transform, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            stopWatch.stop();
            getLogger().debug("Sawmill Transform completed {}", original);
        } catch (final Exception e) {
            getLogger().error("Sawmill Transform failed {}", original, e);
            session.transfer(original, REL_FAILURE);
        }
    }

    @OnStopped
    @OnShutdown
    public void onStopped() {
        if (transformCache != null) {
            transformCache.cleanUp();
        }
    }
}