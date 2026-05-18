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

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.joltcommunity.jolt.JoltTransform;
import io.joltcommunity.jolt.JsonUtil;
import io.joltcommunity.jolt.JsonUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.jolt.util.JoltTransformStrategy;
import org.apache.nifi.jolt.util.TransformUtils;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "chainr", "shift", "default", "remove", "cardinality", "sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type", description = "Set to application/json or application/jsonl based on configuration")

@CapabilityDescription("""
        Reformat JSON to JSON using a Jolt Transform with Domain Specific Language manipulation instructions.
        The JOLT Community Edition documentation provides examples of supported operations and syntax standards.
        """
)
@RequiresInstanceClassLoading
public class JoltTransformJSON extends AbstractJoltTransform {

    public static final PropertyDescriptor JSON_SOURCE = new PropertyDescriptor.Builder()
            .name("JSON Source")
            .description("Specifies whether the Jolt transformation is applied to FlowFile JSON content or to specified FlowFile JSON attribute.")
            .required(true)
            .allowableValues(JsonSourceStrategy.class)
            .defaultValue(JsonSourceStrategy.FLOW_FILE)
            .build();

    public static final PropertyDescriptor JSON_SOURCE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("JSON Source Attribute")
            .description("The FlowFile attribute containing JSON to be transformed.")
            .dependsOn(JSON_SOURCE, JsonSourceStrategy.ATTRIBUTE)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("Pretty Print")
            .description("Apply pretty print formatting to the output of the Jolt transform")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(JSON_SOURCE, JsonSourceStrategy.FLOW_FILE, JsonSourceStrategy.ATTRIBUTE)
            .build();

    public static final PropertyDescriptor MAX_STRING_LENGTH = new PropertyDescriptor.Builder()
            .name("Max String Length")
            .description("The maximum allowed length of a string value when parsing the JSON document")
            .required(true)
            .defaultValue("20 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETAIN_UNICODE_ESCAPE_SEQUENCES = new PropertyDescriptor.Builder()
            .name("Retain Unicode Escape Sequences")
            .description("Allows for retaining Unicode escape sequences instead of resolving them (e.g. retain text \\u00E9 without resolving to é)")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with successfully transformed content or updated attribute will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the JSON transformation fails (e.g., due to invalid JSON in the content or attribute), the original FlowFile is routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    JSON_SOURCE,
                    JSON_SOURCE_ATTRIBUTE,
                    PRETTY_PRINT,
                    MAX_STRING_LENGTH,
                    RETAIN_UNICODE_ESCAPE_SEQUENCES
            )
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final char LINE_FEED = '\n';

    private volatile ClassLoader configuredClassLoader;
    private volatile JsonUtil jsonUtil;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    @Override
    public void setup(final ProcessContext context) {
        super.setup(context);
        final int maxStringLength = context.getProperty(MAX_STRING_LENGTH).asDataSize(DataUnit.B).intValue();
        final StreamReadConstraints streamReadConstraints = StreamReadConstraints.builder().maxStringLength(maxStringLength).build();

        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.getFactory().setStreamReadConstraints(streamReadConstraints);

        final boolean retainUnicodeEscapeSequences = context.getProperty(RETAIN_UNICODE_ESCAPE_SEQUENCES).asBoolean();
        if (retainUnicodeEscapeSequences) {
            objectMapper.getFactory().enable(JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature());
        } else {
            objectMapper.getFactory().disable(JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature());
        }

        jsonUtil = JsonUtils.customJsonUtil(objectMapper);

        configuredClassLoader = getClass().getClassLoader();
        try {
            final JoltTransformStrategy strategy = context.getProperty(JOLT_TRANSFORM).asAllowableValue(JoltTransformStrategy.class);

            if (strategy == JoltTransformStrategy.CUSTOMR && context.getProperty(MODULES).isSet()) {
                configuredClassLoader = ClassLoaderUtils.getCustomClassLoader(
                        context.getProperty(MODULES).evaluateAttributeExpressions().getValue(),
                        getClass().getClassLoader(),
                        getJarFilenameFilter()
                );
            }
        } catch (final Exception e) {
            getLogger().error("ClassLoader configuration failed", e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final JsonSourceStrategy sourceStrategy = context.getProperty(JSON_SOURCE).asAllowableValue(JsonSourceStrategy.class);
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(configuredClassLoader);

            final JoltTransform transform = getTransform(context, original);
            final FlowFile transformedFlowFile = switch (sourceStrategy) {
                case FLOW_FILE -> transformFlowFile(context, session, original, transform);
                case ATTRIBUTE -> transformAttribute(context, session, original, transform);
                case JSON_LINES -> transformNewlineDelimited(session, original, transform);
            };

            onSuccess(context, session, transformedFlowFile, sourceStrategy, stopWatch);
        } catch (final Exception e) {
            getLogger().error("Failed to Transform {}", original, e);
            session.transfer(original, REL_FAILURE);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    private FlowFile transformFlowFile(
            final ProcessContext context,
            final ProcessSession session,
            final FlowFile flowFile,
            final JoltTransform transform
    ) {
        final Object inputJson;
        try (final InputStream in = session.read(flowFile)) {
            inputJson = jsonUtil.jsonToObject(in);
        } catch (final Exception e) {
            throw new ProcessException("JSON parsing failed for FlowFile", e);
        }

        final String transformedJson = getTransformedJson(context, transform, inputJson);
        return session.write(flowFile, out -> out.write(transformedJson.getBytes(StandardCharsets.UTF_8)));
    }

    private FlowFile transformAttribute(
            final ProcessContext context,
            final ProcessSession session,
            final FlowFile flowFile,
            final JoltTransform transform
    ) {
        final String jsonSourceAttributeName = context.getProperty(JSON_SOURCE_ATTRIBUTE).getValue();
        final String jsonSourceAttributeValue = flowFile.getAttribute(jsonSourceAttributeName);

        if (jsonSourceAttributeValue == null || jsonSourceAttributeValue.isBlank()) {
            throw new ProcessException("Content not found in FlowFile Attribute [%s]".formatted(jsonSourceAttributeName));
        }

        final Object inputJson;
        try {
            inputJson = jsonUtil.jsonToObject(jsonSourceAttributeValue);
        } catch (final Exception e) {
            throw new ProcessException("JSON parsing failed for FlowFile Attribute [%s]".formatted(jsonSourceAttributeName), e);
        }

        final String transformedJson = getTransformedJson(context, transform, inputJson);
        return session.putAttribute(flowFile, jsonSourceAttributeName, transformedJson);
    }

    private FlowFile transformNewlineDelimited(
            final ProcessSession session,
            final FlowFile flowFile,
            final JoltTransform transform
    ) {
        return session.write(flowFile, (in, out) -> {
            try (
                    final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                    final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    final Object inputJson = jsonUtil.jsonToObject(line);
                    final Object transformedJson = TransformUtils.transform(transform, inputJson);
                    writer.write(jsonUtil.toJsonString(transformedJson));
                    writer.write(LINE_FEED);
                }
            }
        });
    }

    private String getTransformedJson(
            final ProcessContext context,
            final JoltTransform transform,
            final Object inputJson
    ) {
        final Object transformedJson = TransformUtils.transform(transform, inputJson);
        final boolean prettyPrintEnabled = context.getProperty(PRETTY_PRINT).asBoolean();
        return prettyPrintEnabled ? jsonUtil.toPrettyJsonString(transformedJson) : jsonUtil.toJsonString(transformedJson);
    }

    private void onSuccess(
            final ProcessContext context,
            final ProcessSession session,
            final FlowFile flowFile,
            final JsonSourceStrategy jsonSourceStrategy,
            final StopWatch stopWatch
    ) {
        final FlowFile transformedFlowFile;
        if (JsonSourceStrategy.FLOW_FILE == jsonSourceStrategy || JsonSourceStrategy.JSON_LINES == jsonSourceStrategy) {
            transformedFlowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), jsonSourceStrategy.getContentType());
            final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
            final long elapsed = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            session.getProvenanceReporter().modifyContent(transformedFlowFile, transformType, elapsed);
        } else {
            transformedFlowFile = flowFile;
        }

        session.transfer(transformedFlowFile, REL_SUCCESS);
        getLogger().info("Transform Completed for [{}] {}", jsonSourceStrategy, flowFile);
    }
}
