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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.joltcommunity.jolt.JoltTransform;
import io.joltcommunity.jolt.JsonUtil;
import io.joltcommunity.jolt.JsonUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.jolt.util.JoltTransformStrategy;
import org.apache.nifi.jolt.util.TransformUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "chainr", "shift", "default", "remove", "cardinality", "sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type", description = "Always set to application/json")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Enables configuration of custom code for Jolt Transforms")
        }
)
@CapabilityDescription("Applies a list of Jolt specifications to either the FlowFile JSON content or a specified FlowFile JSON attribute. "
        + "If the JSON transform fails, the original FlowFile is routed to the 'failure' relationship.")
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
            .build();

    public static final PropertyDescriptor MAX_STRING_LENGTH = new PropertyDescriptor.Builder()
            .name("Max String Length")
            .description("The maximum allowed length of a string value when parsing the JSON document")
            .required(true)
            .defaultValue("20 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
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
                    MAX_STRING_LENGTH
            )
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile ClassLoader customClassLoader;
    private volatile JsonUtil jsonUtil;

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
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        final Object inputJson;
        final boolean sourceStrategyFlowFile = JsonSourceStrategy.FLOW_FILE == context.getProperty(JSON_SOURCE).asAllowableValue(JsonSourceStrategy.class);
        String jsonSourceAttributeName = null;

        if (sourceStrategyFlowFile) {
            try (final InputStream in = session.read(original)) {
                inputJson = jsonUtil.jsonToObject(in);
            } catch (final Exception e) {
                logger.error("JSON parsing failed on FlowFile content for {}", original, e);
                session.transfer(original, REL_FAILURE);
                return;
            }
        } else {
            jsonSourceAttributeName = context.getProperty(JSON_SOURCE_ATTRIBUTE).getValue();
            final String jsonSourceAttributeValue = original.getAttribute(jsonSourceAttributeName);
            if (StringUtils.isBlank(jsonSourceAttributeValue)) {
                logger.error("FlowFile attribute '{}' value is blank", jsonSourceAttributeName);
                session.transfer(original, REL_FAILURE);
                return;
            } else {
                try {
                    inputJson = jsonUtil.jsonToObject(jsonSourceAttributeValue);
                } catch (final Exception e) {
                    logger.error("JSON parsing failed on attribute '{}' of FlowFile {}", jsonSourceAttributeName, original, e);
                    session.transfer(original, REL_FAILURE);
                    return;
                }
            }
        }

        final String jsonString;
        final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            final JoltTransform transform = getTransform(context, original);
            if (customClassLoader != null) {
                Thread.currentThread().setContextClassLoader(customClassLoader);
            }

            final Object transformedJson = TransformUtils.transform(transform, inputJson);
            jsonString = context.getProperty(PRETTY_PRINT).asBoolean() ? jsonUtil.toPrettyJsonString(transformedJson) : jsonUtil.toJsonString(transformedJson);
        } catch (final Exception e) {
            logger.error("Transform failed for {}", original, e);
            session.transfer(original, REL_FAILURE);
            return;
        } finally {
            if (customClassLoader != null && originalContextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(originalContextClassLoader);
            }
        }

        if (sourceStrategyFlowFile) {
            FlowFile transformed = session.write(original, out -> out.write(jsonString.getBytes(StandardCharsets.UTF_8)));
            final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
            transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transformType, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transform completed on FlowFile content for {}", original);
        } else {
            session.putAttribute(original, jsonSourceAttributeName, jsonString);
            session.transfer(original, REL_SUCCESS);
            logger.info("Transform completed on attribute '{}' of FlowFile {}", jsonSourceAttributeName, original);
        }
    }

    @OnScheduled
    @Override
    public void setup(final ProcessContext context) {
        super.setup(context);
        final int maxStringLength = context.getProperty(MAX_STRING_LENGTH).asDataSize(DataUnit.B).intValue();
        final StreamReadConstraints streamReadConstraints = StreamReadConstraints.builder().maxStringLength(maxStringLength).build();

        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.getFactory().setStreamReadConstraints(streamReadConstraints);
        jsonUtil = JsonUtils.customJsonUtil(objectMapper);

        try {
            final JoltTransformStrategy strategy = context.getProperty(JOLT_TRANSFORM).asAllowableValue(JoltTransformStrategy.class);

            if (strategy == JoltTransformStrategy.CUSTOMR && context.getProperty(MODULES).isSet()) {
                customClassLoader = ClassLoaderUtils.getCustomClassLoader(
                        context.getProperty(MODULES).evaluateAttributeExpressions().getValue(),
                        this.getClass().getClassLoader(),
                        getJarFilenameFilter()
                );
            } else {
                customClassLoader = this.getClass().getClassLoader();
            }
        } catch (final Exception e) {
            getLogger().error("ClassLoader configuration failed", e);
        }
    }
}
