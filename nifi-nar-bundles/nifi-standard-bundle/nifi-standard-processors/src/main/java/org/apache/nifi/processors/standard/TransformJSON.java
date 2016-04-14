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


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.StopWatch;

import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.Transform;
import com.bazaarvoice.jolt.JsonUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Applies a list of JOLT specifications to the flowfile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the JSON transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship")
public class TransformJSON extends AbstractProcessor {

    public static final AllowableValue SHIFTR = new AllowableValue("Shift", "Shift Transform DSL", "This JOLT transformation will shift input JSON/data to create the output JSON/data.");
    public static final AllowableValue CHAINR = new AllowableValue("Chain", "Chain Transform DSL", "Execute list of JOLT transformations.");
    public static final AllowableValue DEFAULTR = new AllowableValue("Default", "Default Transform DSL", " This JOLT transformation will apply default values to the output JSON/data.");
    public static final AllowableValue REMOVR = new AllowableValue("Remove", "Remove Transform DSL", " This JOLT transformation will apply default values to the output JSON/data.");

    public static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor.Builder()
            .name("Jolt Specification")
            .description("Jolt Specification for transform of JSON data.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(new JOLTSpecValidator())
            .required(true)
            .build();

    public static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("Jolt Transformation")
            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(SHIFTR, CHAINR, DEFAULTR, REMOVR)
            .defaultValue(CHAINR.getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON), it will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private Transform transform;


    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(JOLT_TRANSFORM);
        properties.add(JOLT_SPEC);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
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
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);

        try {

            FlowFile transformed = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream out) throws IOException {

                    try (final InputStream in = new BufferedInputStream(rawIn)) {
                        Object inputJson = JsonUtils.jsonToObject(in);
                        Object transformedJson = transform.transform(inputJson);
                        out.write(JsonUtils.toJsonString(transformedJson).getBytes());
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }

                }
            });

            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.info("Transformed {}", new Object[]{original});

        } catch (ProcessException e) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }

    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        Object specJson = JsonUtils.jsonToObject(context.getProperty(JOLT_SPEC).getValue());
        transform = TransformationFactory.getTransform(context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
    }

    private static class TransformationFactory {

        static Transform getTransform(String transform, Object specJson) {

            if (transform.equals(DEFAULTR.getValue())) {
                return new Defaultr(specJson);
            } else if (transform.equals(SHIFTR.getValue())) {
                return new Shiftr(specJson);
            } else if (transform.equals(REMOVR.getValue())) {
                return new Removr(specJson);
            } else {
                return Chainr.fromSpec(specJson);
            }
        }
    }

    private static class JOLTSpecValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {

            try {
                Object specJson = JsonUtils.jsonToObject(input);
                TransformationFactory.getTransform(context.getProperty(JOLT_TRANSFORM).getValue(), specJson);
            } catch (final Exception e) {
                String message = "Input is not a valid JOLT specification - " + e.getMessage();

                return new ValidationResult.Builder()
                        .input(input).subject(subject).valid(false)
                        .explanation(message)
                        .build();
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    }


}
