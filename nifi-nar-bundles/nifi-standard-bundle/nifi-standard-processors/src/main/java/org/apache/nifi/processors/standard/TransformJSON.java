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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import com.bazaarvoice.jolt.CardinalityTransform;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Sortr;
import com.bazaarvoice.jolt.Transform;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"json", "jolt", "transform", "shiftr", "chainr", "defaultr", "removr","cardinality","sort"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute = "mime.type",description = "Always set to application/json")
@CapabilityDescription("Applies a list of JOLT specifications to the flowfile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the JSON transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class TransformJSON extends AbstractProcessor {

    public static final AllowableValue SHIFTR = new AllowableValue("jolt-transform-shift", "Shift Transform DSL", "Shift input JSON/data to create the output JSON.");
    public static final AllowableValue CHAINR = new AllowableValue("jolt-transform-chain", "Chain Transform DSL", "Execute list of JOLT transformations.");
    public static final AllowableValue DEFAULTR = new AllowableValue("jolt-transform-default", "Default Transform DSL", " Apply default values to the output JSON.");
    public static final AllowableValue REMOVR = new AllowableValue("jolt-transform-remove", "Remove Transform DSL", " Remove values from input data to create the output JSON.");
    public static final AllowableValue CARDINALITY = new AllowableValue("jolt-transform-card", "Cardinality Transform DSL", "Change the cardinality of input elements to create the output JSON.");
    public static final AllowableValue SORTR = new AllowableValue("jolt-transform-sort", "Sort Transform DSL", "Sort input json key values alphabetically. Any specification set is ignored.");

    public static final PropertyDescriptor JOLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jolt-transform")
            .displayName("Jolt Transformation")
            .description("Specifies the Jolt Transformation that should be used with the provided specification.")
            .required(true)
            .allowableValues(CARDINALITY, CHAINR, DEFAULTR, REMOVR, SHIFTR, SORTR)
            .defaultValue(CHAINR.getValue())
            .build();

    public static final PropertyDescriptor JOLT_SPEC = new PropertyDescriptor.Builder()
            .name("jolt-spec")
            .displayName("Jolt Specification")
            .description("Jolt Specification for transform of JSON data. This value is ignored if the Jolt Sort Transformation is selected.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
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
    private volatile Transform transform;
    private final static String DEFAULT_CHARSET = "UTF-8";

    static{

        final List<PropertyDescriptor> _properties = new ArrayList<>();
        _properties.add(JOLT_TRANSFORM);
        _properties.add(JOLT_SPEC);
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
        String transform = validationContext.getProperty(JOLT_TRANSFORM).getValue();
        String specValue = validationContext.getProperty(JOLT_SPEC).isSet() ? validationContext.getProperty(JOLT_SPEC).getValue() : null;

        if(StringUtils.isEmpty(specValue)){
            if(!SORTR.getValue().equals(transform)) {
                String message = "A specification is required for this transformation";
                results.add(new ValidationResult.Builder().valid(false)
                        .explanation(message)
                        .build());
            }
        } else {
            try {
                Object specJson = JsonUtils.jsonToObject(specValue, DEFAULT_CHARSET);
                TransformationFactory.getTransform(transform, specJson);
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

        final byte[] originalContent = new byte[(int) original.getSize()];
        session.read(original, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, originalContent, true);
            }
        });

        final String jsonString;

        try {
            final ByteArrayInputStream bais = new ByteArrayInputStream(originalContent);
            final Object inputJson = JsonUtils.jsonToObject(bais);
            final Object transformedJson = transform.transform(inputJson);
            jsonString = JsonUtils.toJsonString(transformedJson);

        } catch (RuntimeException re) {
            logger.error("Unable to transform {} due to {}", new Object[]{original, re});
            session.transfer(original, REL_FAILURE);
            return;
        }

        FlowFile transformed = session.write(original, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(jsonString.getBytes(DEFAULT_CHARSET));
            }
        });

        final String transformType = context.getProperty(JOLT_TRANSFORM).getValue();
        transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(),"application/json");
        session.transfer(transformed, REL_SUCCESS);
        session.getProvenanceReporter().modifyContent(transformed,"Modified With " + transformType ,stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        logger.info("Transformed {}", new Object[]{original});

    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        Object specJson = null;
        if(context.getProperty(JOLT_SPEC).isSet()){
            specJson = JsonUtils.jsonToObject(context.getProperty(JOLT_SPEC).getValue(), DEFAULT_CHARSET);
        }
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
            } else if (transform.equals(CARDINALITY.getValue())) {
                return new CardinalityTransform(specJson);
            } else if(transform.equals(SORTR.getValue())){
                return new Sortr();
            } else {
                return Chainr.fromSpec(specJson);
            }
        }

    }

}
