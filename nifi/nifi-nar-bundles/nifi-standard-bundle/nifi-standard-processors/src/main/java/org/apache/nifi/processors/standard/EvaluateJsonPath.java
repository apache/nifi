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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
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
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
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

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the JsonPath evaluation are written to the FlowFile content or a FlowFile attribute; if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one JsonPath may be specified, and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder().name("matched").description("FlowFiles are routed to this relationship when the JsonPath is successfully evaluated and the FlowFile is modified as a result").build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder().name("unmatched").description("FlowFiles are routed to this relationship when the JsonPath does not match the content of the FlowFile and the Destination is set to flowfile-content").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles are routed to this relationship when the JsonPath cannot be evaluated against the content of the FlowFile; for instance, if the FlowFile is not valid JSON").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
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
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        final FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                // Parse the document once to support multiple path evaluations if specified
                Object document = Configuration.defaultConfiguration().jsonProvider().parse(in, StandardCharsets.UTF_8.displayName());
            }
        });
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
