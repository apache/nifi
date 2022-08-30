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

package org.apache.nifi.processors.tests.system;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class WriteRuntimePropertiesToFlowFile extends AbstractProcessor {
    static final PropertyDescriptor ALWAYS_OPTIONAL = new Builder()
            .name("Always Optional")
            .displayName("Always Optional")
            .description("This property is always optional")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor REQUIRED_IF_OPTIONAL_PROPERTY_SET = new Builder()
            .name("Required If Optional Property Set")
            .displayName("Required If Optional Property Set")
            .description("This property is required if and only if the 'Always Optional' property is set (to anything)")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(ALWAYS_OPTIONAL)
            .build();

    static final PropertyDescriptor REQUIRED_IF_OPTIONAL_IS_FOO = new Builder()
            .name("Required If Optional Property Set To Foo")
            .displayName("Required If Optional Property Set To Foo")
            .description("This property is required if and only if the 'Always Optional' property is set to the value 'foo'")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(ALWAYS_OPTIONAL, "foo")
            .build();

    static final PropertyDescriptor DEPENDS_ON_ALWAYS_OPTIONAL = new Builder()
            .name("Depends On Always Optional")
            .displayName("Depends On Always Optional")
            .description("This property depends on 'Always Optional' but is not required")
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(ALWAYS_OPTIONAL)
            .build();

    static final PropertyDescriptor DEPENDS_ON_DEPENDS_ON_ALWAYS_OPTIONAL = new Builder()
            .name("Depends On Depends On Always Optional")
            .displayName("Depends On Depends On Always Optional")
            .description("This property depends on 'Depends On Always Optional' but is not required")
            .addValidator(NON_EMPTY_VALIDATOR)
            .dependsOn(DEPENDS_ON_ALWAYS_OPTIONAL)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
                ALWAYS_OPTIONAL,
                REQUIRED_IF_OPTIONAL_PROPERTY_SET,
                REQUIRED_IF_OPTIONAL_IS_FOO,
                DEPENDS_ON_ALWAYS_OPTIONAL,
                DEPENDS_ON_DEPENDS_ON_ALWAYS_OPTIONAL
        );
    }

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(SUCCESS));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        final String allProperties = getFormattedPropertiesString(extractRuntimeProperties(context));
        flowFile = session.write(flowFile, out -> {
            out.write(allProperties.getBytes(StandardCharsets.UTF_8));
        });

        session.transfer(flowFile, SUCCESS);
    }

    private String getFormattedPropertiesString(final Map<String, String> properties) {
        final StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append(String.format("%s:%s,", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    /**
     * Utility function to return a map of property descriptor and their values. There will be three entries for each property,
     * one with the "-allProperties" suffix to indicate that it was fetched from the {@code ProcessContext.getAllProperties()},
     * one with "-usingPropertyDescriptor" suffix to indicate that it was fetched using {@code ProcessContext.getProperty(PropertyDescriptor)},
     * one with "-usingName" suffix to indicate that it was fetched using {@code ProcessContext.getProperty(String)}.
     * @param context the {@code ProcessContext}
     * @return a {@code Map} with all the properties from the {@code ProcessContext}
     */
    private Map<String, String> extractRuntimeProperties(final ProcessContext context) {
        final Map<String, String> propertyMap = new TreeMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final String descriptorName = entry.getKey().getName();
            propertyMap.put(String.format("%s-allProperties", descriptorName), entry.getValue());
            propertyMap.put(String.format("%s-usingPropertyDescriptor", descriptorName), context.getProperty(entry.getKey()).getValue());
            propertyMap.put(String.format("%s-usingName", descriptorName), context.getProperty(descriptorName).getValue());
        }
        return propertyMap;
    }
}
