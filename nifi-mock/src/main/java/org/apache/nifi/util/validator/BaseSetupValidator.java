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
package org.apache.nifi.util.validator;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public abstract class BaseSetupValidator {
    public static final String MISSING_TAGS = "Missing @Tags annotation";
    public static final String MISSING_CAPABILITY_DESCRIPTION = "Missing @CapabilityDescription annotation";
    protected ConfigurableComponent component;

    public BaseSetupValidator() {}

    public BaseSetupValidator(ConfigurableComponent component) {
        this.component = component;

        validateBasicAnnotations();
        validatePropertyDescriptors();
        validateExistenceOfServicesConf();
        continueValidation();
    }

    /**
     * This method should only be used in the unit tests for nifi-mock. It's intended to allow these validation
     * components to be more easily split up for testing.
     *
     * @param component The component to be validated
     */
    public void setComponent(ConfigurableComponent component) {
        this.component = component;
    }

    /**
     * This method is intended to enforce minimal good practices on annotating a component
     * with documentation attributes
     */
    public void validateBasicAnnotations() {
        assertTrue(component.getClass().isAnnotationPresent(Tags.class), MISSING_TAGS);
        assertTrue(component.getClass().isAnnotationPresent(CapabilityDescription.class), MISSING_CAPABILITY_DESCRIPTION);
    }

    /**
     * This method is intended to check for property descriptors that might either not be present
     * in this list of supported descriptors or that have a problem like having the same property name.
     */
    public void validatePropertyDescriptors() {
        List<Field> fields = Arrays.asList(component.getClass().getDeclaredFields());
        List<Field> propertyDescriptors = fields
                .stream().filter(field -> field.getType() == PropertyDescriptor.class)
                .collect(Collectors.toList());
        if (!propertyDescriptors.isEmpty()) {
            Map<String, Integer> nameCounts = new HashMap<>();
            List<PropertyDescriptor> declaredDescriptors = new ArrayList<>();

            propertyDescriptors.forEach(descriptorField -> {
                try {
                    Object rawField = descriptorField.get(component);
                    PropertyDescriptor descriptor = (PropertyDescriptor) rawField;

                    countPropertyName(descriptor, nameCounts);

                    declaredDescriptors.add(descriptor);
                } catch (Exception ex) {
                    fail("An exception was thrown while retrieving a field on the component.");
                }
            });

            checkForDuplicates(nameCounts);

            try {
                List<PropertyDescriptor> values = component.getPropertyDescriptors();
                assertNotNull(values, "getSupportedPropertyDescriptors should not return null on " + component.getClass());
                assertFalse(values.isEmpty(), "getSupportedPropertyDescriptors should not return an empty list on " + component.getClass());

                nameCounts.clear();

                for (PropertyDescriptor descriptor : declaredDescriptors) {
                    assertTrue(values.contains(descriptor),
                            String.format("%s was not in the list returned by declaredDescriptors", descriptor.getName()));
                }

                values.forEach(value -> countPropertyName(value, nameCounts));

                checkForDuplicates(nameCounts);
            } catch (Exception ex) {
                fail("Could not call getSupportedPropertyDescriptors");
            }
        }
    }

    private void checkForDuplicates(Map<String, Integer> nameCounts) {
        List<Map.Entry<String, Integer>> filtered = nameCounts.entrySet()
                .stream().filter(entry -> entry.getValue() > 1).collect(Collectors.toList());
        if (!filtered.isEmpty()) {
            StringBuilder builder = new StringBuilder().append("Detected duplicate property names:\n");
            filtered.forEach(entry -> builder.append(entry.getKey()).append(" => ").append(entry.getValue()).append("\n"));
            fail(builder.toString());
        }
    }

    private void countPropertyName(PropertyDescriptor descriptor, Map<String, Integer> nameCounts) {
        String name = descriptor.getName();

        if (nameCounts.containsKey(name)) {
            Integer count = nameCounts.get(name);
            nameCounts.put(name, ++count);
        } else {
            nameCounts.put(name, 1);
        }
    }

    /**
     * Check for the existence of META-INF/services on the classpath. Its absence means that no components will be
     * visible to NiFi from the deployed NAR.
     */
    public void validateExistenceOfServicesConf() {
        assertNotNull(getClasspathResource("/META-INF/services"),
                "No META-INF/services folder found on the classpath");
    }

    protected InputStream getClasspathResourceAsInputStream(String path) {
        return component.getClass().getResourceAsStream(path);
    }

    protected URL getClasspathResource(String path) {
        return component.getClass().getResource(path);
    }

    /**
     * Break out additional validation by sub-component type
     */
    protected abstract void continueValidation();
}
