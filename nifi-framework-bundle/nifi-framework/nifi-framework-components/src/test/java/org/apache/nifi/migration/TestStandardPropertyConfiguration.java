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

package org.apache.nifi.migration;

import org.apache.nifi.controller.service.ControllerServiceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardPropertyConfiguration {

    private static final String COMPONENT_NAME = "Test Component";
    private static final String SERVICE_PROPERTY = "service";
    private static final String SERVICE_PARAMETER_REFERENCE = "#{service-parameter}";
    private static final String PROPERTY_NAME = "a";
    private static final String PROPERTY_VALUE = "A";

    private Map<String, String> originalProperties;
    private ControllerServiceFactory controllerServiceFactory;
    private StandardPropertyConfiguration config;

    @BeforeEach
    public void setup() {
        originalProperties = new HashMap<>();
        originalProperties.put("a", "A");
        originalProperties.put("b", "B");
        originalProperties.put("c", null);

        controllerServiceFactory = new ControllerServiceFactory() {
            @Override
            public ControllerServiceCreationDetails getCreationDetails(final String implementationClassName, final Map<String, String> propertyValues) {
                return new ControllerServiceCreationDetails("id", implementationClassName, null, propertyValues, ControllerServiceCreationDetails.CreationState.SERVICE_TO_BE_CREATED);
            }

            @Override
            public ControllerServiceNode create(final ControllerServiceCreationDetails creationDetails) {
                return null;
            }
        };

        config = new StandardPropertyConfiguration(originalProperties, originalProperties, raw -> raw, COMPONENT_NAME, controllerServiceFactory);
    }

    @Test
    public void testParameterReferenceWithoutValueIsRawValueOnly() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(SERVICE_PROPERTY, SERVICE_PARAMETER_REFERENCE);
        rawProperties.put(PROPERTY_NAME, PROPERTY_VALUE);

        final Map<String, String> effectiveProperties = new HashMap<>();
        effectiveProperties.put(SERVICE_PROPERTY, null);
        effectiveProperties.put(PROPERTY_NAME, PROPERTY_VALUE);

        final StandardPropertyConfiguration parameterConfig = new StandardPropertyConfiguration(
                effectiveProperties, rawProperties, raw -> SERVICE_PARAMETER_REFERENCE.equals(raw) ? null : raw, COMPONENT_NAME, controllerServiceFactory);

        assertTrue(parameterConfig.hasProperty(SERVICE_PROPERTY));
        assertFalse(parameterConfig.isPropertySet(SERVICE_PROPERTY));
        assertEquals(Optional.empty(), parameterConfig.getPropertyValue(SERVICE_PROPERTY));
        assertEquals(Optional.of(SERVICE_PARAMETER_REFERENCE), parameterConfig.getRawPropertyValue(SERVICE_PROPERTY));

        assertTrue(parameterConfig.isPropertySet(PROPERTY_NAME));
        assertEquals(Optional.of(PROPERTY_VALUE), parameterConfig.getPropertyValue(PROPERTY_NAME));
        assertEquals(Optional.of(PROPERTY_VALUE), parameterConfig.getRawPropertyValue(PROPERTY_NAME));
    }

    @Test
    public void testGetOperations() {
        assertEquals(originalProperties, config.getProperties());
        assertFalse(config.isModified());

        assertEquals(Optional.of("A"), config.getPropertyValue("a"));
        assertEquals(Optional.empty(), config.getPropertyValue("other"));

        assertTrue(config.hasProperty("a"));
        assertTrue(config.hasProperty("c"));
        assertTrue(config.isPropertySet("a"));
        assertFalse(config.isPropertySet("c"));
    }

    @Test
    public void testSetProperty() {
        config.setProperty("a", "A");
        assertFalse(config.isModified());

        config.setProperty("a", "Another");
        assertTrue(config.isModified());

        assertEquals(Optional.of("Another"), config.getPropertyValue("a"));

        final Map<String, String> expectedProperties = new HashMap<>(originalProperties);
        expectedProperties.put("a", "Another");
        assertEquals(expectedProperties, config.getProperties());

        // Ensure that the original map was not modified
        assertNotEquals(originalProperties, config.getProperties());
    }

    @Test
    public void testRenamePopulatedProperty() {
        assertTrue(config.renameProperty("a", "X"));

        assertTrue(config.isModified());
        assertFalse(config.hasProperty("a"));
        assertTrue(config.hasProperty("X"));
        assertFalse(config.isPropertySet("a"));
        assertTrue(config.isPropertySet("X"));

        final Map<String, String> expectedProperties = new HashMap<>(originalProperties);
        expectedProperties.remove("a");
        expectedProperties.put("X", "A");
        assertEquals(expectedProperties, config.getProperties());
    }

    @Test
    public void testRenamePropertyToSameName() {
        assertFalse(config.renameProperty("a", "a"));

        assertFalse(config.isModified());
        assertTrue(config.hasProperty("a"));
        assertTrue(config.isPropertySet("a"));

        final Map<String, String> expectedProperties = new HashMap<>(originalProperties);
        assertEquals(expectedProperties, config.getProperties());
    }

    @Test
    public void testRenameNullProperty() {
        assertTrue(config.renameProperty("c", "X"));

        assertTrue(config.isModified());
        assertFalse(config.hasProperty("c"));
        assertTrue(config.hasProperty("X"));
        assertFalse(config.isPropertySet("c"));
        assertFalse(config.isPropertySet("X"));

        final Map<String, String> expectedProperties = new HashMap<>(originalProperties);
        expectedProperties.remove("c");
        expectedProperties.put("X", null);
        assertEquals(expectedProperties, config.getProperties());
    }

    @Test
    public void testRenameUnsetProperty() {
        assertFalse(config.renameProperty("new-property", "X"));

        assertFalse(config.isModified());
        assertFalse(config.hasProperty("new-property"));
        assertFalse(config.isPropertySet("new-property"));
        assertEquals(originalProperties, config.getProperties());
    }

    @Test
    public void testRemoveProperty() {
        assertFalse(config.removeProperty("X"));
        assertFalse(config.isModified());

        assertTrue(config.removeProperty("a"));
        assertTrue(config.isModified());
    }

}
