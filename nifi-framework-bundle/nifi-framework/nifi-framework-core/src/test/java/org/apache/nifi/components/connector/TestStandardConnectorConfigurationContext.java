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

package org.apache.nifi.components.connector;

import org.apache.nifi.asset.AssetManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class TestStandardConnectorConfigurationContext {
    private StandardConnectorConfigurationContext context;

    @BeforeEach
    public void setUp() {
        final AssetManager assetManager = mock(AssetManager.class);
        final SecretsManager secretsManager = mock(SecretsManager.class);
        context = new StandardConnectorConfigurationContext(assetManager, secretsManager);
    }

    private StepConfiguration toStepConfiguration(final Map<String, String> stringProperties) {
        final Map<String, ConnectorValueReference> valueReferences = new HashMap<>();
        for (final Map.Entry<String, String> entry : stringProperties.entrySet()) {
            final String value = entry.getValue();
            valueReferences.put(entry.getKey(), value == null ? null : new StringLiteralValue(value));
        }
        return new StepConfiguration(valueReferences);
    }

    @Test
    public void testSetPropertiesWithNoExistingConfigurations() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        context.setProperties("step1", toStepConfiguration(properties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value2", context.getProperty("step1", "key2").getValue());
    }

    @Test
    public void testSetPropertiesAddsNewProperties() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        final Map<String, String> newProperties = new HashMap<>();
        newProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(newProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesMergesExistingPropertiesWithNewValues() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", "updatedValue2");
        updatedProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(updatedProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesRemovesKeyWhenValueIsNull() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        initialProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        assertEquals("value2", context.getProperty("step1", "key2").getValue());

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", null);
        context.setProperties("step1", toStepConfiguration(updatedProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertNull(context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesLeavesUnprovidedKeysAsIs() {
        final Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key1", "value1");
        initialProperties.put("key2", "value2");
        initialProperties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(initialProperties));

        final Map<String, String> updatedProperties = new HashMap<>();
        updatedProperties.put("key2", "updatedValue2");
        context.setProperties("step1", toStepConfiguration(updatedProperties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertEquals("updatedValue2", context.getProperty("step1", "key2").getValue());
        assertEquals("value3", context.getProperty("step1", "key3").getValue());
    }

    @Test
    public void testSetPropertiesForDifferentSteps() {
        final Map<String, String> step1Properties = new HashMap<>();
        step1Properties.put("key1", "value1");
        context.setProperties("step1", toStepConfiguration(step1Properties));

        final Map<String, String> step2Properties = new HashMap<>();
        step2Properties.put("key2", "value2");
        context.setProperties("step2", toStepConfiguration(step2Properties));

        assertEquals("value1", context.getProperty("step1", "key1").getValue());
        assertNull(context.getProperty("step1", "key2").getValue());
        assertEquals("value2", context.getProperty("step2", "key2").getValue());
        assertNull(context.getProperty("step2", "key1").getValue());
    }

    @Test
    public void testGetPropertyReturnsEmptyForNonExistentStep() {
        final ConnectorPropertyValue propertyValue = context.getProperty("nonExistentStep", "someProperty");
        assertNull(propertyValue.getValue());
    }

    @Test
    public void testGetPropertyReturnsEmptyForNonExistentProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        context.setProperties("step1", toStepConfiguration(properties));

        final ConnectorPropertyValue propertyValue = context.getProperty("step1", "nonExistentProperty");
        assertNull(propertyValue.getValue());
    }

    @Test
    public void testComplexMergingScenario() {
        final Map<String, String> initialProps = new HashMap<>();
        initialProps.put("a", "1");
        initialProps.put("b", "2");
        initialProps.put("c", "3");
        initialProps.put("d", "4");
        initialProps.put("e", "5");
        initialProps.put("f", "6");
        context.setProperties("step1", toStepConfiguration(initialProps));

        final Map<String, String> updateProps = new HashMap<>();
        updateProps.put("b", null);
        updateProps.put("c", "30");
        updateProps.put("g", "7");
        updateProps.put("h", "8");
        context.setProperties("step1", toStepConfiguration(updateProps));

        assertEquals("1", context.getProperty("step1", "a").getValue());
        assertNull(context.getProperty("step1", "b").getValue());
        assertEquals("30", context.getProperty("step1", "c").getValue());
        assertEquals("4", context.getProperty("step1", "d").getValue());
        assertEquals("5", context.getProperty("step1", "e").getValue());
        assertEquals("6", context.getProperty("step1", "f").getValue());
        assertEquals("7", context.getProperty("step1", "g").getValue());
        assertEquals("8", context.getProperty("step1", "h").getValue());
    }

    @Test
    public void testCloneHasCorrectResolvedValues() {
        final Map<String, String> step1Properties = new HashMap<>();
        step1Properties.put("key1", "value1");
        step1Properties.put("key2", "value2");
        step1Properties.put("key3", "value3");
        context.setProperties("step1", toStepConfiguration(step1Properties));

        final Map<String, String> step2Properties = new HashMap<>();
        step2Properties.put("keyA", "valueA");
        step2Properties.put("keyB", "valueB");
        context.setProperties("step2", toStepConfiguration(step2Properties));

        final MutableConnectorConfigurationContext clonedContext = context.clone();

        assertEquals("value1", clonedContext.getProperty("step1", "key1").getValue());
        assertEquals("value2", clonedContext.getProperty("step1", "key2").getValue());
        assertEquals("value3", clonedContext.getProperty("step1", "key3").getValue());
        assertEquals("valueA", clonedContext.getProperty("step2", "keyA").getValue());
        assertEquals("valueB", clonedContext.getProperty("step2", "keyB").getValue());
        assertNull(clonedContext.getProperty("step1", "nonExistent").getValue());
        assertNull(clonedContext.getProperty("nonExistentStep", "key1").getValue());
    }
}
