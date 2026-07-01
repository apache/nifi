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

package org.apache.nifi.mock.connector.migration;

import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.migration.ConnectorStepPropertyConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMockConnectorPropertyConfiguration {

    private static final String STEP_ONE = "Step One";
    private static final String STEP_TWO = "Step Two";

    private static MockConnectorPropertyConfiguration newConfig() {
        final Map<String, ConnectorValueReference> stepOne = new HashMap<>();
        stepOne.put("literal", new StringLiteralValue("value"));
        stepOne.put("secret", new SecretReference("provider-id", "Provider", "top-secret", "provider:top-secret"));

        final Map<String, ConnectorValueReference> stepTwo = new HashMap<>();
        stepTwo.put("other", new StringLiteralValue("value-two"));

        final Map<String, StepConfiguration> initial = new HashMap<>();
        initial.put(STEP_ONE, new StepConfiguration(stepOne));
        initial.put(STEP_TWO, new StepConfiguration(stepTwo));

        return MockConnectorPropertyConfiguration.fromValueReferences(initial);
    }

    @Test
    public void testFromStringLiteralsWrapsInStringLiteralValue() {
        final Map<String, MockConnectorPropertyConfiguration.StepLiteralProperties> initial =
            Map.of(STEP_ONE, new MockConnectorPropertyConfiguration.StepLiteralProperties().add("greeting", "Hi"));

        final MockConnectorPropertyConfiguration config = MockConnectorPropertyConfiguration.fromStringLiterals(initial);

        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        assertTrue(stepOne.hasProperty("greeting"));
        assertInstanceOf(StringLiteralValue.class, stepOne.getValueReference("greeting").orElseThrow());
        assertEquals("Hi", stepOne.getPropertyValue("greeting").orElseThrow());
    }

    @Test
    public void testRenamePropertyRecordsOnlyWhenMutated() {
        final MockConnectorPropertyConfiguration config = newConfig();
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);

        assertFalse(stepOne.renameProperty("missing", "renamed"), "Rename of missing property should return false");
        assertFalse(stepOne.renameProperty("literal", "literal"), "Same-name rename should return false");

        MockConnectorPropertyConfiguration.MigrationResult afterNoops = config.toMigrationResult();
        assertTrue(afterNoops.propertiesRenamed().isEmpty(),
                "No-op renames should not be recorded in propertiesRenamed()");

        assertTrue(stepOne.renameProperty("literal", "renamed"), "Real rename should return true");

        MockConnectorPropertyConfiguration.MigrationResult afterRealRename = config.toMigrationResult();
        assertEquals(new MockConnectorPropertyConfiguration.StepPropertyRenames().add("literal", "renamed"),
                afterRealRename.propertiesRenamed().get(STEP_ONE));
    }

    @Test
    public void testRemovePropertyRecordsOnlyWhenMutated() {
        final MockConnectorPropertyConfiguration config = newConfig();
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);

        assertFalse(stepOne.removeProperty("missing"), "Remove of missing property should return false");

        MockConnectorPropertyConfiguration.MigrationResult afterNoop = config.toMigrationResult();
        assertTrue(afterNoop.propertiesRemoved().isEmpty(),
                "No-op removes should not be recorded in propertiesRemoved()");

        assertTrue(stepOne.removeProperty("literal"), "Real remove should return true");

        MockConnectorPropertyConfiguration.MigrationResult afterRealRemove = config.toMigrationResult();
        assertEquals(Set.of("literal"), afterRealRemove.propertiesRemoved().get(STEP_ONE));
    }

    @Test
    public void testSetValueReferenceRecordsOnlyWhenValueChanges() {
        final MockConnectorPropertyConfiguration config = newConfig();
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);

        stepOne.setValueReference("literal", new StringLiteralValue("value"));

        MockConnectorPropertyConfiguration.MigrationResult afterSameValue = config.toMigrationResult();
        assertFalse(afterSameValue.propertiesUpdated().getOrDefault(STEP_ONE, Set.of()).contains("literal"),
                "Same-value set should not be recorded in propertiesUpdated()");

        stepOne.setValueReference("literal", new StringLiteralValue("different"));

        MockConnectorPropertyConfiguration.MigrationResult afterChange = config.toMigrationResult();
        assertEquals(Set.of("literal"), afterChange.propertiesUpdated().get(STEP_ONE));
    }

    @Test
    public void testGetOrCreateStepMapAddsToAddedSteps() {
        final MockConnectorPropertyConfiguration config = newConfig();

        config.forStep(STEP_ONE).setProperty("literal", "value-updated");
        assertFalse(config.toMigrationResult().addedSteps().contains(STEP_ONE),
                "Writing to an initial step should not add it to addedSteps()");

        config.forStep("Fresh Step").setProperty("prop", "value");
        assertTrue(config.toMigrationResult().addedSteps().contains("Fresh Step"),
                "Writing to a previously unknown step should add it to addedSteps()");
    }

    @Test
    public void testRenameStepCarriesTrackingMaps() {
        final MockConnectorPropertyConfiguration config = newConfig();
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);

        stepOne.renameProperty("literal", "renamed-literal");
        stepOne.removeProperty("secret");
        stepOne.setProperty("added", "value");

        assertTrue(config.renameStep(STEP_ONE, "Renamed Step"));

        MockConnectorPropertyConfiguration.MigrationResult result = config.toMigrationResult();
        assertEquals(new MockConnectorPropertyConfiguration.StepPropertyRenames().add("literal", "renamed-literal"),
                result.propertiesRenamed().get("Renamed Step"),
                "Renamed properties should follow the renamed step");
        assertEquals(Set.of("secret"), result.propertiesRemoved().get("Renamed Step"),
                "Removed properties should follow the renamed step");
        assertEquals(Set.of("added"), result.propertiesUpdated().get("Renamed Step"),
                "Updated properties should follow the renamed step");
        assertFalse(result.propertiesRenamed().containsKey(STEP_ONE),
                "Old step key should not remain in propertiesRenamed() after rename");
        assertEquals(Map.of(STEP_ONE, "Renamed Step"), result.renamedSteps());
    }

    @Test
    public void testRemoveStepClearsPerStepTracking() {
        final MockConnectorPropertyConfiguration config = newConfig();
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);

        stepOne.renameProperty("literal", "renamed-literal");
        stepOne.removeProperty("secret");
        stepOne.setProperty("added", "value");

        assertTrue(config.removeStep(STEP_ONE));

        MockConnectorPropertyConfiguration.MigrationResult result = config.toMigrationResult();
        assertEquals(Set.of(STEP_ONE), result.removedSteps());
        assertFalse(result.propertiesRenamed().containsKey(STEP_ONE),
                "propertiesRenamed() should not contain a removed step");
        assertFalse(result.propertiesRemoved().containsKey(STEP_ONE),
                "propertiesRemoved() should not contain a removed step");
        assertFalse(result.propertiesUpdated().containsKey(STEP_ONE),
                "propertiesUpdated() should not contain a removed step");
    }
}
