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

import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardConnectorPropertyConfiguration {

    private static final String STEP_ONE = "Step One";
    private static final String STEP_TWO = "Step Two";

    private StandardConnectorPropertyConfiguration config;

    @BeforeEach
    public void setup() {
        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        final Map<String, ConnectorValueReference> stepOne = new HashMap<>();
        stepOne.put("literal", new StringLiteralValue("value"));
        stepOne.put("secret", new SecretReference("provider-id", "Provider", "top-secret", "provider:top-secret"));
        stepOne.put("null-literal", StringLiteralValue.EMPTY);
        initial.put(STEP_ONE, stepOne);

        final Map<String, ConnectorValueReference> stepTwo = new HashMap<>();
        stepTwo.put("asset", new AssetReference(Set.of("asset-id-1")));
        initial.put(STEP_TWO, stepTwo);

        config = new StandardConnectorPropertyConfiguration(initial, "Test Connector");
    }

    @Test
    public void testInitialState() {
        assertFalse(config.isModified());
        assertEquals(Set.of(STEP_ONE, STEP_TWO), config.getStepNames());
        assertTrue(config.hasStep(STEP_ONE));
        assertFalse(config.hasStep("Missing Step"));
        assertTrue(config.getModifiedStepNames().isEmpty());
    }

    @Test
    public void testForStepReadOnlyLookups() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        assertEquals(STEP_ONE, stepOne.getStepName());
        assertTrue(stepOne.hasProperty("literal"));
        assertTrue(stepOne.isPropertySet("literal"));
        assertTrue(stepOne.hasProperty("null-literal"));
        assertFalse(stepOne.isPropertySet("null-literal"));
        assertEquals(Optional.of("value"), stepOne.getPropertyValue("literal"));
        assertEquals(Optional.empty(), stepOne.getPropertyValue("null-literal"));
        assertEquals(Optional.empty(), stepOne.getPropertyValue("secret"));
        assertInstanceOf(SecretReference.class, stepOne.getValueReference("secret").orElseThrow());

        final Map<String, String> literalProperties = stepOne.getProperties();
        assertEquals(2, literalProperties.size());
        assertEquals("value", literalProperties.get("literal"));
        assertTrue(literalProperties.containsKey("null-literal"));
        assertNull(literalProperties.get("null-literal"));
        assertEquals(3, stepOne.getValueReferences().size());
    }

    @Test
    public void testAddProperty() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        stepOne.setProperty("added", "value");

        assertTrue(config.isModified());
        assertEquals(Set.of(STEP_ONE), config.getModifiedStepNames());
        assertTrue(stepOne.hasProperty("added"));
        assertEquals(Optional.of("value"), stepOne.getPropertyValue("added"));
    }

    @Test
    public void testAddPropertyToNewStepLazyRegisters() {
        assertFalse(config.hasStep("New Step"));
        final ConnectorStepPropertyConfiguration newStep = config.forStep("New Step");

        assertFalse(config.hasStep("New Step"));
        assertFalse(config.isModified());

        newStep.setProperty("prop", "value");

        assertTrue(config.hasStep("New Step"));
        assertTrue(config.isModified());
        assertEquals(Optional.of("value"), config.forStep("New Step").getPropertyValue("prop"));
    }

    @Test
    public void testForStepWithoutWriteDoesNotRegister() {
        config.forStep("Unseen");
        assertFalse(config.hasStep("Unseen"));
        assertFalse(config.isModified());
    }

    @Test
    public void testRemoveProperty() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        assertFalse(stepOne.removeProperty("missing"));
        assertFalse(config.isModified());

        assertTrue(stepOne.removeProperty("literal"));
        assertTrue(config.isModified());
        assertFalse(stepOne.hasProperty("literal"));
    }

    @Test
    public void testRenamePropertyPreservesStringLiteral() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        assertTrue(stepOne.renameProperty("literal", "renamed"));

        assertTrue(config.isModified());
        assertFalse(stepOne.hasProperty("literal"));
        assertEquals(Optional.of("value"), stepOne.getPropertyValue("renamed"));
        assertInstanceOf(StringLiteralValue.class, stepOne.getValueReference("renamed").orElseThrow());
    }

    @Test
    public void testRenamePropertyPreservesSecretReference() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        final ConnectorValueReference originalSecret = stepOne.getValueReference("secret").orElseThrow();

        assertTrue(stepOne.renameProperty("secret", "renamed-secret"));

        final ConnectorValueReference renamedSecret = stepOne.getValueReference("renamed-secret").orElseThrow();
        assertInstanceOf(SecretReference.class, renamedSecret);
        assertSame(originalSecret, renamedSecret);
    }

    @Test
    public void testRenamePropertyPreservesAssetReference() {
        final ConnectorStepPropertyConfiguration stepTwo = config.forStep(STEP_TWO);
        final ConnectorValueReference originalAsset = stepTwo.getValueReference("asset").orElseThrow();

        assertTrue(stepTwo.renameProperty("asset", "renamed-asset"));

        final ConnectorValueReference renamedAsset = stepTwo.getValueReference("renamed-asset").orElseThrow();
        assertInstanceOf(AssetReference.class, renamedAsset);
        assertSame(originalAsset, renamedAsset);
    }

    @Test
    public void testRenamePropertyToSameNameIsNoOp() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        assertFalse(stepOne.renameProperty("literal", "literal"));
        assertFalse(config.isModified());
    }

    @Test
    public void testRenameUnknownPropertyReturnsFalse() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        assertFalse(stepOne.renameProperty("missing", "renamed"));
        assertFalse(config.isModified());
    }

    @Test
    public void testSetValueReferenceForEachType() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        final SecretReference newSecret = new SecretReference("p", "P", "sec", "P:sec");
        stepOne.setValueReference("added-secret", newSecret);
        assertSame(newSecret, stepOne.getValueReference("added-secret").orElseThrow());

        final AssetReference newAsset = new AssetReference(Set.of("id-2"));
        stepOne.setValueReference("added-asset", newAsset);
        assertSame(newAsset, stepOne.getValueReference("added-asset").orElseThrow());

        final StringLiteralValue newLiteral = new StringLiteralValue("literal-value");
        stepOne.setValueReference("added-literal", newLiteral);
        assertEquals(Optional.of("literal-value"), stepOne.getPropertyValue("added-literal"));
    }

    @Test
    public void testSetValueReferenceNullRemovesProperty() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        stepOne.setValueReference("literal", null);
        assertFalse(stepOne.hasProperty("literal"));
        assertTrue(config.isModified());
    }

    @Test
    public void testRenameStepMovesAllProperties() {
        final Map<String, ConnectorValueReference> originalSnapshot = new HashMap<>(config.forStep(STEP_ONE).getValueReferences());

        assertTrue(config.renameStep(STEP_ONE, "Renamed"));

        assertTrue(config.isModified());
        assertFalse(config.hasStep(STEP_ONE));
        assertTrue(config.hasStep("Renamed"));
        final ConnectorStepPropertyConfiguration renamed = config.forStep("Renamed");
        assertEquals(originalSnapshot, renamed.getValueReferences());
        for (final Map.Entry<String, ConnectorValueReference> entry : originalSnapshot.entrySet()) {
            assertSame(entry.getValue(), renamed.getValueReference(entry.getKey()).orElse(null));
        }
    }

    @Test
    public void testRenameStepToSameNameIsNoOp() {
        assertFalse(config.renameStep(STEP_ONE, STEP_ONE));
        assertFalse(config.isModified());
    }

    @Test
    public void testRenameStepToExistingNameThrows() {
        assertThrows(IllegalStateException.class, () -> config.renameStep(STEP_ONE, STEP_TWO));
    }

    @Test
    public void testRenameUnknownStepReturnsFalse() {
        assertFalse(config.renameStep("Missing", "Whatever"));
        assertFalse(config.isModified());
    }

    @Test
    public void testRemoveStep() {
        assertFalse(config.removeStep("Missing"));
        assertFalse(config.isModified());

        assertTrue(config.removeStep(STEP_ONE));
        assertTrue(config.isModified());
        assertFalse(config.hasStep(STEP_ONE));
        assertFalse(config.getStepNames().contains(STEP_ONE));
    }

    @Test
    public void testGetPropertiesFiltersNonLiteralAndPreservesNulls() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        final Map<String, String> literals = stepOne.getProperties();
        assertEquals(2, literals.size());
        assertEquals("value", literals.get("literal"));
        assertTrue(literals.containsKey("null-literal"));
        assertNull(literals.get("null-literal"));
        assertFalse(literals.containsKey("secret"));
    }

    @Test
    public void testGetMutatedPropertiesReflectsFinalState() {
        final ConnectorStepPropertyConfiguration stepOne = config.forStep(STEP_ONE);
        stepOne.renameProperty("literal", "renamed");
        stepOne.removeProperty("secret");
        stepOne.setProperty("added", "new-value");
        config.removeStep(STEP_TWO);
        config.forStep("Fresh Step").setProperty("prop", "value");

        final Map<String, Map<String, ConnectorValueReference>> mutated = config.getMutatedProperties();
        assertEquals(Set.of(STEP_ONE, "Fresh Step"), mutated.keySet());

        final Map<String, ConnectorValueReference> stepOneMap = mutated.get(STEP_ONE);
        assertTrue(stepOneMap.containsKey("renamed"));
        assertFalse(stepOneMap.containsKey("literal"));
        assertFalse(stepOneMap.containsKey("secret"));
        assertTrue(stepOneMap.containsKey("added"));
        assertTrue(stepOneMap.containsKey("null-literal"));

        assertEquals(1, mutated.get("Fresh Step").size());
    }
}
