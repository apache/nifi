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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link org.apache.nifi.components.connector.Connector#migrateProperties(ConnectorPropertyConfiguration)}
 * from real {@link AbstractConnector} subclasses. Each scenario is expressed as a tiny standalone connector whose only
 * behaviour is a single {@code migrateProperties} override, driven against a live
 * {@link StandardConnectorPropertyConfiguration}, so the tests double as canonical usage examples for connector
 * authors.
 */
public class TestConnectorMigrateProperties {

    private static final String STEP_A = "Step A";
    private static final String LEGACY_STEP = "Legacy Step";
    private static final String OBSOLETE_STEP = "Obsolete Step";
    private static final String COMPONENT_DESCRIPTION = "Test Connector";

    @Test
    public void testAddProperty() {
        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        final Map<String, ConnectorValueReference> stepMap = new HashMap<>();
        stepMap.put("existing", new StringLiteralValue("keep"));
        initial.put(STEP_A, stepMap);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new AddPropertyConnector().migrateProperties(config);

        assertTrue(config.isModified());
        final Map<String, ConnectorValueReference> migrated = config.forStep(STEP_A).getValueReferences();
        assertTrue(migrated.containsKey("added"));
        assertInstanceOf(StringLiteralValue.class, migrated.get("added"));
        assertEquals("value", ((StringLiteralValue) migrated.get("added")).getValue());
        assertTrue(migrated.containsKey("existing"));
    }

    @Test
    public void testRemoveProperty() {
        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        final Map<String, ConnectorValueReference> stepMap = new HashMap<>();
        stepMap.put("legacy", new StringLiteralValue("unused"));
        stepMap.put("keep", new StringLiteralValue("kept"));
        initial.put(STEP_A, stepMap);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new RemovePropertyConnector().migrateProperties(config);

        assertTrue(config.isModified());
        assertFalse(config.forStep(STEP_A).hasProperty("legacy"));
        assertTrue(config.forStep(STEP_A).hasProperty("keep"));
    }

    @Test
    public void testRenamePropertyPreservesStringLiteral() {
        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        final Map<String, ConnectorValueReference> stepMap = new HashMap<>();
        stepMap.put("old", new StringLiteralValue("value"));
        initial.put(STEP_A, stepMap);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new RenamePropertyStringConnector().migrateProperties(config);

        assertTrue(config.isModified());
        final Map<String, ConnectorValueReference> migrated = config.forStep(STEP_A).getValueReferences();
        assertFalse(migrated.containsKey("old"));
        assertInstanceOf(StringLiteralValue.class, migrated.get("new"));
        assertEquals("value", ((StringLiteralValue) migrated.get("new")).getValue());
    }

    @Test
    public void testRenamePropertyPreservesSecretReference() {
        final SecretReference secret = new SecretReference("vault", "Vault", "credentials/path", "vault:credentials/path");
        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        final Map<String, ConnectorValueReference> stepMap = new HashMap<>();
        stepMap.put("old-secret", secret);
        initial.put(STEP_A, stepMap);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new RenamePropertySecretConnector().migrateProperties(config);

        assertTrue(config.isModified());
        final ConnectorValueReference migrated = config.forStep(STEP_A).getValueReference("credentials").orElseThrow();
        assertInstanceOf(SecretReference.class, migrated);
        assertSame(secret, migrated);
    }

    @Test
    public void testRenameStepCarriesAllPropertiesIntact() {
        final SecretReference secret = new SecretReference("vault", "Vault", "credentials/path", "vault:credentials/path");
        final Map<String, ConnectorValueReference> legacyProperties = new HashMap<>();
        legacyProperties.put("string-prop", new StringLiteralValue("string-value"));
        legacyProperties.put("secret-prop", secret);

        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        initial.put(LEGACY_STEP, legacyProperties);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new RenameStepConnector().migrateProperties(config);

        assertTrue(config.isModified());
        assertFalse(config.hasStep(LEGACY_STEP));
        assertTrue(config.hasStep(STEP_A));
        final Map<String, ConnectorValueReference> migrated = config.forStep(STEP_A).getValueReferences();
        assertEquals(legacyProperties, migrated);
        assertSame(secret, migrated.get("secret-prop"));
    }

    @Test
    public void testRemoveStepDropsAllProperties() {
        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        final Map<String, ConnectorValueReference> obsolete = new HashMap<>();
        obsolete.put("prop", new StringLiteralValue("value"));
        initial.put(OBSOLETE_STEP, obsolete);
        final Map<String, ConnectorValueReference> survivor = new HashMap<>();
        survivor.put("keep", new StringLiteralValue("kept"));
        initial.put(STEP_A, survivor);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new RemoveStepConnector().migrateProperties(config);

        assertTrue(config.isModified());
        assertFalse(config.hasStep(OBSOLETE_STEP));
        assertEquals(Set.of(STEP_A), config.getStepNames());
    }

    @Test
    public void testCompositeMigrationAcrossAllScenarios() {
        final SecretReference secret = new SecretReference("vault", "Vault", "credentials/path", "vault:credentials/path");
        final Map<String, ConnectorValueReference> legacyProperties = new HashMap<>();
        legacyProperties.put("old", new StringLiteralValue("string-value"));
        legacyProperties.put("old-secret", secret);
        legacyProperties.put("obsolete", new StringLiteralValue("drop-me"));

        final Map<String, ConnectorValueReference> obsoleteStep = new HashMap<>();
        obsoleteStep.put("prop", new StringLiteralValue("gone"));

        final Map<String, Map<String, ConnectorValueReference>> initial = new HashMap<>();
        initial.put(LEGACY_STEP, legacyProperties);
        initial.put(OBSOLETE_STEP, obsoleteStep);

        final StandardConnectorPropertyConfiguration config = new StandardConnectorPropertyConfiguration(initial, COMPONENT_DESCRIPTION);
        new FullHistoryConnector().migrateProperties(config);

        assertTrue(config.isModified());
        assertFalse(config.hasStep(LEGACY_STEP));
        assertFalse(config.hasStep(OBSOLETE_STEP));
        assertEquals(Set.of(STEP_A), config.getStepNames());

        final Map<String, ConnectorValueReference> migrated = config.forStep(STEP_A).getValueReferences();
        assertEquals(Set.of("new", "credentials", "added"), migrated.keySet());
        assertInstanceOf(StringLiteralValue.class, migrated.get("new"));
        assertEquals("string-value", ((StringLiteralValue) migrated.get("new")).getValue());
        assertSame(secret, migrated.get("credentials"));
        assertInstanceOf(StringLiteralValue.class, migrated.get("added"));
        assertEquals("value", ((StringLiteralValue) migrated.get("added")).getValue());
    }

    /**
     * Base connector for migration scenarios. Provides no-op implementations of every abstract member of
     * {@link org.apache.nifi.components.connector.Connector} and {@link AbstractConnector} other than
     * {@code migrateProperties}, so scenario subclasses only need to express the migration itself.
     */
    private abstract static class MigrationScenarioConnector extends AbstractConnector {
        @Override
        public VersionedExternalFlow getInitialFlow() {
            return null;
        }

        @Override
        public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
            return null;
        }

        @Override
        public List<ConfigurationStep> getConfigurationSteps() {
            return List.of();
        }

        @Override
        public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
            return List.of();
        }

        @Override
        public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        }

        @Override
        protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        }
    }

    private static final class AddPropertyConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.forStep(STEP_A).setProperty("added", "value");
        }
    }

    private static final class RemovePropertyConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.forStep(STEP_A).removeProperty("legacy");
        }
    }

    private static final class RenamePropertyStringConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.forStep(STEP_A).renameProperty("old", "new");
        }
    }

    private static final class RenamePropertySecretConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.forStep(STEP_A).renameProperty("old-secret", "credentials");
        }
    }

    private static final class RenameStepConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.renameStep(LEGACY_STEP, STEP_A);
        }
    }

    private static final class RemoveStepConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.removeStep(OBSOLETE_STEP);
        }
    }

    private static final class FullHistoryConnector extends MigrationScenarioConnector {
        @Override
        public void migrateProperties(final ConnectorPropertyConfiguration config) {
            config.renameStep(LEGACY_STEP, STEP_A);
            final ConnectorStepPropertyConfiguration stepA = config.forStep(STEP_A);
            stepA.renameProperty("old", "new");
            stepA.renameProperty("old-secret", "credentials");
            stepA.removeProperty("obsolete");
            stepA.setProperty("added", "value");
            config.removeStep(OBSOLETE_STEP);
        }
    }
}
