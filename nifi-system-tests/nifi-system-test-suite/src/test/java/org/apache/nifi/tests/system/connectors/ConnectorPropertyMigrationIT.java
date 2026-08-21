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

package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end system test for {@code Connector.migrateProperties}. A pre-migration
 * {@code MigratePropertiesConnector} fixture in {@code nifi-system-test-extensions-nar} is created and configured
 * through the REST API using legacy step / property names. NiFi is then stopped, the system-test extensions NARs are
 * swapped for {@code nifi-alternate-config-extensions-nar} (which contains a same-simple-typed-name fixture that
 * implements {@code migrateProperties}), and NiFi is restarted. The framework's {@code StandardConnectorRepository}
 * sync path invokes {@code StandardConnectorNode.inheritConfiguration}, which drives
 * {@code Connector.migrateProperties}. The test asserts that the persisted active and working configurations both
 * reflect the migrated step and property names / values.
 */
public class ConnectorPropertyMigrationIT extends AbstractNarSwapMigrationIT {

    private static final String CONNECTOR_TYPE_SIMPLE_NAME = "MigratePropertiesConnector";

    private static final String LEGACY_STEP_NAME = "Legacy Step";
    private static final String LEGACY_BROKER_URL_NAME = "legacy-broker-url";
    private static final String LEGACY_CREDENTIALS_NAME = "legacy-credentials";
    private static final String LEGACY_OBSOLETE_NAME = "legacy-obsolete";

    private static final String MIGRATED_STEP_NAME = "Kafka Connection";
    private static final String BROKER_URL_NAME = "Broker URL";
    private static final String CREDENTIALS_NAME = "Credentials";
    private static final String CLIENT_ID_NAME = "Client Id";

    private static final String BROKER_URL_VALUE = "kafka-broker:9092";
    private static final String OBSOLETE_VALUE = "gone";
    private static final String CLIENT_ID_MIGRATED_VALUE = "auto-migrated";

    private static final String SECRET_NAME = "supersecret";
    private static final String FULLY_QUALIFIED_SECRET_NAME = "PropertiesParameterProvider.Parameters.supersecret";
    private static final String STRING_LITERAL_TYPE = "STRING_LITERAL";
    private static final String SECRET_REFERENCE_TYPE = "SECRET_REFERENCE";

    @Test
    public void testConnectorPropertyMigration() throws NiFiClientException, IOException, InterruptedException {
        // Stand up a Parameter Provider with a single secret so that the legacy secret property can be configured with
        // a SECRET_REFERENCE that survives the migration.
        final ParameterProviderEntity paramProvider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        getClientUtil().updateParameterProviderProperties(paramProvider, Map.of("parameters", SECRET_NAME + "=" + SECRET_NAME));

        // Create the pre-migration connector and configure the legacy step with a string literal, a secret reference,
        // and an obsolete string literal that will be removed by migration.
        final ConnectorEntity connector = getClientUtil().createConnector(CONNECTOR_TYPE_SIMPLE_NAME);
        assertNotNull(connector);

        final ConnectorValueReferenceDTO brokerUrlRef = createStringLiteralReference(BROKER_URL_VALUE);
        final ConnectorValueReferenceDTO credentialsRef = getClientUtil().createSecretValueReference(paramProvider.getId(),
            SECRET_NAME, FULLY_QUALIFIED_SECRET_NAME);
        final ConnectorValueReferenceDTO obsoleteRef = createStringLiteralReference(OBSOLETE_VALUE);
        final Map<String, ConnectorValueReferenceDTO> legacyProperties = Map.of(
            LEGACY_BROKER_URL_NAME, brokerUrlRef,
            LEGACY_CREDENTIALS_NAME, credentialsRef,
            LEGACY_OBSOLETE_NAME, obsoleteRef
        );
        getClientUtil().configureConnectorWithReferences(connector.getId(), LEGACY_STEP_NAME, legacyProperties);
        getClientUtil().applyConnectorUpdate(connector);

        // Sanity check: pre-restart state still has the legacy schema before we swap NARs.
        final ConnectorEntity beforeRestart = getNifiClient().getConnectorClient().getConnector(connector.getId());
        final Map<String, ConnectorValueReferenceDTO> legacyActive = propertyValues(beforeRestart.getComponent().getActiveConfiguration(), LEGACY_STEP_NAME);
        assertEquals(BROKER_URL_VALUE, legacyActive.get(LEGACY_BROKER_URL_NAME).getValue());
        assertEquals(SECRET_REFERENCE_TYPE, legacyActive.get(LEGACY_CREDENTIALS_NAME).getValueType());
        assertEquals(OBSOLETE_VALUE, legacyActive.get(LEGACY_OBSOLETE_NAME).getValue());

        // Swap out the system-test-extensions NARs for the alternate-config NAR that defines the same simple-typed
        // MigratePropertiesConnector with the migrated schema and a migrateProperties implementation.
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);

        // On restart, StandardConnectorRepository.syncConnector -> StandardConnectorNode.inheritConfiguration invokes
        // Connector.migrateProperties for both the active and working configurations. Assert that each context now
        // reflects the renamed step, renamed / removed / added properties, and that typed value references survive.
        final ConnectorEntity afterRestart = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertMigratedConfiguration(afterRestart.getComponent().getActiveConfiguration(), "active");
        assertMigratedConfiguration(afterRestart.getComponent().getWorkingConfiguration(), "working");

        getClientUtil().waitForValidConnector(connector.getId());
    }

    private void assertMigratedConfiguration(final ConnectorConfigurationDTO config, final String label) {
        final List<ConfigurationStepConfigurationDTO> steps = config.getConfigurationStepConfigurations();
        assertNotNull(steps, label + " configuration is missing steps");
        assertTrue(steps.stream().noneMatch(step -> LEGACY_STEP_NAME.equals(step.getConfigurationStepName())),
            label + " configuration still contains legacy step");

        final Map<String, ConnectorValueReferenceDTO> migrated = propertyValues(config, MIGRATED_STEP_NAME);

        final ConnectorValueReferenceDTO brokerUrl = migrated.get(BROKER_URL_NAME);
        assertNotNull(brokerUrl, label + " configuration is missing " + BROKER_URL_NAME);
        assertEquals(STRING_LITERAL_TYPE, brokerUrl.getValueType());
        assertEquals(BROKER_URL_VALUE, brokerUrl.getValue());

        final ConnectorValueReferenceDTO credentials = migrated.get(CREDENTIALS_NAME);
        assertNotNull(credentials, label + " configuration is missing " + CREDENTIALS_NAME);
        assertEquals(SECRET_REFERENCE_TYPE, credentials.getValueType());
        assertEquals(SECRET_NAME, credentials.getSecretName());

        final ConnectorValueReferenceDTO clientId = migrated.get(CLIENT_ID_NAME);
        assertNotNull(clientId, label + " configuration is missing " + CLIENT_ID_NAME);
        assertEquals(STRING_LITERAL_TYPE, clientId.getValueType());
        assertEquals(CLIENT_ID_MIGRATED_VALUE, clientId.getValue());

        assertFalse(migrated.containsKey(LEGACY_OBSOLETE_NAME));
        assertFalse(migrated.containsKey(LEGACY_BROKER_URL_NAME));
        assertFalse(migrated.containsKey(LEGACY_CREDENTIALS_NAME));
    }

    private Map<String, ConnectorValueReferenceDTO> propertyValues(final ConnectorConfigurationDTO config, final String stepName) {
        final ConfigurationStepConfigurationDTO step = config.getConfigurationStepConfigurations().stream()
            .filter(s -> stepName.equals(s.getConfigurationStepName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Configuration step not found: " + stepName));

        final List<PropertyGroupConfigurationDTO> groups = step.getPropertyGroupConfigurations();
        assertNotNull(groups, "No property groups on step " + stepName);
        assertFalse(groups.isEmpty(), "No property groups on step " + stepName);
        return groups.getFirst().getPropertyValues();
    }

    private ConnectorValueReferenceDTO createStringLiteralReference(final String value) {
        final ConnectorValueReferenceDTO valueRef = new ConnectorValueReferenceDTO();
        valueRef.setValueType(STRING_LITERAL_TYPE);
        valueRef.setValue(value);
        return valueRef;
    }
}
