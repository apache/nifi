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

import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorCrudIT extends NiFiSystemIT {

    @Test
    public void testCreateConfigureRestart() throws NiFiClientException, IOException, InterruptedException {
        // Create Connector
        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");
        assertNotNull(connector);

        // Configure the connector and apply the configuration
        getClientUtil().configureConnector(connector, "Ignored Step", Map.of("Ignored Property", "Hello, World!"));
        getClientUtil().applyConnectorUpdate(connector);

        // Configure with a different value
        getClientUtil().configureConnector(connector, "Ignored Step", Map.of("Ignored Property", "Hola, Mundo!"));

        // Restart NiFi and ensure that we have the expected values for both the active and working configurations
        getNiFiInstance().stop();
        getNiFiInstance().start();

        // If running in cluster, wait for all nodes to be connected
        if (getNumberOfNodes() > 1) {
            waitForAllNodesConnected();
        }

        final ConnectorEntity connectorAfterRestart = getNifiClient().getConnectorClient().getConnector(connector.getId());
        assertNotNull(connectorAfterRestart);

        final ConnectorConfigurationDTO activeConfig = connectorAfterRestart.getComponent().getActiveConfiguration();
        final Map<String, ConnectorValueReferenceDTO> activeProperties = activeConfig.getConfigurationStepConfigurations().getFirst().getPropertyGroupConfigurations().getFirst().getPropertyValues();
        final String activeIgnoredProperty = activeProperties.get("Ignored Property").getValue();
        assertEquals("Hello, World!", activeIgnoredProperty);

        final ConnectorConfigurationDTO workingConfig = connectorAfterRestart.getComponent().getWorkingConfiguration();
        final Map<String, ConnectorValueReferenceDTO> workingProperties = workingConfig.getConfigurationStepConfigurations().getFirst().getPropertyGroupConfigurations().getFirst().getPropertyValues();
        final String workingIgnoredProperty = workingProperties.get("Ignored Property").getValue();
        assertEquals("Hola, Mundo!", workingIgnoredProperty);
    }

    @Test
    public void testConfigVerification() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");

        final List<ConfigVerificationResultDTO> resultDtos = getClientUtil().verifyConnectorStepConfig(connector.getId(), "Ignored Step",
            Map.of("Ignored Property", "Test Value"));
        assertNotNull(resultDtos);
        assertEquals(2, resultDtos.size());

        assertTrue(resultDtos.stream().allMatch(result -> Outcome.SUCCESSFUL.name().equals(result.getOutcome())));
        assertEquals("Property Validation", resultDtos.getFirst().getVerificationStepName());

        final ConfigVerificationResultDTO resultDto = resultDtos.get(1);
        assertEquals("Nop Verification", resultDto.getVerificationStepName());
        assertTrue(resultDto.getExplanation().contains("Test Value"));
    }

    @Test
    public void testSecretReferences() throws NiFiClientException, IOException, InterruptedException {
        // Create and configure a Parameter Provider with two secrets
        final ParameterProviderEntity paramProvider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        getClientUtil().updateParameterProviderProperties(paramProvider, Map.of("parameters", "supersecret=supersecret\nother=other"));

        // Create the Nop Connector
        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");
        assertNotNull(connector);

        // Verify that using a String Literal for a SECRET property should fail validation
        final Map<String, String> stringLiteralProperties = Map.of("Secret Property", "supersecret");
        List<ConfigVerificationResultDTO> verificationResults = getClientUtil().verifyConnectorStepConfig(connector.getId(), "Ignored Step", stringLiteralProperties);
        assertTrue(verificationResults.stream().anyMatch(result -> Outcome.FAILED.name().equals(result.getOutcome())));

        // Configure and apply the String Literal value, then wait for invalid
        getClientUtil().configureConnector(connector, "Ignored Step", stringLiteralProperties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForInvalidConnector(connector.getId());

        // Verify that using a Secret Reference to 'other' (value "other") should fail validation
        final ConnectorValueReferenceDTO otherSecretRef = getClientUtil().createSecretValueReference(paramProvider.getId(), "other", "Parameters.other");
        final Map<String, ConnectorValueReferenceDTO> otherSecretProperties = Map.of("Secret Property", otherSecretRef);
        verificationResults = getClientUtil().verifyConnectorStepConfigWithReferences(connector.getId(), "Ignored Step", otherSecretProperties);
        assertTrue(verificationResults.stream().anyMatch(result -> Outcome.FAILED.name().equals(result.getOutcome())));

        // Configure and apply the 'other' secret reference, then wait for invalid
        getClientUtil().configureConnectorWithReferences(connector.getId(), "Ignored Step", otherSecretProperties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForInvalidConnector(connector.getId());

        // Verify that using an invalid reference should fail validation
        final ConnectorValueReferenceDTO invalidRef = getClientUtil().createSecretValueReference(paramProvider.getId(), "nonexistent", "Parameters.nonexistent");
        final Map<String, ConnectorValueReferenceDTO> invalidSecretProperties = Map.of("Secret Property", invalidRef);
        verificationResults = getClientUtil().verifyConnectorStepConfigWithReferences(connector.getId(), "Ignored Step", invalidSecretProperties);
        assertTrue(verificationResults.stream().anyMatch(result -> Outcome.FAILED.name().equals(result.getOutcome())));

        // Configure and apply the invalid secret reference, then wait for invalid
        getClientUtil().configureConnectorWithReferences(connector.getId(), "Ignored Step", invalidSecretProperties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForInvalidConnector(connector.getId());

        // Verify that using a Secret Reference to 'supersecret' (value "supersecret") should pass validation
        final ConnectorValueReferenceDTO supersecretRef = getClientUtil().createSecretValueReference(paramProvider.getId(), "supersecret", "Parameters.supersecret");
        final Map<String, ConnectorValueReferenceDTO> supersecretProperties = Map.of("Secret Property", supersecretRef);
        verificationResults = getClientUtil().verifyConnectorStepConfigWithReferences(connector.getId(), "Ignored Step", supersecretProperties);
        assertTrue(verificationResults.stream().allMatch(result -> Outcome.SUCCESSFUL.name().equals(result.getOutcome())));

        // Configure and apply the 'supersecret' secret reference, then wait for valid
        getClientUtil().configureConnectorWithReferences(connector.getId(), "Ignored Step", supersecretProperties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
    }
}
