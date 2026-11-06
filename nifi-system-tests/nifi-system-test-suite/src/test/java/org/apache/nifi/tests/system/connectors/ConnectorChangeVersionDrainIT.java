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

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-node upgrade-drain scenarios for a version change.
 */
public class ConnectorChangeVersionDrainIT extends AbstractChangeVersionDrainIT {

    private static final String UPGRADE_SECRET = "Upgrade Secret";
    private static final String PROVIDER_NAME = "PropertiesParameterProvider";
    private static final String SECRET_NAME = "upgrade-secret";
    private static final String SECRET_FULLY_QUALIFIED_NAME = PROVIDER_NAME + ".Parameters." + SECRET_NAME;

    @Test
    @Timeout(360)
    public void testPreUpgradeParameterSecretIsRefreshedAndCanBeRestored() throws Exception {
        final SecretConnector secretConnector = createSecretConnector("original-secret");
        changeVersion(secretConnector.connectorId(), DRAIN_V2);
        getClientUtil().updateParameterProviderProperties(secretConnector.parameterProvider(), Map.of("parameters", SECRET_NAME + "=rotated-secret"));

        restartKeepingIncomingVersion();
        requestStart(secretConnector.connectorId());
        getClientUtil().waitForConnectorState(secretConnector.connectorId(), ConnectorState.RUNNING);
        assertNotPending(secretConnector.connectorId());
    }

    @Test
    @Timeout(360)
    public void testPendingUpgradeEditsThenStartsWithoutReEvolving() throws Exception {
        final String connectorId = queueStoppedUpgrade("INHERITED_DRAIN", true, "original");
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        triggerUpgrade(connectorId);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);

        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", true, "working-edit"));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
        assertNotNull(findProcessor(persisted.getManagedProcessGroup(), "TerminateFlowFile"));
        getNiFiInstance().start(true);
        waitForCluster();

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertEquals("working-edit", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
        assertTrue(findProcessor(connectorId, "GenerateFlowFile").getComponent().getBundle().getArtifact().contains("nifi-system-test-extensions2"));

        getNiFiInstance().stop();
        final VersionedConnector evolved = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertManagedFlowBundle(evolved, incomingBundle());
        assertEquals(VersionedConnectorState.RUNNING, evolved.getScheduledState());
        assertEquals("working-edit", configurationProperty(evolved.getActiveFlowConfiguration(), "Comment"));
        assertEquals("working-edit", configurationProperty(evolved.getWorkingFlowConfiguration(), "Comment"));
        assertEquals("working-edit", findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getComments());
        assertTrue(findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        getNiFiInstance().start(true);
        waitForCluster();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);

        getClientUtil().stopConnector(connectorId);
        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", true, "after-evolution"));
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        getClientUtil().waitForValidConnector(connectorId);
        assertEquals("after-evolution", findProcessor(connectorId, "GenerateFlowFile").getComponent().getConfig().getComments());
    }

    @Test
    @Timeout(360)
    public void testPendingUpgradeClearsWhenBundleMatchesManagedFlow() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);

        changeVersion(connectorId, DRAIN_V2);
        assertPending(connectorId);
        changeVersion(connectorId, DRAIN_V1);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(DRAIN_V1, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        getClientUtil().waitForValidConnector(connectorId);

        createGates();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, outgoingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testRestartDuringDrainingComesBackPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        deleteGates();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        restartKeepingIncomingVersion();
        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(firstNode(), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testInvalidEvolutionResultLeavesUpgradePending() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);

        final Map<String, String> violations = new LinkedHashMap<>();
        violations.put("NULL_FUTURE", "must return a CompletableFuture");
        violations.put("FAILED_FUTURE", "hook failed");
        violations.put("NULL", "Upgraded flow was not returned");
        violations.put("THROW", "active flow failed");
        for (final Map.Entry<String, String> violation : violations.entrySet()) {
            deleteNodeFiles(HOOK_RESULT_FILE);
            deleteNodeFiles(ACTIVE_FLOW_FILE);
            if ("NULL".equals(violation.getKey()) || "THROW".equals(violation.getKey())) {
                writeNodeFiles(ACTIVE_FLOW_FILE, violation.getKey());
            } else {
                writeNodeFiles(HOOK_RESULT_FILE, violation.getKey());
            }

            requestStart(connectorId);
            assertStoppedPending(connectorId, violation.getValue());
            assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
            assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
            assertPersistedOutgoing(connectorId);
        }
    }

    @Test
    @Timeout(240)
    public void testChangeVersionRejectedWhileDraining() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        deleteGates();
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        assertNotNull(actionReason(connectorId, "CHANGE_VERSION"));
        assertThrows(NiFiClientException.class, () -> changeVersion(connectorId, DRAIN_V1));
        assertConnectorState(connectorId, ConnectorState.DRAINING);
        assertEquals(DRAIN_V2, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());

        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertPending(connectorId);
        assertNull(actionReason(connectorId, "CHANGE_VERSION"));
    }

    private SecretConnector createSecretConnector(final String secretValue) throws Exception {
        final ParameterProviderEntity parameterProvider = getClientUtil().createParameterProvider(PROVIDER_NAME);
        final ParameterProviderEntity configuredProvider = getClientUtil().updateParameterProviderProperties(
            parameterProvider, Map.of("parameters", SECRET_NAME + "=" + secretValue));
        final ConnectorEntity connector = getClientUtil().createConnector(
            DRAIN_CONNECTOR_TYPE, DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, DRAIN_V1);

        final Map<String, ConnectorValueReferenceDTO> propertyReferences = new HashMap<>();
        for (final Map.Entry<String, String> property : properties("NO_OP", false, null).entrySet()) {
            final ConnectorValueReferenceDTO reference = new ConnectorValueReferenceDTO();
            reference.setValueType("STRING_LITERAL");
            reference.setValue(property.getValue());
            propertyReferences.put(property.getKey(), reference);
        }
        propertyReferences.put(UPGRADE_SECRET, getClientUtil().createSecretValueReference(
            configuredProvider.getId(), SECRET_NAME, SECRET_FULLY_QUALIFIED_NAME));

        getClientUtil().configureConnectorWithReferences(connector, "Upgrade", propertyReferences);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
        return new SecretConnector(connector.getId(), configuredProvider);
    }

    private void assertStoppedPending(final String connectorId, final String validationError) throws Exception {
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.STOPPED);
        assertPending(connectorId);
        if (validationError == null) {
            return;
        }
        final String errors = getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getValidationErrors().toString();
        assertTrue(errors.contains(validationError), errors);
    }

    private record SecretConnector(String connectorId, ParameterProviderEntity parameterProvider) {
    }
}
