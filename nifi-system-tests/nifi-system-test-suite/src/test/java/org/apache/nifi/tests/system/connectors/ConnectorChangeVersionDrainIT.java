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
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedConnectorState;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.security.encryption.PropertyEncryptionEncoder;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    private static final String GENERATOR = "GenerateFlowFile";
    private static final String CANCELLED_EVOLUTION = "Flow evolution was cancelled";
    private static final String REWIRED_ENDPOINT = "Rewired Endpoint";
    private static final String DRAINING_GATE_FILE = "upgrade-drain-gate-draining.txt";

    /**
     * Restarts NiFi once with four Connectors whose upgrade is pending: one whose pre-upgrade parameter secret was rotated, one
     * with working edits and queued FlowFiles, one in Troubleshooting, and one draining with its own gate file.
     */
    @Test
    @Timeout(480)
    public void testRestartKeepsPendingUpgrades() throws Exception {
        final SecretConnector secretConnector = createSecretConnector("original-secret");
        final String secretConnectorId = secretConnector.connectorId();
        changeVersion(secretConnectorId, DRAIN_V2);
        getClientUtil().updateParameterProviderProperties(secretConnector.parameterProvider(), Map.of("parameters", SECRET_NAME + "=rotated-secret"));
        assertSecretPersistedAsReference(waitForPersistedConnector(1, secretConnectorId, connector -> incomingBundle().matches(connector.getBundle())));

        final String editedConnectorId = queueStoppedUpgrade("INHERITED_DRAIN", true, "original");
        final String editedConnectionId = findConnection(editedConnectorId, REMOVED_CONNECTION).getId();
        final int editedQueued = getConnectorQueuedFlowFileCount(editedConnectorId);
        final long editedGenerated = generatorCount(editedConnectorId);
        assertQueueMatchesCount(editedConnectorId);
        triggerUpgrade(editedConnectorId);
        assertConnectorState(editedConnectorId, ConnectorState.STOPPED);
        assertEquals(editedConnectionId, findConnection(editedConnectorId, REMOVED_CONNECTION).getId());
        assertEquals(editedQueued, getConnectorQueuedFlowFileCount(editedConnectorId));
        assertPending(editedConnectorId);
        getClientUtil().configureConnector(editedConnectorId, "Upgrade", properties("DRAIN", true, "working-edit"));
        assertPending(editedConnectorId);
        final VersionedConnector editedPersisted = waitForPersistedConnector(1, editedConnectorId, connector -> incomingBundle().matches(connector.getBundle()));
        assertManagedFlowBundle(editedPersisted, outgoingBundle());
        assertNotNull(findProcessor(editedPersisted.getManagedProcessGroup(), "TerminateFlowFile"));

        final String troubleshootingConnectorId = queueStoppedUpgrade("NO_OP", false, null);
        final int troubleshootingQueued = getConnectorQueuedFlowFileCount(troubleshootingConnectorId);
        changeVersion(troubleshootingConnectorId, DRAIN_V2);
        getClientUtil().enterTroubleshooting(troubleshootingConnectorId);
        final ConnectorEntity troubleshootingConnector = getNifiClient().getConnectorClient().getConnector(troubleshootingConnectorId);
        final NiFiClientException endTroubleshootingFailure = assertThrows(NiFiClientException.class, () -> getNifiClient().getConnectorClient()
            .endTroubleshooting(troubleshootingConnectorId, troubleshootingConnector.getRevision().getClientId(), troubleshootingConnector.getRevision().getVersion()));
        assertTrue(endTroubleshootingFailure.getMessage().contains(troubleshootingQueued + " FlowFiles"), endTroubleshootingFailure.getMessage());
        assertTrue(endTroubleshootingFailure.getMessage().contains("upgrade is pending"), endTroubleshootingFailure.getMessage());
        assertConnectorState(troubleshootingConnectorId, ConnectorState.TROUBLESHOOTING);
        waitUntilPreUpgradeFlowSaved(troubleshootingConnectorId);

        final Map<String, String> drainingProperties = properties("DRAIN", true, null);
        drainingProperties.put("Gate File Path", DRAINING_GATE_FILE);
        holdFlowFiles(DRAINING_GATE_FILE);
        final String drainingConnectorId = queueAtLeastOneFlowFileAndStop(createOutgoingConnector(drainingProperties));
        triggerUpgrade(drainingConnectorId);
        requestStart(drainingConnectorId);
        waitUntilDraining(drainingConnectorId);
        final int drainingQueued = getConnectorQueuedFlowFileCount(drainingConnectorId);
        final long drainingGenerated = generatorCount(drainingConnectorId);
        // The start that begins an evolution is saved, so the restarted node resumes the upgrade.
        waitForPersistedConnector(1, drainingConnectorId, connector -> connector.getScheduledState() == VersionedConnectorState.RUNNING);

        restartKeepingIncomingVersion();

        assertConnectorState(troubleshootingConnectorId, ConnectorState.TROUBLESHOOTING);
        assertFlowEvolutionStatus(troubleshootingConnectorId, FlowEvolutionStatus.EVOLVE_DRAIN_PENDING);
        assertEquals(troubleshootingQueued, getConnectorQueuedFlowFileCount(troubleshootingConnectorId));

        getClientUtil().waitForConnectorState(drainingConnectorId, ConnectorState.DRAINING);
        getClientUtil().cancelDrain(drainingConnectorId);
        getClientUtil().waitForConnectorStopped(drainingConnectorId);
        assertEquals(drainingQueued, getConnectorQueuedFlowFileCount(drainingConnectorId));
        assertEquals(drainingGenerated, generatorCount(drainingConnectorId));
        assertQueueMatchesCount(drainingConnectorId);
        assertNotNull(findConnection(drainingConnectorId, REMOVED_CONNECTION));
        assertPending(drainingConnectorId);
        final VersionedConnector drainingPersisted = waitForPersistedConnector(1, drainingConnectorId,
            connector -> connector.getScheduledState() == VersionedConnectorState.ENABLED);
        assertLoadedBundle(drainingPersisted, incomingBundle());
        assertManagedFlowBundle(drainingPersisted, outgoingBundle());

        requestStart(secretConnectorId);
        getClientUtil().waitForConnectorState(secretConnectorId, ConnectorState.RUNNING);
        waitUntilUpgradedFlowSaved(secretConnectorId);

        assertEquals(editedGenerated, generatorCount(editedConnectorId));
        assertQueueMatchesCount(editedConnectorId);
        // Releasing FlowFiles stops the generator, so its count must not move while the pre-upgrade flow drains and evolves.
        releaseFlowFiles();
        requestStart(editedConnectorId);
        getClientUtil().waitForConnectorState(editedConnectorId, ConnectorState.RUNNING);
        assertNull(findConnection(editedConnectorId, REMOVED_CONNECTION));
        assertEquals("working-edit", findProcessor(editedConnectorId, GENERATOR).getComponent().getConfig().getComments());
        assertTrue(findProcessor(editedConnectorId, GENERATOR).getComponent().getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        assertEquals(editedGenerated, generatorCount(editedConnectorId));
        assertFlowEvolutionStatus(editedConnectorId, FlowEvolutionStatus.INACTIVE);

        final VersionedConnector evolved = waitForPersistedConnector(1, editedConnectorId, connector -> incomingBundle().matches(connector.getManagedFlowBundle()));
        assertEquals(VersionedConnectorState.RUNNING, evolved.getScheduledState());
        assertEquals("working-edit", configurationProperty(evolved.getActiveFlowConfiguration(), "Comment"));
        assertEquals("working-edit", configurationProperty(evolved.getWorkingFlowConfiguration(), "Comment"));
        assertEquals("working-edit", findProcessor(evolved.getManagedProcessGroup(), GENERATOR).getComments());
        assertTrue(findProcessor(evolved.getManagedProcessGroup(), GENERATOR).getBundle().getArtifact().contains("nifi-system-test-extensions2"));

        getClientUtil().stopConnector(editedConnectorId);
        getClientUtil().configureConnector(editedConnectorId, "Upgrade", properties("DRAIN", true, "after-evolution"));
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(editedConnectorId));
        getClientUtil().waitForValidConnector(editedConnectorId);
        assertEquals("after-evolution", findProcessor(editedConnectorId, GENERATOR).getComponent().getConfig().getComments());
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

        releaseFlowFiles();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        final VersionedConnector persisted = waitForPersistedConnector(1, connectorId, connector -> outgoingBundle().matches(connector.getBundle()));
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testUpdatingPreUpgradeFlowDuringPreparationFailsEvolution() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);
        injectFault(UpgradeDrainFault.PREPARATION_UPDATES_PRE_UPGRADE_FLOW);
        requestStart(connectorId);

        assertStoppedPending(connectorId, "not an instance provided by the framework");
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.FAILED);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertEquals(generated, generatorCount(connectorId));

        final VersionedConnector persisted = waitForPersistedConnector(1, connectorId, connector -> incomingBundle().matches(connector.getBundle()));
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testInvalidEvolutionResultLeavesUpgradePendingUntilValid() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        assertQueueMatchesCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);

        final Map<UpgradeDrainFault, String> expectedErrors = new LinkedHashMap<>();
        expectedErrors.put(UpgradeDrainFault.PREPARATION_RETURNS_NULL_FUTURE, "must return a CompletableFuture");
        expectedErrors.put(UpgradeDrainFault.PREPARATION_FAILS, PREPARATION_FAILED);
        expectedErrors.put(UpgradeDrainFault.ACTIVE_FLOW_RETURNS_NULL, "Upgraded flow was not returned");
        expectedErrors.put(UpgradeDrainFault.ACTIVE_FLOW_THROWS, "active flow failed");
        for (final Map.Entry<UpgradeDrainFault, String> expectedError : expectedErrors.entrySet()) {
            clearFaults();
            injectFault(expectedError.getKey());

            requestStart(connectorId);
            assertStoppedPending(connectorId, expectedError.getValue());
            assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
            assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
            assertEquals(generated, generatorCount(connectorId));
            waitUntilPreUpgradeFlowSaved(connectorId);
        }

        // The upgraded flow keeps the queued connection. FlowFiles are still held, so the generator keeps running and its
        // count grows, but it must still equal the number of FlowFiles queued behind the terminator.
        clearFaults();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertQueueMatchesCount(connectorId);
        assertTrue(generatorCount(connectorId) >= generated);
        waitUntilUpgradedFlowSaved(connectorId);
    }

    @Test
    @Timeout(240)
    public void testMovingSourceOfQueuedConnectionFailsEvolutionUntilQueueIsEmpty() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);
        final Map<String, String> properties = properties("NO_OP", false, null);
        properties.put(REWIRED_ENDPOINT, "SOURCE");
        getClientUtil().configureConnector(connectorId, "Upgrade", properties);

        // Moving the source of a connection removes and recreates it, which would drop its queued FlowFiles.
        requestStart(connectorId);
        assertStoppedPending(connectorId, "changes the source of");
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.FAILED);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertEquals(generated, generatorCount(connectorId));
        waitUntilPreUpgradeFlowSaved(connectorId);

        getClientUtil().purgeConnectorFlowFiles(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertEquals(RELAY_PROCESSOR, findConnection(connectorId, REMOVED_CONNECTION).getComponent().getSource().getName());
        waitUntilUpgradedFlowSaved(connectorId);
    }

    @Test
    @Timeout(240)
    public void testMovingDestinationOfQueuedConnectionKeepsQueuedFlowFiles() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final long generated = generatorCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);
        final Map<String, String> properties = properties("NO_OP", false, null);
        properties.put(REWIRED_ENDPOINT, "DESTINATION");
        getClientUtil().configureConnector(connectorId, "Upgrade", properties);

        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        // A destination change updates the connection in place. FlowFiles are still held, so every FlowFile generated
        // before and after the upgrade is still queued in the connector.
        final ConnectionEntity connection = findConnection(connectorId, REMOVED_CONNECTION);
        assertEquals(connectionId, connection.getId());
        assertEquals(RELAY_PROCESSOR, connection.getComponent().getDestination().getName());
        final long generatedAfterUpgrade = generatorCount(connectorId);
        assertTrue(generatedAfterUpgrade >= generated);
        assertEquals(generatedAfterUpgrade, getConnectorQueuedFlowFileCount(connectorId));
        waitUntilUpgradedFlowSaved(connectorId);
    }

    @Test
    @Timeout(240)
    public void testChangeVersionRejectedWhileDraining() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        holdFlowFiles();
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        assertNotNull(getClientUtil().getConnectorActionReasonNotAllowed(connectorId, "CHANGE_VERSION"));
        assertThrows(NiFiClientException.class, () -> changeVersion(connectorId, DRAIN_V1));
        assertConnectorState(connectorId, ConnectorState.DRAINING);
        assertEquals(DRAIN_V2, getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getBundle().getVersion());

        getClientUtil().cancelDrain(connectorId);
        assertStoppedPending(connectorId, CANCELLED_EVOLUTION);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.FAILED);
        assertNull(getClientUtil().getConnectorActionReasonNotAllowed(connectorId, "CHANGE_VERSION"));
    }

    @Test
    @Timeout(240)
    public void testStopDuringEvolutionDrainFailsEvolution() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        changeVersion(connectorId, DRAIN_V2);
        holdFlowFiles();
        requestStart(connectorId);
        waitUntilDraining(connectorId);

        getClientUtil().stopConnector(connectorId);
        assertStoppedPending(connectorId, CANCELLED_EVOLUTION);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.FAILED);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        waitUntilPreUpgradeFlowSaved(connectorId);
    }

    @Test
    @Timeout(240)
    public void testEndingTroubleshootingAfterFailedApplyCompletesUpgrade() throws Exception {
        final String connectorId = createOutgoingConnector("NO_OP", false, null);
        changeVersion(connectorId, DRAIN_V2);
        injectFault(UpgradeDrainFault.APPLY_FAILS);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.UPDATE_FAILED);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE);

        clearFault(UpgradeDrainFault.APPLY_FAILS);
        getClientUtil().enterTroubleshooting(connectorId);
        getClientUtil().endTroubleshooting(connectorId);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.INACTIVE);
        getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId));
        getClientUtil().waitForValidConnector(connectorId);
        waitUntilUpgradedFlowSaved(connectorId);
    }

    @Test
    @Timeout(240)
    public void testSecondUpgradeAfterCompletedUpgrade() throws Exception {
        releaseFlowFiles();
        final String connectorId = createOutgoingConnector("NO_OP", false, null);
        changeVersion(connectorId, DRAIN_V2);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        getClientUtil().stopConnector(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);

        changeVersion(connectorId, DRAIN_V1);
        assertPending(connectorId);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.INACTIVE);
        waitUntilPreUpgradeFlowSaved(connectorId);
    }

    /**
     * While FlowFiles are held, the terminator keeps every generated FlowFile, so the generator's stored count equals the
     * number of queued FlowFiles. Lost FlowFiles make the queue smaller; cleared state makes the count smaller.
     */
    private void assertQueueMatchesCount(final String connectorId) throws NiFiClientException, IOException {
        assertEquals(getConnectionQueuedCount(connectorId, REMOVED_CONNECTION), generatorCount(connectorId));
    }

    private long generatorCount(final String connectorId) throws NiFiClientException, IOException {
        final String generatorId = findProcessor(connectorId, GENERATOR).getId();
        final ComponentStateEntity stateEntity = getNifiClient().getConnectorClient().getProcessorState(connectorId, generatorId);
        for (final StateEntryDTO entry : stateEntity.getComponentState().getLocalState().getState()) {
            if ("count".equals(entry.getKey())) {
                return Long.parseLong(entry.getValue());
            }
        }

        return 0L;
    }

    private void assertSecretPersistedAsReference(final VersionedConnector connector) {
        final VersionedParameter parameter = connector.getManagedParameterContext().getParameters().stream()
            .filter(candidate -> UPGRADE_SECRET.equals(candidate.getName()))
            .findFirst()
            .orElseThrow();
        assertTrue(parameter.isSensitive());
        assertFalse(parameter.isProvided());
        assertTrue(PropertyEncryptionEncoder.isEncrypted(parameter.getValue()));

        final VersionedConnectorValueReference parameterReference = connector.getManagedParameterSecretReferences().get(UPGRADE_SECRET);
        assertNotNull(parameterReference);
        assertEquals(SECRET_FULLY_QUALIFIED_NAME, parameterReference.getFullyQualifiedSecretName());

        final VersionedConnectorValueReference propertyReference = connector.getActiveFlowConfiguration().stream()
            .filter(step -> "Upgrade".equals(step.getName()))
            .map(step -> step.getProperties().get(UPGRADE_SECRET))
            .findFirst()
            .orElseThrow();
        assertEquals(SECRET_FULLY_QUALIFIED_NAME, propertyReference.getFullyQualifiedSecretName());
        assertManagedFlowBundle(connector, outgoingBundle());
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

    private record SecretConnector(String connectorId, ParameterProviderEntity parameterProvider) {
    }
}
