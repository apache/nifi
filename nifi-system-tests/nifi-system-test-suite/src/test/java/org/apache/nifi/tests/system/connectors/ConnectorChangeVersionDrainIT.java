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
import org.apache.nifi.tests.system.FlowDefinitionFiles;
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

    @Test
    @Timeout(360)
    public void testPreUpgradeParameterSecretIsRefreshedAndCanBeRestored() throws Exception {
        final SecretConnector secretConnector = createSecretConnector("original-secret");
        changeVersion(secretConnector.connectorId(), DRAIN_V2);
        getClientUtil().updateParameterProviderProperties(secretConnector.parameterProvider(), Map.of("parameters", SECRET_NAME + "=rotated-secret"));

        getNiFiInstance().stop();
        assertSecretPersistedAsReference(FlowDefinitionFiles.readConnector(nodeInstance(1), secretConnector.connectorId()));
        getNiFiInstance().start(true);
        waitForCluster();

        requestStart(secretConnector.connectorId());
        getClientUtil().waitForConnectorState(secretConnector.connectorId(), ConnectorState.RUNNING);
        stopAndAssertNotPending(secretConnector.connectorId());
    }

    @Test
    @Timeout(360)
    public void testPendingUpgradeEditsThenStartsWithoutReEvolving() throws Exception {
        final String connectorId = queueStoppedUpgrade("INHERITED_DRAIN", true, "original");
        final String connectionId = findConnection(connectorId, REMOVED_CONNECTION).getId();
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);
        assertQueueMatchesCount(connectorId);
        triggerUpgrade(connectorId);

        assertConnectorState(connectorId, ConnectorState.STOPPED);
        assertEquals(connectionId, findConnection(connectorId, REMOVED_CONNECTION).getId());
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertPending(connectorId);

        getClientUtil().configureConnector(connectorId, "Upgrade", properties("DRAIN", true, "working-edit"));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
        assertNotNull(findProcessor(persisted.getManagedProcessGroup(), "TerminateFlowFile"));
        getNiFiInstance().start(true);
        waitForCluster();

        assertEquals(generated, generatorCount(connectorId));
        assertQueueMatchesCount(connectorId);

        // Releasing FlowFiles stops the generator, so its count must not move while the pre-upgrade flow drains and evolves.
        releaseFlowFiles();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertEquals("working-edit", findProcessor(connectorId, GENERATOR).getComponent().getConfig().getComments());
        assertTrue(findProcessor(connectorId, GENERATOR).getComponent().getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        assertEquals(generated, generatorCount(connectorId));

        getNiFiInstance().stop();
        final VersionedConnector evolved = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertManagedFlowBundle(evolved, incomingBundle());
        assertEquals(VersionedConnectorState.RUNNING, evolved.getScheduledState());
        assertEquals("working-edit", configurationProperty(evolved.getActiveFlowConfiguration(), "Comment"));
        assertEquals("working-edit", configurationProperty(evolved.getWorkingFlowConfiguration(), "Comment"));
        assertEquals("working-edit", findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getComments());
        assertTrue(findProcessor(evolved.getManagedProcessGroup(), "GenerateFlowFile").getBundle().getArtifact().contains("nifi-system-test-extensions2"));
        getNiFiInstance().start(true);
        waitForCluster();
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.INACTIVE);

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

        releaseFlowFiles();
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertLoadedBundle(persisted, outgoingBundle());
        assertManagedFlowBundle(persisted, outgoingBundle());
    }

    @Test
    @Timeout(240)
    public void testRestartDuringDrainingComesBackPending() throws Exception {
        final String connectorId = queueStoppedUpgrade("DRAIN", true, null);
        triggerUpgrade(connectorId);
        holdFlowFiles();
        assertTrue(getConnectorQueuedFlowFileCount(connectorId) > 0);
        requestStart(connectorId);
        waitUntilDraining(connectorId);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        final long generated = generatorCount(connectorId);

        // An evolution in progress keeps the Connector's desired state RUNNING, so the restarted node resumes it.
        getNiFiInstance().stop();
        assertEquals(VersionedConnectorState.RUNNING, FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId).getScheduledState());
        getNiFiInstance().start(true);
        waitForCluster();

        getClientUtil().cancelDrain(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
        assertEquals(generated, generatorCount(connectorId));
        assertQueueMatchesCount(connectorId);
        assertNotNull(findConnection(connectorId, REMOVED_CONNECTION));
        assertPending(connectorId);
        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertEquals(VersionedConnectorState.ENABLED, persisted.getScheduledState());
        assertLoadedBundle(persisted, incomingBundle());
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

        getNiFiInstance().stop();
        final VersionedConnector persisted = FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId);
        assertLoadedBundle(persisted, incomingBundle());
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
            assertPersistedOutgoing(connectorId);
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
        stopAndAssertNotPending(connectorId);
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
        assertPersistedOutgoing(connectorId);

        getClientUtil().purgeConnectorFlowFiles(connectorId);
        getClientUtil().waitForConnectorStopped(connectorId);
        requestStart(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        assertEquals(RELAY_PROCESSOR, findConnection(connectorId, REMOVED_CONNECTION).getComponent().getSource().getName());
        stopAndAssertNotPending(connectorId);
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
        stopAndAssertNotPending(connectorId);
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
        assertPersistedOutgoing(connectorId);
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
        stopAndAssertNotPending(connectorId);
    }

    @Test
    @Timeout(240)
    public void testPendingUpgradeBlocksEndingTroubleshootingWithQueuedFlowFiles() throws Exception {
        final String connectorId = queueStoppedUpgrade("NO_OP", false, null);
        final int queued = getConnectorQueuedFlowFileCount(connectorId);
        changeVersion(connectorId, DRAIN_V2);
        getClientUtil().enterTroubleshooting(connectorId);

        final ConnectorEntity connector = getNifiClient().getConnectorClient().getConnector(connectorId);
        final NiFiClientException failure = assertThrows(NiFiClientException.class, () -> getNifiClient().getConnectorClient()
            .endTroubleshooting(connectorId, connector.getRevision().getClientId(), connector.getRevision().getVersion()));
        assertTrue(failure.getMessage().contains(queued + " FlowFiles"), failure.getMessage());
        assertTrue(failure.getMessage().contains("upgrade is pending"), failure.getMessage());
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertPersistedOutgoing(connectorId);

        restartKeepingIncomingVersion();
        assertConnectorState(connectorId, ConnectorState.TROUBLESHOOTING);
        assertFlowEvolutionStatus(connectorId, FlowEvolutionStatus.EVOLVE_DRAIN_PENDING);
        assertEquals(queued, getConnectorQueuedFlowFileCount(connectorId));
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

        getNiFiInstance().stop();
        assertManagedFlowBundle(FlowDefinitionFiles.readConnector(nodeInstance(1), connectorId), outgoingBundle());
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
