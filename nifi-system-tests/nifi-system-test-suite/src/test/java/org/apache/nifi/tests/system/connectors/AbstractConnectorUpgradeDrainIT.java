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
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnector;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.nar.NarState;
import org.apache.nifi.tests.system.AbstractNarSwapMigrationIT;
import org.apache.nifi.tests.system.FlowDefinitionFiles;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.nar.NarUploadUtil;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectorActionDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upgrade-drain scenarios that hold for every way a Connector can move to a new version. Each subclass supplies the
 * trigger: a NAR swap followed by a restart, or a version change while the Connector is stopped.
 */
public abstract class AbstractConnectorUpgradeDrainIT extends AbstractNarSwapMigrationIT {

    static final String GATE_FILE = "upgrade-drain-gate.txt";
    static final String APPLY_FAIL_FILE = "upgrade-drain-apply-fail.txt";
    static final String HOOK_RESULT_FILE = "upgrade-drain-hook-result.txt";
    static final String ACTIVE_FLOW_FILE = "upgrade-drain-active-flow.txt";
    private static final List<String> SWITCH_FILES = List.of(APPLY_FAIL_FILE, HOOK_RESULT_FILE, ACTIVE_FLOW_FILE);
    static final String REMOVED_CONNECTION = "Removed Connection";
    static final String REMOVED_PROCESSOR_CHAIN = "Removed Processor Chain";
    static final String RELAY_PROCESSOR = "RelayFlowFile";
    static final String REMOVED_PROCESSOR = "RemovedFlowFile";
    static final String SWAPPED_PROCESSOR = "SwappedFlowFile";

    static final String DRAIN_CONNECTOR_TYPE = "org.apache.nifi.connectors.tests.system.UpgradeDrainConnector";
    static final String DRAIN_BUNDLE_GROUP = "org.apache.nifi";
    static final String DRAIN_BUNDLE_ARTIFACT = "nifi-connector-upgrade-drain-nar";
    static final String DRAIN_V1 = "1.0.0";
    static final String DRAIN_V2 = "2.0.0";
    private static final String DRAIN_NARS_LOCATION = "target/nifi-connector-upgrade-drain-nars";
    private static final Map<String, String> DRAIN_NAR_IDS = Map.of(
        DRAIN_V1, "nifi-connector-upgrade-drain-v1-nar",
        DRAIN_V2, "nifi-connector-upgrade-drain-v2-nar");

    /**
     * Identifies a Connector bundle in assertions. A null version matches any version of the artifact.
     */
    protected record ExpectedBundle(String artifact, String version) {

        String recordPrefix() {
            return "org.apache.nifi:" + artifact + ":" + (version == null ? "" : version);
        }

        void assertMatches(final Bundle bundle, final String description) {
            assertNotNull(bundle, description);
            assertTrue(bundle.getArtifact().contains(artifact), description + " " + bundle.getArtifact());
            if (version != null) {
                assertEquals(version, bundle.getVersion(), description);
            }
        }
    }

    /**
     * Creates the Connector on its outgoing version and applies the given configuration.
     */
    protected abstract String createOutgoingConnector(Map<String, String> properties) throws Exception;

    /**
     * Moves the stopped Connector to its incoming version, leaving it STOPPED with the upgrade pending.
     */
    protected abstract void triggerUpgrade(String connectorId) throws Exception;

    protected abstract ExpectedBundle outgoingBundle();

    protected abstract ExpectedBundle incomingBundle();

    @BeforeEach
    public void clearUpgradeDrainSwitches() {
        for (final String switchFile : SWITCH_FILES) {
            deleteNodeFiles(switchFile);
        }
    }

    protected String createOutgoingConnector(final String mode, final boolean removeConnection, final String comment) throws Exception {
        return createOutgoingConnector(properties(mode, removeConnection, comment));
    }

    protected String queueStoppedUpgrade(final String mode, final boolean removeConnection, final String comment) throws Exception {
        return queueStopped(createOutgoingConnector(mode, removeConnection, comment));
    }

    protected String queueStopped(final String connectorId) throws Exception {
        deleteGates();
        getClientUtil().startConnector(connectorId);
        waitForConnectorMinQueueCount(connectorId, 1);
        getClientUtil().stopConnector(connectorId);
        return connectorId;
    }

    protected void restartKeepingIncomingVersion() {
        getNiFiInstance().stop();
        getNiFiInstance().start(true);
        waitForCluster();
    }

    protected void uploadDrainNars(final String... versions) throws Exception {
        final File narsLocation = new File(DRAIN_NARS_LOCATION);
        if (!narsLocation.exists()) {
            throw new IllegalStateException("NARs location does not exist at: " + narsLocation.getAbsolutePath());
        }

        final NarUploadUtil narUploadUtil = new NarUploadUtil(getNifiClient());
        for (final String version : versions) {
            final NarSummaryDTO nar = narUploadUtil.uploadNar(narsLocation, DRAIN_NAR_IDS.get(version));
            waitFor(narUploadUtil.getWaitForNarStateSupplier(nar.getIdentifier(), NarState.INSTALLED));
        }
    }

    protected String createDrainConnector(final String version, final Map<String, String> properties) throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector(DRAIN_CONNECTOR_TYPE, DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, version);
        getClientUtil().configureConnector(connector, "Upgrade", properties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
        return connector.getId();
    }

    /**
     * Sends the version change the way the UI does: a PUT carrying only the new bundle.
     */
    protected ConnectorEntity changeVersion(final String connectorId, final String version) throws NiFiClientException, IOException {
        return changeVersion(getNifiClient().getConnectorClient(), connectorId, version, false);
    }

    protected ConnectorEntity changeVersion(final ConnectorClient connectorClient, final String connectorId, final String version,
                                            final boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        final ConnectorEntity current = connectorClient.getConnector(connectorId);
        final ConnectorDTO component = new ConnectorDTO();
        component.setId(connectorId);
        component.setBundle(new BundleDTO(DRAIN_BUNDLE_GROUP, DRAIN_BUNDLE_ARTIFACT, version));

        final ConnectorEntity update = new ConnectorEntity();
        update.setId(connectorId);
        update.setRevision(current.getRevision());
        update.setComponent(component);
        update.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);
        return connectorClient.updateConnector(update);
    }

    protected void createGates() throws IOException {
        writeNodeFile(GATE_FILE);
    }

    private void writeNodeFile(final String fileName) throws IOException {
        writeNodeFiles(fileName, "");
    }

    protected void writeNodeFiles(final String fileName, final String contents) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            writeNodeFile(nodeIndex, fileName, contents);
        }
    }

    protected void writeNodeFile(final int nodeIndex, final String fileName, final String contents) throws IOException {
        Files.writeString(new File(nodeDirectory(nodeIndex), fileName).toPath(), contents);
    }

    protected void deleteNodeFiles(final String fileName) {
        for (final File directory : nodeDirectories()) {
            final File file = new File(directory, fileName);
            if (file.exists()) {
                assertTrue(file.delete(), file.getAbsolutePath());
            }
        }
    }

    protected void assertNotPending(final String connectorId) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final VersionedConnector connector = FlowDefinitionFiles.readConnector(nodeInstance(nodeIndex), connectorId);
            assertManagedFlowBundle(connector, incomingBundle());
        }
    }

    protected void assertPersistedOutgoing(final String connectorId) throws IOException {
        for (int nodeIndex = 1; nodeIndex <= nodeCount(); nodeIndex++) {
            final VersionedConnector connector = FlowDefinitionFiles.readConnector(nodeInstance(nodeIndex), connectorId);
            assertManagedFlowBundle(connector, outgoingBundle());
        }
    }

    protected int nodeCount() {
        return Math.max(1, getNumberOfNodes());
    }

    protected NiFiInstance nodeInstance(final int nodeIndex) {
        return getNumberOfNodes() <= 1 ? getNiFiInstance() : getNiFiInstance().getNodeInstance(nodeIndex);
    }

    protected File nodeDirectory(final int nodeIndex) {
        return nodeInstance(nodeIndex).getInstanceDirectory();
    }

    protected NiFiInstance firstNode() {
        return getNumberOfNodes() <= 1 ? getNiFiInstance() : getNiFiInstance().getNodeInstance(1);
    }

    protected List<File> nodeDirectories() {
        if (getNumberOfNodes() <= 1) {
            return List.of(getNiFiInstance().getInstanceDirectory());
        }
        final List<File> directories = new ArrayList<>();
        for (int nodeIndex = 1; nodeIndex <= getNumberOfNodes(); nodeIndex++) {
            directories.add(getNiFiInstance().getNodeInstance(nodeIndex).getInstanceDirectory());
        }
        return directories;
    }

    protected void requestStart(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        entity.setDisconnectedNodeAcknowledged(true);
        getNifiClient().getConnectorClient().startConnector(entity);
    }

    protected static void assertLoadedBundle(final VersionedConnector connector, final ExpectedBundle expected) {
        expected.assertMatches(connector.getBundle(), "loaded bundle");
    }

    protected static void assertManagedFlowBundle(final VersionedConnector connector, final ExpectedBundle expected) {
        expected.assertMatches(connector.getManagedFlowBundle(), "managed flow bundle");
    }

    protected void assertPending(final String connectorId) {
        final Exception failure = assertThrows(Exception.class,
            () -> getClientUtil().applyConnectorUpdate(getNifiClient().getConnectorClient().getConnector(connectorId)));
        assertTrue(failure.getMessage().contains("start the connector instead"), failure.getMessage());
    }

    protected void waitUntilDraining(final String connectorId) throws NiFiClientException, IOException, InterruptedException {
        for (int iteration = 0; iteration < 600; iteration++) {
            final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
            final String state = entity.getComponent().getState();
            if (ConnectorState.DRAINING.name().equals(state)) {
                return;
            }
            if (ConnectorState.RUNNING.name().equals(state) || ConnectorState.STOPPED.name().equals(state) || ConnectorState.UPDATE_FAILED.name().equals(state)) {
                throw new AssertionError("Expected DRAINING but the connector is " + state + ": " + entity.getComponent().getValidationErrors());
            }
            Thread.sleep(100L);
        }
        final ConnectorEntity entity = getNifiClient().getConnectorClient().getConnector(connectorId);
        throw new AssertionError("Connector did not reach DRAINING. State is " + entity.getComponent().getState());
    }

    protected String createConnector(final String type, final Map<String, String> properties) throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector(type);
        getClientUtil().configureConnector(connector, properties.containsKey("Evolution Mode") ? "Upgrade" : "Gate", properties);
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());
        return connector.getId();
    }

    protected Map<String, String> properties(final String mode, final boolean removeConnection, final String comment) {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put("Gate File Path", GATE_FILE);
        properties.put("Evolution Mode", mode);
        properties.put("Remove Connection", Boolean.toString(removeConnection));
        if (comment != null) {
            properties.put("Comment", comment);
        }
        return properties;
    }

    protected void deleteGates() {
        for (final File directory : nodeDirectories()) {
            final File gate = new File(directory, GATE_FILE);
            if (gate.exists()) {
                assertTrue(gate.delete());
            }
        }
    }

    protected void assertConnectorState(final String connectorId, final ConnectorState state) throws NiFiClientException, IOException {
        assertEquals(state.name(), getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getState());
    }

    protected String actionReason(final String connectorId, final String actionName) throws NiFiClientException, IOException {
        for (final ConnectorActionDTO action : getNifiClient().getConnectorClient().getConnector(connectorId).getComponent().getAvailableActions()) {
            if (actionName.equals(action.getName()) && !Boolean.TRUE.equals(action.getAllowed())) {
                return action.getReasonNotAllowed();
            }
        }
        return null;
    }

    protected ProcessorEntity findProcessor(final String connectorId, final String name) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId);
        for (final ProcessorEntity processor : flow.getProcessGroupFlow().getFlow().getProcessors()) {
            if (name.equals(processor.getComponent().getName())) {
                return processor;
            }
        }
        return null;
    }

    protected VersionedProcessor findProcessor(final VersionedProcessGroup group, final String name) {
        if (group == null || group.getProcessors() == null) {
            return null;
        }
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (name.equals(processor.getName())) {
                return processor;
            }
        }
        return null;
    }

    protected String configurationProperty(final List<VersionedConfigurationStep> configurationSteps, final String propertyName) {
        for (final VersionedConfigurationStep configurationStep : configurationSteps) {
            if ("Upgrade".equals(configurationStep.getName()) && configurationStep.getProperties().containsKey(propertyName)) {
                return configurationStep.getProperties().get(propertyName).getValue();
            }
        }

        return null;
    }

    protected int getConnectionQueuedCount(final String connectorId, final String connectionName) throws NiFiClientException, IOException {
        final ConnectionEntity connection = findConnection(connectorId, connectionName);
        if (connection == null || connection.getStatus() == null || connection.getStatus().getAggregateSnapshot() == null) {
            return 0;
        }

        return connection.getStatus().getAggregateSnapshot().getFlowFilesQueued();
    }

    protected void waitForConnectionMinQueueCount(final String connectorId, final String connectionName, final int minQueueSize) throws InterruptedException {
        waitFor(() -> {
            try {
                return getConnectionQueuedCount(connectorId, connectionName) >= minQueueSize;
            } catch (final Exception ignored) {
                return false;
            }
        });
    }

    protected ConnectionEntity findConnection(final String connectorId, final String name) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flow = getNifiClient().getConnectorClient().getFlow(connectorId);
        if (flow.getProcessGroupFlow().getFlow().getConnections() == null) {
            return null;
        }
        for (final ConnectionEntity connection : flow.getProcessGroupFlow().getFlow().getConnections()) {
            if (name.equals(connection.getComponent().getName())) {
                return connection;
            }
        }
        return null;
    }

    protected void waitForCluster() {
        if (getNumberOfNodes() > 1) {
            waitForAllNodesConnected();
        }
    }
}
