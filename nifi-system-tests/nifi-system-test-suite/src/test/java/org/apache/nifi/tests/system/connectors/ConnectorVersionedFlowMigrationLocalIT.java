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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.VersionedFlowMigrationSourceDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionedFlowMigrationSourcesEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorVersionedFlowMigrationLocalIT extends NiFiSystemIT {
    protected static final String TEST_BUCKET = "test-flows";
    private static final String ASSET_PARAMETER_NAME = "Asset File";
    private static final String SOURCE_TOPIC_PARAMETER_NAME = "Source Topic";
    protected static final File SAMPLE_ASSET_FILE = new File("src/test/resources/sample-assets/helloworld.txt");

    @Test
    public void testMigrateConnectorFromLocalVersionedFlow() throws Exception {
        final File outputFile = new File("target/migration/local-output.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("LocalMigrationSource", registryClient, true, outputFile, true);

        prepareSourceForMigration(sourceFixture, outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();
        final VersionedFlowMigrationSourcesEntity sourcesEntity = getClientUtil().listMigrationSources(connectorId);
        assertTrue(isSourceListed(sourcesEntity, sourceFixture.processGroup().getId()));

        outputFile.delete();

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        final String requestId = requestEntity.getRequest().getRequestId();
        getClientUtil().waitForMigrationSuccess(connectorId, requestId);

        final ProcessGroupEntity migratedSourceGroup = getNifiClient().getProcessGroupClient().getProcessGroup(sourceFixture.processGroup().getId());
        assertTrue(migratedSourceGroup.getComponent().getName().startsWith("(Migrated) "));

        // The source process group's components must all be disabled after a successful migration so
        // they cannot be inadvertently restarted, regardless of whether the migration also renamed the group.
        final ProcessGroupFlowEntity migratedSourceFlow =
                getNifiClient().getFlowClient().getProcessGroup(sourceFixture.processGroup().getId());
        for (final ProcessorEntity sourceProcessor : migratedSourceFlow.getProcessGroupFlow().getFlow().getProcessors()) {
            assertEquals("DISABLED", sourceProcessor.getComponent().getState(),
                    "Migrated source processor " + sourceProcessor.getComponent().getName() + " must be DISABLED after successful migration");
        }

        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();
        final AssetsEntity connectorAssets = connectorClient.getAssets(connectorId);
        assertNotNull(connectorAssets.getAssets());
        assertFalse(connectorAssets.getAssets().isEmpty());

        final ConnectorEntity migratedConnector = connectorClient.getConnector(connectorId);
        final String managedGroupId = migratedConnector.getComponent().getManagedProcessGroupId();
        final String migratedCountProcessorId = getProcessorId(connectorId, managedGroupId, "StatefulCountProcessor");
        assertMigratedProcessorState(connectorId, migratedCountProcessorId);

        getClientUtil().startConnector(connectorId);
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        assertEquals(Files.readString(SAMPLE_ASSET_FILE.toPath()).trim(), Files.readString(outputFile.toPath()).trim());
    }

    protected SourceFixture createSourceFixture(final String name, final FlowRegistryClientEntity registryClient, final boolean includeAsset, final File outputFile,
                                                final boolean versionControlled) throws Exception {
        final ProcessGroupEntity processGroup = getClientUtil().createProcessGroup(name, "root");

        final ParameterContextEntity assetContext = getClientUtil().createParameterContext(name + "-asset", Map.of());
        if (includeAsset) {
            final AssetEntity assetEntity = getNifiClient().getParamContextClient().createAsset(assetContext.getId(), SAMPLE_ASSET_FILE.getName(), SAMPLE_ASSET_FILE);
            final ParameterContextUpdateRequestEntity requestEntity = getClientUtil().updateParameterAssetReferences(
                assetContext, Map.of(ASSET_PARAMETER_NAME, List.of(assetEntity.getAsset().getId())));
            getClientUtil().waitForParameterContextRequestToComplete(assetContext.getId(), requestEntity.getRequest().getRequestId());
        }

        final ParameterContextEntity sourceContext = getClientUtil().createParameterContext(
            name + "-source",
            Map.of(SOURCE_TOPIC_PARAMETER_NAME, "orders"),
            List.of(assetContext.getId()),
            null);
        getClientUtil().setParameterContext(processGroup.getId(), sourceContext);

        final ProcessorEntity statefulCount = getClientUtil().createProcessor("StatefulCountProcessor", processGroup.getId());
        final ProcessorEntity assetReader = getClientUtil().updateProcessorProperties(getClientUtil().createProcessor("AssetReadingProcessor", processGroup.getId()), Map.of(
            "Source File", includeAsset ? "#{" + ASSET_PARAMETER_NAME + "}" : SAMPLE_ASSET_FILE.getAbsolutePath(),
            "Output File", outputFile.getAbsolutePath()));
        getClientUtil().setAutoTerminatedRelationships(assetReader, Set.of("success", "failure"));

        final ConnectionEntity connection = getClientUtil().createConnection(statefulCount, assetReader, "success");

        if (versionControlled) {
            assertNotNull(getClientUtil().startVersionControl(processGroup, registryClient, TEST_BUCKET, name));
        }

        return new SourceFixture(processGroup, connection);
    }

    protected String getProcessorId(final String connectorId, final String groupId, final String processorTypeSuffix) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        for (final ProcessorEntity processor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            if (processor.getComponent().getType().endsWith(processorTypeSuffix)) {
                return processor.getId();
            }
        }

        throw new IllegalStateException("Could not find processor ending with type " + processorTypeSuffix);
    }

    protected boolean isSourceListed(final VersionedFlowMigrationSourcesEntity sourcesEntity, final String processGroupId) {
        return findListedSource(sourcesEntity, processGroupId) != null;
    }

    protected VersionedFlowMigrationSourceDTO findListedSource(final VersionedFlowMigrationSourcesEntity sourcesEntity, final String processGroupId) {
        if (sourcesEntity.getMigrationSources() == null) {
            return null;
        }

        for (final VersionedFlowMigrationSourceDTO migrationSource : sourcesEntity.getMigrationSources()) {
            if (processGroupId.equals(migrationSource.getProcessGroupId())) {
                return migrationSource;
            }
        }

        return null;
    }

    protected void assertMigratedProcessorState(final String connectorId, final String processorId) throws Exception {
        if (!getNiFiInstance().isClustered()) {
            final ComponentStateEntity stateEntity = getNifiClient().getConnectorClient().getProcessorState(connectorId, processorId);
            assertStatePresent(stateEntity);
            return;
        }

        for (int nodeIndex = 1; nodeIndex <= getNumberOfNodes(); nodeIndex++) {
            switchClientToNode(nodeIndex);
            final ComponentStateEntity stateEntity = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getProcessorState(connectorId, processorId);
            assertStatePresent(stateEntity);
            assertNotNull(stateEntity.getComponentState().getClusterState());
            assertFalse(stateEntity.getComponentState().getClusterState().getState().isEmpty());
        }
    }

    private void assertStatePresent(final ComponentStateEntity stateEntity) {
        assertNotNull(stateEntity.getComponentState());
        assertNotNull(stateEntity.getComponentState().getLocalState());
        assertFalse(stateEntity.getComponentState().getLocalState().getState().isEmpty());
    }

    protected void prepareSourceForMigration(final SourceFixture sourceFixture, final File outputFile) throws Exception {
        getClientUtil().startProcessGroupComponents(sourceFixture.processGroup().getId());
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        getClientUtil().stopProcessGroupComponents(sourceFixture.processGroup().getId());
        getClientUtil().emptyQueue(sourceFixture.connection().getId());
        getClientUtil().assertFlowUpToDate(sourceFixture.processGroup().getId());
    }

    protected void assertConnectorFresh(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();
        final ConnectorEntity connectorEntity = connectorClient.getConnector(connectorId);

        final String managedGroupId = connectorEntity.getComponent().getManagedProcessGroupId();
        final ProcessGroupFlowEntity flowEntity = connectorClient.getFlow(connectorId, managedGroupId);
        assertTrue(flowEntity.getProcessGroupFlow().getFlow().getProcessors() == null
                || flowEntity.getProcessGroupFlow().getFlow().getProcessors().isEmpty());
        assertTrue(flowEntity.getProcessGroupFlow().getFlow().getConnections() == null
                || flowEntity.getProcessGroupFlow().getFlow().getConnections().isEmpty());

        final AssetsEntity assetsEntity = connectorClient.getAssets(connectorId);
        assertTrue(assetsEntity.getAssets() == null || assetsEntity.getAssets().isEmpty());
    }

    /**
     * Asserts that the source process group has been left in the same state it was in before a failed
     * migration attempt. The source must keep its original name, its original version-control state
     * (registry coordinates and UP_TO_DATE state), its originally-applied parameter context, and must
     * not have been disabled by the migration framework.
     */
    protected void assertSourceUntouched(final SourceFixture sourceFixture, final String expectedName) throws NiFiClientException, IOException {
        final ProcessGroupEntity processGroupEntity = getNifiClient().getProcessGroupClient().getProcessGroup(sourceFixture.processGroup().getId());
        assertEquals(expectedName, processGroupEntity.getComponent().getName());

        // Version control coordinates and state must be unchanged.
        assertNotNull(processGroupEntity.getComponent().getVersionControlInformation(),
                "Source process group must remain under version control after a failed migration attempt");
        getClientUtil().assertFlowUpToDate(sourceFixture.processGroup().getId());

        // Parameter context binding must be unchanged from the original setup.
        final ProcessGroupEntity originalProcessGroupEntity = sourceFixture.processGroup();
        if (originalProcessGroupEntity.getComponent().getParameterContext() != null) {
            assertNotNull(processGroupEntity.getComponent().getParameterContext(),
                    "Source process group must retain its parameter context after a failed migration attempt");
            assertEquals(originalProcessGroupEntity.getComponent().getParameterContext().getId(),
                    processGroupEntity.getComponent().getParameterContext().getId());
        }

        // Processors and controller services must NOT have been disabled by the migration framework.
        // prepareSourceForMigration leaves processors STOPPED (started, output produced, then stopped),
        // and any controller services in their original ENABLED state. A failed migration must leave
        // those scheduled/enabled states unchanged.
        final ProcessGroupFlowEntity flowEntity =
                getNifiClient().getFlowClient().getProcessGroup(sourceFixture.processGroup().getId());
        for (final ProcessorEntity sourceProcessor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            final String state = sourceProcessor.getComponent().getState();
            assertNotEquals("DISABLED", state,
                    "Source processor " + sourceProcessor.getComponent().getName() + " must not be DISABLED after a failed migration attempt");
            assertEquals("STOPPED", state,
                    "Source processor " + sourceProcessor.getComponent().getName() + " must remain STOPPED after a failed migration attempt");
        }
        final ControllerServicesEntity controllerServicesEntity =
                getNifiClient().getFlowClient().getControllerServices(sourceFixture.processGroup().getId());
        if (controllerServicesEntity.getControllerServices() != null) {
            for (final ControllerServiceEntity sourceService : controllerServicesEntity.getControllerServices()) {
                final String state = sourceService.getComponent().getState();
                assertNotEquals("DISABLED", state,
                        "Source controller service " + sourceService.getComponent().getName() + " must not be DISABLED after a failed migration attempt");
            }
        }
    }

    protected record SourceFixture(ProcessGroupEntity processGroup, ConnectionEntity connection) {
    }
}
