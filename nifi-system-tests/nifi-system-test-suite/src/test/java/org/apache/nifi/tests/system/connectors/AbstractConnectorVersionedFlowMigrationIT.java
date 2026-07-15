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
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowMigrationSourceDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionedFlowMigrationSourcesEntity;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared infrastructure for the Versioned-Process-Group-to-Connector migration system tests. It builds the modeled
 * source flow ({@code GenerateFlowFile} -> {@code StatefulCountProcessor} -> {@code AssetReadingProcessor}), runs it,
 * and exposes assertions used by the concrete migration tests. It intentionally declares no {@code @Test} methods; the
 * end-to-end migration scenario is exposed through {@link #verifyMigrationFromVersionedFlow(File)} so that the
 * single-node and clustered tests can each run it against their own topology without inheriting one another's tests.
 */
public abstract class AbstractConnectorVersionedFlowMigrationIT extends NiFiSystemIT {
    protected static final String TEST_BUCKET = "test-flows";
    protected static final String MIGRATABLE_FLOW_NAME = "Asset Ingest Flow";
    protected static final File SAMPLE_ASSET_FILE = new File("src/test/resources/sample-assets/helloworld.txt");

    protected static final String ASSET_PARAMETER_NAME = "Asset File";
    protected static final String GENERATE_SCHEDULE = "1 sec";
    protected static final String FLOW_CONFIGURATION_STEP = "Flow Configuration";

    protected static final String GENERATE_TYPE = "GenerateFlowFile";
    protected static final String COUNT_TYPE = "StatefulCountProcessor";
    protected static final String ASSET_READER_TYPE = "AssetReadingProcessor";

    /**
     * Runs the full end-to-end migration of the modeled versioned flow into a Connector and asserts that configuration,
     * assets, and component state were copied, that the source Process Group was renamed and disabled, that the
     * Connector is valid and passes configuration verification, and that running the migrated Connector reproduces the
     * source behavior by reading the migrated asset and writing it to {@code outputFile}. The output file is supplied by
     * the caller so that the single-node and clustered tests write to distinct locations.
     */
    protected void verifyMigrationFromVersionedFlow(final File outputFile) throws Exception {
        deleteFile(outputFile);

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture(MIGRATABLE_FLOW_NAME, registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();
        final VersionedFlowMigrationSourcesEntity sourcesEntity = getClientUtil().listMigrationSources(connectorId);
        assertTrue(isSourceListed(sourcesEntity, sourceFixture.processGroup().getId()));

        // Remove the file the source produced so that, once the migrated connector runs, its recreation proves the
        // migrated flow reads the same asset and writes it to the same output file.
        deleteFile(outputFile);
        migrateFromLocalSource(connectorId, sourceFixture.processGroup().getId());

        assertSourceRenamedAndDisabled(sourceFixture, MIGRATABLE_FLOW_NAME);

        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();
        final AssetsEntity connectorAssets = connectorClient.getAssets(connectorId);
        assertNotNull(connectorAssets.getAssets());
        assertFalse(connectorAssets.getAssets().isEmpty());
        assertMigratedAssetReferenceRecorded(connectorId);

        final String managedGroupId = connectorClient.getConnector(connectorId).getComponent().getManagedProcessGroupId();
        assertEquals(outputFile.getAbsolutePath(), getManagedProcessorProperty(connectorId, managedGroupId, ASSET_READER_TYPE, "Output File"));
        assertEquals(GENERATE_SCHEDULE, getManagedProcessorSchedule(connectorId, managedGroupId, GENERATE_TYPE));

        assertMigratedState(connectorId, managedGroupId, GENERATE_TYPE, false);
        assertMigratedState(connectorId, managedGroupId, COUNT_TYPE, true);

        getClientUtil().waitForValidConnector(connectorId);
        assertConfigurationVerified(connectorId);

        getClientUtil().startConnector(connectorId);
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        assertEquals(Files.readString(SAMPLE_ASSET_FILE.toPath()).trim(), Files.readString(outputFile.toPath()).trim());
    }

    protected SourceFixture createSourceFixture(final String flowName, final FlowRegistryClientEntity registryClient, final boolean includeAsset, final File outputFile,
                                                final boolean versionControlled) throws Exception {
        final ProcessGroupEntity processGroup = getClientUtil().createProcessGroup(flowName, "root");

        final String sourceFilePropertyValue;
        if (includeAsset) {
            final ParameterContextEntity assetContext = getClientUtil().createParameterContext(flowName + "-asset", Map.of());
            final AssetEntity assetEntity = getNifiClient().getParamContextClient().createAsset(assetContext.getId(), SAMPLE_ASSET_FILE.getName(), SAMPLE_ASSET_FILE);
            final Map<String, List<String>> assetReferences = Map.of(ASSET_PARAMETER_NAME, List.of(assetEntity.getAsset().getId()));
            final ParameterContextUpdateRequestEntity requestEntity = getClientUtil().updateParameterAssetReferences(assetContext, assetReferences);
            getClientUtil().waitForParameterContextRequestToComplete(assetContext.getId(), requestEntity.getRequest().getRequestId());
            getClientUtil().setParameterContext(processGroup.getId(), assetContext);
            sourceFilePropertyValue = "#{" + ASSET_PARAMETER_NAME + "}";
        } else {
            sourceFilePropertyValue = SAMPLE_ASSET_FILE.getAbsolutePath();
        }

        final ProcessorEntity createdGenerate = getClientUtil().createProcessor(GENERATE_TYPE, processGroup.getId());
        final ProcessorEntity generate = getClientUtil().updateProcessorProperties(createdGenerate, Map.of("Max FlowFiles", "1", "File Size", "0 B"));
        getClientUtil().updateProcessorSchedulingPeriod(generate, GENERATE_SCHEDULE);
        final ProcessorEntity count = getClientUtil().createProcessor(COUNT_TYPE, processGroup.getId());
        final ProcessorEntity assetReader = getClientUtil().updateProcessorProperties(getClientUtil().createProcessor(ASSET_READER_TYPE, processGroup.getId()),
            Map.of("Source File", sourceFilePropertyValue, "Output File", outputFile.getAbsolutePath()));
        getClientUtil().setAutoTerminatedRelationships(assetReader, Set.of("success", "failure"));

        final ConnectionEntity sourceConnection = getClientUtil().createConnection(generate, count, "success");
        getClientUtil().createConnection(count, assetReader, "success");

        if (versionControlled) {
            assertNotNull(getClientUtil().startVersionControl(processGroup, registryClient, TEST_BUCKET, flowName));
        }

        return new SourceFixture(processGroup, sourceConnection);
    }

    protected void prepareSourceForMigration(final SourceFixture sourceFixture, final File outputFile) throws Exception {
        // Run the source so its processors accumulate component state, then stop and drain it so the group is eligible
        // for migration (no running processors, no queued FlowFiles). The accumulated state is what migration copies.
        runSource(sourceFixture, outputFile);
        getClientUtil().assertFlowUpToDate(sourceFixture.processGroup().getId());
    }

    protected void runSource(final SourceFixture sourceFixture, final File outputFile) throws Exception {
        getClientUtil().startProcessGroupComponents(sourceFixture.processGroup().getId());
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        getClientUtil().stopProcessGroupComponents(sourceFixture.processGroup().getId());
        drainAllQueues(sourceFixture.processGroup().getId());
    }

    protected void migrateFromLocalSource(final String connectorId, final String processGroupId) throws Exception {
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, processGroupId);
        getClientUtil().waitForMigrationSuccess(connectorId, requestEntity.getRequest().getRequestId());
    }

    protected void drainAllQueues(final String processGroupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getFlowClient().getProcessGroup(processGroupId);
        for (final ConnectionEntity connection : flowEntity.getProcessGroupFlow().getFlow().getConnections()) {
            getClientUtil().emptyQueue(connection.getId());
        }
    }

    protected void exportSource(final String processGroupId, final File exportFile) throws NiFiClientException, IOException {
        getNifiClient().getProcessGroupClient().exportProcessGroup(processGroupId, true, true, exportFile);
    }

    protected void deleteFile(final File file) {
        assertTrue(file.delete() || !file.exists(), "Failed to delete file " + file.getAbsolutePath());
    }

    protected void assertSourceRenamedAndDisabled(final SourceFixture sourceFixture, final String originalName) throws NiFiClientException, IOException {
        final ProcessGroupEntity migratedSourceGroup = getNifiClient().getProcessGroupClient().getProcessGroup(sourceFixture.processGroup().getId());
        assertEquals("(Migrated) " + originalName, migratedSourceGroup.getComponent().getName());

        final ProcessGroupFlowEntity migratedSourceFlow = getNifiClient().getFlowClient().getProcessGroup(sourceFixture.processGroup().getId());
        for (final ProcessorEntity sourceProcessor : migratedSourceFlow.getProcessGroupFlow().getFlow().getProcessors()) {
            assertEquals("DISABLED", sourceProcessor.getComponent().getState(),
                    "Migrated source processor " + sourceProcessor.getComponent().getName() + " must be DISABLED after successful migration");
        }
    }

    protected void assertMigratedAssetReferenceRecorded(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorEntity connectorEntity = getNifiClient().getConnectorClient().getConnector(connectorId);
        final ConnectorConfigurationDTO activeConfiguration = connectorEntity.getComponent().getActiveConfiguration();
        assertNotNull(activeConfiguration);
        assertNotNull(activeConfiguration.getConfigurationStepConfigurations());

        ConnectorValueReferenceDTO assetReference = null;
        for (final ConfigurationStepConfigurationDTO step : activeConfiguration.getConfigurationStepConfigurations()) {
            if (!FLOW_CONFIGURATION_STEP.equals(step.getConfigurationStepName()) || step.getPropertyGroupConfigurations() == null) {
                continue;
            }

            for (final PropertyGroupConfigurationDTO group : step.getPropertyGroupConfigurations()) {
                if (group.getPropertyValues() != null && group.getPropertyValues().get(ASSET_PARAMETER_NAME) != null) {
                    assetReference = group.getPropertyValues().get(ASSET_PARAMETER_NAME);
                }
            }
        }

        assertNotNull(assetReference, "Asset File property must be recorded on the Flow Configuration step");
        assertEquals("ASSET_REFERENCE", assetReference.getValueType());
        assertNotNull(assetReference.getAssetReferences());
        assertFalse(assetReference.getAssetReferences().isEmpty());
    }

    protected String getManagedProcessorProperty(final String connectorId, final String groupId, final String processorTypeSuffix, final String propertyName) throws NiFiClientException, IOException {
        return findManagedProcessor(connectorId, groupId, processorTypeSuffix).getComponent().getConfig().getProperties().get(propertyName);
    }

    protected String getManagedProcessorSchedule(final String connectorId, final String groupId, final String processorTypeSuffix) throws NiFiClientException, IOException {
        return findManagedProcessor(connectorId, groupId, processorTypeSuffix).getComponent().getConfig().getSchedulingPeriod();
    }

    protected String getProcessorId(final String connectorId, final String groupId, final String processorTypeSuffix) throws NiFiClientException, IOException {
        return findManagedProcessor(connectorId, groupId, processorTypeSuffix).getId();
    }

    private ProcessorEntity findManagedProcessor(final String connectorId, final String groupId, final String processorTypeSuffix) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getConnectorClient().getFlow(connectorId, groupId);
        for (final ProcessorEntity processor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            if (processor.getComponent().getType().endsWith(processorTypeSuffix)) {
                return processor;
            }
        }

        throw new IllegalStateException("Could not find managed processor ending with type " + processorTypeSuffix);
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

    protected ProcessorEntity findCanvasProcessor(final String processGroupId, final String processorTypeSuffix) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getFlowClient().getProcessGroup(processGroupId);
        for (final ProcessorEntity processor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            if (processor.getComponent().getType().endsWith(processorTypeSuffix)) {
                return getNifiClient().getProcessorClient().getProcessor(processor.getId());
            }
        }

        throw new IllegalStateException("Could not find processor with type ending in " + processorTypeSuffix + " in Process Group " + processGroupId);
    }

    protected void assertMigratedState(final String connectorId, final String managedGroupId, final String processorTypeSuffix, final boolean expectClusterState) throws Exception {
        final String processorId = getProcessorId(connectorId, managedGroupId, processorTypeSuffix);
        if (!getNiFiInstance().isClustered()) {
            assertLocalStatePresent(getNifiClient().getConnectorClient().getProcessorState(connectorId, processorId));
            return;
        }

        for (int nodeIndex = 1; nodeIndex <= getNumberOfNodes(); nodeIndex++) {
            switchClientToNode(nodeIndex);
            final ComponentStateEntity stateEntity = getNifiClient().getConnectorClient(DO_NOT_REPLICATE).getProcessorState(connectorId, processorId);
            assertLocalStatePresent(stateEntity);
            if (expectClusterState) {
                assertNotNull(stateEntity.getComponentState().getClusterState());
                assertFalse(stateEntity.getComponentState().getClusterState().getState().isEmpty());
            }
        }
    }

    private void assertLocalStatePresent(final ComponentStateEntity stateEntity) {
        assertNotNull(stateEntity.getComponentState());
        assertNotNull(stateEntity.getComponentState().getLocalState());
        assertFalse(stateEntity.getComponentState().getLocalState().getState().isEmpty());
    }

    protected void assertConfigurationVerified(final String connectorId) throws Exception {
        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfig(connectorId, FLOW_CONFIGURATION_STEP, Map.of());
        assertFalse(results.isEmpty());
        for (final ConfigVerificationResultDTO result : results) {
            assertEquals("SUCCESSFUL", result.getOutcome(), "Configuration verification result was not successful: " + result.getExplanation());
        }
    }

    protected void assertConnectorFresh(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();
        final String managedGroupId = connectorClient.getConnector(connectorId).getComponent().getManagedProcessGroupId();
        final ProcessGroupFlowEntity flowEntity = connectorClient.getFlow(connectorId, managedGroupId);
        assertTrue(flowEntity.getProcessGroupFlow().getFlow().getProcessors() == null || flowEntity.getProcessGroupFlow().getFlow().getProcessors().isEmpty());
        assertTrue(flowEntity.getProcessGroupFlow().getFlow().getConnections() == null || flowEntity.getProcessGroupFlow().getFlow().getConnections().isEmpty());

        final AssetsEntity assetsEntity = connectorClient.getAssets(connectorId);
        assertTrue(assetsEntity.getAssets() == null || assetsEntity.getAssets().isEmpty());
    }

    /**
     * Asserts that the source process group has been left in the same state it was in before a failed migration attempt:
     * its original name, its original version-control state, its originally-applied parameter context, and processors
     * that remain STOPPED.
     */
    protected void assertSourceUntouched(final SourceFixture sourceFixture, final String expectedName) throws NiFiClientException, IOException {
        final ProcessGroupEntity processGroupEntity = getNifiClient().getProcessGroupClient().getProcessGroup(sourceFixture.processGroup().getId());
        assertEquals(expectedName, processGroupEntity.getComponent().getName());

        assertNotNull(processGroupEntity.getComponent().getVersionControlInformation(), "Source process group must remain under version control after a failed migration attempt");
        getClientUtil().assertFlowUpToDate(sourceFixture.processGroup().getId());

        final ProcessGroupEntity originalProcessGroupEntity = sourceFixture.processGroup();
        if (originalProcessGroupEntity.getComponent().getParameterContext() != null) {
            assertNotNull(processGroupEntity.getComponent().getParameterContext(), "Source process group must retain its parameter context after a failed migration attempt");
            assertEquals(originalProcessGroupEntity.getComponent().getParameterContext().getId(), processGroupEntity.getComponent().getParameterContext().getId());
        }

        final ProcessGroupFlowEntity flowEntity = getNifiClient().getFlowClient().getProcessGroup(sourceFixture.processGroup().getId());
        for (final ProcessorEntity sourceProcessor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            assertEquals("STOPPED", sourceProcessor.getComponent().getState(),
                    "Source processor " + sourceProcessor.getComponent().getName() + " must remain STOPPED after a failed migration attempt");
        }
    }

    protected record SourceFixture(ProcessGroupEntity processGroup, ConnectionEntity sourceConnection) {
    }
}
