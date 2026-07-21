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

import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.VersionedFlowMigrationSourceDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.MigrationPayloadEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionedFlowMigrationSourcesEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Single-node system tests for migrating a versioned Process Group into a Connector. This covers the end-to-end happy
 * path, migration-failure rollback, migration-source eligibility listing, and migration from an uploaded payload.
 * Tests that require restarting NiFi live in {@link ConnectorVersionedFlowMigrationRestartIT}, and the clustered
 * variants live in {@link ClusteredConnectorVersionedFlowMigrationIT}.
 */
public class ConnectorVersionedFlowMigrationIT extends AbstractConnectorVersionedFlowMigrationIT {
    private static final String UNSUPPORTED_FLOW_NAME = "Unsupported Ingest Flow";

    @Test
    public void testMigrateConnectorFromVersionedFlow() throws Exception {
        verifyMigrationFromVersionedFlow(new File("target/migration/local-output.txt"));
    }

    @Test
    public void testMigrationFailureRollsBackConnectorAndLeavesSourceUntouched() throws Exception {
        final File outputFile = new File("target/migration/failure-local-output.txt");
        deleteFile(outputFile);

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("FailureSource", registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final String sourceGroupId = sourceFixture.processGroup().getId();
        final String originalName = getNifiClient().getProcessGroupClient().getProcessGroup(sourceGroupId).getComponent().getName();

        final ConnectorEntity connector = getClientUtil().createConnector("FailingConfigurationMigrationConnector");
        final String connectorId = connector.getId();
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceGroupId);
        final MigrationRequestEntity completedRequest = getClientUtil().waitForMigrationFailure(connectorId, requestEntity.getRequest().getRequestId());
        assertNotNull(completedRequest.getRequest().getFailureReason());

        assertConnectorFresh(connectorId);
        assertSourceUntouched(sourceFixture, originalName);
    }

    @Test
    public void testCorruptPayloadUploadRejectedAndConnectorRemainsFresh() throws Exception {
        final File corruptPayload = new File("target/migration/corrupt-payload.json");
        corruptPayload.getParentFile().mkdirs();
        Files.writeString(corruptPayload.toPath(), "{ this is not valid json");

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        assertThrows(NiFiClientException.class, () -> getClientUtil().uploadMigrationPayload(connector.getId(), corruptPayload));

        assertConnectorFresh(connector.getId());
    }

    @Test
    public void testMigrationSourcesListingFiltersSourcesWithNonMatchingFlowName() throws Exception {
        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture matchingFixture = createSourceFixture(MIGRATABLE_FLOW_NAME, registryClient, false, new File("target/migration/eligibility-matching.txt"), true);
        final SourceFixture nonMatchingFixture = createSourceFixture(UNSUPPORTED_FLOW_NAME, registryClient, false, new File("target/migration/eligibility-non-matching.txt"), true);

        final ConnectorEntity targetConnector = getClientUtil().createConnector("MigrationTargetConnector");
        final VersionedFlowMigrationSourcesEntity sourcesEntity = getClientUtil().listMigrationSources(targetConnector.getId());
        assertTrue(isSourceListed(sourcesEntity, matchingFixture.processGroup().getId()));
        assertFalse(isSourceListed(sourcesEntity, nonMatchingFixture.processGroup().getId()));

        final ConnectorEntity nonMigratingConnector = getClientUtil().createConnector("NonMigratingConnector");
        final VersionedFlowMigrationSourcesEntity nonMigratingSources = getClientUtil().listMigrationSources(nonMigratingConnector.getId());
        assertTrue(nonMigratingSources.getMigrationSources() == null || nonMigratingSources.getMigrationSources().isEmpty());
    }

    @Test
    public void testLocalMigrationRejectsNonVersionControlledSource() throws Exception {
        final SourceFixture sourceFixture = createSourceFixture(MIGRATABLE_FLOW_NAME, registerClient(), false, new File("target/migration/eligibility-local-failure.txt"), false);
        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");

        assertThrows(NiFiClientException.class, () -> getClientUtil().startMigrationFromLocalSource(connector.getId(), sourceFixture.processGroup().getId()));
    }

    @Test
    public void testMigrationSourcesListingReportsAllConcurrentIneligibilityReasons() throws Exception {
        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture(MIGRATABLE_FLOW_NAME, registryClient, false, new File("target/migration/eligibility-not-ready.txt"), true);

        // Start only the GenerateFlowFile source so a backlog builds in the source-to-consumer connection while the
        // downstream processors stay stopped. This puts the Process Group into a state where two remediable conditions
        // hold at once: a running processor and queued FlowFiles.
        final ProcessorEntity producer = findCanvasProcessor(sourceFixture.processGroup().getId(), "GenerateFlowFile");
        getClientUtil().startProcessor(producer);
        waitForMinQueueCount(sourceFixture.sourceConnection().getId(), 1);

        try {
            final ConnectorEntity targetConnector = getClientUtil().createConnector("MigrationTargetConnector");
            final VersionedFlowMigrationSourcesEntity sourcesEntity = getClientUtil().listMigrationSources(targetConnector.getId());

            final VersionedFlowMigrationSourceDTO listedSource = findListedSource(sourcesEntity, sourceFixture.processGroup().getId());
            assertNotNull(listedSource, "A flow with a matching name must still be listed even when it is not currently ready for migration");
            assertFalse(listedSource.isReadyForMigration());
            assertEquals(sourceFixture.processGroup().getId(), listedSource.getProcessGroupId());
            assertEquals(MIGRATABLE_FLOW_NAME, listedSource.getProcessGroupName());
            assertNotNull(listedSource.getParentProcessGroupId(), "The parent Process Group identifier must be populated so callers know where the source lives on the canvas");

            final List<String> ineligibilityReasons = listedSource.getIneligibilityReasons();
            assertNotNull(ineligibilityReasons);
            assertTrue(containsSubstring(ineligibilityReasons, "Running or enabled processor"), "Ineligibility reasons must surface running processors: " + ineligibilityReasons);
            assertTrue(containsSubstring(ineligibilityReasons, "Queued FlowFile"), "Ineligibility reasons must surface queued FlowFiles: " + ineligibilityReasons);
            assertTrue(ineligibilityReasons.size() >= 2, "All applicable ineligibility reasons must be reported together, not just the first one. Found: " + ineligibilityReasons);
        } finally {
            getClientUtil().stopProcessor(producer);
        }
    }

    @Test
    public void testUploadedPayloadAcceptsNonVersionControlledSource() throws Exception {
        final File outputFile = new File("target/migration/eligibility-uploaded.txt");
        final File exportFile = new File("target/migration/eligibility-uploaded.json");
        deleteFile(outputFile);
        deleteFile(exportFile);

        final SourceFixture sourceFixture = createSourceFixture(MIGRATABLE_FLOW_NAME, registerClient(), false, outputFile, false);
        runSource(sourceFixture, outputFile);
        exportSource(sourceFixture.processGroup().getId(), exportFile);
        deleteFile(outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final MigrationPayloadEntity payloadEntity = getClientUtil().uploadMigrationPayload(connector.getId(), exportFile);
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromPayload(connector.getId(), payloadEntity.getPayload().getPayloadId());
        getClientUtil().waitForMigrationSuccess(connector.getId(), requestEntity.getRequest().getRequestId());
    }

    @Test
    public void testMigrateConnectorFromUploadedPayload() throws Exception {
        final File outputFile = new File("target/migration/uploaded-output.txt");
        final File exportFile = new File("target/migration/uploaded-source.json");
        deleteFile(outputFile);
        deleteFile(exportFile);

        final SourceFixture sourceFixture = createSourceFixture(MIGRATABLE_FLOW_NAME, registerClient(), false, outputFile, false);
        runSource(sourceFixture, outputFile);

        exportSource(sourceFixture.processGroup().getId(), exportFile);
        final ProcessGroupEntity sourceProcessGroup = getNifiClient().getProcessGroupClient().getProcessGroup(sourceFixture.processGroup().getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(sourceProcessGroup);

        deleteFile(outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();
        final MigrationPayloadEntity payloadEntity = getClientUtil().uploadMigrationPayload(connectorId, exportFile);
        assertNotNull(payloadEntity.getPayload());

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromPayload(connectorId, payloadEntity.getPayload().getPayloadId());
        getClientUtil().waitForMigrationSuccess(connectorId, requestEntity.getRequest().getRequestId());

        getClientUtil().startConnector(connectorId);
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        assertEquals(Files.readString(SAMPLE_ASSET_FILE.toPath()).trim(), Files.readString(outputFile.toPath()).trim());
    }

    private static boolean containsSubstring(final List<String> values, final String substring) {
        for (final String value : values) {
            if (value.contains(substring)) {
                return true;
            }
        }

        return false;
    }
}
