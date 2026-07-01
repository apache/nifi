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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorVersionedFlowMigrationEligibilityIT extends ConnectorVersionedFlowMigrationLocalIT {
    @Test
    public void testMigrationSourcesListingFiltersUnsupportedSources() throws Exception {
        final FlowRegistryClientEntity registryClient = registerClient();
        final File matchingOutputFile = new File("target/migration/eligibility-matching.txt");
        final File nonMatchingOutputFile = new File("target/migration/eligibility-non-matching.txt");

        final SourceFixture matchingFixture = createSourceFixture("EligibilityMatching", registryClient, false, matchingOutputFile, true);
        final ProcessGroupEntity nonMatchingGroup = createProcessGroupWithoutRequiredParameter("EligibilityNonMatching", registryClient, nonMatchingOutputFile);

        final ConnectorEntity targetConnector = getClientUtil().createConnector("MigrationTargetConnector");
        final VersionedFlowMigrationSourcesEntity sourcesEntity = getClientUtil().listMigrationSources(targetConnector.getId());
        assertTrue(isSourceListed(sourcesEntity, matchingFixture.processGroup().getId()));
        assertFalse(isSourceListed(sourcesEntity, nonMatchingGroup.getId()));

        final ConnectorEntity nonMigratingConnector = getClientUtil().createConnector("NonMigratingConnector");
        final VersionedFlowMigrationSourcesEntity nonMigratingSources = getClientUtil().listMigrationSources(nonMigratingConnector.getId());
        assertTrue(nonMigratingSources.getMigrationSources() == null || nonMigratingSources.getMigrationSources().isEmpty());
    }

    @Test
    public void testLocalMigrationRejectsNonVersionControlledSource() throws Exception {
        final SourceFixture sourceFixture = createSourceFixture("EligibilityLocalFailure", registerClient(), false,
            new File("target/migration/eligibility-local-failure.txt"), false);
        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");

        assertThrows(NiFiClientException.class, () -> getClientUtil().startMigrationFromLocalSource(connector.getId(), sourceFixture.processGroup().getId()));
    }

    @Test
    public void testMigrationSourcesListingReportsAllConcurrentIneligibilityReasons() throws Exception {
        final FlowRegistryClientEntity registryClient = registerClient();
        final File outputFile = new File("target/migration/eligibility-not-ready.txt");
        outputFile.delete();

        final SourceFixture sourceFixture = createSourceFixture("EligibilityNotReady", registryClient, false, outputFile, true);

        // Start the producer so processors are running, but keep the consumer stopped so a backlog of FlowFiles builds
        // up in the source-to-consumer connection. This puts the Process Group into a state where two remediable
        // conditions hold simultaneously: running processors and queued FlowFiles.
        final ProcessorEntity producer = findProcessorByTypeSuffix(sourceFixture.processGroup().getId(), "StatefulCountProcessor");
        getClientUtil().startProcessor(producer);

        // The producer keeps running while the consumer stays stopped, so the source-to-consumer connection
        // accumulates an unbounded backlog. Wait for at least one queued FlowFile rather than an exact count, since
        // the backlog can grow past a specific count before the queue is sampled.
        waitForMinQueueCount(sourceFixture.connection().getId(), 1);

        try {
            final ConnectorEntity targetConnector = getClientUtil().createConnector("MigrationTargetConnector");
            final VersionedFlowMigrationSourcesEntity sourcesEntity =
                    getClientUtil().listMigrationSources(targetConnector.getId());

            final VersionedFlowMigrationSourceDTO listedSource =
                    findListedSource(sourcesEntity, sourceFixture.processGroup().getId());
            assertNotNull(listedSource,
                    "A structurally compatible Process Group must still be listed even when it is not currently ready for migration");

            assertFalse(listedSource.isReadyForMigration());
            assertEquals(sourceFixture.processGroup().getId(), listedSource.getProcessGroupId());
            assertEquals("EligibilityNotReady", listedSource.getProcessGroupName());
            assertNotNull(listedSource.getParentProcessGroupId(),
                    "The parent Process Group identifier must be populated so callers know where the source lives on the canvas");

            final List<String> ineligibilityReasons = listedSource.getIneligibilityReasons();
            assertNotNull(ineligibilityReasons);
            assertTrue(containsSubstring(ineligibilityReasons, "running or enabled"),
                    "Ineligibility reasons must surface running processors: " + ineligibilityReasons);
            assertTrue(containsSubstring(ineligibilityReasons, "queued FlowFile"),
                    "Ineligibility reasons must surface queued FlowFiles: " + ineligibilityReasons);
            assertTrue(ineligibilityReasons.size() >= 2,
                    "All applicable ineligibility reasons must be reported together, not just the first one. Found: " + ineligibilityReasons);
        } finally {
            getClientUtil().stopProcessor(producer);
        }
    }

    @Test
    public void testUploadedPayloadAcceptsNonVersionControlledSource() throws Exception {
        final File outputFile = new File("target/migration/eligibility-uploaded.txt");
        final File exportFile = new File("target/migration/eligibility-uploaded.json");
        outputFile.delete();
        exportFile.delete();

        final SourceFixture sourceFixture = createSourceFixture("EligibilityUploaded", registerClient(), false, outputFile, false);
        getClientUtil().startProcessGroupComponents(sourceFixture.processGroup().getId());
        waitFor(() -> outputFile.exists() && outputFile.length() > 0);
        getClientUtil().stopProcessGroupComponents(sourceFixture.processGroup().getId());
        getClientUtil().emptyQueue(sourceFixture.connection().getId());

        getNifiClient().getProcessGroupClient().exportProcessGroup(sourceFixture.processGroup().getId(), true, true, exportFile);

        outputFile.delete();

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final MigrationPayloadEntity payloadEntity = getClientUtil().uploadMigrationPayload(connector.getId(), exportFile);
        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromPayload(connector.getId(), payloadEntity.getPayload().getPayloadId());
        getClientUtil().waitForMigrationSuccess(connector.getId(), requestEntity.getRequest().getRequestId());
    }

    private ProcessGroupEntity createProcessGroupWithoutRequiredParameter(final String name, final FlowRegistryClientEntity registryClient, final File outputFile) throws Exception {
        final ProcessGroupEntity processGroup = getClientUtil().createProcessGroup(name, "root");
        final ProcessorEntity statefulCount = getClientUtil().createProcessor("StatefulCountProcessor", processGroup.getId());
        final ProcessorEntity assetReader = getClientUtil().updateProcessorProperties(
            getClientUtil().createProcessor("AssetReadingProcessor", processGroup.getId()),
            Map.of(
                "Source File", SAMPLE_ASSET_FILE.getAbsolutePath(),
                "Output File", outputFile.getAbsolutePath()));
        getClientUtil().setAutoTerminatedRelationships(assetReader, Set.of("success", "failure"));
        getClientUtil().createConnection(statefulCount, assetReader, "success");
        getClientUtil().startVersionControl(processGroup, registryClient, TEST_BUCKET, name);
        return processGroup;
    }

    private ProcessorEntity findProcessorByTypeSuffix(final String processGroupId, final String typeSuffix) throws Exception {
        final Set<ProcessorEntity> processors =
                getNifiClient().getFlowClient().getProcessGroup(processGroupId).getProcessGroupFlow().getFlow().getProcessors();
        for (final ProcessorEntity processor : processors) {
            if (processor.getComponent().getType().endsWith(typeSuffix)) {
                return getNifiClient().getProcessorClient().getProcessor(processor.getId());
            }
        }
        throw new IllegalStateException("Could not find processor with type ending in " + typeSuffix + " in Process Group " + processGroupId);
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
