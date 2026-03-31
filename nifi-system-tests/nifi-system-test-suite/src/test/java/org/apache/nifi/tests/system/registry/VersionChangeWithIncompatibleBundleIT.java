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

package org.apache.nifi.tests.system.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that changing a flow version in a cluster correctly resolves incompatible bundle
 * versions on all nodes. This reproduces a scenario where a flow registry contains snapshots
 * with bundle versions that don't match the installed NARs (e.g., after a NiFi upgrade).
 * The coordinator resolves bundles via discoverCompatibleBundles(), and this test verifies
 * the resolution propagates to all cluster nodes during replication.
 */
public class VersionChangeWithIncompatibleBundleIT extends NiFiSystemIT {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String INCOMPATIBLE_VERSION = "0.0.0-DOES-NOT-EXIST";

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    /**
     * Tests that when changing to a flow version whose registry snapshot contains bundle
     * versions not installed on the cluster, the bundles are resolved to compatible versions
     * and no ghost processors are created on any node.
     *
     * Steps:
     * 1. Create a flow with two processors and save as v1 and v2 (both with correct bundles)
     * 2. Switch back to v1
     * 3. Modify v2's snapshot.json on disk to use a non-existent bundle version, simulating
     *    what happens when a flow was registered with a different NiFi version
     * 4. Change to v2 — the coordinator should resolve the incompatible bundles
     * 5. Verify that all processors on both cluster nodes have the correct bundle version
     *    and are not ghosted
     */
    @Test
    public void testVersionChangeResolvesIncompatibleBundleAcrossCluster() throws NiFiClientException, IOException, InterruptedException {
        final File storageDir = new File("target/flowRegistryStorage/" + getTestName().replaceAll("\\(.*?\\)", ""));
        Files.createDirectories(storageDir.toPath());
        final FlowRegistryClientEntity clientEntity = registerClient(storageDir);
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity group = util.createProcessGroup("Parent", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", group.getId());
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", group.getId());
        util.createConnection(generate, terminate, "success");

        final VersionControlInformationEntity vci = util.startVersionControl(group, clientEntity, TEST_FLOWS_BUCKET, "Parent");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        util.updateProcessorProperties(generate, Collections.singletonMap("Text", "Hello World"));
        util.saveFlowVersion(group, clientEntity, vci);

        util.changeFlowVersion(group.getId(), "1");
        util.assertFlowStaleAndUnmodified(group.getId());

        modifySnapshotBundleVersions(storageDir, flowId, "2", INCOMPATIBLE_VERSION);

        final VersionedFlowUpdateRequestEntity updateResult = util.changeFlowVersion(group.getId(), "2");
        assertNull(updateResult.getRequest().getFailureReason());
        util.assertFlowUpToDate(group.getId());

        verifyProcessorsResolved(group.getId());

        switchClientToNode(2);
        verifyProcessorsResolved(group.getId());
    }

    /**
     * Tests that when a version change fails and triggers a rollback, the rollback snapshot
     * (fetched from the flow registry) also has its bundles resolved. This reproduces the
     * production scenario where:
     *
     * 1. NiFi is upgraded, so all registry snapshots have old bundle versions
     * 2. A version change is initiated but fails (e.g., connection has data)
     * 3. The rollback fetches the ORIGINAL version's snapshot from the registry
     * 4. That snapshot also has incompatible bundles that must be resolved
     */
    @Test
    public void testRollbackResolvesIncompatibleBundles() throws NiFiClientException, IOException, InterruptedException {
        final File storageDir = new File("target/flowRegistryStorage/" + getTestName().replaceAll("\\(.*?\\)", ""));
        Files.createDirectories(storageDir.toPath());
        final FlowRegistryClientEntity clientEntity = registerClient(storageDir);
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity group = util.createProcessGroup("Parent", "root");
        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", group.getId());
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", group.getId());
        util.createConnection(generate, terminate, "success");

        final VersionControlInformationEntity vci = util.startVersionControl(group, clientEntity, TEST_FLOWS_BUCKET, "Parent");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        // v2: remove connection and TerminateFlowFile, auto-terminate GenerateFlowFile
        final FlowDTO currentFlow = getNifiClient().getFlowClient().getProcessGroup(group.getId()).getProcessGroupFlow().getFlow();
        for (final ConnectionEntity connectionEntity : currentFlow.getConnections()) {
            getNifiClient().getConnectionClient().deleteConnection(connectionEntity);
        }

        getNifiClient().getProcessorClient().deleteProcessor(terminate);
        util.setAutoTerminatedRelationships(generate, "success");
        util.saveFlowVersion(group, clientEntity, vci);

        // Go back to v1 so we have the connection (GenerateFlowFile -> TerminateFlowFile)
        util.changeFlowVersion(group.getId(), "1");
        util.assertFlowStaleAndUnmodified(group.getId());

        // Get the newly re-created connection ID (it changes after version switch)
        final FlowDTO v1Flow = getNifiClient().getFlowClient().getProcessGroup(group.getId()).getProcessGroupFlow().getFlow();
        final String connectionId = v1Flow.getConnections().iterator().next().getId();

        // Get the re-created GenerateFlowFile processor entity
        ProcessorEntity generateEntity = null;
        for (final ProcessorEntity processor : v1Flow.getProcessors()) {
            if ("GenerateFlowFile".equals(processor.getComponent().getName())) {
                generateEntity = processor;
                break;
            }
        }

        assertNotNull(generateEntity, "GenerateFlowFile processor should exist after reverting to v1");

        // Generate FlowFiles to put data in the connection
        util.startProcessor(generateEntity);
        waitForQueueCount(connectionId, getNumberOfNodes());
        util.stopProcessor(generateEntity);

        // Modify v1's snapshot (the rollback target) to have incompatible bundles
        modifySnapshotBundleVersions(storageDir, flowId, "1", INCOMPATIBLE_VERSION);

        // Change to v2. This should fail because the connection has data and v2 removes it.
        // The rollback will fetch v1 from the registry, which now has incompatible bundles.
        final VersionedFlowUpdateRequestEntity updateResult = util.changeFlowVersion(group.getId(), "2", false);
        assertNotNull(updateResult.getRequest().getFailureReason());

        // After rollback, we should still be at v1 with resolved (non-ghost) processors
        util.assertFlowStaleAndUnmodified(group.getId());

        verifyProcessorsResolved(group.getId());

        switchClientToNode(2);
        verifyProcessorsResolved(group.getId());
    }

    private void modifySnapshotBundleVersions(final File storageDir, final String flowId, final String version,
                                               final String newBundleVersion) throws IOException {
        final File snapshotFile = new File(storageDir, "test-flows/" + flowId + "/" + version + "/snapshot.json");
        final JsonNode root = OBJECT_MAPPER.readTree(snapshotFile);

        modifyBundleVersionsInProcessGroup((ObjectNode) root.get("flowContents"), newBundleVersion);

        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(snapshotFile, root);
    }

    private void modifyBundleVersionsInProcessGroup(final ObjectNode processGroup, final String newVersion) {
        if (processGroup == null) {
            return;
        }

        final JsonNode processors = processGroup.get("processors");
        if (processors != null && processors.isArray()) {
            for (final JsonNode processor : processors) {
                final ObjectNode bundle = (ObjectNode) processor.get("bundle");
                if (bundle != null) {
                    bundle.put("version", newVersion);
                }
            }
        }

        final JsonNode childGroups = processGroup.get("processGroups");
        if (childGroups != null && childGroups.isArray()) {
            for (final JsonNode childGroup : childGroups) {
                modifyBundleVersionsInProcessGroup((ObjectNode) childGroup, newVersion);
            }
        }
    }

    private void verifyProcessorsResolved(final String groupId) throws NiFiClientException, IOException {
        final FlowDTO flow = getNifiClient().getFlowClient(DO_NOT_REPLICATE).getProcessGroup(groupId).getProcessGroupFlow().getFlow();
        final Set<ProcessorEntity> processors = flow.getProcessors();

        assertNotNull(processors);
        assertFalse(processors.isEmpty());

        for (final ProcessorEntity processorEntity : processors) {
            final ProcessorDTO processor = processorEntity.getComponent();

            assertFalse(processor.getExtensionMissing());
            assertEquals(getNiFiVersion(), processor.getBundle().getVersion());
        }
    }
}
