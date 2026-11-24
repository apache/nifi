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
package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a Cluster Coordinator whose own flow loses the flow election synchronizes the assets it is missing
 * from the nodes whose flow was elected.
 */
public class ClusteredAssetCoordinatorElectionIT extends NiFiSystemIT {

    private static final File SOURCE_ASSET_FILE = new File("src/test/resources/sample-assets/helloworld.txt");
    private static final String COORDINATOR_ELECTED_LOG_MESSAGE = "This node has been elected Active " + ClusterRoles.CLUSTER_COORDINATOR;

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        final Map<String, String> propertyOverrides = Map.of(
                "nifi.cluster.flow.election.max.candidates", "2",
                "nifi.cluster.flow.election.max.wait.time", "2 mins");

        return new SpawnedClusterNiFiInstanceFactory(
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                        .instanceDirectory("target/node1")
                        .overrideNifiProperties(propertyOverrides)
                        .build(),
                new InstanceConfiguration.Builder()
                        .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                        .instanceDirectory("target/node2")
                        .overrideNifiProperties(propertyOverrides)
                        .build());
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testCoordinatorWithoutParameterContextAssetsSynchronizesFromElectedFlowNode() throws NiFiClientException, IOException, InterruptedException {
        waitForAllNodesConnected();

        final ParameterContextEntity paramContext = getClientUtil().createParameterContext("testCoordinatorAssetSync", Map.of("fileToIngest", ""));
        getClientUtil().setParameterContext("root", paramContext);

        final ProcessorEntity ingest = getClientUtil().createProcessor("IngestFile");
        getClientUtil().updateProcessorProperties(ingest, Map.of("Filename", "#{fileToIngest}", "Delete File", "false"));
        getClientUtil().updateProcessorSchedulingPeriod(ingest, "10 mins");

        final AssetEntity asset = getNifiClient().getParamContextClient().createAsset(paramContext.getId(), SOURCE_ASSET_FILE.getName(), SOURCE_ASSET_FILE);
        final ParameterContextUpdateRequestEntity referenceAssetUpdateRequest = getClientUtil().updateParameterAssetReferences(
                paramContext, Map.of("fileToIngest", List.of(asset.getAsset().getId())));
        getClientUtil().waitForParameterContextRequestToComplete(paramContext.getId(), referenceAssetUpdateRequest.getRequest().getRequestId());

        final ProcessorEntity terminate = getClientUtil().createProcessor(TERMINATE_FLOWFILE);
        getClientUtil().createConnection(ingest, terminate, SUCCESS);
        getClientUtil().waitForValidProcessor(ingest.getId());

        restartWithNode1AsEmptyCoordinator(List.of("assets"));

        final File node1Dir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File restoredAssetFile = new File(node1Dir, "assets/" + paramContext.getId() + "/" + SOURCE_ASSET_FILE.getName());
        assertTrue(restoredAssetFile.exists());
        assertArrayEquals(Files.readAllBytes(SOURCE_ASSET_FILE.toPath()), Files.readAllBytes(restoredAssetFile.toPath()));

        getClientUtil().waitForValidProcessor(ingest.getId());
    }

    @Test
    public void testCoordinatorWithoutConnectorAssetsSynchronizesFromElectedFlowNode() throws NiFiClientException, IOException, InterruptedException {
        waitForAllNodesConnected();

        final ConnectorEntity connector = getClientUtil().createConnector("AssetConnector");
        final String connectorId = connector.getId();

        final AssetEntity assetEntity = getNifiClient().getConnectorClient().createAsset(connectorId, SOURCE_ASSET_FILE.getName(), SOURCE_ASSET_FILE);
        final String uploadedAssetId = assetEntity.getAsset().getId();

        final ConnectorValueReferenceDTO assetValueReference = new ConnectorValueReferenceDTO();
        assetValueReference.setValueType("ASSET_REFERENCE");
        assetValueReference.setAssetReferences(List.of(new AssetReferenceDTO(uploadedAssetId)));
        getClientUtil().configureConnectorWithReferences(connectorId, "Asset Configuration", Map.of("Test Asset", assetValueReference));
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        restartWithNode1AsEmptyCoordinator(List.of("connector_assets"));

        final File node1Dir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File restoredAssetFile = new File(node1Dir, "connector_assets/" + connectorId + "/" + uploadedAssetId + "/" + SOURCE_ASSET_FILE.getName());
        assertTrue(restoredAssetFile.exists());
        assertArrayEquals(Files.readAllBytes(SOURCE_ASSET_FILE.toPath()), Files.readAllBytes(restoredAssetFile.toPath()));

        getClientUtil().waitForValidConnector(connectorId);
    }

    /**
     * Stops the cluster, removes the flow and the given asset directories from node 1, and restarts the cluster so that node 1 becomes
     * Cluster Coordinator with an empty flow while node 2 provides the elected flow. Node 1 hosts the embedded ZooKeeper and is started
     * alone first so that it gains the coordinator role, while the flow election waits for the vote from node 2.
     */
    private void restartWithNode1AsEmptyCoordinator(final List<String> assetDirectoryNames) throws IOException, InterruptedException, NiFiClientException {
        getNiFiInstance().stop();

        final NiFiInstance node1 = getNiFiInstance().getNodeInstance(1);
        final File node1Dir = node1.getInstanceDirectory();
        final File node1ConfDir = new File(node1Dir, "conf");
        Files.deleteIfExists(new File(node1ConfDir, "flow.json.gz").toPath());

        final File node1ArchiveDir = new File(node1ConfDir, "archive");
        if (node1ArchiveDir.exists()) {
            FileUtils.deleteFilesInDir(node1ArchiveDir, (dir, name) -> true, null, true, true);
        }

        for (final String assetDirectoryName : assetDirectoryNames) {
            final File assetDir = new File(node1Dir, assetDirectoryName);
            if (assetDir.exists()) {
                FileUtils.deleteFilesInDir(assetDir, (dir, name) -> true, null, true, true);
                Files.delete(assetDir.toPath());
            }
        }

        final File node1AppLog = new File(node1Dir, "logs/nifi-app.log");
        Files.deleteIfExists(node1AppLog.toPath());

        node1.start(false);
        waitFor(() -> node1AppLog.exists() && Files.readString(node1AppLog.toPath(), StandardCharsets.UTF_8).contains(COORDINATOR_ELECTED_LOG_MESSAGE), 500L,
                "node 1 to be elected Cluster Coordinator");

        getNiFiInstance().getNodeInstance(2).start(true);
        setupClient();
        waitForAllNodesConnected();

        assertTrue(getNodeEntity(1).getNode().getRoles().contains(ClusterRoles.CLUSTER_COORDINATOR));
    }
}
