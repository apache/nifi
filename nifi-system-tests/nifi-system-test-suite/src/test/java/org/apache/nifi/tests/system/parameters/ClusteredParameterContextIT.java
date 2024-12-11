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
package org.apache.nifi.tests.system.parameters;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repeats all tests in ParameterContextIT but in a clustered mode
 */
public class ClusteredParameterContextIT extends ParameterContextIT {
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testSynchronizeAssets() throws NiFiClientException, IOException, InterruptedException {
        waitForAllNodesConnected();

        final Map<String, String> paramValues = Map.of("name", "foo", "fileToIngest", "");
        final ParameterContextEntity paramContext = getClientUtil().createParameterContext("testSynchronizeAssets", paramValues);

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", paramContext);

        // Create a Processor and update it to reference Parameter "name"
        final ProcessorEntity ingest = getClientUtil().createProcessor("IngestFile");
        getClientUtil().updateProcessorProperties(ingest, Map.of("Filename", "#{fileToIngest}", "Delete File", "false"));
        getClientUtil().updateProcessorSchedulingPeriod(ingest, "10 mins");

        // Create an asset and update the parameter to reference the asset
        final File assetFile = new File("src/test/resources/sample-assets/helloworld.txt");
        final AssetEntity asset = createAsset(paramContext.getId(), assetFile);

        final ParameterContextUpdateRequestEntity referenceAssetUpdateRequest = getClientUtil().updateParameterAssetReferences(
                paramContext, Map.of("fileToIngest", List.of(asset.getAsset().getId())));
        getClientUtil().waitForParameterContextRequestToComplete(paramContext.getId(), referenceAssetUpdateRequest.getRequest().getRequestId());

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(ingest, terminate, "success");
        waitForValidProcessor(ingest.getId());

        getClientUtil().startProcessor(ingest);
        waitForQueueCount(connection.getId(), getNumberOfNodes());
        final String contents = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        assertEquals("Hello, World!", contents);

        // Check that the file exists in the assets directory.
        final File node1Dir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File node1AssetsDir = new File(node1Dir, "assets");
        final File node1ContextDir = new File(node1AssetsDir, paramContext.getId());
        assertTrue(node1ContextDir.exists());

        final File node2Dir = getNiFiInstance().getNodeInstance(2).getInstanceDirectory();
        final File node2AssetsDir = new File(node2Dir, "assets");
        final File node2ContextDir = new File(node2AssetsDir, paramContext.getId());
        assertTrue(node2ContextDir.exists());

        // List assets and verify the expected asset is returned
        final AssetsEntity assetListing = assertAssetListing(paramContext.getId(), 1);
        assertAssetExists(asset, assetListing);

        // Stop node2
        disconnectNode(2);
        getNiFiInstance().getNodeInstance(2).stop();

        // Delete node2's assets
        FileUtils.deleteFilesInDir(node2AssetsDir, (dir, name) -> true, null, true, true);
        assertTrue(node2AssetsDir.delete());
        assertFalse(node2AssetsDir.exists());

        // Start node2 again
        getNiFiInstance().getNodeInstance(2).start(true);
        reconnectNode(2);
        waitForAllNodesConnected();

        // Verify node2 asset directories are back
        assertTrue(node2AssetsDir.exists());
        assertTrue(node2ContextDir.exists());

        final File[] node2AssetFiles = node2ContextDir.listFiles();
        assertNotNull(node2AssetFiles);
        assertEquals(1, node2AssetFiles.length);

        // Ensure ingest processor is valid
        waitForValidProcessor(ingest.getId());

        getClientUtil().stopProcessor(ingest);
        getClientUtil().waitForStoppedProcessor(ingest.getId());
    }

    @Test
    public void testSynchronizeAssetsAfterChangingReferences() throws NiFiClientException, IOException, InterruptedException {
        waitForAllNodesConnected();

        final Map<String, String> paramValues = Map.of("name", "foo", "filesToIngest", "");
        final ParameterContextEntity paramContext = getClientUtil().createParameterContext("testSynchronizeAssetsAfterChangingReferences", paramValues);

        // Set the Parameter Context on the root Process Group
        setParameterContext("root", paramContext);

        // Create a Processor and update it to reference Parameter "name"
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generateFlowFile, Map.of("Text", "#{filesToIngest}"));
        getClientUtil().updateProcessorSchedulingPeriod(generateFlowFile, "10 mins");

        // Create two assets and update the parameter to reference the assets
        final AssetEntity asset1 = createAsset(paramContext.getId(), new File("src/test/resources/sample-assets/helloworld.txt"));
        final AssetEntity asset2 = createAsset(paramContext.getId(), new File("src/test/resources/sample-assets/helloworld2.txt"));
        final List<String> assetIds = List.of(asset1.getAsset().getId(), asset2.getAsset().getId());

        final ParameterContextUpdateRequestEntity referenceAssetUpdateRequest = getClientUtil().updateParameterAssetReferences(paramContext, Map.of("filesToIngest", assetIds));
        getClientUtil().waitForParameterContextRequestToComplete(paramContext.getId(), referenceAssetUpdateRequest.getRequest().getRequestId());

        // Connect the Generate processor to a Terminate processor and generate flow files
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(generateFlowFile, terminate, "success");
        waitForValidProcessor(generateFlowFile.getId());

        getClientUtil().startProcessor(generateFlowFile);
        waitForQueueCount(connection.getId(), getNumberOfNodes());

        // Verify flow files reference both assets
        final String flowFileContents = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        assertTrue(flowFileContents.contains(asset1.getAsset().getName()));
        assertTrue(flowFileContents.contains(asset2.getAsset().getName()));

        // Verify the contents of the assets directory for each node
        final File node1Dir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File node1AssetsDir = new File(node1Dir, "assets");
        final File node1ContextDir = new File(node1AssetsDir, paramContext.getId());
        assertTrue(node1ContextDir.exists());

        final File node2Dir = getNiFiInstance().getNodeInstance(2).getInstanceDirectory();
        final File node2AssetsDir = new File(node2Dir, "assets");
        final File node2ContextDir = new File(node2AssetsDir, paramContext.getId());
        assertTrue(node2ContextDir.exists());

        // List assets and verify the expected asset is returned
        final AssetsEntity assetListing = assertAssetListing(paramContext.getId(), 2);
        assertAssetExists(asset1, assetListing);

        // Stop node2
        disconnectNode(2);
        getNiFiInstance().getNodeInstance(2).stop();

        // Delete node2's assets
        FileUtils.deleteFilesInDir(node2AssetsDir, (dir, name) -> true, null, true, true);
        assertTrue(node2AssetsDir.delete());
        assertFalse(node2AssetsDir.exists());

        // Modify the parameter context on node1 to only reference asset1
        final ParameterContextEntity retrievedContext = getNifiClient().getParamContextClient().getParamContext(paramContext.getId(), false);
        final ParameterContextUpdateRequestEntity changeAssetsUpdateRequest = getClientUtil().updateParameterAssetReferences(
                retrievedContext, Map.of("filesToIngest", List.of(asset1.getAsset().getId())));
        getClientUtil().waitForParameterContextRequestToComplete(paramContext.getId(), changeAssetsUpdateRequest.getRequest().getRequestId());

        // Start node2 again
        getNiFiInstance().getNodeInstance(2).start(true);
        reconnectNode(2);
        waitForAllNodesConnected();

        // Verify node2 asset directories are back
        assertTrue(node2AssetsDir.exists());
        assertTrue(node2ContextDir.exists());

        // Stop generate processor and clear queue
        getClientUtil().stopProcessor(generateFlowFile);
        getClientUtil().waitForStoppedProcessor(generateFlowFile.getId());

        getClientUtil().startProcessor(terminate);
        waitForQueueCount(connection.getId(), 0);

        getClientUtil().stopProcessor(terminate);
        getClientUtil().waitForStoppedProcessor(terminate.getId());

        // Start generate again
        getClientUtil().startProcessor(generateFlowFile);
        waitForQueueCount(connection.getId(), getNumberOfNodes());

        // Verify flow files only reference the first asset
        final String flowFile1Contents = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 0);
        assertTrue(flowFile1Contents.contains(asset1.getAsset().getName()));
        assertFalse(flowFile1Contents.contains(asset2.getAsset().getName()));

        final String flowFile2Contents = getClientUtil().getFlowFileContentAsUtf8(connection.getId(), 1);
        assertTrue(flowFile2Contents.contains(asset1.getAsset().getName()));
        assertFalse(flowFile2Contents.contains(asset2.getAsset().getName()));

        getClientUtil().stopProcessor(generateFlowFile);
        getClientUtil().waitForStoppedProcessor(generateFlowFile.getId());
    }
}
