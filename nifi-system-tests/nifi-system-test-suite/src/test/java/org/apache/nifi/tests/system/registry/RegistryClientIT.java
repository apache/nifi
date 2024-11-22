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

import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RegistryClientIT extends NiFiSystemIT {
    public static final String TEST_FLOWS_BUCKET = "test-flows";

    public static final String FIRST_FLOW_ID = "first-flow";

    /**
     * Test a scenario where we have Parent Process Group with a child process group. The child group is under Version Control.
     * Then the parent is placed under Version Control. Then modify a Processor in child. Register snapshot for child, then for parent.
     * Then start Flow.
     * Then change between versions at the Parent level while the flow is stopped and while it's running.
     */
    @Test
    public void testChangeVersionOnParentThatCascadesToChild() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity parent = util.createProcessGroup("Parent", "root");
        final ProcessGroupEntity child = util.createProcessGroup("Child", parent.getId());
        final PortEntity inputPort = util.createInputPort("Input Port", child.getId());
        final PortEntity outputPort = util.createOutputPort("Output Port", child.getId());
        final ProcessorEntity updateContents = util.createProcessor("UpdateContent", child.getId());
        util.updateProcessorProperties(updateContents, Collections.singletonMap("Content", "Updated"));

        util.createConnection(inputPort, updateContents);
        util.createConnection(updateContents, outputPort, "success");

        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", parent.getId());
        util.updateProcessorProperties(generate, Collections.singletonMap("Text", "Hello World"));
        util.createConnection(generate, inputPort, "success");

        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", parent.getId());
        final ConnectionEntity connectionToTerminate = util.createConnection(outputPort, terminate);

        final VersionControlInformationEntity childVci = util.startVersionControl(child, clientEntity, TEST_FLOWS_BUCKET, "Child");
        final VersionControlInformationEntity parentVci = util.startVersionControl(parent, clientEntity, TEST_FLOWS_BUCKET, "Parent");

        // Change the properties of the UpdateContent processor and commit as v2
        util.updateProcessorProperties(updateContents, Collections.singletonMap("Content", "Updated v2"));

        util.saveFlowVersion(child, clientEntity, childVci);
        util.saveFlowVersion(parent, clientEntity, parentVci);

        // Ensure that we have the correct state
        util.assertFlowUpToDate(parent.getId());
        util.assertFlowUpToDate(child.getId());

        // Verify that we are able to switch back to v1 while everything is stopped
        util.changeFlowVersion(parent.getId(), "1");
        util.assertFlowStaleAndUnmodified(parent.getId());
        util.assertFlowStaleAndUnmodified(child.getId());

        // Start the flow and verify the contents of the flow file
        util.waitForValidProcessor(updateContents.getId());
        util.waitForValidProcessor(generate.getId());
        util.startProcessGroupComponents(child.getId());
        util.startProcessor(generate);

        waitForQueueCount(connectionToTerminate.getId(), getNumberOfNodes());

        final String contents = util.getFlowFileContentAsUtf8(connectionToTerminate.getId(), 0);
        assertEquals("Updated", contents);

        // Switch Version back to v2 while it's running
        util.changeFlowVersion(parent.getId(), "2");
        util.assertFlowUpToDate(parent.getId());
        util.assertFlowUpToDate(child.getId());

        // With flow running, change version to v1. Restart GenerateFlowFile to trigger another FlowFile to be generated
        util.stopProcessor(generate);
        util.startProcessor(generate);
        waitForQueueCount(connectionToTerminate.getId(), 2 * getNumberOfNodes());

        // Ensure that the contents are correct
        final String secondFlowFileContents = util.getFlowFileContentAsUtf8(connectionToTerminate.getId(), getNumberOfNodes());
        assertEquals("Updated v2", secondFlowFileContents);

        // Switch back to v1 while flow is running to verify that the version can change back to a lower version as well
        util.changeFlowVersion(parent.getId(), "1");
        util.assertFlowStaleAndUnmodified(parent.getId());
        util.assertFlowStaleAndUnmodified(child.getId());

        util.stopProcessor(generate);
        util.startProcessor(generate);
        waitForQueueCount(connectionToTerminate.getId(), 3 * getNumberOfNodes());

        final String thirdFlowFileContents = util.getFlowFileContentAsUtf8(connectionToTerminate.getId(), getNumberOfNodes() * 2);
        assertEquals("Updated", thirdFlowFileContents);
    }


    @Test
    public void testChangeConnectionDestinationRemoveOldAndMoveGroup() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        // Create a PG that contains Generate -> Count
        final ProcessGroupEntity parent = util.createProcessGroup("Parent", "root");

        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", parent.getId());
        final ProcessorEntity countProcessor = util.createProcessor("CountFlowFiles", parent.getId());

        final ConnectionEntity generateToCount = util.createConnection(generate, countProcessor, "success");

        // Save the flow as v1
        final VersionControlInformationEntity v1Vci = util.startVersionControl(parent, clientEntity, TEST_FLOWS_BUCKET, "Parent");

        // Create a Terminate processor and change flow to be:
        // Generate -> Terminate - remove the old Count Processor
        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", parent.getId());

        generateToCount.setDestinationId(terminate.getId());
        generateToCount.getComponent().setDestination(util.createConnectableDTO(terminate));
        final ConnectionEntity generateToTerminate = getNifiClient().getConnectionClient().updateConnection(generateToCount);
        getNifiClient().getProcessorClient().deleteProcessor(countProcessor);

        final ProcessGroupEntity childGroup = util.createProcessGroup("Child", parent.getId());

        // Move the Generate, Terminate, and Connection to the child group
        final Map<String, RevisionDTO> processorRevisions = new HashMap<>();
        processorRevisions.put(generate.getId(), generate.getRevision());
        processorRevisions.put(terminate.getId(), terminate.getRevision());

        final SnippetDTO snippetDto = new SnippetDTO();
        snippetDto.setConnections(Collections.singletonMap(generateToTerminate.getId(), generateToTerminate.getRevision()));
        snippetDto.setProcessors(processorRevisions);
        snippetDto.setParentGroupId(parent.getId());
        final SnippetEntity snippet = new SnippetEntity();
        snippet.setSnippet(snippetDto);
        final SnippetEntity createdSnippet = getNifiClient().getSnippetClient().createSnippet(snippet);

        createdSnippet.getSnippet().setParentGroupId(childGroup.getId());
        getNifiClient().getSnippetClient().updateSnippet(createdSnippet);

        // Save the flow as v2
        util.saveFlowVersion(parent, clientEntity, v1Vci);

        util.changeFlowVersion(parent.getId(), "1");
        util.changeFlowVersion(parent.getId(), "2");
    }


    @Test
    public void testControllerServiceUpdateWhileRunning() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ProcessGroupEntity group = util.createProcessGroup("Parent", "root");
        final ControllerServiceEntity service = util.createControllerService("StandardCountService", group.getId());

        final ProcessorEntity generate = util.createProcessor("GenerateFlowFile", group.getId());
        final ProcessorEntity countProcessor = util.createProcessor("CountFlowFiles", group.getId());
        util.updateProcessorProperties(countProcessor, Collections.singletonMap("Count Service", service.getComponent().getId()));

        final ProcessorEntity terminate = util.createProcessor("TerminateFlowFile", group.getId());
        final ConnectionEntity connectionToTerminate = util.createConnection(countProcessor, terminate, "success");
        util.setFifoPrioritizer(connectionToTerminate);
        util.createConnection(generate, countProcessor, "success");

        // Save the flow as v1
        final VersionControlInformationEntity vci = util.startVersionControl(group, clientEntity, TEST_FLOWS_BUCKET, "Parent");

        // Change the value of of the Controller Service's start value to 2000, and change the text of the GenerateFlowFile just to make it run each time the version is changed
        util.updateControllerServiceProperties(service, Collections.singletonMap("Start Value", "2000"));
        util.updateProcessorProperties(generate, Collections.singletonMap("Text", "Hello World"));

        // Save the flow as v2
        util.saveFlowVersion(group, clientEntity, vci);

        // Change back to v1 and start the flow
        util.changeFlowVersion(group.getId(), "1");
        util.assertFlowStaleAndUnmodified(group.getId());
        util.enableControllerService(service);

        util.waitForValidProcessor(generate.getId());
        util.startProcessor(generate);
        util.waitForValidProcessor(countProcessor.getId());
        util.startProcessor(countProcessor);

        // Ensure that we get the expected result
        waitForQueueCount(connectionToTerminate.getId(), getNumberOfNodes());
        final Map<String, String> firstFlowFileAttributes = util.getQueueFlowFile(connectionToTerminate.getId(), 0).getFlowFile().getAttributes();
        assertEquals("1", firstFlowFileAttributes.get("count"));

        // Change to v2 and ensure that the output is correct
        util.changeFlowVersion(group.getId(), "2");
        util.assertFlowUpToDate(group.getId());
        waitForQueueCount(connectionToTerminate.getId(), 2 * getNumberOfNodes());
        final Map<String, String> secondFlowFileAttributes = util.getQueueFlowFile(connectionToTerminate.getId(), getNumberOfNodes()).getFlowFile().getAttributes();
        assertEquals("2001", secondFlowFileAttributes.get("count"));

        // Change back to v1 and ensure that the output is correct. It should reset count back to 0.
        util.changeFlowVersion(group.getId(), "1");
        util.assertFlowStaleAndUnmodified(group.getId());
        waitForQueueCount(connectionToTerminate.getId(), 3 * getNumberOfNodes());
        final Map<String, String> thirdFlowFileAttributes = util.getQueueFlowFile(connectionToTerminate.getId(), getNumberOfNodes() * 2).getFlowFile().getAttributes();
        assertEquals("1", thirdFlowFileAttributes.get("count"));
    }


    @Test
    public void testChangeVersionWithPortMoveBetweenGroups() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient(new File("src/test/resources/versioned-flows"));

        final ProcessGroupEntity imported = getClientUtil().importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, "port-moved-groups", "1");
        assertNotNull(imported);
        getClientUtil().assertFlowStaleAndUnmodified(imported.getId());

        // Ensure that the import worked as expected
        final FlowSnippetDTO groupContents = imported.getComponent().getContents();
        final List<ProcessorDTO> replaceTextProcessors = groupContents.getProcessors().stream()
            .filter(proc -> proc.getName().equals("ReplaceText"))
            .toList();
        assertEquals(1, replaceTextProcessors.size());

        assertTrue(groupContents.getInputPorts().isEmpty());

        // Change to version 2
        final VersionedFlowUpdateRequestEntity version2Result = getClientUtil().changeFlowVersion(imported.getId(), "2");
        assertNull(version2Result.getRequest().getFailureReason());

        final FlowDTO v2Contents = getNifiClient().getFlowClient().getProcessGroup(imported.getId()).getProcessGroupFlow().getFlow();
        getClientUtil().assertFlowUpToDate(imported.getId());

        // Ensure that the ReplaceText processor still exists
        final long replaceTextCount = v2Contents.getProcessors().stream()
            .map(ProcessorEntity::getComponent)
            .filter(proc -> proc.getName().equals("ReplaceText"))
            .filter(proc -> proc.getId().equals(replaceTextProcessors.get(0).getId()))
            .count();
        assertEquals(1, replaceTextCount);

        // Ensure that we now have a Port at the top level
        assertEquals(1, v2Contents.getInputPorts().size());

        // Change back to Version 1
        final VersionedFlowUpdateRequestEntity changeBackToV1Result = getClientUtil().changeFlowVersion(imported.getId(), "1");
        assertNull(changeBackToV1Result.getRequest().getFailureReason());

        final FlowDTO v1Contents = getNifiClient().getFlowClient().getProcessGroup(imported.getId()).getProcessGroupFlow().getFlow();
        getClientUtil().assertFlowStaleAndUnmodified(imported.getId());

        // Ensure that we no longer have a Port at the top level
        assertTrue(v1Contents.getInputPorts().isEmpty());
    }


    @Test
    public void testRollbackOnFailure() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient(new File("src/test/resources/versioned-flows"));

        final ProcessGroupEntity imported = getClientUtil().importFlowFromRegistry("root", clientEntity.getId(), TEST_FLOWS_BUCKET, "flow-with-invalid-connection", "1");
        assertNotNull(imported);
        getClientUtil().assertFlowStaleAndUnmodified(imported.getId());

        final VersionedFlowUpdateRequestEntity version2Result = getClientUtil().changeFlowVersion(imported.getId(), "2", false);
        final String failureReason = version2Result.getRequest().getFailureReason();
        assertNotNull(failureReason);

        // Ensure that we're still on v1 of the flow and there are no local modifications
        getClientUtil().assertFlowStaleAndUnmodified(imported.getId());

        // Ensure that the processors still exist
        final FlowDTO contents = getNifiClient().getFlowClient().getProcessGroup(imported.getId()).getProcessGroupFlow().getFlow();
        assertEquals(1, contents.getProcessors().size());
    }


    @Test
    public void testStartVersionControlThenImport() throws NiFiClientException, IOException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Outer", "root");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", group.getId());

        final VersionControlInformationEntity vci = getClientUtil().startVersionControl(group, clientEntity, TEST_FLOWS_BUCKET, FIRST_FLOW_ID);

        final ProcessGroupEntity imported = getClientUtil().importFlowFromRegistry("root", vci.getVersionControlInformation());
        assertNotNull(imported);

        final ProcessGroupFlowDTO importedFlow = getNifiClient().getFlowClient().getProcessGroup(imported.getId()).getProcessGroupFlow();
        final FlowDTO importedGroupContents = importedFlow.getFlow();
        final Set<ProcessorEntity> importedProcessors = importedGroupContents.getProcessors();
        assertEquals(1, importedProcessors.size());

        final ProcessorDTO importedProcessor = importedProcessors.iterator().next().getComponent();
        assertEquals(terminate.getComponent().getType(), importedProcessor.getType());
        assertEquals(terminate.getComponent().getName(), importedProcessor.getName());
        assertNotEquals(terminate.getComponent().getId(), importedProcessor.getId());
    }


    @Test
    public void testStartVersionControlThenModifyAndRevert() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Outer", "root");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", group.getId());

        getClientUtil().startVersionControl(group, clientEntity, TEST_FLOWS_BUCKET, FIRST_FLOW_ID);

        String versionedFlowState = getClientUtil().getVersionedFlowState(group.getId(), "root");
        assertEquals("UP_TO_DATE", versionedFlowState);

        getClientUtil().updateProcessorExecutionNode(terminate, ExecutionNode.PRIMARY);
        versionedFlowState = getClientUtil().getVersionedFlowState(group.getId(), "root");
        assertEquals("LOCALLY_MODIFIED", versionedFlowState);

        final ProcessorEntity locallyModifiedTerminate = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
        assertEquals(ExecutionNode.PRIMARY.name(), locallyModifiedTerminate.getComponent().getConfig().getExecutionNode());

        getClientUtil().revertChanges(group);

        final ProcessorEntity updatedTerminate = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
        assertEquals(ExecutionNode.ALL.name(), updatedTerminate.getComponent().getConfig().getExecutionNode());

        versionedFlowState = getClientUtil().getVersionedFlowState(group.getId(), "root");
        assertEquals("UP_TO_DATE", versionedFlowState);
    }

}
