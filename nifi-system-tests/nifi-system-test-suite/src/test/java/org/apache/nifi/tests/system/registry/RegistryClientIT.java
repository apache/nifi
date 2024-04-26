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
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
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
    private static final String TEST_FLOWS_BUCKET = "test-flows";

    private static final String FIRST_FLOW_ID = "first-flow";

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


    @Test
    public void testCopyPasteProcessGroupDoesNotDuplicateVersionedComponentId() throws NiFiClientException, IOException {
        // Create a top-level PG and version it with nothing in it.
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity outerGroup = getClientUtil().createProcessGroup("Outer", "root");
        getClientUtil().startVersionControl(outerGroup, clientEntity, TEST_FLOWS_BUCKET, FIRST_FLOW_ID);

        // Create a lower level PG and add a Processor.
        // Commit as Version 2 of the group.
        final ProcessGroupEntity inner1 = getClientUtil().createProcessGroup("Inner 1", outerGroup.getId());
        ProcessorEntity terminate1 = getClientUtil().createProcessor("TerminateFlowFile", inner1.getId());
        VersionControlInformationEntity vciEntity = getClientUtil().startVersionControl(outerGroup, clientEntity, TEST_FLOWS_BUCKET, FIRST_FLOW_ID);
        assertEquals("2", vciEntity.getVersionControlInformation().getVersion());

        // Get an up-to-date copy of terminate1 because it should now have a non-null versioned component id
        terminate1 = getNifiClient().getProcessorClient().getProcessor(terminate1.getId());
        assertNotNull(terminate1.getComponent().getVersionedComponentId());

        // Copy and paste the inner Process Group
        final FlowEntity flowEntity = getClientUtil().copyAndPaste(inner1, outerGroup.getId());
        final ProcessGroupEntity inner2Entity = flowEntity.getFlow().getProcessGroups().iterator().next();

        final ProcessGroupFlowEntity inner2FlowEntity = getNifiClient().getFlowClient().getProcessGroup(inner2Entity.getId());
        final Set<ProcessorEntity> inner2FlowProcessors = inner2FlowEntity.getProcessGroupFlow().getFlow().getProcessors();
        assertEquals(1, inner2FlowProcessors.size());

        ProcessorEntity terminate2 = inner2FlowProcessors.iterator().next();
        assertEquals(terminate1.getComponent().getName(), terminate2.getComponent().getName());
        assertEquals(terminate1.getComponent().getType(), terminate2.getComponent().getType());
        assertNotEquals(terminate1.getComponent().getId(), terminate2.getComponent().getId());
        assertNotEquals(terminate1.getComponent().getVersionedComponentId(), terminate2.getComponent().getVersionedComponentId());

        // First Control again with the newly created components
        vciEntity = getClientUtil().startVersionControl(outerGroup, clientEntity, TEST_FLOWS_BUCKET, FIRST_FLOW_ID);
        assertEquals("3", vciEntity.getVersionControlInformation().getVersion());

        // Get new version of terminate2 processor and terminate1 processor. Ensure that both have version control ID's but that they are different.
        terminate1 = getNifiClient().getProcessorClient().getProcessor(terminate1.getId());
        terminate2 = getNifiClient().getProcessorClient().getProcessor(terminate2.getId());

        assertNotNull(terminate1.getComponent().getVersionedComponentId());
        assertNotNull(terminate2.getComponent().getVersionedComponentId());
        assertNotEquals(terminate1.getComponent().getVersionedComponentId(), terminate2.getComponent().getVersionedComponentId());
    }

    @Test
    public void testCopyPasteProcessGroupUnderVersionControlMaintainsVersionedComponentId() throws NiFiClientException, IOException, InterruptedException {
        // Create a top-level PG and version it with nothing in it.
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity topLevel1 = getClientUtil().createProcessGroup("Top Level 1", "root");

        // Create a lower level PG and add a Processor.
        // Commit as Version 2 of the group.
        final ProcessGroupEntity innerGroup = getClientUtil().createProcessGroup("Inner 1", topLevel1.getId());
        ProcessorEntity terminate1 = getClientUtil().createProcessor("TerminateFlowFile", innerGroup.getId());
        VersionControlInformationEntity vciEntity = getClientUtil().startVersionControl(innerGroup, clientEntity, TEST_FLOWS_BUCKET, FIRST_FLOW_ID);
        assertEquals("1", vciEntity.getVersionControlInformation().getVersion());

        // Now that the inner group is under version control, copy it and paste it to a new PG.
        // This should result in the pasted Process Group having a processor with the same Versioned Component ID, because the Processors
        // have different Versioned groups, so they can have duplicate Versioned Component IDs.
        final ProcessGroupEntity topLevel2 = getClientUtil().createProcessGroup("Top Level 2", "root");
        final FlowEntity flowEntity = getClientUtil().copyAndPaste(innerGroup, topLevel2.getId());
        final String pastedGroupId = flowEntity.getFlow().getProcessGroups().iterator().next().getId();
        final ProcessGroupFlowEntity pastedGroupFlowEntity = getNifiClient().getFlowClient().getProcessGroup(pastedGroupId);
        final ProcessorEntity terminate2 = pastedGroupFlowEntity.getProcessGroupFlow().getFlow().getProcessors().iterator().next();

        // Get an up-to-date copy of terminate1 because it should now have a non-null versioned component id
        terminate1 = getNifiClient().getProcessorClient().getProcessor(terminate1.getId());
        assertNotNull(terminate1.getComponent().getVersionedComponentId());

        // Both the pasted Process Group and the original should have the same Version Control Information.
        final VersionControlInformationDTO originalGroupVci = getNifiClient().getProcessGroupClient().getProcessGroup(innerGroup.getId()).getComponent().getVersionControlInformation();
        final VersionControlInformationDTO pastedGroupVci = getNifiClient().getProcessGroupClient().getProcessGroup(pastedGroupId).getComponent().getVersionControlInformation();
        assertNotNull(originalGroupVci);
        assertNotNull(pastedGroupVci);
        assertEquals(originalGroupVci.getBucketId(), pastedGroupVci.getBucketId());
        assertEquals(originalGroupVci.getFlowId(), pastedGroupVci.getFlowId());
        assertEquals(originalGroupVci.getVersion(), pastedGroupVci.getVersion());

        // Wait for the Version Control Information to show a state of UP_TO_DATE. We have to wait for this because it initially is set to SYNC_FAILURE and a background task
        // is kicked off to determine the state.
        waitFor(() -> VersionControlInformationDTO.UP_TO_DATE.equals(getClientUtil().getVersionControlState(innerGroup.getId())) );
        waitFor(() -> VersionControlInformationDTO.UP_TO_DATE.equals(getClientUtil().getVersionControlState(pastedGroupId)) );

        // The two processors should have the same Versioned Component ID
        assertEquals(terminate1.getComponent().getName(), terminate2.getComponent().getName());
        assertEquals(terminate1.getComponent().getType(), terminate2.getComponent().getType());
        assertNotEquals(terminate1.getComponent().getId(), terminate2.getComponent().getId());
        assertEquals(terminate1.getComponent().getVersionedComponentId(), terminate2.getComponent().getVersionedComponentId());
    }

}
