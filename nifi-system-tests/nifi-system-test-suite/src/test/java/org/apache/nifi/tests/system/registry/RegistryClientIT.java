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
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RegistryClientIT extends NiFiSystemIT {
    @Test
    public void testStartVersionControlThenImport() throws NiFiClientException, IOException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Outer", "root");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", group.getId());

        final VersionControlInformationEntity vci = getClientUtil().startVersionControl(group, clientEntity, "First Bucket", "First Flow");

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

    private FlowRegistryClientEntity registerClient() throws NiFiClientException, IOException {
        final String clientName = String.format("FileRegistry-%s", UUID.randomUUID());
        final FlowRegistryClientEntity clientEntity = getClientUtil().createFlowRegistryClient(clientName);
        final File storageDir = new File("target/flowRegistryStorage/" + getTestName().replace("\\(.*?\\)", ""));
        Files.createDirectories(storageDir.toPath());
        getClientUtil().updateRegistryClientProperties(clientEntity, Collections.singletonMap("Directory", storageDir.getAbsolutePath()));

        return clientEntity;
    }

    @Test
    public void testStartVersionControlThenModifyAndRevert() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity group = getClientUtil().createProcessGroup("Outer", "root");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile", group.getId());

        final VersionControlInformationEntity vci = getClientUtil().startVersionControl(group, clientEntity, "First Bucket", "First Flow");

        String versionedFlowState = getVersionedFlowState(group.getId(), "root");
        assertEquals("UP_TO_DATE", versionedFlowState);

        getClientUtil().updateProcessorExecutionNode(terminate, ExecutionNode.PRIMARY);
        versionedFlowState = getVersionedFlowState(group.getId(), "root");
        assertEquals("LOCALLY_MODIFIED", versionedFlowState);

        final ProcessorEntity locallyModifiedTerminate = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
        assertEquals(ExecutionNode.PRIMARY.name(), locallyModifiedTerminate.getComponent().getConfig().getExecutionNode());

        getClientUtil().revertChanges(group);

        final ProcessorEntity updatedTerminate = getNifiClient().getProcessorClient().getProcessor(terminate.getId());
        assertEquals(ExecutionNode.ALL.name(), updatedTerminate.getComponent().getConfig().getExecutionNode());

        versionedFlowState = getVersionedFlowState(group.getId(), "root");
        assertEquals("UP_TO_DATE", versionedFlowState);
    }

    private String getVersionedFlowState(final String groupId, final String parentGroupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowDTO parentGroup = getNifiClient().getFlowClient().getProcessGroup(parentGroupId).getProcessGroupFlow();
        final Set<ProcessGroupEntity> childGroups = parentGroup.getFlow().getProcessGroups();

        return childGroups.stream()
            .filter(childGroup -> groupId.equals(childGroup.getId()))
            .map(ProcessGroupEntity::getVersionedFlowState)
            .findAny()
            .orElse(null);
    }

    @Test
    public void testCopyPasteProcessGroupDoesNotDuplicateVersionedComponentId() throws NiFiClientException, IOException {
        // Create a top-level PG and version it with nothing in it.
        final FlowRegistryClientEntity clientEntity = registerClient();
        final ProcessGroupEntity outerGroup = getClientUtil().createProcessGroup("Outer", "root");
        getClientUtil().startVersionControl(outerGroup, clientEntity, "First Bucket", "First Flow");

        // Create a lower level PG and add a Processor.
        // Commit as Version 2 of the group.
        final ProcessGroupEntity inner1 = getClientUtil().createProcessGroup("Inner 1", outerGroup.getId());
        ProcessorEntity terminate1 = getClientUtil().createProcessor("TerminateFlowFile", inner1.getId());
        VersionControlInformationEntity vciEntity = getClientUtil().startVersionControl(outerGroup, clientEntity, "First Bucket", "First Flow");
        assertEquals(2, vciEntity.getVersionControlInformation().getVersion());

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
        vciEntity = getClientUtil().startVersionControl(outerGroup, clientEntity, "First Bucket", "First Flow");
        assertEquals(3, vciEntity.getVersionControlInformation().getVersion());

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
        VersionControlInformationEntity vciEntity = getClientUtil().startVersionControl(innerGroup, clientEntity, "First Bucket", "First Flow");
        assertEquals(1, vciEntity.getVersionControlInformation().getVersion());

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
        waitFor(() -> VersionControlInformationDTO.UP_TO_DATE.equals(getVersionControlState(innerGroup.getId())) );
        waitFor(() -> VersionControlInformationDTO.UP_TO_DATE.equals(getVersionControlState(pastedGroupId)) );

        // The two processors should have the same Versioned Component ID
        assertEquals(terminate1.getComponent().getName(), terminate2.getComponent().getName());
        assertEquals(terminate1.getComponent().getType(), terminate2.getComponent().getType());
        assertNotEquals(terminate1.getComponent().getId(), terminate2.getComponent().getId());
        assertEquals(terminate1.getComponent().getVersionedComponentId(), terminate2.getComponent().getVersionedComponentId());
    }

    private String getVersionControlState(final String groupId) {
        try {
            final VersionControlInformationDTO vci = getNifiClient().getProcessGroupClient().getProcessGroup(groupId).getComponent().getVersionControlInformation();
            return vci.getState();
        } catch (final Exception e) {
            Assertions.fail("Could not obtain Version Control Information for Group with ID " + groupId, e);
            return null;
        }
    }
}
