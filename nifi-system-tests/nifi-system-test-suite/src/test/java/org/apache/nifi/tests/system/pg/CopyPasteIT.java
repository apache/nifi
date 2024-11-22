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

package org.apache.nifi.tests.system.pg;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.CopyRequestEntity;
import org.apache.nifi.web.api.entity.CopyResponseEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.PasteRequestEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.nifi.tests.system.registry.RegistryClientIT.FIRST_FLOW_ID;
import static org.apache.nifi.tests.system.registry.RegistryClientIT.TEST_FLOWS_BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CopyPasteIT extends NiFiSystemIT {

    @Test
    public void testSimpleCopyPaste() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("child group", topLevel.getId());

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile", childGroup.getId());
        final ProcessorEntity count = getClientUtil().createProcessor("CountEvents", childGroup.getId());
        getClientUtil().setAutoTerminatedRelationships(count, "success");

        getClientUtil().createConnection(generate, count, "success");

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessGroups(Set.of(childGroup.getId()));

        final FlowDTO flowDto = getClientUtil().copyAndPaste(topLevel.getId(), copyRequestEntity, topLevel.getRevision(), topLevel.getId());
        final ProcessGroupEntity pastedProcessGroup = flowDto.getProcessGroups().iterator().next();

        assertNotNull(pastedProcessGroup);
        assertEquals("child group", pastedProcessGroup.getComponent().getName());

        final ProcessGroupFlowEntity pastedGroupFlowEntity = getNifiClient().getFlowClient().getProcessGroup(pastedProcessGroup.getId());
        final FlowDTO childFlowDto = pastedGroupFlowEntity.getProcessGroupFlow().getFlow();
        assertEquals(2, childFlowDto.getProcessors().size());
        assertEquals(1, childFlowDto.getConnections().size());
    }

    @Test
    public void testPortNameUniquenessCopyPaste() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final PortEntity in = getClientUtil().createInputPort("in", topLevel.getId());

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setInputPorts(Set.of(in.getId()));

        // paste into the current group where the port must be renamed to ensure uniqueness
        final FlowDTO renamePortFlowDto = getClientUtil().copyAndPaste(topLevel.getId(), copyRequestEntity, topLevel.getRevision(), topLevel.getId());
        final PortEntity renamedPastedPort = renamePortFlowDto.getInputPorts().iterator().next();

        assertNotNull(renamedPastedPort);
        assertTrue(Pattern.matches("in \\([a-f0-9\\-]{36}\\)", renamedPastedPort.getComponent().getName()));

        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("child group", topLevel.getId());

        // paste into a child group where the port name will not conflict, and it's proposed name will not change
        final FlowDTO notRenamePortFlowDto = getClientUtil().copyAndPaste(topLevel.getId(), copyRequestEntity, childGroup.getRevision(), childGroup.getId());
        final PortEntity notRenamedPastedPort = notRenamePortFlowDto.getInputPorts().iterator().next();

        assertNotNull(notRenamedPastedPort);
        assertEquals("in", notRenamedPastedPort.getComponent().getName());
    }

    @Test
    public void testPastedPortInRootGroupMustBePublic() throws NiFiClientException, IOException {
        final ProcessGroupEntity root = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", root.getId());
        final PortEntity in = getClientUtil().createInputPort("in", topLevel.getId());

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setInputPorts(Set.of(in.getId()));

        final CopyResponseEntity copyResponseEntity = getNifiClient().getProcessGroupClient().copy(topLevel.getId(), copyRequestEntity);
        assertEquals(1, copyResponseEntity.getInputPorts().size());
        assertFalse(copyResponseEntity.getInputPorts().iterator().next().getAllowRemoteAccess());

        final PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(root.getRevision());

        final PasteResponseEntity pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(root.getId(), pasteRequestEntity);
        final FlowDTO flowDto = pasteResponseEntity.getFlow();
        assertEquals(1, flowDto.getInputPorts().size());
        assertTrue(flowDto.getInputPorts().iterator().next().getComponent().getAllowRemoteAccess());
    }

    @Test
    public void testPastedPortInChildGroupHonorsCopiedPayload() throws NiFiClientException, IOException {
        final ProcessGroupEntity root = getNifiClient().getProcessGroupClient().getProcessGroup("root");
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", root.getId());
        final PortEntity in = getClientUtil().createInputPort("in", topLevel.getId());

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setInputPorts(Set.of(in.getId()));

        // not public
        final CopyResponseEntity copyResponseEntity = getNifiClient().getProcessGroupClient().copy(topLevel.getId(), copyRequestEntity);
        assertEquals(1, copyResponseEntity.getInputPorts().size());
        assertFalse(copyResponseEntity.getInputPorts().iterator().next().getAllowRemoteAccess());

        PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(topLevel.getRevision());

        PasteResponseEntity pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(topLevel.getId(), pasteRequestEntity);
        FlowDTO flowDto = pasteResponseEntity.getFlow();
        assertEquals(1, flowDto.getInputPorts().size());
        assertNull(flowDto.getInputPorts().iterator().next().getComponent().getAllowRemoteAccess());

        // public
        copyResponseEntity.getInputPorts().iterator().next().setAllowRemoteAccess(true);

        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(topLevel.getRevision());

        pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(topLevel.getId(), pasteRequestEntity);
        flowDto = pasteResponseEntity.getFlow();
        assertEquals(1, flowDto.getInputPorts().size());
        assertTrue(flowDto.getInputPorts().iterator().next().getComponent().getAllowRemoteAccess());
    }

    @Test
    public void testGhostComponent() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile", topLevel.getId());

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessors(Set.of(generate.getId()));

        final CopyResponseEntity copyResponseEntity = getNifiClient().getProcessGroupClient().copy(topLevel.getId(), copyRequestEntity);
        assertEquals(1, copyResponseEntity.getProcessors().size());

        // update the type to something unknown
        copyResponseEntity.getProcessors().iterator().next().setType("NotARealExtensionType");

        final PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(topLevel.getRevision());

        final PasteResponseEntity pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(topLevel.getId(), pasteRequestEntity);
        final FlowDTO flowDto = pasteResponseEntity.getFlow();
        assertEquals(1, flowDto.getProcessors().size());
        assertTrue(flowDto.getProcessors().iterator().next().getComponent().getExtensionMissing());
    }

    @Test
    public void testExternalControllerService() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ControllerServiceEntity service = getClientUtil().createControllerService("StandardCountService", topLevel.getId());
        final ProcessorEntity count = getClientUtil().createProcessor("CountFlowFiles", topLevel.getId());

        // reference the controller service
        getClientUtil().updateProcessorProperties(count, Map.of("Count Service", service.getId()));

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessors(Set.of(count.getId()));

        final FlowDTO flowDto = getClientUtil().copyAndPaste(topLevel.getId(), copyRequestEntity, topLevel.getRevision(), topLevel.getId());
        assertEquals(1, flowDto.getProcessors().size());
        assertEquals(service.getId(), flowDto.getProcessors().iterator().next().getComponent().getConfig().getProperties().get("Count Service"));
    }

    @Test
    public void testSensitiveValueCopiedFromInstance() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents", topLevel.getId());

        // set a sensitive property
        getClientUtil().updateProcessorProperties(countEvents, Map.of("Sensitive", "sensitive value"));

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessors(Set.of(countEvents.getId()));

        final CopyResponseEntity copyResponseEntity = getNifiClient().getProcessGroupClient().copy(topLevel.getId(), copyRequestEntity);
        assertEquals(1, copyResponseEntity.getProcessors().size());
        assertNull(copyResponseEntity.getProcessors().iterator().next().getProperties().get("Sensitive"));

        final PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(topLevel.getRevision());

        final PasteResponseEntity pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(topLevel.getId(), pasteRequestEntity);
        final FlowDTO flowDto = pasteResponseEntity.getFlow();
        assertEquals(1, flowDto.getProcessors().size());
        assertEquals("********", flowDto.getProcessors().iterator().next().getComponent().getConfig().getProperties().get("Sensitive"));
    }

    @Test
    public void testSensitiveValueNotCopiedFromInstance() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents", topLevel.getId());

        // set a sensitive property
        getClientUtil().updateProcessorProperties(countEvents, Map.of("Sensitive", "sensitive value"));

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessors(Set.of(countEvents.getId()));

        final CopyResponseEntity copyResponseEntity = getNifiClient().getProcessGroupClient().copy(topLevel.getId(), copyRequestEntity);
        assertEquals(1, copyResponseEntity.getProcessors().size());
        assertNull(copyResponseEntity.getProcessors().iterator().next().getProperties().get("Sensitive"));

        // delete the copied component so the sensitive value isn't copied over
        getClientUtil().deleteAll(topLevel.getId());

        final PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(topLevel.getRevision());

        final PasteResponseEntity pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(topLevel.getId(), pasteRequestEntity);
        final FlowDTO flowDto = pasteResponseEntity.getFlow();
        assertEquals(1, flowDto.getProcessors().size());
        assertNull(flowDto.getProcessors().iterator().next().getComponent().getConfig().getProperties().get("Sensitive"));
    }

    @Test
    public void testParameterContextReferencedCopied() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("child group", topLevel.getId());
        final ParameterContextEntity parameterContextEntity = getClientUtil().createParameterContext("my parameters", "param", "value", false);
        getClientUtil().setParameterContext(childGroup.getId(), parameterContextEntity);

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessGroups(Set.of(childGroup.getId()));

        final FlowDTO flowDto = getClientUtil().copyAndPaste(topLevel.getId(), copyRequestEntity, topLevel.getRevision(), topLevel.getId());
        assertEquals(1, flowDto.getProcessGroups().size());

        final ProcessGroupEntity pastedProcessGroup = flowDto.getProcessGroups().iterator().next();
        assertNotNull(pastedProcessGroup.getParameterContext());
        assertEquals(parameterContextEntity.getId(), pastedProcessGroup.getParameterContext().getId());
    }

    @Test
    public void testNewParameterContextCreated() throws NiFiClientException, IOException {
        final ProcessGroupEntity topLevel = getClientUtil().createProcessGroup("parent group", "root");
        final ProcessGroupEntity childGroup = getClientUtil().createProcessGroup("child group", topLevel.getId());
        final ParameterContextEntity parameterContextEntity = getClientUtil().createParameterContext("my parameters", "param", "value", false);
        getClientUtil().setParameterContext(childGroup.getId(), parameterContextEntity);

        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessGroups(Set.of(childGroup.getId()));

        final CopyResponseEntity copyResponseEntity = getNifiClient().getProcessGroupClient().copy(topLevel.getId(), copyRequestEntity);
        assertEquals(1, copyResponseEntity.getProcessGroups().size());

        // delete the parameter contexts before pasting
        getClientUtil().deleteParameterContexts();

        final PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(topLevel.getRevision());

        final PasteResponseEntity pasteResponseEntity = getNifiClient().getProcessGroupClient().paste(topLevel.getId(), pasteRequestEntity);
        final FlowDTO flowDto = pasteResponseEntity.getFlow();

        assertEquals(1, flowDto.getProcessGroups().size());

        final ProcessGroupEntity pastedProcessGroup = flowDto.getProcessGroups().iterator().next();
        assertNotNull(pastedProcessGroup.getParameterContext());
        assertNotEquals(parameterContextEntity.getId(), pastedProcessGroup.getParameterContext().getId());
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

        // Build the copy request
        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessGroups(Set.of(inner1.getId()));

        // Copy and paste the inner Process Group
        final FlowDTO flowDto = getClientUtil().copyAndPaste(outerGroup.getId(), copyRequestEntity, outerGroup.getRevision(), outerGroup.getId());
        final ProcessGroupEntity inner2Entity = flowDto.getProcessGroups().iterator().next();

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

        // Build the request and copy and paste
        final CopyRequestEntity copyRequestEntity = new CopyRequestEntity();
        copyRequestEntity.setProcessGroups(Set.of(innerGroup.getId()));

        final FlowDTO flowDto = getClientUtil().copyAndPaste(topLevel1.getId(), copyRequestEntity, topLevel2.getRevision(), topLevel2.getId());
        final String pastedGroupId = flowDto.getProcessGroups().iterator().next().getId();
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
