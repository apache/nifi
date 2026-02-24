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

import org.apache.nifi.tests.system.NiFiClientUtil;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System test to verify that parameter context bindings are preserved during versioned flow upgrades
 * when new process groups are added.
 *
 * This test reproduces a bug where:
 * 1. v1: Process Group A with Parameter Context P, containing only a Processor X using param1
 * 2. v2: Process Group A with Parameter Context P, now containing a NEW Process Group B also attached to P
 * 3. When checking out v1 twice with "do not keep parameter context":
 *    - First checkout creates A1 with Parameter Context P
 *    - Second checkout creates A2 with Parameter Context P (1) since P already exists
 * 4. When upgrading A2 from v1 to v2, the newly added Process Group B incorrectly gets
 *    bound to P instead of P (1)
 *
 * The expectation is that the new Process Group B should be bound to P (1), the same
 * parameter context that its parent A2 uses.
 */
class ParameterContextPreservationIT extends NiFiSystemIT {
    private static final String TEST_FLOWS_BUCKET = "test-flows";
    private static final String PARAMETER_CONTEXT_NAME = "P";
    private static final String PARAMETER_NAME = "param1";
    private static final String PARAMETER_VALUE = "value1";
    private static final String PARAMETER_REFERENCE = "#{" + PARAMETER_NAME + "}";
    private static final String GROUP_A_NAME = "A";
    private static final String GROUP_B_NAME = "B";
    private static final String FLOW_NAME = "FlowWithParameterContext";
    private static final String PROCESSOR_TYPE = "GenerateFlowFile";
    private static final String PROCESSOR_PROPERTY_TEXT = "Text";
    private static final String RELATIONSHIP_SUCCESS = "success";
    private static final String VERSION_1 = "1";
    private static final String VERSION_2 = "2";

    @Test
    void testNewProcessGroupUsesCorrectParameterContextDuringUpgrade() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        // Step 1: Create Parameter Context "P" with param1
        final ParameterContextEntity paramContextP = util.createParameterContext(PARAMETER_CONTEXT_NAME, Map.of(PARAMETER_NAME, PARAMETER_VALUE));

        // Step 2: Create v1 - Process Group A with just a Processor X using param1
        final ProcessGroupEntity groupA = util.createProcessGroup(GROUP_A_NAME, "root");
        util.setParameterContext(groupA.getId(), paramContextP);

        final ProcessorEntity processorX = util.createProcessor(PROCESSOR_TYPE, groupA.getId());
        util.updateProcessorProperties(processorX, Collections.singletonMap(PROCESSOR_PROPERTY_TEXT, PARAMETER_REFERENCE));
        util.setAutoTerminatedRelationships(processorX, RELATIONSHIP_SUCCESS);

        // Save as v1
        final VersionControlInformationEntity vciV1 = util.startVersionControl(groupA, clientEntity, TEST_FLOWS_BUCKET, FLOW_NAME);
        final String flowId = vciV1.getVersionControlInformation().getFlowId();

        // Step 3: Create v2 - Add Process Group B inside A, also attached to P
        final ProcessGroupEntity groupB = util.createProcessGroup(GROUP_B_NAME, groupA.getId());
        util.setParameterContext(groupB.getId(), paramContextP);

        // Add a processor in B that uses param1
        final ProcessorEntity processorInB = util.createProcessor(PROCESSOR_TYPE, groupB.getId());
        util.updateProcessorProperties(processorInB, Collections.singletonMap(PROCESSOR_PROPERTY_TEXT, PARAMETER_REFERENCE));
        util.setAutoTerminatedRelationships(processorInB, RELATIONSHIP_SUCCESS);

        // Save as v2
        final ProcessGroupEntity refreshedGroupA = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        util.saveFlowVersion(refreshedGroupA, clientEntity, vciV1);

        // Step 4: Clean up original flow
        final ProcessGroupEntity groupAForStopVc = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        getNifiClient().getVersionsClient().stopVersionControl(groupAForStopVc);
        util.deleteAll(groupA.getId());
        final ProcessGroupEntity groupAToDelete = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(groupAToDelete);

        final ParameterContextEntity contextToDelete = getNifiClient().getParamContextClient().getParamContext(paramContextP.getId(), false);
        getNifiClient().getParamContextClient().deleteParamContext(paramContextP.getId(),
                String.valueOf(contextToDelete.getRevision().getVersion()));

        // Step 5: Import v1 FIRST time - creates A1 with Parameter Context "P"
        final ProcessGroupEntity importedGroup1 = importFlowWithReplaceParameterContext(
                clientEntity.getId(), flowId, VERSION_1);

        final ProcessGroupEntity fetchedGroup1 = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup1.getId());
        final String paramContextId1 = fetchedGroup1.getComponent().getParameterContext().getId();

        // Step 6: Import v1 SECOND time - creates A2 with Parameter Context "P (1)"
        final ProcessGroupEntity importedGroup2 = importFlowWithReplaceParameterContext(
                clientEntity.getId(), flowId, VERSION_1);

        final ProcessGroupEntity fetchedGroup2 = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup2.getId());
        final String paramContextId2 = fetchedGroup2.getComponent().getParameterContext().getId();

        // Verify the two imports have DIFFERENT parameter contexts
        assertNotEquals(paramContextId1, paramContextId2,
                "Second import should have a different parameter context");

        // Step 7: Upgrade A2 from v1 to v2 (this adds Process Group B)
        util.changeFlowVersion(importedGroup2.getId(), VERSION_2);

        // Step 8: Verify the NEW Process Group B uses the CORRECT parameter context
        final ProcessGroupEntity upgradedGroup2 = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup2.getId());
        final String upgradedA2ParamContextId = upgradedGroup2.getComponent().getParameterContext().getId();

        // A2 should still use P (1)
        assertEquals(paramContextId2, upgradedA2ParamContextId,
                "After upgrade, A2 should still reference its original parameter context");

        // Get the newly added Process Group B
        final ProcessGroupEntity newGroupB = getNestedProcessGroup(upgradedGroup2, GROUP_B_NAME);
        assertNotNull(newGroupB, "Process Group B should exist after upgrade");
        assertNotNull(newGroupB.getComponent().getParameterContext(),
                "Process Group B should have a parameter context assigned");

        final String groupBParamContextId = newGroupB.getComponent().getParameterContext().getId();

        // The NEW Process Group B should use the SAME parameter context as its parent A2 (P (1))
        assertEquals(paramContextId2, groupBParamContextId,
                "NEW Process Group B should use the same parameter context as its parent A2, not be bound to the first parameter context P");
    }

    private ProcessGroupEntity importFlowWithReplaceParameterContext(
            final String registryClientId,
            final String flowId,
            final String version) throws NiFiClientException, IOException {

        final VersionControlInformationDTO vci = new VersionControlInformationDTO();
        vci.setBucketId(TEST_FLOWS_BUCKET);
        vci.setFlowId(flowId);
        vci.setVersion(version);
        vci.setRegistryId(registryClientId);

        final ProcessGroupDTO processGroupDto = new ProcessGroupDTO();
        processGroupDto.setVersionControlInformation(vci);

        final ProcessGroupEntity groupEntity = new ProcessGroupEntity();
        groupEntity.setComponent(processGroupDto);
        groupEntity.setRevision(getClientUtil().createNewRevision());

        return getNifiClient().getProcessGroupClient().createProcessGroup("root", groupEntity, false);
    }

    private ProcessGroupEntity getNestedProcessGroup(final ProcessGroupEntity parent, final String name) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getFlowClient().getProcessGroup(parent.getId());
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();

        for (final ProcessGroupEntity childGroup : flowDto.getProcessGroups()) {
            if (name.equals(childGroup.getComponent().getName())) {
                return getNifiClient().getProcessGroupClient().getProcessGroup(childGroup.getId());
            }
        }
        return null;
    }
}
