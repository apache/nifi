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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(ParameterContextPreservationIT.class);
    static final String TEST_FLOWS_BUCKET = "test-flows";

    @Test
    void testNewProcessGroupUsesCorrectParameterContextDuringUpgrade() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        // Step 1: Create Parameter Context "P" with param1
        final String paramContextName = "P";
        final ParameterContextEntity paramContextP = util.createParameterContext(paramContextName, Map.of("param1", "value1"));

        // Step 2: Create v1 - Process Group A with just a Processor X using param1
        final ProcessGroupEntity groupA = util.createProcessGroup("A", "root");
        util.setParameterContext(groupA.getId(), paramContextP);

        final ProcessorEntity processorX = util.createProcessor("GenerateFlowFile", groupA.getId());
        util.updateProcessorProperties(processorX, Collections.singletonMap("Text", "#{param1}"));
        util.setAutoTerminatedRelationships(processorX, "success");

        // Save as v1
        final VersionControlInformationEntity vciV1 = util.startVersionControl(groupA, clientEntity, TEST_FLOWS_BUCKET, "FlowWithParameterContext");
        final String flowId = vciV1.getVersionControlInformation().getFlowId();
        logger.info("Saved v1: Process Group A with Processor X");

        // Step 3: Create v2 - Add Process Group B inside A, also attached to P
        final ProcessGroupEntity groupB = util.createProcessGroup("B", groupA.getId());
        util.setParameterContext(groupB.getId(), paramContextP);

        // Add a processor in B that uses param1
        final ProcessorEntity processorInB = util.createProcessor("GenerateFlowFile", groupB.getId());
        util.updateProcessorProperties(processorInB, Collections.singletonMap("Text", "#{param1}"));
        util.setAutoTerminatedRelationships(processorInB, "success");

        // Save as v2
        final ProcessGroupEntity refreshedGroupA = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        util.saveFlowVersion(refreshedGroupA, clientEntity, vciV1);
        logger.info("Saved v2: Added Process Group B inside A, attached to P");

        // Step 4: Clean up original flow
        final ProcessGroupEntity groupAForStopVc = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        getNifiClient().getVersionsClient().stopVersionControl(groupAForStopVc);
        util.deleteAll(groupA.getId());
        final ProcessGroupEntity groupAToDelete = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(groupAToDelete);

        final ParameterContextEntity contextToDelete = getNifiClient().getParamContextClient().getParamContext(paramContextP.getId(), false);
        getNifiClient().getParamContextClient().deleteParamContext(paramContextP.getId(),
                String.valueOf(contextToDelete.getRevision().getVersion()));
        logger.info("Cleaned up original flow and parameter context");

        // Step 5: Import v1 FIRST time - creates A1 with Parameter Context "P"
        final ProcessGroupEntity importedGroup1 = importFlowWithReplaceParameterContext(
                "root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");

        final ProcessGroupEntity fetchedGroup1 = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup1.getId());
        final String paramContextId1 = fetchedGroup1.getComponent().getParameterContext().getId();
        final ParameterContextEntity paramContext1 = getNifiClient().getParamContextClient().getParamContext(paramContextId1, false);
        logger.info("First import (A1): Parameter Context id={}, name={}", paramContextId1, paramContext1.getComponent().getName());

        // Step 6: Import v1 SECOND time - creates A2 with Parameter Context "P (1)"
        final ProcessGroupEntity importedGroup2 = importFlowWithReplaceParameterContext(
                "root", clientEntity.getId(), TEST_FLOWS_BUCKET, flowId, "1");

        final ProcessGroupEntity fetchedGroup2 = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup2.getId());
        final String paramContextId2 = fetchedGroup2.getComponent().getParameterContext().getId();
        final ParameterContextEntity paramContext2 = getNifiClient().getParamContextClient().getParamContext(paramContextId2, false);
        logger.info("Second import (A2): Parameter Context id={}, name={}", paramContextId2, paramContext2.getComponent().getName());

        // Verify the two imports have DIFFERENT parameter contexts
        assertNotEquals(paramContextId1, paramContextId2,
                "Second import should have a different parameter context");
        logger.info("Verified: A1 uses '{}', A2 uses '{}'",
                paramContext1.getComponent().getName(), paramContext2.getComponent().getName());

        // Step 7: Upgrade A2 from v1 to v2 (this adds Process Group B)
        logger.info("Upgrading A2 from v1 to v2...");
        util.changeFlowVersion(importedGroup2.getId(), "2");
        logger.info("Upgrade complete");

        // Step 8: Verify the NEW Process Group B uses the CORRECT parameter context
        final ProcessGroupEntity upgradedGroup2 = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup2.getId());
        final String upgradedA2ParamContextId = upgradedGroup2.getComponent().getParameterContext().getId();
        logger.info("After upgrade - A2 Parameter Context: id={}", upgradedA2ParamContextId);

        // A2 should still use P (1)
        assertEquals(paramContextId2, upgradedA2ParamContextId,
                "After upgrade, A2 should still reference its original parameter context");

        // Get the newly added Process Group B
        final ProcessGroupEntity newGroupB = getNestedProcessGroup(upgradedGroup2, "B");
        assertNotNull(newGroupB, "Process Group B should exist after upgrade");
        assertNotNull(newGroupB.getComponent().getParameterContext(),
                "Process Group B should have a parameter context assigned");

        final String groupBParamContextId = newGroupB.getComponent().getParameterContext().getId();
        final ParameterContextEntity groupBParamContext = getNifiClient().getParamContextClient().getParamContext(groupBParamContextId, false);
        logger.info("After upgrade - New Process Group B Parameter Context: id={}, name={}",
                groupBParamContextId, groupBParamContext.getComponent().getName());

        // The NEW Process Group B should use the SAME parameter context as its parent A2 (P (1))
        assertEquals(paramContextId2, groupBParamContextId,
                "NEW Process Group B should use the same parameter context as its parent A2, not be bound to the first parameter context P");
    }

    private ProcessGroupEntity importFlowWithReplaceParameterContext(
            final String parentGroupId,
            final String registryClientId,
            final String bucketId,
            final String flowId,
            final String version) throws NiFiClientException, IOException {

        final VersionControlInformationDTO vci = new VersionControlInformationDTO();
        vci.setBucketId(bucketId);
        vci.setFlowId(flowId);
        vci.setVersion(version);
        vci.setRegistryId(registryClientId);

        final ProcessGroupDTO processGroupDto = new ProcessGroupDTO();
        processGroupDto.setVersionControlInformation(vci);

        final ProcessGroupEntity groupEntity = new ProcessGroupEntity();
        groupEntity.setComponent(processGroupDto);
        groupEntity.setRevision(getClientUtil().createNewRevision());

        return getNifiClient().getProcessGroupClient().createProcessGroup(parentGroupId, groupEntity, false);
    }

    private ProcessGroupEntity getNestedProcessGroup(ProcessGroupEntity parent, String name) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = getNifiClient().getFlowClient().getProcessGroup(parent.getId());
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();

        for (ProcessGroupEntity childGroup : flowDto.getProcessGroups()) {
            if (name.equals(childGroup.getComponent().getName())) {
                return getNifiClient().getProcessGroupClient().getProcessGroup(childGroup.getId());
            }
        }
        return null;
    }
}
