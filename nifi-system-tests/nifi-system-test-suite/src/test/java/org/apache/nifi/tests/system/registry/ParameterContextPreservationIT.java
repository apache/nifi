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
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System tests to verify that parameter context bindings and inheritance chains are preserved
 * during versioned flow operations (upgrades and deployments with different handling strategies).
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

    /**
     * Verifies that inherited parameter context chains are preserved when deploying a versioned flow
     * with the KEEP_EXISTING parameter context handling strategy (NIFI-15746).
     *
     * Reproduces a multi-environment scenario where shared-service-params is configured to inherit from
     * target-system-params on the target instance, but the versioned flow snapshot defines it as inheriting
     * from source-system-params. Deploying with KEEP_EXISTING should preserve the target's inheritance chain.
     */
    @Test
    void testInheritedParameterContextsPreservedWithKeepExistingStrategy() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final ParameterContextEntity sourceSystemParams = util.createParameterContext("source-system-params", Map.of("env", "source"));
        final ParameterContextEntity sharedServiceParams = util.createParameterContext(
                "shared-service-params", Map.of("shared-param", "shared-value"), List.of(sourceSystemParams.getId()), null);

        final ProcessGroupEntity workflowGroup = util.createProcessGroup("MyWorkflow", "root");
        util.setParameterContext(workflowGroup.getId(), sharedServiceParams);

        final ProcessGroupEntity serviceGroup = util.createProcessGroup("MyService", workflowGroup.getId());
        util.setParameterContext(serviceGroup.getId(), sharedServiceParams);

        final ProcessorEntity processor = util.createProcessor(PROCESSOR_TYPE, serviceGroup.getId());
        util.updateProcessorProperties(processor, Collections.singletonMap(PROCESSOR_PROPERTY_TEXT, "#{shared-param}"));
        util.setAutoTerminatedRelationships(processor, RELATIONSHIP_SUCCESS);

        final VersionControlInformationEntity vci = util.startVersionControl(workflowGroup, clientEntity, TEST_FLOWS_BUCKET, "InheritedParamContextFlow");
        final String flowId = vci.getVersionControlInformation().getFlowId();

        final ProcessGroupEntity groupForStopVc = getNifiClient().getProcessGroupClient().getProcessGroup(workflowGroup.getId());
        getNifiClient().getVersionsClient().stopVersionControl(groupForStopVc);
        util.deleteAll(workflowGroup.getId());
        final ProcessGroupEntity groupToDelete = getNifiClient().getProcessGroupClient().getProcessGroup(workflowGroup.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(groupToDelete);

        final ParameterContextEntity targetSystemParams = util.createParameterContext("target-system-params", Map.of("env", "target"));

        final ParameterContextEntity currentSharedParams = getNifiClient().getParamContextClient().getParamContext(sharedServiceParams.getId(), false);
        final ParameterContextUpdateRequestEntity updateRequest = util.updateParameterContext(
                currentSharedParams, Map.of("shared-param", "shared-value"), List.of(targetSystemParams.getId()));
        util.waitForParameterContextRequestToComplete(sharedServiceParams.getId(), updateRequest.getRequest().getRequestId());

        final ParameterContextEntity verifyBeforeImport = getNifiClient().getParamContextClient().getParamContext(sharedServiceParams.getId(), false);
        final List<ParameterContextReferenceEntity> inheritedBeforeImport = verifyBeforeImport.getComponent().getInheritedParameterContexts();
        assertEquals(1, inheritedBeforeImport.size());
        assertEquals(targetSystemParams.getId(), inheritedBeforeImport.get(0).getId());

        importFlowWithKeepExisting(clientEntity.getId(), flowId, VERSION_1);

        final ParameterContextEntity sharedParamsAfterImport = getNifiClient().getParamContextClient().getParamContext(sharedServiceParams.getId(), false);
        final List<ParameterContextReferenceEntity> inheritedAfterImport = sharedParamsAfterImport.getComponent().getInheritedParameterContexts();
        assertNotNull(inheritedAfterImport, "Inherited parameter contexts should not be null after KEEP_EXISTING import");
        assertFalse(inheritedAfterImport.isEmpty(), "Inherited parameter contexts should not be empty after KEEP_EXISTING import");
        assertEquals(1, inheritedAfterImport.size());
        assertEquals(targetSystemParams.getId(), inheritedAfterImport.get(0).getId(),
                "After KEEP_EXISTING import, shared-service-params should still inherit from target-system-params");
    }

    private ProcessGroupEntity importFlowWithKeepExisting(final String registryClientId, final String flowId,
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

        return getNifiClient().getProcessGroupClient().createProcessGroup("root", groupEntity, true);
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
