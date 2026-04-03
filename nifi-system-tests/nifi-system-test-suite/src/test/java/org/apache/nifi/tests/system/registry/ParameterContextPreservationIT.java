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
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    private static final String DESCRIPTION_UPDATE_FLOW_NAME = "DescriptionUpdateFlow";
    private static final String PARAMETER_DESCRIPTION_V1 = "Description for version 1";
    private static final String PARAMETER_DESCRIPTION_V2 = "Description for version 2";
    private static final String CONTEXT_DESCRIPTION_V1 = "Context description v1";
    private static final String CONTEXT_DESCRIPTION_V2 = "Context description v2";

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

    /**
     * Verifies that new parameters added to an inherited parameter context are properly synchronized
     * when upgrading a versioned process group.
     *
     * Scenario: P1 inherits from P2. PG is bound to P1. In version 2, a new parameter is added to P2.
     * After importing version 1 fresh and upgrading to version 2, P2 should contain the new parameter.
     */
    @Test
    void testNewParameterInInheritedContextSynchronizedDuringUpgrade() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        // Create P2 with parameter "paramA"
        final ParameterContextEntity paramContextP2 = util.createParameterContext("P2", Map.of("paramA", "valueA"));

        // Create P1 inheriting from P2 with its own parameter
        final ParameterContextEntity paramContextP1 = util.createParameterContext("P1", Map.of("paramOwn", "ownValue"), List.of(paramContextP2.getId()), null);

        // Create PG bound to P1 with a processor that references paramA (inherited from P2)
        final ProcessGroupEntity groupPG = util.createProcessGroup("PG", "root");
        util.setParameterContext(groupPG.getId(), paramContextP1);

        final ProcessorEntity processor = util.createProcessor(PROCESSOR_TYPE, groupPG.getId());
        util.updateProcessorProperties(processor, Collections.singletonMap(PROCESSOR_PROPERTY_TEXT, "#{paramA}"));
        util.setAutoTerminatedRelationships(processor, RELATIONSHIP_SUCCESS);

        // Save as version 1 (P2 has only paramA)
        final VersionControlInformationEntity vciV1 = util.startVersionControl(groupPG, clientEntity, TEST_FLOWS_BUCKET, "InheritedParamFlow");
        final String flowId = vciV1.getVersionControlInformation().getFlowId();

        // Add new parameter "paramX" to P2 and save as version 2
        final ParameterContextEntity currentP2 = getNifiClient().getParamContextClient().getParamContext(paramContextP2.getId(), false);
        final ParameterContextUpdateRequestEntity updateRequest = util.updateParameterContext(currentP2, Map.of("paramA", "valueA", "paramX", "valueX"));
        util.waitForParameterContextRequestToComplete(paramContextP2.getId(), updateRequest.getRequest().getRequestId());

        final ProcessGroupEntity refreshedPG = getNifiClient().getProcessGroupClient().getProcessGroup(groupPG.getId());
        util.saveFlowVersion(refreshedPG, clientEntity, vciV1);

        // Clean up the original flow and parameter contexts
        final ProcessGroupEntity pgForStopVc = getNifiClient().getProcessGroupClient().getProcessGroup(groupPG.getId());
        getNifiClient().getVersionsClient().stopVersionControl(pgForStopVc);
        util.deleteAll(groupPG.getId());
        final ProcessGroupEntity pgToDelete = getNifiClient().getProcessGroupClient().getProcessGroup(groupPG.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(pgToDelete);

        final ParameterContextEntity p1ToDelete = getNifiClient().getParamContextClient().getParamContext(paramContextP1.getId(), false);
        getNifiClient().getParamContextClient().deleteParamContext(paramContextP1.getId(), String.valueOf(p1ToDelete.getRevision().getVersion()));

        final ParameterContextEntity p2ToDelete = getNifiClient().getParamContextClient().getParamContext(paramContextP2.getId(), false);
        getNifiClient().getParamContextClient().deleteParamContext(paramContextP2.getId(), String.valueOf(p2ToDelete.getRevision().getVersion()));

        // Import version 1 fresh -- creates new P1 and P2 with only paramA on P2
        final ProcessGroupEntity importedPG = importFlowWithReplaceParameterContext(clientEntity.getId(), flowId, VERSION_1);

        // Locate the imported P2 by finding P1's inherited context
        final ProcessGroupEntity fetchedPG = getNifiClient().getProcessGroupClient().getProcessGroup(importedPG.getId());
        final String importedP1Id = fetchedPG.getComponent().getParameterContext().getId();
        final ParameterContextEntity importedP1 = getNifiClient().getParamContextClient().getParamContext(importedP1Id, false);
        assertEquals(1, importedP1.getComponent().getInheritedParameterContexts().size());
        final String importedP2Id = importedP1.getComponent().getInheritedParameterContexts().get(0).getId();

        // Verify P2 has only paramA after importing version 1
        final ParameterContextEntity importedP2 = getNifiClient().getParamContextClient().getParamContext(importedP2Id, false);
        final Set<String> p2NamesAfterImport = getParameterNames(importedP2);
        assertTrue(p2NamesAfterImport.contains("paramA"), "paramA should exist on P2 after importing version 1");
        assertFalse(p2NamesAfterImport.contains("paramX"), "paramX should not exist on P2 after importing version 1");

        // Upgrade from version 1 to version 2
        util.changeFlowVersion(importedPG.getId(), VERSION_2);

        // Verify P2 now contains paramX after the upgrade
        final ParameterContextEntity p2AfterUpgrade = getNifiClient().getParamContextClient().getParamContext(importedP2Id, false);
        final Set<String> p2NamesAfterUpgrade = getParameterNames(p2AfterUpgrade);
        assertTrue(p2NamesAfterUpgrade.contains("paramA"), "paramA should still exist on P2 after upgrading to version 2");
        assertTrue(p2NamesAfterUpgrade.contains("paramX"), "paramX should exist on P2 after upgrading to version 2");
    }

    /**
     * Verifies that parameter descriptions are updated when upgrading a versioned process group from one version
     * to the next, even when the parameter value itself remains unchanged.
     */
    @Test
    void testParameterDescriptionUpdatedDuringVersionUpgrade() throws NiFiClientException, IOException, InterruptedException {
        final FlowRegistryClientEntity clientEntity = registerClient();
        final NiFiClientUtil util = getClientUtil();

        final Set<ParameterEntity> parameterEntitiesV1 = Set.of(util.createParameterEntity(PARAMETER_NAME, PARAMETER_DESCRIPTION_V1, false, PARAMETER_VALUE));
        final ParameterContextEntity paramContext = getNifiClient().getParamContextClient().createParamContext(
            util.createParameterContextEntity(PARAMETER_CONTEXT_NAME, CONTEXT_DESCRIPTION_V1, parameterEntitiesV1));

        final ProcessGroupEntity groupA = util.createProcessGroup(GROUP_A_NAME, "root");
        util.setParameterContext(groupA.getId(), paramContext);

        final ProcessorEntity processor = util.createProcessor(PROCESSOR_TYPE, groupA.getId());
        util.updateProcessorProperties(processor, Collections.singletonMap(PROCESSOR_PROPERTY_TEXT, PARAMETER_REFERENCE));
        util.setAutoTerminatedRelationships(processor, RELATIONSHIP_SUCCESS);

        final VersionControlInformationEntity vciV1 = util.startVersionControl(groupA, clientEntity, TEST_FLOWS_BUCKET, DESCRIPTION_UPDATE_FLOW_NAME);
        final String flowId = vciV1.getVersionControlInformation().getFlowId();

        // Update the parameter description (keeping the same value) and context description, then save as version 2
        final ParameterContextEntity currentContext = getNifiClient().getParamContextClient().getParamContext(paramContext.getId(), false);
        final Set<ParameterEntity> parameterEntitiesV2 = Set.of(util.createParameterEntity(PARAMETER_NAME, PARAMETER_DESCRIPTION_V2, false, PARAMETER_VALUE));
        final ParameterContextEntity entityUpdate = util.createParameterContextEntity(PARAMETER_CONTEXT_NAME, CONTEXT_DESCRIPTION_V2, parameterEntitiesV2);
        entityUpdate.setId(currentContext.getId());
        entityUpdate.setRevision(currentContext.getRevision());
        entityUpdate.getComponent().setId(currentContext.getComponent().getId());
        final ParameterContextUpdateRequestEntity updateRequest = getNifiClient().getParamContextClient().updateParamContext(entityUpdate);
        util.waitForParameterContextRequestToComplete(paramContext.getId(), updateRequest.getRequest().getRequestId());

        final ProcessGroupEntity refreshedGroupA = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        util.saveFlowVersion(refreshedGroupA, clientEntity, vciV1);

        // Clean up the original flow
        final ProcessGroupEntity groupAForStopVc = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        getNifiClient().getVersionsClient().stopVersionControl(groupAForStopVc);
        util.deleteAll(groupA.getId());
        final ProcessGroupEntity groupAToDelete = getNifiClient().getProcessGroupClient().getProcessGroup(groupA.getId());
        getNifiClient().getProcessGroupClient().deleteProcessGroup(groupAToDelete);

        final ParameterContextEntity contextToDelete = getNifiClient().getParamContextClient().getParamContext(paramContext.getId(), false);
        getNifiClient().getParamContextClient().deleteParamContext(paramContext.getId(), String.valueOf(contextToDelete.getRevision().getVersion()));

        // Import version 1 fresh
        final ProcessGroupEntity importedGroup = importFlowWithReplaceParameterContext(clientEntity.getId(), flowId, VERSION_1);

        final ProcessGroupEntity fetchedGroup = getNifiClient().getProcessGroupClient().getProcessGroup(importedGroup.getId());
        final String importedContextId = fetchedGroup.getComponent().getParameterContext().getId();

        // Verify version 1 descriptions
        final ParameterContextEntity importedContext = getNifiClient().getParamContextClient().getParamContext(importedContextId, false);
        final String descriptionAfterV1 = getParameterDescription(importedContext, PARAMETER_NAME);
        assertEquals(PARAMETER_DESCRIPTION_V1, descriptionAfterV1);
        assertEquals(CONTEXT_DESCRIPTION_V1, importedContext.getComponent().getDescription());

        // Upgrade from version 1 to version 2
        util.changeFlowVersion(importedGroup.getId(), VERSION_2);

        // Verify descriptions were updated to version 2
        final ParameterContextEntity contextAfterUpgrade = getNifiClient().getParamContextClient().getParamContext(importedContextId, false);
        final String descriptionAfterV2 = getParameterDescription(contextAfterUpgrade, PARAMETER_NAME);
        assertEquals(PARAMETER_DESCRIPTION_V2, descriptionAfterV2);
        assertEquals(CONTEXT_DESCRIPTION_V2, contextAfterUpgrade.getComponent().getDescription());

        // Verify the value was not changed
        final String valueAfterUpgrade = getParameterValue(contextAfterUpgrade, PARAMETER_NAME);
        assertEquals(PARAMETER_VALUE, valueAfterUpgrade);
    }

    private String getParameterDescription(final ParameterContextEntity context, final String parameterName) {
        return context.getComponent().getParameters().stream()
            .filter(entity -> parameterName.equals(entity.getParameter().getName()))
            .map(entity -> entity.getParameter().getDescription())
            .findFirst()
            .orElse(null);
    }

    private String getParameterValue(final ParameterContextEntity context, final String parameterName) {
        return context.getComponent().getParameters().stream()
            .filter(entity -> parameterName.equals(entity.getParameter().getName()))
            .map(entity -> entity.getParameter().getValue())
            .findFirst()
            .orElse(null);
    }

    private Set<String> getParameterNames(final ParameterContextEntity context) {
        return context.getComponent().getParameters().stream()
            .map(entity -> entity.getParameter().getName())
            .collect(Collectors.toSet());
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
