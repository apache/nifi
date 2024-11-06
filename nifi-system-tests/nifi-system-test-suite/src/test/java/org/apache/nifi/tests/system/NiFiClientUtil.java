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
package org.apache.nifi.tests.system;

import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.controller.AbstractPort;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.parameter.ParameterProviderConfiguration;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.toolkit.client.ConnectionClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessorClient;
import org.apache.nifi.toolkit.client.VersionsClient;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersSnapshotDTO;
import org.apache.nifi.web.api.dto.DifferenceDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextReferenceDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.ParameterProviderConfigurationDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceRequestDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceSearchValueDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.CopyRequestEntity;
import org.apache.nifi.web.api.entity.CopyResponseEntity;
import org.apache.nifi.web.api.entity.CountersEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleRunStatusEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterApplicationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterFetchEntity;
import org.apache.nifi.web.api.entity.ParameterProvidersEntity;
import org.apache.nifi.web.api.entity.PasteRequestEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskRunStatusEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NiFiClientUtil {
    private static final Logger logger = LoggerFactory.getLogger(NiFiClientUtil.class);

    private final NiFiClient nifiClient;
    private final String nifiVersion;
    private final String testName;

    public NiFiClientUtil(final NiFiClient client, final String nifiVersion, final String testName) {
        this.nifiClient = client;
        this.nifiVersion = nifiVersion;
        this.testName = testName.replace("()", "");
    }

    private ProcessorClient getProcessorClient() {
        final ProcessorClient client = nifiClient.getProcessorClient();
        client.acknowledgeDisconnectedNode();
        return client;
    }

    private ConnectionClient getConnectionClient() {
        final ConnectionClient client = nifiClient.getConnectionClient();
        client.acknowledgeDisconnectedNode();
        return client;
    }

    public ProcessorEntity startProcessor(final ProcessorEntity currentEntity) throws NiFiClientException, IOException, InterruptedException {
        waitForValidationCompleted(currentEntity);

        currentEntity.setDisconnectedNodeAcknowledged(true);
        return getProcessorClient().startProcessor(currentEntity);
    }

    public void stopProcessor(final ProcessorEntity currentEntity) throws NiFiClientException, IOException, InterruptedException {
        currentEntity.setDisconnectedNodeAcknowledged(true);
        getProcessorClient().stopProcessor(currentEntity);
        waitForStoppedProcessor(currentEntity.getId());
    }

    public ProcessorEntity createPythonProcessor(final String typeName) throws NiFiClientException, IOException {
        return createProcessor(typeName, NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_PYTHON_EXTENSIONS_ARTIFACT_ID, "0.0.1-SNAPSHOT");
    }

    public ProcessorEntity createProcessor(final String simpleTypeName) throws NiFiClientException, IOException {
        return createProcessor(NiFiSystemIT.TEST_PROCESSORS_PACKAGE + "." + simpleTypeName, NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_ARTIFACT_ID, nifiVersion);
    }

    public ProcessorEntity createProcessor(final String simpleTypeName, final String groupId) throws NiFiClientException, IOException {
        return createProcessor(NiFiSystemIT.TEST_PROCESSORS_PACKAGE + "." + simpleTypeName, groupId, NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_ARTIFACT_ID, nifiVersion);
    }

    public ProcessorEntity createProcessor(final String type, final String bundleGroupId, final String artifactId, final String version) throws NiFiClientException, IOException {
        return createProcessor(type, "root", bundleGroupId, artifactId, version);
    }

    public ProcessorEntity createProcessor(final String type, final String processGroupId, final String bundleGroupId, final String artifactId, final String version)
            throws NiFiClientException, IOException {
        final ProcessorDTO dto = new ProcessorDTO();
        dto.setType(type);

        final BundleDTO bundle = new BundleDTO();
        bundle.setGroup(bundleGroupId);
        bundle.setArtifact(artifactId);
        bundle.setVersion(version);
        dto.setBundle(bundle);

        final ProcessorEntity entity = new ProcessorEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setDisconnectedNodeAcknowledged(true);

        final ProcessorEntity processor = getProcessorClient().createProcessor(processGroupId, entity);
        logger.info("Created Processor [type={}, id={}, name={}, parentGroupId={}] for Test [{}]", simpleName(type), processor.getId(), processor.getComponent().getName(), processGroupId, testName);
        return processor;
    }

    private String simpleName(final String type) {
        final int lastIndex = type.lastIndexOf(".");
        if (lastIndex <= 0) {
            return type;
        }
        if (lastIndex == type.length() - 1) {
            return type;
        }

        return type.substring(lastIndex + 1);
    }

    public ParameterProviderEntity createParameterProvider(final String simpleTypeName) throws NiFiClientException, IOException {
        return createParameterProvider(NiFiSystemIT.TEST_PARAM_PROVIDERS_PACKAGE + "." + simpleTypeName, NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_ARTIFACT_ID, nifiVersion);
    }

    public ParameterProviderEntity createParameterProvider(final String type, final String bundleGroupId, final String artifactId, final String version)
            throws NiFiClientException, IOException {
        final ParameterProviderDTO dto = new ParameterProviderDTO();
        dto.setType(type);

        final BundleDTO bundle = new BundleDTO();
        bundle.setGroup(bundleGroupId);
        bundle.setArtifact(artifactId);
        bundle.setVersion(version);
        dto.setBundle(bundle);

        final ParameterProviderEntity entity = new ParameterProviderEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());

        return nifiClient.getControllerClient().createParamProvider(entity);
    }

    public ParameterProviderEntity updateParameterProviderProperties(final ParameterProviderEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final ParameterProviderDTO dto = new ParameterProviderDTO();
        dto.setProperties(properties);
        dto.setId(currentEntity.getId());

        final ParameterProviderEntity updatedEntity = new ParameterProviderEntity();
        updatedEntity.setRevision(currentEntity.getRevision());
        updatedEntity.setComponent(dto);
        updatedEntity.setId(currentEntity.getId());

        return nifiClient.getParamProviderClient().updateParamProvider(updatedEntity);
    }

    public ParameterProviderEntity fetchParameters(final ParameterProviderEntity existingEntity) throws NiFiClientException, IOException {
        ParameterProviderParameterFetchEntity parameterFetchEntity = new ParameterProviderParameterFetchEntity();
        parameterFetchEntity.setId(existingEntity.getId());
        parameterFetchEntity.setRevision(existingEntity.getRevision());

        return nifiClient.getParamProviderClient().fetchParameters(parameterFetchEntity);
    }

    public ParameterProviderApplyParametersRequestEntity applyParameters(final ParameterProviderEntity existingEntity,
                                                                         final Collection<ParameterGroupConfigurationEntity> parameterNameGroups) throws NiFiClientException, IOException {
        final ParameterProviderParameterApplicationEntity parameterApplicationEntity = new ParameterProviderParameterApplicationEntity();
        parameterApplicationEntity.setParameterGroupConfigurations(parameterNameGroups);
        parameterApplicationEntity.setId(existingEntity.getId());
        parameterApplicationEntity.setRevision(existingEntity.getRevision());

        return nifiClient.getParamProviderClient().applyParameters(parameterApplicationEntity);
    }

    public void waitForParameterProviderApplicationRequestToComplete(final String providerId, final String requestId) throws NiFiClientException, IOException, InterruptedException {
        while (true) {
            final ParameterProviderApplyParametersRequestEntity entity = nifiClient.getParamProviderClient().getParamProviderApplyParametersRequest(providerId, requestId);
            if (entity.getRequest().isComplete()) {
                if (entity.getRequest().getFailureReason() == null) {
                    return;
                }

                throw new RuntimeException("Parameter Provider Application failed: " + entity.getRequest().getFailureReason());
            }

            Thread.sleep(100L);
        }
    }

    public ControllerServiceEntity createControllerService(final String simpleTypeName) throws NiFiClientException, IOException {
        return createControllerService(simpleTypeName, "root");
    }

    public ControllerServiceEntity createRootLevelControllerService(final String simpleTypeName) throws NiFiClientException, IOException {
        return createControllerService(NiFiSystemIT.TEST_CS_PACKAGE + "." + simpleTypeName, null, NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_SERVICES_ARTIFACT_ID, nifiVersion);
    }

    public ControllerServiceEntity createControllerService(final String simpleTypeName, final String groupId) throws NiFiClientException, IOException {
        return createControllerService(NiFiSystemIT.TEST_CS_PACKAGE + "." + simpleTypeName, groupId, NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_SERVICES_ARTIFACT_ID, nifiVersion);
    }

    public ControllerServiceEntity createControllerService(final String type, final String processGroupId, final String bundleGroupId, final String artifactId, final String version)
                throws NiFiClientException, IOException {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
        dto.setType(type);

        final BundleDTO bundle = new BundleDTO();
        bundle.setGroup(bundleGroupId);
        bundle.setArtifact(artifactId);
        bundle.setVersion(version);
        dto.setBundle(bundle);

        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setDisconnectedNodeAcknowledged(true);

        final ControllerServiceEntity service;
        if (processGroupId == null) {
            service = nifiClient.getControllerClient().createControllerService(entity);
        } else {
            service = nifiClient.getControllerServicesClient().createControllerService(processGroupId, entity);
        }

        logger.info("Created Controller Service [type={}, id={}, name={}, groupId={}] for Test [{}]", simpleName(type), service.getId(), service.getComponent().getName(), processGroupId, testName);
        return service;
    }

    public BundleDTO getTestBundle() {
        return new BundleDTO(NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_ARTIFACT_ID, nifiVersion);
    }

    public ReportingTaskEntity createReportingTask(final String type) throws NiFiClientException, IOException {
        return createReportingTask(NiFiSystemIT.TEST_REPORTING_TASK_PACKAGE + "." + type, getTestBundle());
    }

    public ReportingTaskEntity createReportingTask(final String type, final BundleDTO bundle) throws NiFiClientException, IOException {
        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setBundle(bundle);
        dto.setType(type);

        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setDisconnectedNodeAcknowledged(true);

        final ReportingTaskEntity reportingTask = nifiClient.getControllerClient().createReportingTask(entity);
        logger.info("Created Reporting Task [type={}, id={}] for Test [{}]", simpleName(type), reportingTask.getId(), testName);

        return reportingTask;
    }

    public ReportingTaskEntity updateReportingTaskProperties(final ReportingTaskEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final ReportingTaskDTO taskDto = new ReportingTaskDTO();
        taskDto.setProperties(properties);
        taskDto.setId(currentEntity.getId());

        final ReportingTaskEntity updatedEntity = new ReportingTaskEntity();
        updatedEntity.setRevision(currentEntity.getRevision());
        updatedEntity.setComponent(taskDto);
        updatedEntity.setId(currentEntity.getId());
        updatedEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getReportingTasksClient().updateReportingTask(updatedEntity);

    }

    public ReportingTaskEntity startReportingTask(final ReportingTaskEntity reportingTask) throws NiFiClientException, IOException {
        return activateReportingTask(reportingTask, "RUNNING");
    }

    public ReportingTaskEntity stopReportingTask(final ReportingTaskEntity reportingTask) throws NiFiClientException, IOException, InterruptedException {
        final ReportingTaskEntity entity = activateReportingTask(reportingTask, "STOPPED");
        return waitForReportingTaskState(entity.getId(), "STOPPED");
    }

    public void stopReportingTasks() throws NiFiClientException, IOException, InterruptedException {
        final ReportingTasksEntity tasksEntity = nifiClient.getFlowClient().getReportingTasks();
        for (final ReportingTaskEntity taskEntity : tasksEntity.getReportingTasks()) {
            stopReportingTask(taskEntity);
        }
    }

    public ReportingTaskEntity activateReportingTask(final ReportingTaskEntity reportingTask, final String state) throws NiFiClientException, IOException {
        final ReportingTaskRunStatusEntity runStatusEntity = new ReportingTaskRunStatusEntity();
        runStatusEntity.setState(state);
        runStatusEntity.setRevision(reportingTask.getRevision());
        runStatusEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getReportingTasksClient().activateReportingTask(reportingTask.getId(), runStatusEntity);
    }

    public void deleteReportingTasks() throws NiFiClientException, IOException {
        final ReportingTasksEntity tasksEntity = nifiClient.getFlowClient().getReportingTasks();
        for (final ReportingTaskEntity taskEntity : tasksEntity.getReportingTasks()) {
            taskEntity.setDisconnectedNodeAcknowledged(true);
            nifiClient.getReportingTasksClient().deleteReportingTask(taskEntity);
        }
    }

    public FlowAnalysisRuleEntity createFlowAnalysisRule(final String type) throws NiFiClientException, IOException {
        return createFlowAnalysisRule(NiFiSystemIT.TEST_FLOW_ANALYSIS_RULE_PACKAGE + "." + type, getTestBundle());
    }

    public FlowAnalysisRuleEntity createFlowAnalysisRule(final String type, final BundleDTO bundle) throws NiFiClientException, IOException {
        final FlowAnalysisRuleDTO dto = new FlowAnalysisRuleDTO();
        dto.setBundle(bundle);
        dto.setType(type);

        final FlowAnalysisRuleEntity entity = new FlowAnalysisRuleEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setDisconnectedNodeAcknowledged(true);

        final FlowAnalysisRuleEntity flowAnalysisRule = nifiClient.getControllerClient().createFlowAnalysisRule(entity);
        logger.info("Created Flow Analysis Rule [type={}, id={}] for Test [{}]", simpleName(type), flowAnalysisRule.getId(), testName);

        return flowAnalysisRule;
    }

    public FlowAnalysisRuleEntity updateFlowAnalysisRuleProperties(final FlowAnalysisRuleEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final FlowAnalysisRuleDTO ruleDto = new FlowAnalysisRuleDTO();
        ruleDto.setProperties(properties);
        ruleDto.setId(currentEntity.getId());

        final FlowAnalysisRuleEntity updatedEntity = new FlowAnalysisRuleEntity();
        updatedEntity.setRevision(currentEntity.getRevision());
        updatedEntity.setComponent(ruleDto);
        updatedEntity.setId(currentEntity.getId());
        updatedEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getControllerClient().updateFlowAnalysisRule(updatedEntity);

    }

    public FlowAnalysisRuleEntity enableFlowAnalysisRule(final FlowAnalysisRuleEntity entity) throws NiFiClientException, IOException {
        final FlowAnalysisRuleRunStatusEntity runStatusEntity = new FlowAnalysisRuleRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(entity.getRevision());
        runStatusEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getControllerClient().activateFlowAnalysisRule(entity.getId(), runStatusEntity);
    }

    public FlowAnalysisRuleEntity disableFlowAnalysisRule(final FlowAnalysisRuleEntity entity) throws NiFiClientException, IOException {
        final FlowAnalysisRuleRunStatusEntity runStatusEntity = new FlowAnalysisRuleRunStatusEntity();
        runStatusEntity.setState("DISABLED");
        runStatusEntity.setRevision(entity.getRevision());
        runStatusEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getControllerClient().activateFlowAnalysisRule(entity.getId(), runStatusEntity);
    }

    public void disableFlowAnalysisRules() throws NiFiClientException, IOException {
        final FlowAnalysisRulesEntity rules = nifiClient.getControllerClient().getFlowAnalysisRules();

        Collection<String> toBeDisabledRuleIds = new ArrayList<>();
        for (final FlowAnalysisRuleEntity rule : rules.getFlowAnalysisRules()) {
            disableFlowAnalysisRule(rule);
            toBeDisabledRuleIds.add(rule.getId());
        }

        waitForFlowAnalysisRuleState("DISABLED", toBeDisabledRuleIds);
    }

    public void deleteFlowAnalysisRules() throws NiFiClientException, IOException {
        final FlowAnalysisRulesEntity rulesEntity = nifiClient.getControllerClient().getFlowAnalysisRules();
        for (final FlowAnalysisRuleEntity taskEntity : rulesEntity.getFlowAnalysisRules()) {
            taskEntity.setDisconnectedNodeAcknowledged(true);
            nifiClient.getControllerClient().deleteFlowAnalysisRule(taskEntity);
        }
    }

    public void deleteParameterContexts() throws NiFiClientException, IOException {
        final ParameterContextsEntity parameterContextsEntity = nifiClient.getParamContextClient().getParamContexts();
        final Map<String, ParameterContextEntity> parameterContextMap = parameterContextsEntity.getParameterContexts().stream()
                .collect(Collectors.toMap(ParameterContextEntity::getId, Function.identity()));

        // If parameter context have inherited contexts then they needed to be deleted from the bottom up, so we just keep iterating
        // over the set of ids and retrying deletes until all have been deleted, knowing that some delete calls will fail the first time
        final Set<String> parameterContextIds = new HashSet<>(parameterContextMap.keySet());
        while (!parameterContextIds.isEmpty()) {
            final Iterator<String> parameterContextIdIterator = parameterContextIds.iterator();
            while (parameterContextIdIterator.hasNext()) {
                final String parameterContextId = parameterContextIdIterator.next();
                final ParameterContextEntity parameterContextEntity = parameterContextMap.get(parameterContextId);
                try {
                    final String version = String.valueOf(parameterContextEntity.getRevision().getVersion());
                    nifiClient.getParamContextClient().deleteParamContext(parameterContextId, version, true);
                    parameterContextIdIterator.remove();
                } catch (final Exception e) {
                    logger.warn("Failed to delete parameter context [{}] due to: {}", parameterContextId, e.getMessage());
                }
            }
        }
    }

    public void deleteParameterProviders() throws NiFiClientException, IOException {
        final ParameterProvidersEntity parameterProvidersEntity = nifiClient.getFlowClient().getParamProviders();
        for (final ParameterProviderEntity parameterProviderEntity : parameterProvidersEntity.getParameterProviders()) {
            final String version = String.valueOf(parameterProviderEntity.getRevision().getVersion());
            nifiClient.getParamProviderClient().deleteParamProvider(parameterProviderEntity.getId(), version, true);
        }
    }

    public void waitForFlowAnalysisRuleState(final String desiredState, final Collection<String> ruleIdsOfInterest) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);

        while (System.currentTimeMillis() < maxTimestamp) {
            final List<FlowAnalysisRuleEntity> flowAnalysisRulesNotInState = getFlowAnalysisRulesNotInState(desiredState, ruleIdsOfInterest);
            if (flowAnalysisRulesNotInState.isEmpty()) {
                logger.info("Flow Analysis Rules have desired state [{}]", desiredState);
                return;
            }

            final FlowAnalysisRuleEntity entity = flowAnalysisRulesNotInState.get(0);
            logger.info(
                    "Flow Analysis Rule ID [{}] Type [{}] State [{}] waiting for State [{}]: sleeping for 500 ms before retrying",
                    entity.getId(), entity.getComponent().getType(), entity.getComponent().getState(), desiredState
            );

            try {
                Thread.sleep(500L);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<FlowAnalysisRuleEntity> getFlowAnalysisRulesNotInState(final String desiredState, final Collection<String> ruleIds) throws NiFiClientException, IOException {
        final FlowAnalysisRulesEntity rulesEntity = nifiClient.getControllerClient().getFlowAnalysisRules();

        return rulesEntity.getFlowAnalysisRules().stream()
                .filter(rule -> ruleIds == null || ruleIds.isEmpty() || ruleIds.contains(rule.getId()))
                .filter(rule -> !desiredState.equalsIgnoreCase(rule.getComponent().getState()))
                .collect(Collectors.toList());
    }

    public void waitForFlowAnalysisRuleValid(final String reportingTaskId) throws NiFiClientException, IOException {
        waitForFlowAnalysisRuleValidationStatus(reportingTaskId, "Valid");
    }

    public void waitForFlowAnalysisRuleValidationStatus(final String flowAnalysisRuleId, final String validationStatus) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);

        while (System.currentTimeMillis() < maxTimestamp) {
            final FlowAnalysisRuleEntity flowAnalysisRuleEntity = nifiClient.getControllerClient().getFlowAnalysisRule(flowAnalysisRuleId);
            final String currentValidationStatus = flowAnalysisRuleEntity.getStatus().getValidationStatus();
            if (validationStatus.equalsIgnoreCase(currentValidationStatus)) {
                logger.info("Flow Analysis Rule ID [{}] Type [{}] Validation Status [{}] matched",
                        flowAnalysisRuleId, flowAnalysisRuleEntity.getComponent().getType(), validationStatus
                );
                return;
            }

            logger.info("Flow Analysis Rule ID [{}] Type [{}] Validation Status [{}] waiting for [{}]: sleeping for 500 ms before retrying",
                    flowAnalysisRuleEntity, flowAnalysisRuleEntity.getComponent().getType(), currentValidationStatus, validationStatus
            );

            try {
                Thread.sleep(500L);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ParameterEntity createParameterEntity(final String name, final String description, final boolean sensitive, final String value) {
        final ParameterDTO dto = new ParameterDTO();
        dto.setName(name);
        dto.setDescription(description);
        dto.setSensitive(sensitive);
        dto.setValue(value);
        dto.setProvided(false);

        final ParameterEntity entity = new ParameterEntity();
        entity.setParameter(dto);
        return entity;
    }

    public ParameterEntity createAssetReferenceParameterEntity(final String name, final List<String> referencedAssets) {
        final ParameterDTO dto = new ParameterDTO();
        dto.setName(name);
        dto.setReferencedAssets(referencedAssets.stream().map(assetId -> new AssetReferenceDTO(assetId)).toList());
        dto.setValue(null);
        dto.setProvided(false);

        final ParameterEntity entity = new ParameterEntity();
        entity.setParameter(dto);
        return entity;
    }

    public ParameterContextEntity createParameterContextEntity(final String name, final String description, final Set<ParameterEntity> parameters) {
        return createParameterContextEntity(name, description, parameters, Collections.emptyList(), null);
    }

    public ParameterContextEntity createParameterContextEntity(final String name, final String description, final Set<ParameterEntity> parameters,
                                                               final List<String> inheritedParameterContextIds,
                                                               final ParameterProviderConfiguration parameterProviderConfiguration) {
        final ParameterContextDTO contextDto = new ParameterContextDTO();
        contextDto.setName(name);
        contextDto.setDescription(description);
        contextDto.setParameters(parameters);

        final List<ParameterContextReferenceEntity> inheritedRefs = new ArrayList<>();
        if (inheritedParameterContextIds != null) {
            inheritedRefs.addAll(inheritedParameterContextIds.stream().map(id -> {
                ParameterContextReferenceEntity ref = new ParameterContextReferenceEntity();
                ref.setId(id);
                ParameterContextReferenceDTO refDto = new ParameterContextReferenceDTO();
                refDto.setId(id);
                ref.setComponent(refDto);
                return ref;
            }).collect(Collectors.toList()));
        }
        contextDto.setInheritedParameterContexts(inheritedRefs);
        contextDto.setParameterProviderConfiguration(createParamProviderConfigEntity(parameterProviderConfiguration));

        final ParameterContextEntity entity = new ParameterContextEntity();
        entity.setComponent(contextDto);
        entity.setRevision(createNewRevision());

        return entity;
    }

    public ParameterProviderConfigurationEntity createParamProviderConfigEntity(final ParameterProviderConfiguration parameterProviderConfiguration) {
        if (parameterProviderConfiguration == null) {
            return null;
        }

        final ParameterProviderConfigurationEntity entity = new ParameterProviderConfigurationEntity();
        entity.setId(parameterProviderConfiguration.getParameterProviderId());
        entity.setComponent(new ParameterProviderConfigurationDTO());
        entity.getComponent().setParameterProviderId(parameterProviderConfiguration.getParameterProviderId());
        entity.getComponent().setParameterGroupName(parameterProviderConfiguration.getParameterGroupName());
        entity.getComponent().setSynchronized(parameterProviderConfiguration.isSynchronized());
        return entity;
    }

    public RevisionDTO createNewRevision() {
        final RevisionDTO revisionDto = new RevisionDTO();
        revisionDto.setClientId(getClass().getName());
        revisionDto.setVersion(0L);
        return revisionDto;
    }

    private ParameterContextReferenceEntity createReferenceEntity(final String id) {
        return createReferenceEntity(id, null);
    }

    private ParameterContextReferenceEntity createReferenceEntity(final String id, final String name) {
        final ParameterContextReferenceDTO referenceDto = new ParameterContextReferenceDTO();
        referenceDto.setId(id);
        referenceDto.setName(name);

        final ParameterContextReferenceEntity referenceEntity = new ParameterContextReferenceEntity();
        referenceEntity.setId(id);
        referenceEntity.setComponent(referenceDto);

        return referenceEntity;
    }

    public ProcessGroupEntity setParameterContext(final String groupId, final ParameterContextEntity parameterContext) throws NiFiClientException, IOException {
        final ProcessGroupEntity processGroup = nifiClient.getProcessGroupClient().getProcessGroup(groupId);
        processGroup.getComponent().setParameterContext(createReferenceEntity(parameterContext.getId()));
        return nifiClient.getProcessGroupClient().updateProcessGroup(processGroup);
    }

    public ParameterContextEntity createParameterContext(final String contextName, final String parameterName, final String parameterValue, final boolean sensitive)
            throws NiFiClientException, IOException {

        final Set<ParameterEntity> parameterEntities = new HashSet<>();
        parameterEntities.add(createParameterEntity(parameterName, null, sensitive, parameterValue));

        final ParameterContextEntity contextEntity = createParameterContextEntity(contextName, null, parameterEntities);
        final ParameterContextEntity createdContextEntity = nifiClient.getParamContextClient().createParamContext(contextEntity);
        return createdContextEntity;
    }

    public ParameterContextEntity createParameterContext(final String contextName, final Map<String, String> parameters) throws NiFiClientException, IOException {
        return this.createParameterContext(contextName, parameters, Collections.emptyList(), null);
    }

    public ParameterContextEntity createParameterContext(final String contextName, final Map<String, String> parameters, final List<String> inheritedParameterContextIds,
                                                         final ParameterProviderConfiguration parameterProviderConfiguration) throws NiFiClientException, IOException {
        final Set<ParameterEntity> parameterEntities = new HashSet<>();
        parameters.forEach((paramName, paramValue) -> parameterEntities.add(createParameterEntity(paramName, null, false, paramValue)));

        final ParameterContextEntity contextEntity = createParameterContextEntity(contextName, null, parameterEntities, inheritedParameterContextIds,
                parameterProviderConfiguration);

        final ParameterContextEntity createdContextEntity = nifiClient.getParamContextClient().createParamContext(contextEntity);
        logger.info("Created Parameter Context [id={}, name={}] for Test [{}]", createdContextEntity.getId(), contextName, testName);

        return createdContextEntity;
    }

    public ParameterContextUpdateRequestEntity updateParameterContext(final ParameterContextEntity existingEntity, final String paramName, final String paramValue)
        throws NiFiClientException, IOException {
        return updateParameterContext(existingEntity, Collections.singletonMap(paramName, paramValue));
    }

    public ParameterContextUpdateRequestEntity updateParameterContext(final ParameterContextEntity existingEntity, final Map<String, String> parameters) throws NiFiClientException, IOException {
        final ParameterContextDTO component = existingEntity.getComponent();
        final List<String> inheritedParameterContextIds = component.getInheritedParameterContexts() == null ? null :
                component.getInheritedParameterContexts().stream().map(ParameterContextReferenceEntity::getId).collect(Collectors.toList());
        return this.updateParameterContext(existingEntity, parameters, inheritedParameterContextIds);
    }

    public ParameterContextUpdateRequestEntity updateParameterContext(final ParameterContextEntity existingEntity, final Map<String, String> parameters,
                                                                      final List<String> inheritedParameterContextIds) throws NiFiClientException, IOException {
        final Set<ParameterEntity> parameterEntities = new HashSet<>();
        parameters.forEach((paramName, paramValue) -> parameterEntities.add(createParameterEntity(paramName, null, false, paramValue)));
        existingEntity.getComponent().setParameters(parameterEntities);

        final ParameterContextEntity entityUpdate = createParameterContextEntity(existingEntity.getComponent().getName(), existingEntity.getComponent().getDescription(),
                parameterEntities, inheritedParameterContextIds, null);
        entityUpdate.setId(existingEntity.getId());
        entityUpdate.setRevision(existingEntity.getRevision());
        entityUpdate.getComponent().setId(existingEntity.getComponent().getId());

        return nifiClient.getParamContextClient().updateParamContext(entityUpdate);
    }

    public ParameterContextUpdateRequestEntity updateParameterAssetReferences(final ParameterContextEntity existingEntity, final Map<String, List<String>> assetReferences)
                throws NiFiClientException, IOException {

        final ParameterContextDTO component = existingEntity.getComponent();
        final List<String> inheritedParameterContextIds = component.getInheritedParameterContexts() == null ? null :
            component.getInheritedParameterContexts().stream().map(ParameterContextReferenceEntity::getId).collect(Collectors.toList());

        final Set<ParameterEntity> parameterEntities = new HashSet<>();
        assetReferences.forEach((paramName, references) -> parameterEntities.add(createAssetReferenceParameterEntity(paramName, references)));
        existingEntity.getComponent().setParameters(parameterEntities);

        final ParameterContextEntity entityUpdate = createParameterContextEntity(existingEntity.getComponent().getName(), existingEntity.getComponent().getDescription(),
            parameterEntities, inheritedParameterContextIds, null);
        entityUpdate.setId(existingEntity.getId());
        entityUpdate.setRevision(existingEntity.getRevision());
        entityUpdate.getComponent().setId(existingEntity.getComponent().getId());

        return nifiClient.getParamContextClient().updateParamContext(entityUpdate);
    }

    public void waitForParameterContextRequestToComplete(final String contextId, final String requestId) throws NiFiClientException, IOException, InterruptedException {
        while (true) {
            final ParameterContextUpdateRequestEntity entity = nifiClient.getParamContextClient().getParamContextUpdateRequest(contextId, requestId);
            if (entity.getRequest().isComplete()) {
                if (entity.getRequest().getFailureReason() == null) {
                    return;
                }

                throw new RuntimeException("Parameter Context Update failed: " + entity.getRequest().getFailureReason());
            }

            Thread.sleep(100L);
        }
    }

    public ControllerServiceEntity updateControllerServiceProperties(final ControllerServiceEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final ControllerServiceDTO serviceDto = new ControllerServiceDTO();
        serviceDto.setProperties(properties);
        serviceDto.setId(currentEntity.getId());

        final ControllerServiceEntity updatedEntity = new ControllerServiceEntity();
        updatedEntity.setRevision(currentEntity.getRevision());
        updatedEntity.setComponent(serviceDto);
        updatedEntity.setId(currentEntity.getId());
        updatedEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getControllerServicesClient().updateControllerService(updatedEntity);
    }

    public ProcessorEntity updateProcessorExecutionNode(final ProcessorEntity currentEntity, final ExecutionNode executionNode) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setExecutionNode(executionNode.name());
        return updateProcessorConfig(currentEntity, config);
    }

    public ProcessorEntity updateProcessorProperties(final ProcessorEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setProperties(properties);
        return updateProcessorConfig(currentEntity, config);
    }

    public ProcessorEntity updateProcessorRunDuration(final ProcessorEntity currentEntity, final int runDuration) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRunDurationMillis((long) runDuration);
        return updateProcessorConfig(currentEntity, config);
    }

    public ProcessorEntity updateProcessorSchedulingPeriod(final ProcessorEntity currentEntity, final String schedulingPeriod) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setSchedulingPeriod(schedulingPeriod);
        return updateProcessorConfig(currentEntity, config);
    }

    public ProcessorEntity updateProcessorSchedulingStrategy(final ProcessorEntity currentEntity, final String schedulingStrategy) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setSchedulingStrategy(schedulingStrategy);
        return updateProcessorConfig(currentEntity, config);
    }

    public ProcessorEntity updateProcessorConfig(final ProcessorEntity currentEntity, final ProcessorConfigDTO config) throws NiFiClientException, IOException {
        final ProcessorDTO processorDto = new ProcessorDTO();
        processorDto.setConfig(config);
        processorDto.setId(currentEntity.getId());

        final ProcessorEntity updatedEntity = new ProcessorEntity();
        updatedEntity.setRevision(currentEntity.getRevision());
        updatedEntity.setComponent(processorDto);
        updatedEntity.setId(currentEntity.getId());
        updatedEntity.setDisconnectedNodeAcknowledged(true);

        return getProcessorClient().updateProcessor(updatedEntity);
    }

    public ProcessorEntity setRetriedRelationships(final ProcessorEntity currentEntity, final String retriedRelationship) throws NiFiClientException, IOException {
        return setRetriedRelationships(currentEntity, Collections.singleton(retriedRelationship));
    }

    public ProcessorEntity setRetriedRelationships(final ProcessorEntity currentEntity, final Set<String> retriedRelationships) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setRetriedRelationships(retriedRelationships);
        return updateProcessorConfig(currentEntity, config);
    }

    public ProcessorEntity setAutoTerminatedRelationships(final ProcessorEntity currentEntity, final String autoTerminatedRelationship) throws NiFiClientException, IOException {
        return setAutoTerminatedRelationships(currentEntity, Collections.singleton(autoTerminatedRelationship));
    }

    public ProcessorEntity setAutoTerminatedRelationships(final ProcessorEntity currentEntity, final Set<String> autoTerminatedRelationships) throws NiFiClientException, IOException {
        final ProcessorConfigDTO config = new ProcessorConfigDTO();
        config.setAutoTerminatedRelationships(autoTerminatedRelationships);
        return updateProcessorConfig(currentEntity, config);
    }

    public void waitForOutputPortValid(final String portId) throws NiFiClientException, IOException, InterruptedException {
        waitForOutputPortValidationStatus(portId, true);
    }

    public void waitForOutputPortInvalid(final String portId) throws NiFiClientException, IOException, InterruptedException {
        waitForOutputPortValidationStatus(portId, false);
    }

    public void waitForOutputPortValidationStatus(final String portId, final boolean expectValid) throws InterruptedException, NiFiClientException, IOException {
        while (true) {
            final PortEntity entity = nifiClient.getOutputPortClient().getOutputPort(portId);
            final Collection<String> validationErrors = entity.getComponent().getValidationErrors();
            if (expectValid == (validationErrors == null || validationErrors.isEmpty())) {
                return;
            }

            if (expectValid) {
                logger.info("Output Port with ID {} is currently invalid due to: {}", portId, entity.getComponent().getValidationErrors());
            }

            Thread.sleep(100L);
        }
    }

    public void waitForValidProcessor(String id) throws InterruptedException, IOException, NiFiClientException {
        waitForValidationStatus(id, ProcessorDTO.VALID);
    }

    public void waitForInvalidProcessor(String id) throws NiFiClientException, IOException, InterruptedException {
        waitForValidationStatus(id, ProcessorDTO.INVALID);
    }

    public void waitForValidationStatus(final String processorId, final String expectedStatus) throws NiFiClientException, IOException, InterruptedException {
        while (true) {
            final ProcessorEntity entity = getProcessorClient().getProcessor(processorId);
            final String validationStatus = entity.getComponent().getValidationStatus();
            if (expectedStatus.equalsIgnoreCase(validationStatus)) {
                return;
            }

            if ("Invalid".equalsIgnoreCase(validationStatus)) {
                logger.info("Processor with ID {} is currently invalid due to: {}", processorId, entity.getComponent().getValidationErrors());
            }

            Thread.sleep(100L);
        }
    }

    public void waitForValidationCompleted(final ProcessorEntity processorEntity) throws NiFiClientException, IOException, InterruptedException {
        String validationStatus;
        do {
            final ProcessorEntity currentEntity = getProcessorClient().getProcessor(processorEntity.getId());
            validationStatus = currentEntity.getComponent().getValidationStatus();

            if (validationStatus.equals(ProcessorDTO.VALIDATING)) {
                logger.debug("Waiting for Processor {} to finish validating...", processorEntity.getId());
                Thread.sleep(100L);
            }
        } while (Objects.equals(validationStatus, ProcessorDTO.VALIDATING));
    }

    public void waitForRunningProcessor(final String processorId) throws InterruptedException, IOException, NiFiClientException {
        waitForProcessorState(processorId, "RUNNING");
    }

    public void waitForStoppedProcessor(final String processorId) throws InterruptedException, IOException, NiFiClientException {
        waitForProcessorState(processorId, "STOPPED");
    }

    public void waitForProcessorState(final String processorId, final String expectedState) throws NiFiClientException, IOException, InterruptedException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        logger.info("Waiting for Processor {} to reach state {}", processorId, expectedState);

        while (System.currentTimeMillis() < maxTimestamp) {
            final ProcessorEntity entity = getProcessorClient().getProcessor(processorId);
            final String state = entity.getComponent().getState();

            // We've reached the desired state if the state equal the expected state, OR if we expect stopped and the state is disabled (because disabled implies stopped)
            final boolean desiredStateReached = expectedState.equals(state) || ("STOPPED".equalsIgnoreCase(expectedState) && "DISABLED".equalsIgnoreCase(state));

            if (!desiredStateReached) {
                Thread.sleep(10L);
                continue;
            }

            if ("RUNNING".equals(expectedState)) {
                return;
            }

            final ProcessorStatusSnapshotDTO snapshotDto = entity.getStatus().getAggregateSnapshot();
            if (snapshotDto.getActiveThreadCount() == 0 && snapshotDto.getTerminatedThreadCount() == 0) {
                logger.info("Processor {} has reached desired state of {}", processorId, expectedState);
                return;
            }

            Thread.sleep(10L);
        }
    }

    public ReportingTaskEntity waitForReportingTaskState(final String reportingTaskId, final String expectedState) throws NiFiClientException, IOException, InterruptedException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);
        logger.info("Waiting for Reporting Task {} to reach desired state of {}", reportingTaskId, expectedState);

        while (System.currentTimeMillis() < maxTimestamp) {
            final ReportingTaskEntity entity = nifiClient.getReportingTasksClient().getReportingTask(reportingTaskId);
            final String state = entity.getComponent().getState();

            // We've reached the desired state if the state equal the expected state, OR if we expect stopped and the state is disabled (because disabled implies stopped)
            final boolean desiredStateReached = expectedState.equals(state) || ("STOPPED".equalsIgnoreCase(expectedState) && "DISABLED".equalsIgnoreCase(state));

            if (!desiredStateReached) {
                Thread.sleep(10L);
                continue;
            }

            if ("RUNNING".equals(expectedState)) {
                logger.info("Reporting task {} is now running", reportingTaskId);
                return entity;
            }

            if (entity.getStatus().getActiveThreadCount() == 0) {
                logger.info("Reporting task {} is now stopped", reportingTaskId);
                return entity;
            }

            Thread.sleep(10L);
        }

        throw new IOException("Timed out waiting for Reporting Task " + reportingTaskId + " to reach state of " + expectedState);
    }

    public void waitForReportingTaskValid(final String reportingTaskId) throws NiFiClientException, IOException {
        waitForReportingTaskValidationStatus(reportingTaskId, "Valid");
    }

    public ControllerServiceEntity updateControllerService(final ControllerServiceEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
        dto.setProperties(properties);
        dto.setId(currentEntity.getId());

        final ControllerServiceEntity entity = new ControllerServiceEntity();
        entity.setId(currentEntity.getId());
        entity.setComponent(dto);
        entity.setRevision(currentEntity.getRevision());

        return nifiClient.getControllerServicesClient().updateControllerService(entity);
    }

    public ActivateControllerServicesEntity enableControllerServices(final String groupId, final boolean waitForEnabled) throws NiFiClientException, IOException {
        final ActivateControllerServicesEntity activateControllerServicesEntity = new ActivateControllerServicesEntity();
        activateControllerServicesEntity.setId(groupId);
        activateControllerServicesEntity.setState(ActivateControllerServicesEntity.STATE_ENABLED);
        activateControllerServicesEntity.setDisconnectedNodeAcknowledged(true);

        final ActivateControllerServicesEntity activateControllerServices = nifiClient.getFlowClient().activateControllerServices(activateControllerServicesEntity);
        if (waitForEnabled) {
            waitForControllerSerivcesEnabled(groupId);
        }

        return activateControllerServices;
    }

    public ControllerServiceEntity enableControllerService(final ControllerServiceEntity entity) throws NiFiClientException, IOException {
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("ENABLED");
        runStatusEntity.setRevision(entity.getRevision());
        runStatusEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getControllerServicesClient().activateControllerService(entity.getId(), runStatusEntity);
    }

    public ControllerServiceEntity disableControllerService(final ControllerServiceEntity entity) throws NiFiClientException, IOException {
        final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
        runStatusEntity.setState("DISABLED");
        runStatusEntity.setRevision(entity.getRevision());
        runStatusEntity.setDisconnectedNodeAcknowledged(true);

        return nifiClient.getControllerServicesClient().activateControllerService(entity.getId(), runStatusEntity);
    }

    public Map<String, Long> waitForCounter(final String context, final String counterName, final long expectedValue) throws NiFiClientException, IOException, InterruptedException {
        Map<String, Long> counterValues = getCountersAsMap(context);
        while (true) {
            final Long counterValue = counterValues.get(counterName);
            if (counterValue != null && counterValue.longValue() == expectedValue) {
                return counterValues;
            }

            Thread.sleep(10L);
            counterValues = getCountersAsMap(context);
        }
    }


    public Map<String, Long> getCountersAsMap(final String processorId) throws NiFiClientException, IOException {
        final CountersEntity firstCountersEntity = nifiClient.getCountersClient().getCounters();
        final CountersSnapshotDTO firstCounters = firstCountersEntity.getCounters().getAggregateSnapshot();
        final Map<String, Long> counterValues = firstCounters.getCounters().stream()
            .filter(dto -> dto.getContext().contains(processorId))
            .collect(Collectors.toMap(CounterDTO::getName, CounterDTO::getValueCount));

        return counterValues;
    }

    public ScheduleComponentsEntity startProcessGroupComponents(final String groupId) throws NiFiClientException, IOException, InterruptedException {
        waitForAllProcessorValidationToComplete(groupId);

        final ScheduleComponentsEntity scheduleComponentsEntity = new ScheduleComponentsEntity();
        scheduleComponentsEntity.setId(groupId);
        scheduleComponentsEntity.setState("RUNNING");
        scheduleComponentsEntity.setDisconnectedNodeAcknowledged(true);
        final ScheduleComponentsEntity scheduleEntity = nifiClient.getFlowClient().scheduleProcessGroupComponents(groupId, scheduleComponentsEntity);

        return scheduleEntity;
    }

    private void waitForAllProcessorValidationToComplete(final String groupId) throws NiFiClientException, IOException, InterruptedException {
        final Set<ProcessorEntity> processors = findAllProcessors(groupId);

        for (final ProcessorEntity processor : processors) {
            waitForValidationCompleted(processor);
        }
    }

    private Set<ProcessorEntity> findAllProcessors(final String groupId) throws NiFiClientException, IOException {
        final Set<ProcessorEntity> processors = new HashSet<>();
        findAllProcessors(groupId, processors);
        return processors;
    }

    private void findAllProcessors(final String groupId, final Set<ProcessorEntity> allProcessors) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity flowEntity = nifiClient.getFlowClient().getProcessGroup(groupId);
        final FlowDTO flowDto = flowEntity.getProcessGroupFlow().getFlow();
        allProcessors.addAll(flowDto.getProcessors());

        for (final ProcessGroupEntity childGroup : flowDto.getProcessGroups()) {
            findAllProcessors(childGroup.getId(), allProcessors);
        }
    }

    public ScheduleComponentsEntity stopProcessGroupComponents(final String groupId) throws NiFiClientException, IOException {
        final ScheduleComponentsEntity scheduleComponentsEntity = new ScheduleComponentsEntity();
        scheduleComponentsEntity.setId(groupId);
        scheduleComponentsEntity.setState("STOPPED");
        scheduleComponentsEntity.setDisconnectedNodeAcknowledged(true);
        final ScheduleComponentsEntity scheduleEntity = nifiClient.getFlowClient().scheduleProcessGroupComponents(groupId, scheduleComponentsEntity);
        waitForProcessorsStopped(groupId);
        waitForNoRunningComponents(groupId);

        return scheduleEntity;
    }

    private void waitForNoRunningComponents(final String groupId) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        logger.info("Waiting for no more running components for group {}", groupId);

        while (System.currentTimeMillis() < maxTimestamp) {
            final boolean anyRunning = isAnyComponentRunning(groupId);
            if (!anyRunning) {
                logger.info("All Process Groups have finished");
                return;
            }

            try {
                Thread.sleep(10L);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean isAnyComponentRunning(final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupStatusEntity statusEntity = nifiClient.getFlowClient().getProcessGroupStatus(groupId, false);
        final ProcessGroupStatusSnapshotDTO snapshotDto = statusEntity.getProcessGroupStatus().getAggregateSnapshot();
        final Integer activeThreadCount = snapshotDto.getActiveThreadCount();
        if (activeThreadCount != null && activeThreadCount > 0) {
            return true;
        }

        final Integer terminatedThreadCount = snapshotDto.getTerminatedThreadCount();
        if (terminatedThreadCount != null && terminatedThreadCount > 0) {
            return true;
        }

        final ProcessGroupEntity rootGroup = nifiClient.getProcessGroupClient().getProcessGroup(groupId);
        final Integer runningCount = rootGroup.getRunningCount();
        if (runningCount != null && runningCount > 0) {
            return true;
        }

        return false;
    }

    private void waitForProcessorsStopped(final String groupId) throws IOException, NiFiClientException {
        logger.info("Waiting for processors in group {} to stop", groupId);

        final ProcessGroupFlowEntity rootGroup = nifiClient.getFlowClient().getProcessGroup(groupId);
        final FlowDTO rootFlowDTO = rootGroup.getProcessGroupFlow().getFlow();
        for (final ProcessorEntity processor : rootFlowDTO.getProcessors()) {
            try {
                waitForStoppedProcessor(processor.getId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NiFiClientException("Interrupted while waiting for Processor with ID " + processor.getId() + " to stop");
            }
        }

        for (final ProcessGroupEntity group : rootFlowDTO.getProcessGroups()) {
            waitForProcessorsStopped(group.getComponent());
        }

        logger.info("All processors in group {} have stopped", groupId);
    }

    private void waitForProcessorsStopped(final ProcessGroupDTO group) throws IOException, NiFiClientException {
        final FlowSnippetDTO groupContents = group.getContents();
        if (groupContents == null) {
            return;
        }

        for (final ProcessorDTO processor : groupContents.getProcessors()) {
            try {
                waitForStoppedProcessor(processor.getId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NiFiClientException("Interrupted while waiting for Processor with ID " + processor.getId() + " to stop");
            }
        }

        for (final ProcessGroupDTO child : groupContents.getProcessGroups()) {
            waitForProcessorsStopped(child);
        }
    }

    public void stopTransmitting(final String processGroupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity rootGroup = nifiClient.getFlowClient().getProcessGroup(processGroupId);
        final FlowDTO flowDto = rootGroup.getProcessGroupFlow().getFlow();

        for (final RemoteProcessGroupEntity rpg : flowDto.getRemoteProcessGroups()) {
            nifiClient.getRemoteProcessGroupClient().stopTransmitting(rpg);
        }

        for (final ProcessGroupEntity childGroup : flowDto.getProcessGroups()) {
            stopTransmitting(childGroup.getId());
        }
    }

    public ActivateControllerServicesEntity disableControllerServices(final String groupId, final boolean recurse) throws NiFiClientException, IOException {
        logger.info("Starting disableControllerServices for group {}, recurse={}", groupId, recurse);

        final ActivateControllerServicesEntity activateControllerServicesEntity = new ActivateControllerServicesEntity();
        activateControllerServicesEntity.setId(groupId);
        activateControllerServicesEntity.setState(ActivateControllerServicesEntity.STATE_DISABLED);
        activateControllerServicesEntity.setDisconnectedNodeAcknowledged(true);

        final ActivateControllerServicesEntity activateControllerServices = nifiClient.getFlowClient().activateControllerServices(activateControllerServicesEntity);
        waitForControllerSerivcesDisabled(groupId);

        if (recurse) {
            final ProcessGroupFlowEntity groupEntity = nifiClient.getFlowClient().getProcessGroup(groupId);
            final FlowDTO flowDto = groupEntity.getProcessGroupFlow().getFlow();
            for (final ProcessGroupEntity childGroupEntity : flowDto.getProcessGroups()) {
                final String childGroupId = childGroupEntity.getId();
                disableControllerServices(childGroupId, recurse);
            }
        }

        logger.info("Finished disableControllerServices for group {}", groupId);
        return activateControllerServices;
    }

    public void disableControllerLevelServices() throws NiFiClientException, IOException {
        final ControllerServicesEntity services = nifiClient.getFlowClient().getControllerServices();

        for (final ControllerServiceEntity service : services.getControllerServices()) {
            final ControllerServiceRunStatusEntity runStatusEntity = new ControllerServiceRunStatusEntity();
            runStatusEntity.setDisconnectedNodeAcknowledged(true);
            runStatusEntity.setRevision(service.getRevision());
            runStatusEntity.setState(ActivateControllerServicesEntity.STATE_DISABLED);
            nifiClient.getControllerServicesClient().activateControllerService(service.getId(), runStatusEntity);
            waitForControllerServiceRunStatus(service.getId(), ActivateControllerServicesEntity.STATE_DISABLED);
        }
    }

    public void deleteControllerLevelServices() throws NiFiClientException, IOException {
        final ControllerServicesEntity services = nifiClient.getFlowClient().getControllerServices();
        for (final ControllerServiceEntity service : services.getControllerServices()) {
            service.setDisconnectedNodeAcknowledged(true);
            nifiClient.getControllerServicesClient().deleteControllerService(service);
        }
    }

    public void waitForControllerServiceRunStatus(final String id, final String requestedRunStatus) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);
        logger.info("Waiting for Controller Service {} to have a Run Status of {}", id, requestedRunStatus);

        while (System.currentTimeMillis() < maxTimestamp) {
            final ControllerServiceEntity serviceEntity = nifiClient.getControllerServicesClient().getControllerService(id);
            final String serviceState = serviceEntity.getComponent().getState();
            if (requestedRunStatus.equals(serviceState)) {
                logger.info("Controller Service [{}] run status [{}] found", id, serviceState);
                break;
            }

            logger.info("Controller Service [{}] run status [{}] not matched [{}]: sleeping before retrying", id, serviceState, requestedRunStatus);

            try {
                Thread.sleep(500L);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void waitForControllerSerivcesDisabled(final String groupId, final String... serviceIdsOfInterest) throws NiFiClientException, IOException {
        waitForControllerServiceState(groupId, "DISABLED", Arrays.asList(serviceIdsOfInterest));
    }

    public void waitForControllerSerivcesEnabled(final String groupId, final String... serviceIdsOfInterest) throws NiFiClientException, IOException {
        waitForControllerServiceState(groupId, "ENABLED", Arrays.asList(serviceIdsOfInterest));
    }

    public void waitForControllerSerivcesEnabled(final String groupId, final List<String> serviceIdsOfInterest) throws NiFiClientException, IOException {
        waitForControllerServiceState(groupId, "ENABLED", serviceIdsOfInterest);
    }

    public void waitForControllerServiceState(final String groupId, final String desiredState, final Collection<String> serviceIdsOfInterest) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);

        while (System.currentTimeMillis() < maxTimestamp) {
            final List<ControllerServiceEntity> nonDisabledServices = getControllerServicesNotInState(groupId, desiredState, serviceIdsOfInterest);
            if (nonDisabledServices.isEmpty()) {
                logger.info("Process Group [{}] Controller Services have desired state [{}]", groupId, desiredState);
                return;
            }

            final ControllerServiceEntity entity = nonDisabledServices.get(0);
            logger.info("Controller Service ID [{}] Type [{}] State [{}] waiting for State [{}]: sleeping for 500 ms before retrying", entity.getId(),
                    entity.getComponent().getType(), entity.getComponent().getState(), desiredState);

            try {
                Thread.sleep(500L);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void waitForControllerServiceValidationStatus(final String controllerServiceId, final String validationStatus) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);

        while (System.currentTimeMillis() < maxTimestamp) {
            final ControllerServiceEntity controllerServiceEntity = nifiClient.getControllerServicesClient().getControllerService(controllerServiceId);
            final String currentValidationStatus = controllerServiceEntity.getComponent().getValidationStatus();
            if (validationStatus.equals(currentValidationStatus)) {
                logger.info("Controller Service ID [{}] Type [{}] Validation Status [{}] matched", controllerServiceId,
                        controllerServiceEntity.getComponent().getType(), validationStatus);
                return;
            }

            logger.info("Controller Service ID [{}] Type [{}] Validation Status [{}] waiting for [{}]: sleeping for 500 ms before retrying", controllerServiceId,
                    controllerServiceEntity.getComponent().getType(), currentValidationStatus, validationStatus);

            try {
                Thread.sleep(500L);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void waitForReportingTaskValidationStatus(final String reportingTaskId, final String validationStatus) throws NiFiClientException, IOException {
        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);

        while (System.currentTimeMillis() < maxTimestamp) {
            final ReportingTaskEntity reportingTaskEntity = nifiClient.getReportingTasksClient().getReportingTask(reportingTaskId);
            final String currentValidationStatus = reportingTaskEntity.getStatus().getValidationStatus();
            if (validationStatus.equalsIgnoreCase(currentValidationStatus)) {
                logger.info("Reporting Task ID [{}] Type [{}] Validation Status [{}] matched", reportingTaskId,
                        reportingTaskEntity.getComponent().getType(), validationStatus);
                return;
            }

            logger.info("Reporting Task ID [{}] Type [{}] Validation Status [{}] waiting for [{}]: sleeping for 500 ms before retrying", reportingTaskEntity,
                    reportingTaskEntity.getComponent().getType(), currentValidationStatus, validationStatus);

            try {
                Thread.sleep(500L);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<ControllerServiceEntity> getControllerServicesNotInState(final String groupId, final String desiredState, final Collection<String> serviceIds) throws NiFiClientException, IOException {
        final ControllerServicesEntity servicesEntity = nifiClient.getFlowClient().getControllerServices(groupId);

        return servicesEntity.getControllerServices().stream()
            .filter(svc -> serviceIds == null || serviceIds.isEmpty() || serviceIds.contains(svc.getId()))
            .filter(svc -> !desiredState.equalsIgnoreCase(svc.getComponent().getState()))
            .collect(Collectors.toList());
    }

    public void deleteAll(final String groupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity rootFlowEntity = nifiClient.getFlowClient().getProcessGroup(groupId);
        final ProcessGroupFlowDTO groupFlowDto = rootFlowEntity.getProcessGroupFlow();
        final FlowDTO flowDto = groupFlowDto.getFlow();

        // Delete all connections
        for (final ConnectionEntity connectionEntity : flowDto.getConnections()) {
            final ConnectionStatusEntity status = nifiClient.getFlowClient().getConnectionStatus(connectionEntity.getId(), false);
            if (status.getConnectionStatus().getAggregateSnapshot().getFlowFilesQueued() > 0) {
                emptyQueue(connectionEntity.getId());
            }

            connectionEntity.setDisconnectedNodeAcknowledged(true);
            getConnectionClient().deleteConnection(connectionEntity);
        }

        // Delete all processors
        for (final ProcessorEntity processorEntity : flowDto.getProcessors()) {
            processorEntity.setDisconnectedNodeAcknowledged(true);
            getProcessorClient().deleteProcessor(processorEntity);
            logger.info("Deleted processor [{}]", processorEntity.getId());
        }

        // Delete all Controller Services
        final ControllerServicesEntity servicesEntity = nifiClient.getFlowClient().getControllerServices(groupId);
        for (final ControllerServiceEntity serviceEntity : servicesEntity.getControllerServices()) {
            serviceEntity.setDisconnectedNodeAcknowledged(true);
            nifiClient.getControllerServicesClient().deleteControllerService(serviceEntity);
        }

        // Delete all RPG's
        for (final RemoteProcessGroupEntity rpgEntity : flowDto.getRemoteProcessGroups()) {
            rpgEntity.setDisconnectedNodeAcknowledged(true);
            nifiClient.getRemoteProcessGroupClient().deleteRemoteProcessGroup(rpgEntity);
        }

        // Delete all Input Ports
        for (final PortEntity port : flowDto.getInputPorts()) {
            port.setDisconnectedNodeAcknowledged(true);
            nifiClient.getInputPortClient().deleteInputPort(port);
        }

        // Delete all Output Ports
        for (final PortEntity port : flowDto.getOutputPorts()) {
            port.setDisconnectedNodeAcknowledged(true);
            nifiClient.getOutputPortClient().deleteOutputPort(port);
        }

        // Recurse
        for (final ProcessGroupEntity childGroupEntity : flowDto.getProcessGroups()) {
            childGroupEntity.setDisconnectedNodeAcknowledged(true);
            deleteAll(childGroupEntity.getId());
            nifiClient.getProcessGroupClient().deleteProcessGroup(childGroupEntity);
        }
    }

    public ConnectionEntity createConnection(final PortEntity source, final PortEntity destination) throws NiFiClientException, IOException {
        if ("OUTPUT_PORT".equals(source.getPortType()) && "INPUT_PORT".equals(destination.getPortType())) {
            throw new IllegalArgumentException("In order to connect an Output Port to an Input Port, the Process Group ID must be specified");
        }

        return createConnection(source, destination, Collections.singleton(AbstractPort.PORT_RELATIONSHIP.getName()));
    }

    public ConnectionEntity createConnection(final PortEntity source, final PortEntity destination, final String connectionGroupId) throws NiFiClientException, IOException {
        final ConnectableDTO sourceDto = createConnectableDTO(source);
        final ConnectableDTO destinationDto = createConnectableDTO(destination);
        return createConnection(sourceDto, destinationDto, Collections.singleton(AbstractPort.PORT_RELATIONSHIP.getName()), connectionGroupId);
    }

    public ConnectionEntity createConnection(final PortEntity source, final ProcessorEntity destination) throws NiFiClientException, IOException {
        return createConnection(source, destination, Collections.singleton(AbstractPort.PORT_RELATIONSHIP.getName()));
    }

    public ConnectionEntity createConnection(final PortEntity source, final ProcessorEntity destination, final String groupId) throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), Collections.singleton(AbstractPort.PORT_RELATIONSHIP.getName()), groupId);
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final PortEntity destination, final String relationship) throws NiFiClientException, IOException {
        return createConnection(source, destination, Collections.singleton(relationship));
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final ProcessorEntity destination, final String relationship) throws NiFiClientException, IOException {
        return createConnection(source, destination, Collections.singleton(relationship));
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final ProcessorEntity destination, final String relationship, final String groupId) throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), Collections.singleton(relationship), groupId);
    }

    public ConnectionEntity createConnection(final ConnectableDTO source, final ConnectableDTO destination, final String relationship) throws NiFiClientException, IOException {
        return createConnection(source, destination, Collections.singleton(relationship));
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final ProcessorEntity destination, final Set<String> relationships) throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), relationships);
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final PortEntity destination, final Set<String> relationships) throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), relationships);
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final PortEntity destination, final String relationship, final String groupId)
        throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), Collections.singleton(relationship), groupId);
    }

    public ConnectionEntity createConnection(final ProcessorEntity source, final PortEntity destination, final Set<String> relationships, final String groupId)
                throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), relationships, groupId);
    }

    public ConnectionEntity createConnection(final PortEntity source, final PortEntity destination, final Set<String> relationships) throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), relationships);
    }

    public ConnectionEntity createConnection(final PortEntity source, final ProcessorEntity destination, final Set<String> relationships) throws NiFiClientException, IOException {
        return createConnection(createConnectableDTO(source), createConnectableDTO(destination), relationships);
    }

    public ConnectionEntity createConnection(final ConnectableDTO source, final ConnectableDTO destination, final Set<String> relationships) throws NiFiClientException, IOException {
        final String connectionGroupId = "OUTPUT_PORT".equals(source.getType()) ? destination.getGroupId() : source.getGroupId();
        return createConnection(source, destination, relationships, connectionGroupId);
    }

    public ConnectionEntity createConnection(final ConnectableDTO source, final ConnectableDTO destination, final Set<String> relationships, final String connectionGroupId)
                throws NiFiClientException, IOException {
        final ConnectionDTO connectionDto = new ConnectionDTO();
        connectionDto.setSelectedRelationships(relationships);
        connectionDto.setDestination(destination);
        connectionDto.setSource(source);
        connectionDto.setParentGroupId(connectionGroupId);

        final ConnectionEntity connectionEntity = new ConnectionEntity();
        connectionEntity.setComponent(connectionDto);

        connectionEntity.setDestinationGroupId(destination.getGroupId());
        connectionEntity.setDestinationId(destination.getId());
        connectionEntity.setDestinationType(destination.getType());

        connectionEntity.setSourceGroupId(source.getGroupId());
        connectionEntity.setSourceId(source.getId());
        connectionEntity.setSourceType(source.getType());

        connectionEntity.setRevision(createNewRevision());
        connectionEntity.setDisconnectedNodeAcknowledged(true);

        final ConnectionEntity connection = getConnectionClient().createConnection(connectionGroupId, connectionEntity);

        final String sourceInfo = String.format("[type=%s, id=%s, name=%s, groupId=%s]", source.getType(), source.getId(), source.getName(), source.getGroupId());
        final String destinationInfo = String.format("[type=%s, id=%s, name=%s, groupId=%s]", destination.getType(), destination.getId(), destination.getName(), destination.getGroupId());

        logger.info("Created Connection [id={}, source={}, destination={}, relationships={}, parentGroupId={}] for Test [{}]",
            connection.getId(), sourceInfo, destinationInfo, relationships, connectionGroupId, testName);

        return connection;
    }

    public ConnectableDTO createConnectableDTO(final ProcessorEntity processor) {
        final ConnectableDTO dto = new ConnectableDTO();
        dto.setGroupId(processor.getComponent().getParentGroupId());
        dto.setId(processor.getId());
        dto.setName(processor.getComponent().getName());
        dto.setRunning("RUNNING".equalsIgnoreCase(processor.getComponent().getState()));
        dto.setType("PROCESSOR");

        return dto;
    }

    public ConnectableDTO createConnectableDTO(final PortEntity port) {
        final ConnectableDTO dto = new ConnectableDTO();
        dto.setGroupId(port.getComponent().getParentGroupId());
        dto.setId(port.getId());
        dto.setName(port.getComponent().getName());
        dto.setRunning("RUNNING".equalsIgnoreCase(port.getComponent().getState()));
        dto.setType(port.getPortType());

        return dto;
    }

    public QueueSize getQueueSize(String connectionId) {
        try {
            final ConnectionStatusEntity statusEntity = nifiClient.getFlowClient().getConnectionStatus(connectionId, false);
            final ConnectionStatusSnapshotDTO snapshotDto = statusEntity.getConnectionStatus().getAggregateSnapshot();
            return new QueueSize(snapshotDto.getFlowFilesQueued(), snapshotDto.getBytesQueued());
        } catch (Exception e) {
            throw new RuntimeException("Failed to obtain queue size for connection with ID " + connectionId, e);
        }
    }

    public ConnectionEntity updateConnectionLoadBalancing(final ConnectionEntity connectionEntity, final LoadBalanceStrategy strategy, final LoadBalanceCompression compression,
                                                          final String loadBalanceAttribute) throws NiFiClientException, IOException {
        final ConnectionDTO connectionDto = new ConnectionDTO();
        connectionDto.setLoadBalancePartitionAttribute(loadBalanceAttribute);
        connectionDto.setLoadBalanceStrategy(strategy.name());
        connectionDto.setLoadBalanceCompression(compression.name());
        connectionDto.setId(connectionEntity.getId());

        final ConnectionEntity updatedEntity = new ConnectionEntity();
        updatedEntity.setComponent(connectionDto);
        updatedEntity.setId(connectionEntity.getId());
        updatedEntity.setRevision(connectionEntity.getRevision());

        return getConnectionClient().updateConnection(updatedEntity);
    }

    public ConnectionEntity updateConnectionBackpressure(final ConnectionEntity connectionEntity, final long flowFileCount, final long bytes) throws NiFiClientException, IOException {
        final ConnectionDTO connectionDto = new ConnectionDTO();
        connectionDto.setBackPressureDataSizeThreshold(bytes + " B");
        connectionDto.setBackPressureObjectThreshold(flowFileCount);
        connectionDto.setId(connectionEntity.getId());

        final ConnectionEntity updatedEntity = new ConnectionEntity();
        updatedEntity.setComponent(connectionDto);
        updatedEntity.setId(connectionEntity.getId());
        updatedEntity.setRevision(connectionEntity.getRevision());

        return getConnectionClient().updateConnection(updatedEntity);
    }

    public ConnectionEntity updateConnectionPrioritizer(final ConnectionEntity connectionEntity, final String prioritizerName) throws NiFiClientException, IOException {
        final ConnectionDTO connectionDto = new ConnectionDTO();
        connectionDto.setPrioritizers(List.of("org.apache.nifi.prioritizer." + prioritizerName));
        connectionDto.setId(connectionEntity.getId());

        final ConnectionEntity updatedEntity = new ConnectionEntity();
        updatedEntity.setComponent(connectionDto);
        updatedEntity.setId(connectionEntity.getId());
        updatedEntity.setRevision(connectionEntity.getRevision());

        return getConnectionClient().updateConnection(updatedEntity);
    }

    public DropRequestEntity emptyQueue(final String connectionId) throws NiFiClientException, IOException {
        final ConnectionClient connectionClient = getConnectionClient();

        final long maxTimestamp = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2L);

        DropRequestEntity requestEntity = null;
        while (System.currentTimeMillis() < maxTimestamp) {
            requestEntity = connectionClient.emptyQueue(connectionId);
            try {
                while (requestEntity.getDropRequest().getPercentCompleted() < 100) {
                    if (System.currentTimeMillis() > maxTimestamp) {
                        throw new IOException("Timed out waiting for queue " + connectionId + " to empty");
                    }

                    try {
                        Thread.sleep(10L);
                    } catch (final InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return null;
                    }

                    requestEntity = connectionClient.getDropRequest(connectionId, requestEntity.getDropRequest().getId());
                }
            } finally {
                requestEntity = connectionClient.deleteDropRequest(connectionId, requestEntity.getDropRequest().getId());
            }

            final Integer count = requestEntity.getDropRequest().getCurrentCount();
            if (count == null || count == 0) {
                break;
            }

            logger.info("Request to Empty FlowFile Queue {} completed but queue still has {} FlowFiles. Will issue another request.", connectionId, count);
        }

        return requestEntity;
    }

    public RemoteProcessGroupEntity createRPG(final String parentGroupId, final int httpPort, final SiteToSiteTransportProtocol transportProtocol) throws NiFiClientException, IOException {
        final RemoteProcessGroupDTO component = new RemoteProcessGroupDTO();
        component.setTargetUri("http://localhost:" + httpPort);
        component.setName(component.getTargetUri());
        component.setTransportProtocol(transportProtocol.name());

        final RemoteProcessGroupEntity entity = new RemoteProcessGroupEntity();
        entity.setComponent(component);
        entity.setRevision(createNewRevision());

        final RemoteProcessGroupEntity rpg = nifiClient.getRemoteProcessGroupClient().createRemoteProcessGroup(parentGroupId, entity);
        logger.info("Created Remote Process Group [id={}, protocol={}, url={}, parentGroupId={}] for Test [{}]", rpg.getId(), transportProtocol, component.getTargetUri(), parentGroupId, testName);
        return rpg;
    }

    public PortEntity createRemoteInputPort(final String parentGroupId, final String portName) throws NiFiClientException, IOException {
        final PortDTO component = new PortDTO();
        component.setName(portName);
        component.setAllowRemoteAccess(true);

        final PortEntity entity = new PortEntity();
        entity.setComponent(component);
        entity.setRevision(createNewRevision());

        return nifiClient.getInputPortClient().createInputPort(parentGroupId, entity);
    }

    public NodeEntity disconnectNode(final String nodeId) throws NiFiClientException, IOException {
        return updateNodeState(nodeId, NodeConnectionState.DISCONNECTING.name());
    }

    public NodeEntity connectNode(final String nodeId) throws NiFiClientException, IOException {
        return updateNodeState(nodeId, NodeConnectionState.CONNECTING.name());
    }

    public NodeEntity offloadNode(final String nodeId) throws NiFiClientException, IOException {
        return updateNodeState(nodeId, NodeConnectionState.OFFLOADING.name());
    }

    private NodeEntity updateNodeState(final String nodeId, final String state) throws NiFiClientException, IOException {
        final NodeDTO nodeDto = new NodeDTO();
        nodeDto.setNodeId(nodeId);
        nodeDto.setStatus(state);

        final NodeEntity nodeEntity = new NodeEntity();
        nodeEntity.setNode(nodeDto);

        return nifiClient.getControllerClient().disconnectNode(nodeId, nodeEntity);
    }

    public ListingRequestEntity performQueueListing(final String connectionId) throws NiFiClientException, IOException {
        try {
            ListingRequestEntity listingRequestEntity = getConnectionClient().listQueue(connectionId);
            while (listingRequestEntity.getListingRequest().getFinished() != Boolean.TRUE) {
                Thread.sleep(10L);

                listingRequestEntity = getConnectionClient().getListingRequest(connectionId, listingRequestEntity.getListingRequest().getId());
            }

            // Delete the listing. Return the previously obtained listing, not the one from the deletion, because the listing request that is returned from deleting the listing does not contain the
            // FlowFile Summaries.
            getConnectionClient().deleteListingRequest(connectionId, listingRequestEntity.getListingRequest().getId());
            return listingRequestEntity;
        } catch (final InterruptedException e) {
            throw new RuntimeException("Failed to obtain connection status");
        }
    }

    public FlowFileEntity getQueueFlowFile(final String connectionId, final int flowFileIndex) throws NiFiClientException, IOException {
        final ListingRequestEntity listing = performQueueListing(connectionId);
        final List<FlowFileSummaryDTO> flowFileSummaries = listing.getListingRequest().getFlowFileSummaries();
        if (flowFileIndex >= flowFileSummaries.size()) {
            throw new IllegalArgumentException("Cannot retrieve FlowFile with index " + flowFileIndex + " because queue only has " + flowFileSummaries.size() + " FlowFiles");
        }

        final FlowFileSummaryDTO flowFileSummary = flowFileSummaries.get(flowFileIndex);
        final String uuid = flowFileSummary.getUuid();
        final String nodeId = flowFileSummary.getClusterNodeId();

        final FlowFileEntity flowFileEntity = getConnectionClient().getFlowFile(connectionId, uuid, nodeId);
        flowFileEntity.getFlowFile().setClusterNodeId(nodeId);
        return flowFileEntity;
    }

    public String getFlowFileContentAsUtf8(final String connectionId, final int flowFileIndex) throws NiFiClientException, IOException {
        final byte[] contents = getFlowFileContentAsByteArray(connectionId, flowFileIndex);
        return new String(contents, StandardCharsets.UTF_8);
    }

    public byte[] getFlowFileContentAsByteArray(final String connectionId, final int flowFileIndex) throws NiFiClientException, IOException {
        try (final InputStream in = getFlowFileContent(connectionId, flowFileIndex);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            StreamUtils.copy(in, baos);
            return baos.toByteArray();
        }
    }

    public InputStream getFlowFileContent(final String connectionId, final int flowFileIndex) throws NiFiClientException, IOException {
        final ListingRequestEntity listing = performQueueListing(connectionId);
        final List<FlowFileSummaryDTO> flowFileSummaries = listing.getListingRequest().getFlowFileSummaries();
        if (flowFileIndex >= flowFileSummaries.size()) {
            throw new IllegalArgumentException("Cannot retrieve FlowFile with index " + flowFileIndex + " because queue only has " + flowFileSummaries.size() + " FlowFiles");
        }

        final FlowFileSummaryDTO flowFileSummary = flowFileSummaries.get(flowFileIndex);
        final String uuid = flowFileSummary.getUuid();
        final String nodeId = flowFileSummary.getClusterNodeId();

        final FlowFileEntity flowFileEntity = getConnectionClient().getFlowFile(connectionId, uuid, nodeId);
        flowFileEntity.getFlowFile().setClusterNodeId(nodeId);

        return getConnectionClient().getFlowFileContent(connectionId, uuid, nodeId);
    }

    public List<ConfigVerificationResultDTO> verifyParameterProviderConfig(final String taskId, final Map<String, String> properties)
            throws InterruptedException, IOException, NiFiClientException {

        final VerifyConfigRequestDTO requestDto = new VerifyConfigRequestDTO();
        requestDto.setComponentId(taskId);
        requestDto.setProperties(properties);

        final VerifyConfigRequestEntity verificationRequest = new VerifyConfigRequestEntity();
        verificationRequest.setRequest(requestDto);

        VerifyConfigRequestEntity results = nifiClient.getParamProviderClient().submitConfigVerificationRequest(verificationRequest);
        while (!results.getRequest().isComplete()) {
            Thread.sleep(50L);
            results = nifiClient.getParamProviderClient().getConfigVerificationRequest(taskId, results.getRequest().getRequestId());
        }

        nifiClient.getParamProviderClient().deleteConfigVerificationRequest(taskId, results.getRequest().getRequestId());

        return results.getRequest().getResults();
    }

    public ProcessGroupEntity createProcessGroup(final String name, final String parentGroupId) throws NiFiClientException, IOException {
        final ProcessGroupDTO component = new ProcessGroupDTO();
        component.setName(name);
        component.setParentGroupId(parentGroupId);

        final ProcessGroupEntity childGroupEntity = new ProcessGroupEntity();
        childGroupEntity.setRevision(createNewRevision());
        childGroupEntity.setComponent(component);

        final ProcessGroupEntity childGroup = nifiClient.getProcessGroupClient().createProcessGroup(parentGroupId, childGroupEntity);
        logger.info("Created Process Group [id={}, name={}, parentGroupId={}] for Test [{}]", childGroup.getId(), name, parentGroupId, testName);
        return childGroup;
    }

    public PortEntity createInputPort(final String name, final String groupId) throws NiFiClientException, IOException {
        final PortDTO component = new PortDTO();
        component.setName(name);
        component.setParentGroupId(groupId);

        final PortEntity inputPortEntity = new PortEntity();
        inputPortEntity.setRevision(createNewRevision());
        inputPortEntity.setComponent(component);

        final PortEntity port = nifiClient.getInputPortClient().createInputPort(groupId, inputPortEntity);
        logger.info("Created Input Port [id={}, name={}, parentGroupId={}] for Test [{}]", port.getId(), name, groupId, testName);
        return port;
    }

    public PortEntity createOutputPort(final String name, final String groupId) throws NiFiClientException, IOException {
        final PortDTO component = new PortDTO();
        component.setName(name);
        component.setParentGroupId(groupId);

        final PortEntity outputPortEntity = new PortEntity();
        outputPortEntity.setRevision(createNewRevision());
        outputPortEntity.setComponent(component);

        final PortEntity port = nifiClient.getOutputPortClient().createOutputPort(groupId, outputPortEntity);
        logger.info("Created Output Port [id={}, name={}, parentGroupId={}] for Test [{}]", port.getId(), name, groupId, testName);
        return port;
    }

    public ProvenanceEntity queryProvenance(final Map<SearchableField, ProvenanceSearchValueDTO> searchTerms, final Long startTime, final Long endTime) throws NiFiClientException, IOException {
        final Map<String, ProvenanceSearchValueDTO> searchTermsAsStrings = searchTerms.entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getSearchableFieldName(), Map.Entry::getValue));

        final ProvenanceRequestDTO requestDto = new ProvenanceRequestDTO();
        requestDto.setSearchTerms(searchTermsAsStrings);
        requestDto.setSummarize(false);
        requestDto.setStartDate(startTime == null ? null : new Date(startTime));
        requestDto.setEndDate(endTime == null ? null : new Date(endTime));
        requestDto.setMaxResults(1000);

        final ProvenanceDTO dto = new ProvenanceDTO();
        dto.setRequest(requestDto);
        dto.setSubmissionTime(new Date());

        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setProvenance(dto);

        ProvenanceEntity responseEntity = nifiClient.getProvenanceClient().submitProvenanceQuery(entity);

        try {
            responseEntity = waitForComplete(responseEntity);
        } catch (final InterruptedException ie) {
            throw new RuntimeException("Interrupted while waiting for Provenance Query to complete");
        }

        nifiClient.getProvenanceClient().deleteProvenanceQuery(responseEntity.getProvenance().getId());
        return responseEntity;
    }

    public ProvenanceEntity waitForComplete(final ProvenanceEntity entity) throws InterruptedException, NiFiClientException, IOException {
        ProvenanceEntity current = entity;
        while (current.getProvenance().isFinished() != Boolean.TRUE) {
            Thread.sleep(100L);
            current = nifiClient.getProvenanceClient().getProvenanceQuery(entity.getProvenance().getId());
        }

        return current;
    }

    public List<ConfigVerificationResultDTO> verifyProcessorConfig(final String processorId, final Map<String, String> properties) throws InterruptedException, IOException, NiFiClientException {
        return verifyProcessorConfig(processorId, properties, Collections.emptyMap());
    }

    public List<ConfigVerificationResultDTO> verifyProcessorConfig(final String processorId, final Map<String, String> properties, final Map<String, String> attributes)
                    throws NiFiClientException, IOException, InterruptedException {
        final VerifyConfigRequestDTO requestDto = new VerifyConfigRequestDTO();
        requestDto.setComponentId(processorId);
        requestDto.setProperties(properties);
        requestDto.setAttributes(attributes);

        final VerifyConfigRequestEntity verificationRequest = new VerifyConfigRequestEntity();
        verificationRequest.setRequest(requestDto);

        VerifyConfigRequestEntity results = nifiClient.getProcessorClient().submitConfigVerificationRequest(verificationRequest);
        while ((!results.getRequest().isComplete()) || (results.getRequest().getResults() == null)) {
            Thread.sleep(50L);
            results = nifiClient.getProcessorClient().getConfigVerificationRequest(processorId, results.getRequest().getRequestId());
        }

        nifiClient.getProcessorClient().deleteConfigVerificationRequest(processorId, results.getRequest().getRequestId());

        return results.getRequest().getResults();
    }

    public List<ConfigVerificationResultDTO> verifyControllerServiceConfig(final String serviceId, final Map<String, String> properties)
                    throws InterruptedException, IOException, NiFiClientException {
        return verifyControllerServiceConfig(serviceId, properties, Collections.emptyMap());
    }

    public List<ConfigVerificationResultDTO> verifyControllerServiceConfig(final String serviceId, final Map<String, String> properties, final Map<String, String> attributes)
                    throws NiFiClientException, IOException, InterruptedException {

        final ControllerServiceDTO serviceDto = new ControllerServiceDTO();
        serviceDto.setProperties(properties);
        serviceDto.setId(serviceId);

        final VerifyConfigRequestDTO requestDto = new VerifyConfigRequestDTO();
        requestDto.setComponentId(serviceId);
        requestDto.setAttributes(attributes);
        requestDto.setProperties(properties);

        final VerifyConfigRequestEntity verificationRequest = new VerifyConfigRequestEntity();
        verificationRequest.setRequest(requestDto);

        VerifyConfigRequestEntity results = nifiClient.getControllerServicesClient().submitConfigVerificationRequest(verificationRequest);
        while ((!results.getRequest().isComplete()) || (results.getRequest().getResults() == null)) {
            Thread.sleep(50L);
            results = nifiClient.getControllerServicesClient().getConfigVerificationRequest(serviceId, results.getRequest().getRequestId());
        }

        nifiClient.getControllerServicesClient().deleteConfigVerificationRequest(serviceId, results.getRequest().getRequestId());

        return results.getRequest().getResults();
    }

    public List<ConfigVerificationResultDTO> verifyReportingTaskConfig(final String taskId, final Map<String, String> properties)
                throws InterruptedException, IOException, NiFiClientException {

        final VerifyConfigRequestDTO requestDto = new VerifyConfigRequestDTO();
        requestDto.setComponentId(taskId);
        requestDto.setProperties(properties);

        final VerifyConfigRequestEntity verificationRequest = new VerifyConfigRequestEntity();
        verificationRequest.setRequest(requestDto);

        VerifyConfigRequestEntity results = nifiClient.getReportingTasksClient().submitConfigVerificationRequest(verificationRequest);
        while ((!results.getRequest().isComplete()) || (results.getRequest().getResults() == null)) {
            Thread.sleep(50L);
            results = nifiClient.getReportingTasksClient().getConfigVerificationRequest(taskId, results.getRequest().getRequestId());
        }

        nifiClient.getReportingTasksClient().deleteConfigVerificationRequest(taskId, results.getRequest().getRequestId());

        return results.getRequest().getResults();
    }

    public List<ConfigVerificationResultDTO> verifyFlowAnalysisRuleConfig(final String ruleId, final Map<String, String> properties)
            throws InterruptedException, IOException, NiFiClientException {

        final VerifyConfigRequestDTO requestDto = new VerifyConfigRequestDTO();
        requestDto.setComponentId(ruleId);
        requestDto.setProperties(properties);

        final VerifyConfigRequestEntity verificationRequest = new VerifyConfigRequestEntity();
        verificationRequest.setRequest(requestDto);

        VerifyConfigRequestEntity results = nifiClient.getControllerClient().submitFlowAnalysisRuleConfigVerificationRequest(verificationRequest);
        while ((!results.getRequest().isComplete()) || (results.getRequest().getResults() == null)) {
            Thread.sleep(50L);
            results = nifiClient.getControllerClient().getFlowAnalysisRuleConfigVerificationRequest(ruleId, results.getRequest().getRequestId());
        }

        nifiClient.getControllerClient().deleteFlowAnalysisRuleConfigVerificationRequest(ruleId, results.getRequest().getRequestId());

        return results.getRequest().getResults();
    }


    public ReportingTaskEntity createReportingTask(final String type, final String bundleGroupId, final String artifactId, final String version)
                throws NiFiClientException, IOException {
        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setType(type);

        final BundleDTO bundle = new BundleDTO();
        bundle.setGroup(bundleGroupId);
        bundle.setArtifact(artifactId);
        bundle.setVersion(version);
        dto.setBundle(bundle);

        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());

        return nifiClient.getControllerClient().createReportingTask(entity);
    }

    public ReportingTaskEntity updateReportingTask(final ReportingTaskEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setProperties(properties);
        dto.setId(currentEntity.getId());

        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setId(currentEntity.getId());
        entity.setComponent(dto);
        entity.setRevision(currentEntity.getRevision());

        return nifiClient.getReportingTasksClient().updateReportingTask(entity);
    }

    public FlowRegistryClientEntity createFlowRegistryClient(final String name) throws NiFiClientException, IOException {
        final BundleDTO bundleDto = new BundleDTO(NiFiSystemIT.NIFI_GROUP_ID, NiFiSystemIT.TEST_EXTENSIONS_ARTIFACT_ID, nifiVersion);
        final FlowRegistryClientDTO clientDto = new FlowRegistryClientDTO();
        clientDto.setBundle(bundleDto);
        clientDto.setName(name);
        clientDto.setType("org.apache.nifi.flow.registry.FileSystemFlowRegistryClient");

        final FlowRegistryClientEntity clientEntity = new FlowRegistryClientEntity();
        clientEntity.setComponent(clientDto);
        clientEntity.setRevision(createNewRevision());

        return nifiClient.getControllerClient().createRegistryClient(clientEntity);
    }

    public FlowRegistryClientEntity updateRegistryClientProperties(final FlowRegistryClientEntity currentEntity, final Map<String, String> properties) throws NiFiClientException, IOException {
        final FlowRegistryClientDTO updateDto = new FlowRegistryClientDTO();
        updateDto.setProperties(properties);
        updateDto.setId(currentEntity.getId());

        final FlowRegistryClientEntity updateEntity = new FlowRegistryClientEntity();
        updateEntity.setRevision(currentEntity.getRevision());
        updateEntity.setId(currentEntity.getId());
        updateEntity.setComponent(updateDto);

        return nifiClient.getControllerClient().updateRegistryClient(updateEntity);
    }

    public VersionControlInformationEntity startVersionControl(final ProcessGroupEntity group, final FlowRegistryClientEntity registryClient, final String bucketId, final String flowName)
            throws NiFiClientException, IOException {

        return publishFlowVersion(group, registryClient, bucketId, flowName, null);
    }

    private VersionControlInformationEntity publishFlowVersion(final ProcessGroupEntity group, final FlowRegistryClientEntity registryClient, final String bucketId, final String flowName,
                                                               final String flowId) throws NiFiClientException, IOException {

        final VersionedFlowDTO versionedFlowDto = new VersionedFlowDTO();
        versionedFlowDto.setBucketId(bucketId);
        versionedFlowDto.setFlowName(flowName);
        versionedFlowDto.setFlowId(flowId);
        versionedFlowDto.setRegistryId(registryClient.getId());
        versionedFlowDto.setAction(VersionedFlowDTO.COMMIT_ACTION);

        final StartVersionControlRequestEntity requestEntity = new StartVersionControlRequestEntity();
        requestEntity.setProcessGroupRevision(group.getRevision());
        requestEntity.setVersionedFlow(versionedFlowDto);

        return nifiClient.getVersionsClient().startVersionControl(group.getId(), requestEntity);
    }

    public VersionControlInformationEntity saveFlowVersion(final ProcessGroupEntity group, final FlowRegistryClientEntity registryClient, final VersionControlInformationEntity currentVci)
            throws NiFiClientException, IOException {

        final VersionControlInformationDTO currentDto = currentVci.getVersionControlInformation();
        return publishFlowVersion(group, registryClient, currentDto.getBucketId(), currentDto.getFlowName(), currentDto.getFlowId());
    }



    public VersionedFlowUpdateRequestEntity revertChanges(final ProcessGroupEntity group) throws NiFiClientException, IOException, InterruptedException {
        final VersionControlInformationEntity vciEntity = nifiClient.getVersionsClient().getVersionControlInfo(group.getId());
        final VersionedFlowUpdateRequestEntity revertRequest = nifiClient.getVersionsClient().initiateRevertFlowVersion(group.getId(), vciEntity);
        return waitForFlowRevertCompleted(revertRequest.getRequest().getRequestId());
    }

    private VersionedFlowUpdateRequestEntity waitForFlowRevertCompleted(final String requestId) throws NiFiClientException, IOException, InterruptedException {
        final VersionsClient versionsClient = nifiClient.getVersionsClient();

        while (true) {
            final VersionedFlowUpdateRequestEntity entity = versionsClient.getRevertFlowVersionRequest(requestId);

            if (entity.getRequest().isComplete()) {
                return entity;
            }

            Thread.sleep(100L);
        }
    }

    public ProcessGroupEntity importFlowFromRegistry(final String parentGroupId, final VersionControlInformationDTO vciDto)
        throws NiFiClientException, IOException {
        return importFlowFromRegistry(parentGroupId, vciDto.getRegistryId(), vciDto.getBucketId(), vciDto.getFlowId(), vciDto.getVersion());
    }

    public ProcessGroupEntity importFlowFromRegistry(final String parentGroupId, final String registryClientId, final String bucketId, final String flowId, final String version)
                throws NiFiClientException, IOException {

        final VersionControlInformationDTO vci = new VersionControlInformationDTO();
        vci.setBucketId(bucketId);
        vci.setFlowId(flowId);
        vci.setVersion(version);
        vci.setRegistryId(registryClientId);

        final ProcessGroupDTO processGroupDto = new ProcessGroupDTO();
        processGroupDto.setVersionControlInformation(vci);

        final ProcessGroupEntity groupEntity = new ProcessGroupEntity();
        groupEntity.setComponent(processGroupDto);
        groupEntity.setRevision(createNewRevision());

        return nifiClient.getProcessGroupClient().createProcessGroup(parentGroupId, groupEntity);
    }

    public VersionedFlowUpdateRequestEntity changeFlowVersion(final String processGroupId, final String version) throws NiFiClientException, IOException, InterruptedException {
        return changeFlowVersion(processGroupId, version, true);
    }

    public VersionedFlowUpdateRequestEntity changeFlowVersion(final String processGroupId, final String version, final boolean throwOnFailure)
                throws NiFiClientException, IOException, InterruptedException {

        logger.info("Submitting Change Flow Version request to change Group with ID {} to Version {}", processGroupId, version);

        try {
            final ProcessGroupEntity groupEntity = nifiClient.getProcessGroupClient().getProcessGroup(processGroupId);
            final ProcessGroupDTO groupDto = groupEntity.getComponent();
            final VersionControlInformationDTO vciDto = groupDto.getVersionControlInformation();
            if (vciDto == null) {
                throw new IllegalArgumentException("Process Group with ID " + processGroupId + " is not under Version Control");
            }

            vciDto.setVersion(version);

            final VersionControlInformationEntity requestEntity = new VersionControlInformationEntity();
            requestEntity.setProcessGroupRevision(groupEntity.getRevision());
            requestEntity.setVersionControlInformation(vciDto);

            final VersionedFlowUpdateRequestEntity result = nifiClient.getVersionsClient().updateVersionControlInfo(processGroupId, requestEntity);
            return waitForVersionFlowUpdateComplete(result.getRequest().getRequestId(), throwOnFailure);
        } catch (final Exception e) {
            logger.error("Failed to change flow version for Process Group {} to version {}", processGroupId, version);
            throw e;
        }
    }

    public VersionedFlowUpdateRequestEntity waitForVersionFlowUpdateComplete(final String updateRequestId, final boolean throwOnFailure) throws NiFiClientException, IOException, InterruptedException {
        while (true) {
            final VersionedFlowUpdateRequestEntity result = nifiClient.getVersionsClient().getUpdateRequest(updateRequestId);
            final boolean complete = result.getRequest().isComplete();
            if (complete) {
                if (throwOnFailure && result.getRequest().getFailureReason() != null) {
                    throw new RuntimeException("Version Flow Update request failed due to: " + result.getRequest().getFailureReason());
                }

                return nifiClient.getVersionsClient().deleteUpdateRequest(updateRequestId);
            }

            logger.debug("Waiting for Version Flow Update request to complete...");
            Thread.sleep(100L);
        }
    }

    public String getVersionControlState(final String groupId) {
        try {
            final VersionControlInformationDTO vci = nifiClient.getProcessGroupClient().getProcessGroup(groupId).getComponent().getVersionControlInformation();
            return vci.getState();
        } catch (final Exception e) {
            Assertions.fail("Could not obtain Version Control Information for Group with ID " + groupId, e);
            return null;
        }
    }


    public void assertFlowStaleAndUnmodified(final String processGroupId) throws NiFiClientException, IOException {
        final String state = nifiClient.getProcessGroupClient().getProcessGroup(processGroupId).getVersionedFlowState();
        if ("STALE".equalsIgnoreCase(state)) {
            return;
        }

        if ("LOCALLY_MODIFIED_AND_STALE".equalsIgnoreCase(state)) {
            final FlowComparisonEntity flowComparisonEntity = nifiClient.getProcessGroupClient().getLocalModifications(processGroupId);
            final String differences = flowComparisonEntity.getComponentDifferences().stream()
                .flatMap(dto -> dto.getDifferences().stream())
                .map(DifferenceDTO::getDifference)
                .collect(Collectors.joining("\n"));
            throw new AssertionError("Expected state to be STALE but was " + state + " with the following modifications:\n" + differences);
        }

        throw new AssertionError("Expected state to be STALE but was " + state);
    }

    public void assertFlowUpToDate(final String processGroupId) throws NiFiClientException, IOException {
        final String state = nifiClient.getProcessGroupClient().getProcessGroup(processGroupId).getVersionedFlowState();
        if ("UP_TO_DATE".equalsIgnoreCase(state)) {
            return;
        }

        if ("LOCALLY_MODIFIED".equalsIgnoreCase(state)) {
            final FlowComparisonEntity flowComparisonEntity = nifiClient.getProcessGroupClient().getLocalModifications(processGroupId);
            final String differences = flowComparisonEntity.getComponentDifferences().stream()
                .flatMap(dto -> dto.getDifferences().stream())
                .map(DifferenceDTO::getDifference)
                .collect(Collectors.joining("\n"));
            throw new AssertionError("Expected state to be UP_TO_DATE but was LOCALLY_MODIFIED with the following modifications:\n" + differences);
        }

        throw new AssertionError("Expected state to be UP_TO_DATE but was " + state);
    }

    public String getVersionedFlowState(final String groupId, final String parentGroupId) throws NiFiClientException, IOException {
        final ProcessGroupFlowDTO parentGroup = nifiClient.getFlowClient().getProcessGroup(parentGroupId).getProcessGroupFlow();
        final Set<ProcessGroupEntity> childGroups = parentGroup.getFlow().getProcessGroups();

        return childGroups.stream()
            .filter(childGroup -> groupId.equals(childGroup.getId()))
            .map(ProcessGroupEntity::getVersionedFlowState)
            .findAny()
            .orElse(null);
    }

    public FlowDTO copyAndPaste(final String sourceGroupId, final CopyRequestEntity copyRequestEntity, final RevisionDTO revisionDTO,
                                final String destinationGroupId) throws NiFiClientException, IOException {
        final CopyResponseEntity copyResponseEntity = nifiClient.getProcessGroupClient().copy(sourceGroupId, copyRequestEntity);

        final PasteRequestEntity pasteRequestEntity = new PasteRequestEntity();
        pasteRequestEntity.setCopyResponse(copyResponseEntity);
        pasteRequestEntity.setRevision(revisionDTO);

        final PasteResponseEntity pasteResponseEntity = nifiClient.getProcessGroupClient().paste(destinationGroupId, pasteRequestEntity);
        return pasteResponseEntity.getFlow();
    }

    public ConnectionEntity setFifoPrioritizer(final ConnectionEntity connectionEntity) throws NiFiClientException, IOException {
        final ConnectionDTO connectionDto = connectionEntity.getComponent();
        connectionDto.setPrioritizers(Collections.singletonList("org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"));
        return nifiClient.getConnectionClient().updateConnection(connectionEntity);
    }

    public ProcessGroupEntity markStateless(final ProcessGroupEntity group, final String timeout) throws NiFiClientException, IOException {
        group.getComponent().setStatelessFlowTimeout(timeout);
        group.getComponent().setExecutionEngine("STATELESS");

        return nifiClient.getProcessGroupClient().updateProcessGroup(group);
    }
}
