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
package org.apache.nifi.toolkit.client.impl;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.client.ControllerClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleRunStatusEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;
import org.apache.nifi.web.api.entity.NarDetailsEntity;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

/**
 * Jersey implementation of ControllerClient.
 */
public class JerseyControllerClient extends AbstractJerseyClient implements ControllerClient {

    private static final String NAR_MANAGER_PATH = "nar-manager";
    private static final String NARS_PATH = NAR_MANAGER_PATH + "/nars";
    private static final String NAR_UPLOAD_PATH = NARS_PATH + "/content";

    private final WebTarget controllerTarget;

    public JerseyControllerClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyControllerClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.controllerTarget = baseTarget.path("/controller");
    }

    @Override
    public FlowRegistryClientsEntity getRegistryClients() throws NiFiClientException, IOException {
        return executeAction("Error retrieving registry clients", () -> {
            final WebTarget target = controllerTarget.path("registry-clients");
            return getRequestBuilder(target).get(FlowRegistryClientsEntity.class);
        });
    }

    @Override
    public FlowRegistryClientEntity getRegistryClient(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Registry client id cannot be null");
        }

        final WebTarget target = controllerTarget
                .path("registry-clients/{id}")
                .resolveTemplate("id", id);

        return getRequestBuilder(target).get(FlowRegistryClientEntity.class);
    }

    @Override
    public FlowRegistryClientEntity createRegistryClient(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException {
        if (registryClient == null) {
            throw new IllegalArgumentException("Registry client entity cannot be null");
        }

        return executeAction("Error creating registry client", () -> {
            final WebTarget target = controllerTarget.path("registry-clients");

            return getRequestBuilder(target).post(
                    Entity.entity(registryClient, MediaType.APPLICATION_JSON),
                    FlowRegistryClientEntity.class);
        });
    }

    @Override
    public FlowRegistryClientEntity updateRegistryClient(final FlowRegistryClientEntity registryClient) throws NiFiClientException, IOException {
        if (registryClient == null) {
            throw new IllegalArgumentException("Registry client entity cannot be null");
        }

        if (StringUtils.isBlank(registryClient.getId())) {
            throw new IllegalArgumentException("Registry client entity must contain an id");
        }

        return executeAction("Error updating registry client", () -> {
            final WebTarget target = controllerTarget
                    .path("registry-clients/{id}")
                    .resolveTemplate("id", registryClient.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(registryClient, MediaType.APPLICATION_JSON),
                    FlowRegistryClientEntity.class);
        });
    }

    @Override
    public NodeEntity deleteNode(final String nodeId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        return executeAction("Error deleting node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).delete(NodeEntity.class);
        });
    }

    @Override
    public NodeEntity connectNode(final String nodeId, final NodeEntity nodeEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        if (nodeEntity == null) {
            throw new IllegalArgumentException("Node entity cannot be null");
        }

        return executeAction("Error connecting node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).put(Entity.entity(nodeEntity, MediaType.APPLICATION_JSON), NodeEntity.class);
        });
    }

    @Override
    public NodeEntity offloadNode(final String nodeId, final NodeEntity nodeEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        if (nodeEntity == null) {
            throw new IllegalArgumentException("Node entity cannot be null");
        }

        return executeAction("Error offloading node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).put(Entity.entity(nodeEntity, MediaType.APPLICATION_JSON), NodeEntity.class);
        });
    }

    @Override
    public NodeEntity disconnectNode(final String nodeId, final NodeEntity nodeEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        if (nodeEntity == null) {
            throw new IllegalArgumentException("Node entity cannot be null");
        }

        return executeAction("Error disconnecting node", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).put(Entity.entity(nodeEntity, MediaType.APPLICATION_JSON), NodeEntity.class);
        });
    }

    @Override
    public NodeEntity getNode(String nodeId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(nodeId)) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        return executeAction("Error retrieving node status", () -> {
            final WebTarget target = controllerTarget.path("cluster/nodes/" + nodeId);

            return getRequestBuilder(target).get(NodeEntity.class);
        });
    }

    @Override
    public ClusterEntity getNodes() throws NiFiClientException, IOException {
        return executeAction("Error retrieving node status", () -> {
            final WebTarget target = controllerTarget.path("cluster");

            return getRequestBuilder(target).get(ClusterEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity createControllerService(final ControllerServiceEntity controllerService) throws NiFiClientException, IOException {
        if (controllerService == null) {
            throw new IllegalArgumentException("Controller service entity cannot be null");
        }

        return executeAction("Error creating controller service", () -> {
            final WebTarget target = controllerTarget.path("controller-services");

            return getRequestBuilder(target).post(
                    Entity.entity(controllerService, MediaType.APPLICATION_JSON),
                    ControllerServiceEntity.class);
        });
    }

    @Override
    public ReportingTaskEntity createReportingTask(ReportingTaskEntity reportingTask) throws NiFiClientException, IOException {
        if (reportingTask == null) {
            throw new IllegalArgumentException("Reporting task entity cannot be null");
        }

        return executeAction("Error creating reporting task", () -> {
            final WebTarget target = controllerTarget.path("reporting-tasks");

            return getRequestBuilder(target).post(
                    Entity.entity(reportingTask, MediaType.APPLICATION_JSON),
                    ReportingTaskEntity.class);
        });
    }

    @Override
    public VersionedReportingTaskImportResponseEntity importReportingTasks(VersionedReportingTaskImportRequestEntity importRequestEntity)
            throws NiFiClientException, IOException {
        if (importRequestEntity == null) {
            throw new IllegalArgumentException("Import request entity cannot be null");
        }

        return executeAction("Error creating reporting task", () -> {
            final WebTarget target = controllerTarget.path("reporting-tasks/import");

            return getRequestBuilder(target).post(
                    Entity.entity(importRequestEntity, MediaType.APPLICATION_JSON),
                    VersionedReportingTaskImportResponseEntity.class);
        });
    }

    @Override
    public FlowAnalysisRulesEntity getFlowAnalysisRules() throws NiFiClientException, IOException {
        return executeAction("Error retrieving flow analysis rules", () -> {
            final WebTarget target = controllerTarget.path("flow-analysis-rules");
            return getRequestBuilder(target).get(FlowAnalysisRulesEntity.class);
        });
    }

    @Override
    public FlowAnalysisRuleEntity getFlowAnalysisRule(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Flow analysis rule id cannot be null");
        }

        return executeAction("Error retrieving status of flow analysis rule", () -> {
            final WebTarget target = controllerTarget.path("flow-analysis-rules/{id}").resolveTemplate("id", id);
            return getRequestBuilder(target).get(FlowAnalysisRuleEntity.class);
        });
    }

    @Override
    public PropertyDescriptorEntity getFlowAnalysisRulePropertyDescriptor(final String componentId, final String propertyName, final Boolean sensitive) throws NiFiClientException, IOException {
        Objects.requireNonNull(componentId, "Component ID required");
        Objects.requireNonNull(propertyName, "Property Name required");

        return executeAction("Error retrieving Flow Analysis Rule Property Descriptor", () -> {
            final WebTarget target = controllerTarget
                    .path("flow-analysis-rules/{id}/descriptors")
                    .resolveTemplate("id", componentId)
                    .queryParam("propertyName", propertyName)
                    .queryParam("sensitive", sensitive);

            return getRequestBuilder(target).get(PropertyDescriptorEntity.class);
        });
    }

    @Override
    public FlowAnalysisRuleEntity createFlowAnalysisRule(FlowAnalysisRuleEntity flowAnalysisRule) throws NiFiClientException, IOException {
        if (flowAnalysisRule == null) {
            throw new IllegalArgumentException("Flow analysis rule entity cannot be null");
        }

        return executeAction("Error creating flow analysis rule", () -> {
            final WebTarget target = controllerTarget.path("flow-analysis-rules");

            return getRequestBuilder(target).post(
                    Entity.entity(flowAnalysisRule, MediaType.APPLICATION_JSON),
                    FlowAnalysisRuleEntity.class);
        });
    }

    @Override
    public FlowAnalysisRuleEntity updateFlowAnalysisRule(final FlowAnalysisRuleEntity flowAnalysisRuleEntity) throws NiFiClientException, IOException {
        if (flowAnalysisRuleEntity == null) {
            throw new IllegalArgumentException("Flow Analysis Rule cannot be null");
        }
        if (flowAnalysisRuleEntity.getComponent() == null) {
            throw new IllegalArgumentException("Component cannot be null");
        }

        return executeAction("Error updating Flow Analysis Rule", () -> {
            final WebTarget target = controllerTarget.path("flow-analysis-rules/{id}").resolveTemplate("id", flowAnalysisRuleEntity.getId());
            return getRequestBuilder(target).put(
                    Entity.entity(flowAnalysisRuleEntity, MediaType.APPLICATION_JSON_TYPE),
                    FlowAnalysisRuleEntity.class);
        });
    }

    @Override
    public FlowAnalysisRuleEntity activateFlowAnalysisRule(
            final String id,
            final FlowAnalysisRuleRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Flow analysis rule id cannot be null");
        }

        if (runStatusEntity == null) {
            throw new IllegalArgumentException("Entity cannot be null");
        }

        return executeAction("Error enabling or disabling flow analysis rule", () -> {
            final WebTarget target = controllerTarget
                    .path("flow-analysis-rules/{id}/run-status")
                    .resolveTemplate("id", id);
            return getRequestBuilder(target).put(
                    Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                    FlowAnalysisRuleEntity.class);
        });
    }

    @Override
    public FlowAnalysisRuleEntity deleteFlowAnalysisRule(final FlowAnalysisRuleEntity flowAnalysisRule) throws NiFiClientException, IOException {
        if (flowAnalysisRule == null) {
            throw new IllegalArgumentException("Flow Analysis Rule Entity cannot be null");
        }
        if (flowAnalysisRule.getId() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule ID cannot be null");
        }

        final RevisionDTO revision = flowAnalysisRule.getRevision();
        if (revision == null) {
            throw new IllegalArgumentException("Revision cannot be null");
        }

        return executeAction("Error deleting Flow Analysis Rule", () -> {
            WebTarget target = controllerTarget
                    .path("flow-analysis-rules/{id}")
                    .resolveTemplate("id", flowAnalysisRule.getId())
                    .queryParam("version", revision.getVersion())
                    .queryParam("clientId", revision.getClientId());

            if (flowAnalysisRule.isDisconnectedNodeAcknowledged() == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(FlowAnalysisRuleEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity submitFlowAnalysisRuleConfigVerificationRequest(final VerifyConfigRequestEntity configRequestEntity) throws NiFiClientException, IOException {
        if (configRequestEntity == null) {
            throw new IllegalArgumentException("Config Request Entity cannot be null");
        }
        if (configRequestEntity.getRequest() == null) {
            throw new IllegalArgumentException("Config Request DTO cannot be null");
        }
        if (configRequestEntity.getRequest().getComponentId() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule ID cannot be null");
        }
        if (configRequestEntity.getRequest().getProperties() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule properties cannot be null");
        }

        return executeAction("Error submitting Flow Analysis Rule Config Verification Request", () -> {
            final WebTarget target = controllerTarget
                    .path("flow-analysis-rules/{id}/config/verification-requests")
                    .resolveTemplate("id", configRequestEntity.getRequest().getComponentId());

            return getRequestBuilder(target).post(
                    Entity.entity(configRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity getFlowAnalysisRuleConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error retrieving Flow Analysis Rule Config Verification Request", () -> {
            final WebTarget target = controllerTarget
                    .path("flow-analysis-rules/{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", taskId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).get(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity deleteFlowAnalysisRuleConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error deleting Flow Analysis Rule Config Verification Request", () -> {
            final WebTarget target = controllerTarget
                    .path("flow-analysis-rules/{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", taskId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).delete(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public ParameterProviderEntity createParamProvider(final ParameterProviderEntity paramProvider) throws NiFiClientException, IOException {
        if (paramProvider == null) {
            throw new IllegalArgumentException("Parameter provider cannot be null or blank");
        }

        return executeAction("Error creating parameter provider", () -> {
            final WebTarget target = controllerTarget.path("parameter-providers");
            return getRequestBuilder(target).post(
                    Entity.entity(paramProvider, MediaType.APPLICATION_JSON),
                    ParameterProviderEntity.class);
        });
    }

    @Override
    public ControllerConfigurationEntity getControllerConfiguration() throws NiFiClientException, IOException {
        return executeAction("Error retrieving controller configuration", () -> {
            final WebTarget target = controllerTarget.path("config");
            return getRequestBuilder(target).get(ControllerConfigurationEntity.class);
        });
    }

    @Override
    public ControllerConfigurationEntity updateControllerConfiguration(ControllerConfigurationEntity controllerConfiguration) throws NiFiClientException, IOException {
        if (controllerConfiguration == null || controllerConfiguration.getComponent() == null) {
            throw new IllegalArgumentException("Controller configuration must be specified");
        }

        if (controllerConfiguration.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        return executeAction("Error updating controller configuration", () -> {
            final WebTarget target = controllerTarget
                    .path("config");

            return getRequestBuilder(target).put(
                    Entity.entity(controllerConfiguration, MediaType.APPLICATION_JSON),
                    ControllerConfigurationEntity.class);
        });
    }

    @Override
    public NarSummaryEntity uploadNar(final String filename, final InputStream narContentStream) throws NiFiClientException, IOException {
        if (narContentStream == null) {
            throw new IllegalArgumentException("NAR content stream is required");
        }

        return executeAction("Error uploading NAR", () -> {
            final WebTarget target = controllerTarget.path(NAR_UPLOAD_PATH);
            return getRequestBuilder(target)
                    .header("Filename", filename)
                    .post(
                            Entity.entity(narContentStream, MediaType.APPLICATION_OCTET_STREAM_TYPE),
                            NarSummaryEntity.class);
        });
    }

    @Override
    public NarSummariesEntity getNarSummaries() throws NiFiClientException, IOException {
        return executeAction("Error retrieving NAR summaries", () -> {
            final WebTarget target = controllerTarget.path(NARS_PATH);
            return getRequestBuilder(target).get(NarSummariesEntity.class);
        });
    }

    @Override
    public NarSummaryEntity getNarSummary(final String identifier) throws NiFiClientException, IOException {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier is required");
        }

        return executeAction("Error getting NAR summary", () -> {
            final WebTarget target = controllerTarget.path(NARS_PATH + "/{identifier}")
                    .resolveTemplate("identifier", identifier);
            return getRequestBuilder(target).get(NarSummaryEntity.class);
        });
    }

    @Override
    public NarSummaryEntity deleteNar(final String identifier, final boolean forceDelete) throws NiFiClientException, IOException {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier is required");
        }

        return executeAction("Error deleting NAR", () -> {
            final WebTarget target = controllerTarget.path(NARS_PATH + "/{identifier}")
                    .resolveTemplate("identifier", identifier)
                    .queryParam("force", String.valueOf(forceDelete));
            return getRequestBuilder(target).delete(NarSummaryEntity.class);
        });
    }

    @Override
    public NarDetailsEntity getNarDetails(final String identifier) throws NiFiClientException, IOException {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier is required");
        }

        return executeAction("Error getting NAR details", () -> {
            final WebTarget target = controllerTarget.path(NARS_PATH + "/{identifier}/details")
                    .resolveTemplate("identifier", identifier);
            return getRequestBuilder(target).get(NarDetailsEntity.class);
        });
    }

    @Override
    public File downloadNar(final String identifier, final File outputDirectory) throws NiFiClientException, IOException {
        if (identifier == null) {
            throw new IllegalArgumentException("Identifier is required");
        }
        if (outputDirectory == null) {
            throw new IllegalArgumentException("Output directory is required");
        }

        return executeAction("Error downloading NAR", () -> {
            final WebTarget target = controllerTarget.path(NARS_PATH + "/{identifier}/content")
                    .resolveTemplate("identifier", identifier);

            final Response response = getRequestBuilder(target)
                    .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                    .get();

            final String filename = getContentDispositionFilename(response);
            final File narFile = new File(outputDirectory, filename);

            try (final InputStream responseInputStream = response.readEntity(InputStream.class)) {
                Files.copy(responseInputStream, narFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return narFile;
            }
        });
    }
}
