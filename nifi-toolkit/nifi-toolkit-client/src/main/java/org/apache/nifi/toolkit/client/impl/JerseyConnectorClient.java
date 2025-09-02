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
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationStepEntity;
import org.apache.nifi.web.api.entity.ConfigurationStepNamesEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorPropertyAllowableValuesEntity;
import org.apache.nifi.web.api.entity.ConnectorRunStatusEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.VerifyConnectorConfigStepRequestEntity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

/**
 * Jersey implementation of ConnectorClient.
 */
public class JerseyConnectorClient extends AbstractJerseyClient implements ConnectorClient {

    private final WebTarget connectorsTarget;
    private volatile WebTarget connectorTarget;

    public JerseyConnectorClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyConnectorClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.connectorsTarget = baseTarget.path("/connectors");
        this.connectorTarget = baseTarget.path("/connectors/{id}");
    }

    @Override
    public void acknowledgeDisconnectedNode() {
        connectorTarget = connectorTarget.queryParam("disconnectedNodeAcknowledged", true);
    }

    @Override
    public ConnectorEntity createConnector(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        if (connectorEntity == null) {
            throw new IllegalArgumentException("Connector entity cannot be null");
        }

        return executeAction("Error creating Connector", () -> {
            return getRequestBuilder(connectorsTarget).post(
                    Entity.entity(connectorEntity, MediaType.APPLICATION_JSON_TYPE),
                    ConnectorEntity.class);
        });
    }

    @Override
    public ConnectorEntity getConnector(final String connectorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector ID cannot be null or blank");
        }

        return executeAction("Error retrieving Connector", () -> {
            final WebTarget target = connectorTarget.resolveTemplate("id", connectorId);
            return getRequestBuilder(target).get(ConnectorEntity.class);
        });
    }

    @Override
    public ConnectorEntity updateConnector(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        if (connectorEntity == null) {
            throw new IllegalArgumentException("Connector entity cannot be null");
        }

        return executeAction("Error updating Connector", () -> {
            final WebTarget target = connectorTarget.resolveTemplate("id", connectorEntity.getId());
            return getRequestBuilder(target).put(
                    Entity.entity(connectorEntity, MediaType.APPLICATION_JSON_TYPE),
                    ConnectorEntity.class);
        });
    }

    @Override
    public ConnectorEntity deleteConnector(final String connectorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return deleteConnector(connectorId, clientId, version, false);
    }

    private ConnectorEntity deleteConnector(final String connectorId, final String clientId, final long version,
            final Boolean acknowledgeDisconnect) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector ID cannot be null or blank");
        }

        return executeAction("Error deleting Connector", () -> {
            WebTarget target = connectorTarget
                    .queryParam("version", version)
                    .queryParam("clientId", clientId)
                    .resolveTemplate("id", connectorId);

            if (acknowledgeDisconnect == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ConnectorEntity.class);
        });
    }

    @Override
    public ConnectorEntity deleteConnector(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        if (connectorEntity == null) {
            throw new IllegalArgumentException("Connector entity cannot be null");
        }
        return deleteConnector(connectorEntity.getId(), connectorEntity.getRevision().getClientId(),
                connectorEntity.getRevision().getVersion(), connectorEntity.isDisconnectedNodeAcknowledged());
    }

    @Override
    public ConnectorEntity startConnector(final String connectorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateConnectorRunStatus(connectorId, "RUNNING", clientId, version, false);
    }

    @Override
    public ConnectorEntity startConnector(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        return updateConnectorRunStatus(connectorEntity.getId(), "RUNNING", connectorEntity.getRevision().getClientId(),
                connectorEntity.getRevision().getVersion(), connectorEntity.isDisconnectedNodeAcknowledged());
    }

    @Override
    public ConnectorEntity stopConnector(final String connectorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateConnectorRunStatus(connectorId, "STOPPED", clientId, version, false);
    }

    @Override
    public ConnectorEntity stopConnector(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        return updateConnectorRunStatus(connectorEntity.getId(), "STOPPED", connectorEntity.getRevision().getClientId(),
                connectorEntity.getRevision().getVersion(), connectorEntity.isDisconnectedNodeAcknowledged());
    }

    @Override
    public ConnectorEntity drainConnector(final String connectorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return drainConnector(connectorId, clientId, version, false);
    }

    @Override
    public ConnectorEntity drainConnector(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        return drainConnector(connectorEntity.getId(), connectorEntity.getRevision().getClientId(),
                connectorEntity.getRevision().getVersion(), connectorEntity.isDisconnectedNodeAcknowledged());
    }

    private ConnectorEntity drainConnector(final String connectorId, final String clientId, final long version,
            final Boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector ID cannot be null or blank");
        }

        return executeAction("Error initiating connector drain", () -> {
            final WebTarget target = connectorTarget
                    .path("/drain")
                    .resolveTemplate("id", connectorId);

            final ConnectorEntity requestEntity = new ConnectorEntity();
            requestEntity.setId(connectorId);
            requestEntity.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);

            final RevisionDTO revisionDto = new RevisionDTO();
            revisionDto.setClientId(clientId);
            revisionDto.setVersion(version);
            requestEntity.setRevision(revisionDto);

            return getRequestBuilder(target).post(
                    Entity.entity(requestEntity, MediaType.APPLICATION_JSON_TYPE),
                    ConnectorEntity.class);
        });
    }

    @Override
    public ConnectorEntity cancelDrain(final String connectorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return cancelDrain(connectorId, clientId, version, false);
    }

    @Override
    public ConnectorEntity cancelDrain(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        return cancelDrain(connectorEntity.getId(), connectorEntity.getRevision().getClientId(),
                connectorEntity.getRevision().getVersion(), connectorEntity.isDisconnectedNodeAcknowledged());
    }

    private ConnectorEntity cancelDrain(final String connectorId, final String clientId, final long version,
            final Boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector ID cannot be null or blank");
        }

        return executeAction("Error canceling connector drain", () -> {
            WebTarget target = connectorTarget
                    .path("/drain")
                    .queryParam("version", version)
                    .queryParam("clientId", clientId)
                    .resolveTemplate("id", connectorId);

            if (disconnectedNodeAcknowledged == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ConnectorEntity.class);
        });
    }

    private ConnectorEntity updateConnectorRunStatus(final String connectorId, final String desiredState, final String clientId,
            final long version, final Boolean disconnectedNodeAcknowledged) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector ID cannot be null or blank");
        }

        return executeAction("Error updating Connector run status", () -> {
            final WebTarget target = connectorTarget
                    .path("/run-status")
                    .resolveTemplate("id", connectorId);

            final ConnectorRunStatusEntity runStatusEntity = new ConnectorRunStatusEntity();
            runStatusEntity.setState(desiredState);
            runStatusEntity.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);

            final RevisionDTO revisionDto = new RevisionDTO();
            revisionDto.setClientId(clientId);
            revisionDto.setVersion(version);
            runStatusEntity.setRevision(revisionDto);

            return getRequestBuilder(target).put(
                    Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                    ConnectorEntity.class);
        });
    }

    @Override
    public ConfigurationStepNamesEntity getConfigurationSteps(final String connectorId) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");

        return executeAction("Error retrieving configuration steps", () -> {
            final WebTarget target = connectorTarget
                    .path("/configuration-steps")
                    .resolveTemplate("id", connectorId);
            return getRequestBuilder(target).get(ConfigurationStepNamesEntity.class);
        });
    }

    @Override
    public ConfigurationStepEntity getConfigurationStep(final String connectorId, final String configurationStepName) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");
        Objects.requireNonNull(configurationStepName, "Configuration step name required");

        return executeAction("Error retrieving configuration step", () -> {
            final WebTarget target = connectorTarget
                    .path("/configuration-steps/{configurationStepName}")
                    .resolveTemplate("id", connectorId)
                    .resolveTemplate("configurationStepName", configurationStepName);
            return getRequestBuilder(target).get(ConfigurationStepEntity.class);
        });
    }

    @Override
    public ConnectorPropertyAllowableValuesEntity getPropertyAllowableValues(final String connectorId, final String configurationStepName,
            final String propertyGroupName, final String propertyName, final String filter) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");
        Objects.requireNonNull(configurationStepName, "Configuration step name required");
        Objects.requireNonNull(propertyGroupName, "Property group name required");
        Objects.requireNonNull(propertyName, "Property name required");

        return executeAction("Error retrieving property allowable values", () -> {
            WebTarget target = connectorTarget
                    .path("/configuration-steps/{configurationStepName}/property-groups/{propertyGroupName}/properties/{propertyName}/allowable-values")
                    .resolveTemplate("id", connectorId)
                    .resolveTemplate("configurationStepName", configurationStepName)
                    .resolveTemplate("propertyGroupName", propertyGroupName)
                    .resolveTemplate("propertyName", propertyName);

            if (filter != null) {
                target = target.queryParam("filter", filter);
            }

            return getRequestBuilder(target).get(ConnectorPropertyAllowableValuesEntity.class);
        });
    }

    @Override
    public ConfigurationStepEntity updateConfigurationStep(final ConfigurationStepEntity configurationStepEntity) throws NiFiClientException, IOException {
        if (configurationStepEntity == null) {
            throw new IllegalArgumentException("Configuration step entity cannot be null");
        }
        if (configurationStepEntity.getParentConnectorId() == null) {
            throw new IllegalArgumentException("Parent connector ID cannot be null");
        }
        if (configurationStepEntity.getConfigurationStep() == null) {
            throw new IllegalArgumentException("Configuration step cannot be null");
        }
        if (configurationStepEntity.getConfigurationStep().getConfigurationStepName() == null) {
            throw new IllegalArgumentException("Configuration step name cannot be null");
        }

        return executeAction("Error updating configuration step", () -> {
            final WebTarget target = connectorTarget
                    .path("/configuration-steps/{configurationStepName}")
                    .resolveTemplate("id", configurationStepEntity.getParentConnectorId())
                    .resolveTemplate("configurationStepName", configurationStepEntity.getConfigurationStep().getConfigurationStepName());

            return getRequestBuilder(target).put(
                    Entity.entity(configurationStepEntity, MediaType.APPLICATION_JSON_TYPE),
                    ConfigurationStepEntity.class);
        });
    }

    @Override
    public VerifyConnectorConfigStepRequestEntity submitConfigStepVerificationRequest(final VerifyConnectorConfigStepRequestEntity requestEntity)
            throws NiFiClientException, IOException {
        if (requestEntity == null) {
            throw new IllegalArgumentException("Verification request entity cannot be null");
        }
        if (requestEntity.getRequest() == null) {
            throw new IllegalArgumentException("Verification request DTO cannot be null");
        }
        if (requestEntity.getRequest().getConnectorId() == null) {
            throw new IllegalArgumentException("Connector ID cannot be null");
        }
        if (requestEntity.getRequest().getConfigurationStepName() == null) {
            throw new IllegalArgumentException("Configuration step name cannot be null");
        }

        return executeAction("Error submitting configuration step verification request", () -> {
            final WebTarget target = connectorTarget
                    .path("/configuration-steps/{configurationStepName}/verify-config")
                    .resolveTemplate("id", requestEntity.getRequest().getConnectorId())
                    .resolveTemplate("configurationStepName", requestEntity.getRequest().getConfigurationStepName());

            return getRequestBuilder(target).post(
                    Entity.entity(requestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VerifyConnectorConfigStepRequestEntity.class);
        });
    }

    @Override
    public VerifyConnectorConfigStepRequestEntity getConfigStepVerificationRequest(final String connectorId, final String configurationStepName,
            final String requestId) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");
        Objects.requireNonNull(configurationStepName, "Configuration step name required");
        Objects.requireNonNull(requestId, "Request ID required");

        return executeAction("Error retrieving configuration step verification request", () -> {
            final WebTarget target = connectorTarget
                    .path("/configuration-steps/{configurationStepName}/verify-config/{requestId}")
                    .resolveTemplate("id", connectorId)
                    .resolveTemplate("configurationStepName", configurationStepName)
                    .resolveTemplate("requestId", requestId);

            return getRequestBuilder(target).get(VerifyConnectorConfigStepRequestEntity.class);
        });
    }

    @Override
    public VerifyConnectorConfigStepRequestEntity deleteConfigStepVerificationRequest(final String connectorId, final String configurationStepName,
            final String requestId) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");
        Objects.requireNonNull(configurationStepName, "Configuration step name required");
        Objects.requireNonNull(requestId, "Request ID required");

        return executeAction("Error deleting configuration step verification request", () -> {
            final WebTarget target = connectorTarget
                    .path("/configuration-steps/{configurationStepName}/verify-config/{requestId}")
                    .resolveTemplate("id", connectorId)
                    .resolveTemplate("configurationStepName", configurationStepName)
                    .resolveTemplate("requestId", requestId);

            return getRequestBuilder(target).delete(VerifyConnectorConfigStepRequestEntity.class);
        });
    }

    @Override
    public ConnectorEntity applyUpdate(final ConnectorEntity connectorEntity) throws NiFiClientException, IOException {
        if (connectorEntity == null) {
            throw new IllegalArgumentException("Connector entity cannot be null");
        }
        if (StringUtils.isBlank(connectorEntity.getId())) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (connectorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision cannot be null");
        }

        final String connectorId = connectorEntity.getId();
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector ID cannot be null or blank");
        }

        return executeAction("Error applying connector update", () -> {
            final WebTarget target = connectorTarget
                    .path("/apply-update")
                    .resolveTemplate("id", connectorId);

            return getRequestBuilder(target).post(
                    Entity.entity(connectorEntity, MediaType.APPLICATION_JSON_TYPE),
                    ConnectorEntity.class);
        });
    }

    @Override
    public ProcessGroupFlowEntity getFlow(final String connectorId) throws NiFiClientException, IOException {
        final ConnectorEntity connector = getConnector(connectorId);
        final String managedProcessGroupId = connector.getComponent().getManagedProcessGroupId();
        return getFlow(connectorId, managedProcessGroupId);
    }

    @Override
    public ProcessGroupFlowEntity getFlow(final String connectorId, final String processGroupId) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");
        Objects.requireNonNull(processGroupId, "Process Group ID required");

        return executeAction("Error retrieving connector flow", () -> {
            WebTarget target = connectorTarget
                    .path("/flow/process-groups/{processGroupId}")
                    .resolveTemplate("id", connectorId)
                    .resolveTemplate("processGroupId", processGroupId);

            return getRequestBuilder(target).get(ProcessGroupFlowEntity.class);
        });
    }

    @Override
    public ProcessGroupStatusEntity getStatus(final String connectorId, final boolean recursive) throws NiFiClientException, IOException {
        Objects.requireNonNull(connectorId, "Connector ID required");

        return executeAction("Error retrieving connector status", () -> {
            WebTarget target = connectorTarget
                    .path("/status")
                    .resolveTemplate("id", connectorId);

            if (recursive) {
                target = target.queryParam("recursive", "true");
            }

            return getRequestBuilder(target).get(ProcessGroupStatusEntity.class);
        });
    }

    @Override
    public AssetEntity createAsset(final String connectorId, final String assetName, final File file) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(assetName)) {
            throw new IllegalArgumentException("Asset name cannot be null or blank");
        }
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }

        try (final InputStream assetInputStream = new FileInputStream(file)) {
            return executeAction("Error creating Connector Asset " + assetName + " for Connector " + connectorId, () -> {
                final WebTarget target = connectorsTarget
                    .path("{id}/assets")
                    .resolveTemplate("id", connectorId);

                return getRequestBuilder(target)
                    .header("Filename", assetName)
                    .post(
                        Entity.entity(assetInputStream, MediaType.APPLICATION_OCTET_STREAM_TYPE),
                        AssetEntity.class);
            });
        }
    }

    @Override
    public AssetsEntity getAssets(final String connectorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }

        return executeAction("Error retrieving Connector assets", () -> {
            final WebTarget target = connectorsTarget
                .path("{id}/assets")
                .resolveTemplate("id", connectorId);
            return getRequestBuilder(target).get(AssetsEntity.class);
        });
    }

    @Override
    public Path getAssetContent(final String connectorId, final String assetId, final File outputDirectory) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(assetId)) {
            throw new IllegalArgumentException("Asset id cannot be null or blank");
        }

        return executeAction("Error getting Connector asset content", () -> {
            final WebTarget target = connectorsTarget
                .path("{id}/assets/{assetId}")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("assetId", assetId);

            final Response response = getRequestBuilder(target)
                .accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .get();

            final String filename = getContentDispositionFilename(response);
            final File assetFile = new File(outputDirectory, filename);

            try (final InputStream responseInputStream = response.readEntity(InputStream.class)) {
                Files.copy(responseInputStream, assetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                return assetFile.toPath();
            }
        });
    }

    @Override
    public DropRequestEntity createPurgeRequest(final String connectorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }

        return executeAction("Error creating purge request for Connector " + connectorId, () -> {
            final WebTarget target = connectorsTarget
                .path("{id}/purge-requests")
                .resolveTemplate("id", connectorId);

            return getRequestBuilder(target).post(null, DropRequestEntity.class);
        });
    }

    @Override
    public DropRequestEntity getPurgeRequest(final String connectorId, final String purgeRequestId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(purgeRequestId)) {
            throw new IllegalArgumentException("Purge request id cannot be null or blank");
        }

        return executeAction("Error getting purge request for Connector " + connectorId, () -> {
            final WebTarget target = connectorsTarget
                .path("{id}/purge-requests/{purgeRequestId}")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("purgeRequestId", purgeRequestId);

            return getRequestBuilder(target).get(DropRequestEntity.class);
        });
    }

    @Override
    public DropRequestEntity deletePurgeRequest(final String connectorId, final String purgeRequestId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(purgeRequestId)) {
            throw new IllegalArgumentException("Purge request id cannot be null or blank");
        }

        return executeAction("Error deleting purge request for Connector " + connectorId, () -> {
            final WebTarget target = connectorsTarget
                .path("{id}/purge-requests/{purgeRequestId}")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("purgeRequestId", purgeRequestId);

            return getRequestBuilder(target).delete(DropRequestEntity.class);
        });
    }

    @Override
    public ComponentStateEntity getProcessorState(final String connectorId, final String processorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(processorId)) {
            throw new IllegalArgumentException("Processor id cannot be null or blank");
        }

        return executeAction("Error retrieving processor state for Connector " + connectorId, () -> {
            final WebTarget target = connectorTarget
                .path("/processors/{processorId}/state")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("processorId", processorId);

            return getRequestBuilder(target).get(ComponentStateEntity.class);
        });
    }

    @Override
    public ComponentStateEntity clearProcessorState(final String connectorId, final String processorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(processorId)) {
            throw new IllegalArgumentException("Processor id cannot be null or blank");
        }

        return executeAction("Error clearing processor state for Connector " + connectorId, () -> {
            final WebTarget target = connectorTarget
                .path("/processors/{processorId}/state/clear-requests")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("processorId", processorId);

            return getRequestBuilder(target).post(null, ComponentStateEntity.class);
        });
    }

    @Override
    public ComponentStateEntity getControllerServiceState(final String connectorId, final String controllerServiceId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(controllerServiceId)) {
            throw new IllegalArgumentException("Controller service id cannot be null or blank");
        }

        return executeAction("Error retrieving controller service state for Connector " + connectorId, () -> {
            final WebTarget target = connectorTarget
                .path("/controller-services/{controllerServiceId}/state")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("controllerServiceId", controllerServiceId);

            return getRequestBuilder(target).get(ComponentStateEntity.class);
        });
    }

    @Override
    public ComponentStateEntity clearControllerServiceState(final String connectorId, final String controllerServiceId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(connectorId)) {
            throw new IllegalArgumentException("Connector id cannot be null or blank");
        }
        if (StringUtils.isBlank(controllerServiceId)) {
            throw new IllegalArgumentException("Controller service id cannot be null or blank");
        }

        return executeAction("Error clearing controller service state for Connector " + connectorId, () -> {
            final WebTarget target = connectorTarget
                .path("/controller-services/{controllerServiceId}/state/clear-requests")
                .resolveTemplate("id", connectorId)
                .resolveTemplate("controllerServiceId", controllerServiceId);

            return getRequestBuilder(target).post(null, ComponentStateEntity.class);
        });
    }
}
