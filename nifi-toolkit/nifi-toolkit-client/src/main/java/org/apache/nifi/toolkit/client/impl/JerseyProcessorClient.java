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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessorClient;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorRunStatusEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;

import java.io.IOException;
import java.util.Objects;

public class JerseyProcessorClient extends AbstractJerseyClient implements ProcessorClient {
    private volatile WebTarget processGroupTarget;
    private volatile WebTarget processorTarget;

    public JerseyProcessorClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyProcessorClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.processGroupTarget = baseTarget.path("/process-groups/{pgId}");
        this.processorTarget = baseTarget.path("/processors/{id}");
    }

    @Override
    public void acknowledgeDisconnectedNode() {
        processGroupTarget = processGroupTarget.queryParam("disconnectedNodeAcknowledged", true);
        processorTarget = processorTarget.queryParam("disconnectedNodeAcknowledged", true);
    }

    @Override
    public ProcessorEntity createProcessor(final String parentGroupdId, final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (processorEntity == null) {
            throw new IllegalArgumentException("Processor entity cannot be null");
        }

        return executeAction("Error creating Processor", () -> {
            final WebTarget target = processGroupTarget
                    .path("/processors")
                    .resolveTemplate("pgId", parentGroupdId);

            return getRequestBuilder(target).post(
                    Entity.entity(processorEntity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessorEntity.class);
        });
    }

    @Override
    public ProcessorEntity getProcessor(final String processorId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processorId)) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }

        return executeAction("Error retrieving status of Processor", () -> {
            final WebTarget target = processorTarget.resolveTemplate("id", processorId);
            return getRequestBuilder(target).get(ProcessorEntity.class);
        });
    }

    @Override
    public ProcessorEntity updateProcessor(final ProcessorEntity entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException("Processor entity cannot be null");
        }

        return executeAction("Error updating Processor", () -> {
            final WebTarget target = processorTarget.resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessorEntity.class);
        });
    }

    @Override
    public ProcessorEntity startProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "RUNNING", clientId, version, false);
    }

    @Override
    public ProcessorEntity startProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return updateProcessorState(processorEntity.getId(), "RUNNING", processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion(),
                processorEntity.isDisconnectedNodeAcknowledged());
    }

    @Override
    public ProcessorEntity runProcessorOnce(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "RUN_ONCE", clientId, version, false);
    }

    @Override
    public ProcessorEntity runProcessorOnce(ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return runProcessorOnce(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    @Override
    public ProcessorEntity stopProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "STOPPED", clientId, version, false);
    }

    @Override
    public ProcessorEntity stopProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return updateProcessorState(processorEntity.getId(), "STOPPED", processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion(),
                processorEntity.isDisconnectedNodeAcknowledged());
    }

    @Override
    public ProcessorEntity disableProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return updateProcessorState(processorId, "DISABLED", clientId, version, false);
    }

    @Override
    public ProcessorEntity disableProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return disableProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion());
    }

    @Override
    public ProcessorEntity deleteProcessor(final String processorId, final String clientId, final long version) throws NiFiClientException, IOException {
        return deleteProcessor(processorId, clientId, version, false);
    }

    public ProcessorEntity deleteProcessor(final String processorId, final String clientId, final long version, final Boolean acknowledgeDisconnect) throws NiFiClientException, IOException {
        if (processorId == null) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }

        return executeAction("Error deleting Processor", () -> {
            WebTarget target = processorTarget
                    .queryParam("version", version)
                    .queryParam("clientId", clientId)
                    .resolveTemplate("id", processorId);

            if (acknowledgeDisconnect == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ProcessorEntity.class);
        });
    }

    @Override
    public ProcessorEntity deleteProcessor(final ProcessorEntity processorEntity) throws NiFiClientException, IOException {
        return deleteProcessor(processorEntity.getId(), processorEntity.getRevision().getClientId(), processorEntity.getRevision().getVersion(), processorEntity.isDisconnectedNodeAcknowledged());
    }

    private ProcessorEntity updateProcessorState(final String processorId, final String desiredState, final String clientId, final long version, final Boolean disconnectedNodeAcknowledged)
            throws NiFiClientException, IOException {

        if (processorId == null) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }

        return executeAction("Error starting Processor", () -> {
            final WebTarget target = processorTarget
                    .path("/run-status")
                    .resolveTemplate("id", processorId);

            final ProcessorRunStatusEntity runStatusEntity = new ProcessorRunStatusEntity();
            runStatusEntity.setState(desiredState);
            runStatusEntity.setDisconnectedNodeAcknowledged(disconnectedNodeAcknowledged);

            final RevisionDTO revisionDto = new RevisionDTO();
            revisionDto.setClientId(clientId);
            revisionDto.setVersion(version);
            runStatusEntity.setRevision(revisionDto);

            return getRequestBuilder(target).put(
                    Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessorEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity submitConfigVerificationRequest(final VerifyConfigRequestEntity configRequestEntity) throws NiFiClientException, IOException {
        if (configRequestEntity == null) {
            throw new IllegalArgumentException("Config Request Entity cannot be null");
        }
        if (configRequestEntity.getRequest() == null) {
            throw new IllegalArgumentException("Config Request DTO cannot be null");
        }
        if (configRequestEntity.getRequest().getComponentId() == null) {
            throw new IllegalArgumentException("Processor ID cannot be null");
        }
        if (configRequestEntity.getRequest().getProperties() == null) {
            throw new IllegalArgumentException("Processor properties cannot be null");
        }

        return executeAction("Error submitting Config Verification Request", () -> {
            final WebTarget target = processorTarget
                    .path("/config/verification-requests")
                    .resolveTemplate("id", configRequestEntity.getRequest().getComponentId());

            return getRequestBuilder(target).post(
                    Entity.entity(configRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity getConfigVerificationRequest(final String processorId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error retrieving Config Verification Request", () -> {
            final WebTarget target = processorTarget
                    .path("/config/verification-requests/{requestId}")
                    .resolveTemplate("id", processorId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).get(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity deleteConfigVerificationRequest(final String processorId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error deleting Config Verification Request", () -> {
            final WebTarget target = processorTarget
                    .path("/config/verification-requests/{requestId}")
                    .resolveTemplate("id", processorId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).delete(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public PropertyDescriptorEntity getPropertyDescriptor(final String processorId, final String propertyName, final Boolean sensitive) throws NiFiClientException, IOException {
        Objects.requireNonNull(processorId, "Processor ID required");
        Objects.requireNonNull(propertyName, "Property Name required");

        return executeAction("Error retrieving Property Descriptor", () -> {
            final WebTarget target = processorTarget
                    .path("/descriptors")
                    .resolveTemplate("id", processorId)
                    .queryParam("propertyName", propertyName)
                    .queryParam("sensitive", sensitive);

            return getRequestBuilder(target).get(PropertyDescriptorEntity.class);
        });
    }

    @Override
    public ProcessorEntity terminateProcessor(final String processorId) throws NiFiClientException, IOException {
        Objects.requireNonNull(processorId, "Processor ID required");

        return executeAction("Error terminating Processor", () -> {
            final WebTarget target = processorTarget
                    .path("/threads")
                    .resolveTemplate("id", processorId);

            return getRequestBuilder(target).delete(ProcessorEntity.class);
        });
    }

    @Override
    public ComponentStateEntity clearProcessorState(String processorId) throws NiFiClientException, IOException {
        Objects.requireNonNull(processorId, "Processor ID required");

        return executeAction("Error clearing state of the Processor", () -> {
            final WebTarget target = processorTarget
                .path("/state/clear-requests")
                .resolveTemplate("id", processorId);

            return getRequestBuilder(target).post(null, ComponentStateEntity.class);
        });
    }
}
