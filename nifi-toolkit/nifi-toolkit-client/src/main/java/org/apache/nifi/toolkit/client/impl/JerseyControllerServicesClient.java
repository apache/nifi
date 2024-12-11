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
import org.apache.nifi.toolkit.client.ControllerServicesClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;

import java.io.IOException;
import java.util.Objects;

/**
 * Jersey implementation of ControllerServicersClient.
 */
public class JerseyControllerServicesClient extends AbstractJerseyClient implements ControllerServicesClient {

    private final WebTarget controllerServicesTarget;
    private final WebTarget processGroupTarget;

    public JerseyControllerServicesClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyControllerServicesClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.controllerServicesTarget = baseTarget.path("/controller-services");
        this.processGroupTarget = baseTarget.path("/process-groups/{pgId}");
    }

    @Override
    public ControllerServiceEntity getControllerService(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Controller service id cannot be null");
        }

        return executeAction("Error retrieving status of controller service", () -> {
            final WebTarget target = controllerServicesTarget.path("{id}").resolveTemplate("id", id);
            return getRequestBuilder(target).get(ControllerServiceEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity activateControllerService(final String id,
            final ControllerServiceRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Controller service id cannot be null");
        }

        if (runStatusEntity == null) {
            throw new IllegalArgumentException("Entity cannnot be null");
        }

        return executeAction("Error enabling or disabling controller service", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("{id}/run-status")
                    .resolveTemplate("id", id);
            return getRequestBuilder(target).put(
                    Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                    ControllerServiceEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity createControllerService(final String parentGroupdId, final ControllerServiceEntity controllerServiceEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (controllerServiceEntity == null) {
            throw new IllegalArgumentException("Controller Service entity cannot be null");
        }

        return executeAction("Error creating Controller Service", () -> {
            final WebTarget target = processGroupTarget
                    .path("/controller-services")
                    .resolveTemplate("pgId", parentGroupdId);

            return getRequestBuilder(target).post(
                    Entity.entity(controllerServiceEntity, MediaType.APPLICATION_JSON_TYPE),
                    ControllerServiceEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity updateControllerService(final ControllerServiceEntity controllerServiceEntity) throws NiFiClientException, IOException {
        if (controllerServiceEntity == null) {
            throw new IllegalArgumentException("Controller Service entity cannot be null");
        }

        return executeAction("Error updating Controller Service", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("/{id}")
                    .resolveTemplate("id", controllerServiceEntity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(controllerServiceEntity, MediaType.APPLICATION_JSON_TYPE),
                    ControllerServiceEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity deleteControllerService(final ControllerServiceEntity controllerServiceEntity) throws NiFiClientException, IOException {
        if (controllerServiceEntity == null) {
            throw new IllegalArgumentException("Controller Service entity cannot be null");
        }

        final RevisionDTO revision = controllerServiceEntity.getRevision();
        if (revision == null) {
            throw new IllegalArgumentException("Controller Service Revision cannot be null");
        }

        return executeAction("Error deleting Controller Service", () -> {
            WebTarget target = controllerServicesTarget
                    .path("/{id}")
                    .queryParam("version", revision.getVersion())
                    .queryParam("clientId", revision.getClientId())
                    .resolveTemplate("id", controllerServiceEntity.getId());

            if (controllerServiceEntity.isDisconnectedNodeAcknowledged() == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ControllerServiceEntity.class);
        });
    }

    @Override
    public ControllerServiceReferencingComponentsEntity getControllerServiceReferences(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Controller service id cannot be null");
        }

        return executeAction("Error retrieving Controller Service's referencing components", () -> {
            final WebTarget target = controllerServicesTarget.path("{id}/references").resolveTemplate("id", id);
            return getRequestBuilder(target).get(ControllerServiceReferencingComponentsEntity.class);
        });
    }

    @Override
    public ControllerServiceReferencingComponentsEntity updateControllerServiceReferences(final UpdateControllerServiceReferenceRequestEntity referencesEntity)
            throws NiFiClientException, IOException {

        if (referencesEntity == null) {
            throw new IllegalArgumentException("Controller Service references entity cannot be null");
        }

        return executeAction("Error updating Controller Service references", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("/{id}/references")
                    .resolveTemplate("id", referencesEntity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(referencesEntity, MediaType.APPLICATION_JSON_TYPE),
                    ControllerServiceReferencingComponentsEntity.class);
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
            throw new IllegalArgumentException("Controller Service ID cannot be null");
        }
        if (configRequestEntity.getRequest().getProperties() == null) {
            throw new IllegalArgumentException("Controller Service properties cannot be null");
        }

        return executeAction("Error submitting Config Verification Request", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("{id}/config/verification-requests")
                    .resolveTemplate("id", configRequestEntity.getRequest().getComponentId());

            return getRequestBuilder(target).post(
                    Entity.entity(configRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VerifyConfigRequestEntity.class);
        });

    }

    @Override
    public VerifyConfigRequestEntity getConfigVerificationRequest(final String serviceId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error retrieving Config Verification Request", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", serviceId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).get(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity deleteConfigVerificationRequest(final String serviceId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error deleting Config Verification Request", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", serviceId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).delete(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public PropertyDescriptorEntity getPropertyDescriptor(final String serviceId, final String propertyName, final Boolean sensitive) throws NiFiClientException, IOException {
        Objects.requireNonNull(serviceId, "Service ID required");
        Objects.requireNonNull(propertyName, "Property Name required");

        return executeAction("Error retrieving Property Descriptor", () -> {
            final WebTarget target = controllerServicesTarget
                    .path("{id}/descriptors")
                    .resolveTemplate("id", serviceId)
                    .queryParam("propertyName", propertyName)
                    .queryParam("sensitive", sensitive);

            return getRequestBuilder(target).get(PropertyDescriptorEntity.class);
        });
    }
}
