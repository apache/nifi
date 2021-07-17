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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerServicesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

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
                    .path("{id}/run-status").resolveTemplate("id", id);
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
                ControllerServiceEntity.class
            );
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
                ControllerServiceEntity.class
            );
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
            final WebTarget target = controllerServicesTarget
                .path("/{id}")
                .queryParam("version", revision.getVersion())
                .queryParam("clientId", revision.getClientId())
                .resolveTemplate("id", controllerServiceEntity.getId());

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
                ControllerServiceReferencingComponentsEntity.class
            );
        });

    }
}
