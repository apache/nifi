/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.nifi.toolkit.client.ReportingTasksClient;
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskRunStatusEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;

import java.io.IOException;
import java.util.Objects;

/**
 * Jersey implementation of ReportingTasksClient.
 */
public class JerseyReportingTasksClient extends AbstractJerseyClient implements ReportingTasksClient {

    private final WebTarget reportingTasksTarget;

    public JerseyReportingTasksClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyReportingTasksClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.reportingTasksTarget = baseTarget.path("/reporting-tasks");
    }

    @Override
    public ReportingTaskEntity getReportingTask(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Reporting task id cannot be null");
        }

        return executeAction("Error retrieving status of reporting task", () -> {
            final WebTarget target = reportingTasksTarget.path(id);
            return getRequestBuilder(target).get(ReportingTaskEntity.class);
        });
    }

    @Override
    public ReportingTaskEntity updateReportingTask(final ReportingTaskEntity reportingTaskEntity) throws NiFiClientException, IOException {
        if (reportingTaskEntity == null) {
            throw new IllegalArgumentException("Reporting Task cannot be null");
        }
        if (reportingTaskEntity.getComponent() == null) {
            throw new IllegalArgumentException("Component cannot be null");
        }

        return executeAction("Error updating Reporting Task", () -> {
            final WebTarget target = reportingTasksTarget.path(reportingTaskEntity.getId());
            return getRequestBuilder(target).put(
                    Entity.entity(reportingTaskEntity, MediaType.APPLICATION_JSON_TYPE),
                    ReportingTaskEntity.class);
        });
    }

    @Override
    public ReportingTaskEntity activateReportingTask(final String id,
            final ReportingTaskRunStatusEntity runStatusEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Reporting task id cannot be null");
        }

        if (runStatusEntity == null) {
            throw new IllegalArgumentException("Entity cannnot be null");
        }

        return executeAction("Error starting or stopping report task", () -> {
            final WebTarget target = reportingTasksTarget
                    .path("{id}/run-status")
                    .resolveTemplate("id", id);
            return getRequestBuilder(target).put(
                    Entity.entity(runStatusEntity, MediaType.APPLICATION_JSON_TYPE),
                    ReportingTaskEntity.class);
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
            throw new IllegalArgumentException("Reporting Task ID cannot be null");
        }
        if (configRequestEntity.getRequest().getProperties() == null) {
            throw new IllegalArgumentException("Reporting Task properties cannot be null");
        }

        return executeAction("Error submitting Config Verification Request", () -> {
            final WebTarget target = reportingTasksTarget
                    .path("{id}/config/verification-requests")
                    .resolveTemplate("id", configRequestEntity.getRequest().getComponentId());

            return getRequestBuilder(target).post(
                    Entity.entity(configRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity getConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error retrieving Config Verification Request", () -> {
            final WebTarget target = reportingTasksTarget
                    .path("{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", taskId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).get(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public VerifyConfigRequestEntity deleteConfigVerificationRequest(final String taskId, final String verificationRequestId) throws NiFiClientException, IOException {
        if (verificationRequestId == null) {
            throw new IllegalArgumentException("Verification Request ID cannot be null");
        }

        return executeAction("Error deleting Config Verification Request", () -> {
            final WebTarget target = reportingTasksTarget
                    .path("{id}/config/verification-requests/{requestId}")
                    .resolveTemplate("id", taskId)
                    .resolveTemplate("requestId", verificationRequestId);

            return getRequestBuilder(target).delete(VerifyConfigRequestEntity.class);
        });
    }

    @Override
    public ReportingTaskEntity deleteReportingTask(final ReportingTaskEntity reportingTask) throws NiFiClientException, IOException {
        if (reportingTask == null) {
            throw new IllegalArgumentException("Reporting Task Entity cannot be null");
        }
        if (reportingTask.getId() == null) {
            throw new IllegalArgumentException("Reporting Task ID cannot be null");
        }

        final RevisionDTO revision = reportingTask.getRevision();
        if (revision == null) {
            throw new IllegalArgumentException("Revision cannot be null");
        }

        return executeAction("Error deleting Reporting Task", () -> {
            WebTarget target = reportingTasksTarget
                    .path("{id}")
                    .resolveTemplate("id", reportingTask.getId())
                    .queryParam("version", revision.getVersion())
                    .queryParam("clientId", revision.getClientId());

            if (reportingTask.isDisconnectedNodeAcknowledged() == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ReportingTaskEntity.class);
        });
    }

    @Override
    public PropertyDescriptorEntity getPropertyDescriptor(final String componentId, final String propertyName, final Boolean sensitive) throws NiFiClientException, IOException {
        Objects.requireNonNull(componentId, "Component ID required");
        Objects.requireNonNull(propertyName, "Property Name required");

        return executeAction("Error retrieving Property Descriptor", () -> {
            final WebTarget target = reportingTasksTarget
                    .path("{id}/descriptors")
                    .resolveTemplate("id", componentId)
                    .queryParam("propertyName", propertyName)
                    .queryParam("sensitive", sensitive);

            return getRequestBuilder(target).get(PropertyDescriptorEntity.class);
        });
    }
}
