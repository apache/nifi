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
import org.apache.nifi.toolkit.client.RequestConfig;
import org.apache.nifi.toolkit.client.VersionsClient;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;

import java.io.IOException;

/**
 * Jersey implementation of VersionsClient.
 */
public class JerseyVersionsClient extends AbstractJerseyClient implements VersionsClient {

    private final WebTarget versionsTarget;

    public JerseyVersionsClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyVersionsClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.versionsTarget = baseTarget.path("/versions");
    }

    // GET /versions/process-groups/id

    @Override
    public VersionControlInformationEntity getVersionControlInfo(final String processGroupId) throws IOException, NiFiClientException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error getting version control info", () -> {
            final WebTarget target = versionsTarget
                    .path("process-groups/{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).get(VersionControlInformationEntity.class);
        });
    }

    // POST /versions/update-requests/process-groups/id

    @Override
    public VersionedFlowUpdateRequestEntity updateVersionControlInfo(final String processGroupId, final VersionControlInformationEntity entity)
            throws IOException, NiFiClientException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (entity == null) {
            throw new IllegalArgumentException("Version control information entity cannot be null");
        }

        return executeAction("Error updating version control information", () -> {
            final WebTarget target = versionsTarget
                    .path("update-requests/process-groups/{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    VersionedFlowUpdateRequestEntity.class);
        });
    }

    // GET /versions/update-requests/id

    @Override
    public VersionedFlowUpdateRequestEntity getUpdateRequest(final String updateRequestId) throws IOException, NiFiClientException {
        if (StringUtils.isBlank(updateRequestId)) {
            throw new IllegalArgumentException("Update request id cannot be null or blank");
        }

        return executeAction("Error getting update request", () -> {
            final WebTarget target = versionsTarget
                    .path("update-requests/{id}")
                    .resolveTemplate("id", updateRequestId);

            return getRequestBuilder(target).get(VersionedFlowUpdateRequestEntity.class);
        });
    }

    // DELETE /versions/update-requests/id

    @Override
    public VersionedFlowUpdateRequestEntity deleteUpdateRequest(final String updateRequestId) throws IOException, NiFiClientException {
        if (StringUtils.isBlank(updateRequestId)) {
            throw new IllegalArgumentException("Update request id cannot be null or blank");
        }

        return executeAction("Error deleting update request", () -> {
            final WebTarget target = versionsTarget
                    .path("update-requests/{id}")
                    .resolveTemplate("id", updateRequestId);

            return getRequestBuilder(target).delete(VersionedFlowUpdateRequestEntity.class);
        });
    }

    @Override
    public VersionControlInformationEntity startVersionControl(final String processGroupId, final StartVersionControlRequestEntity startVersionControlRequestEntity)
            throws IOException, NiFiClientException {

        if (startVersionControlRequestEntity == null) {
            throw new IllegalArgumentException("Request Entity cannot be null");
        }

        return executeAction("Error starting version control", () -> {
            final WebTarget target = versionsTarget
                    .path("process-groups/{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(Entity.entity(startVersionControlRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    VersionControlInformationEntity.class);
        });
    }

    // DELETE /versions/process-groups/{id}

    @Override
    public VersionControlInformationEntity stopVersionControl(ProcessGroupEntity processGroupEntity) throws IOException, NiFiClientException {
        final String pgId = processGroupEntity.getId();

        if (StringUtils.isBlank(pgId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error stopping version control", () -> {
            final WebTarget target = versionsTarget
                    .path("process-groups/{id}")
                    .queryParam("version", processGroupEntity.getRevision().getVersion())
                    .resolveTemplate("id", pgId);

            return getRequestBuilder(target).delete(VersionControlInformationEntity.class);
        });
    }

    // POST /versions/revert-requests/process-groups/id

    @Override
    public VersionedFlowUpdateRequestEntity initiateRevertFlowVersion(final String processGroupId, final VersionControlInformationEntity versionControlInformation)
            throws IOException, NiFiClientException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (versionControlInformation == null) {
            throw new IllegalArgumentException("Version control information entity cannot be null");
        }

        return executeAction("Error reverting flow version", () -> {
            final WebTarget target = versionsTarget
                    .path("revert-requests/process-groups/{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(versionControlInformation, MediaType.APPLICATION_JSON_TYPE),
                    VersionedFlowUpdateRequestEntity.class);
        });
    }

    // GET /versions/revert-requests/process-groups/id

    @Override
    public VersionedFlowUpdateRequestEntity getRevertFlowVersionRequest(final String requestId) throws IOException, NiFiClientException {
        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Update request id cannot be null or blank");
        }

        return executeAction("Error getting revert request", () -> {
            final WebTarget target = versionsTarget
                    .path("revert-requests/{id}")
                    .resolveTemplate("id", requestId);

            return getRequestBuilder(target).get(VersionedFlowUpdateRequestEntity.class);
        });

    }

    // DELETE /versions/revert-requests/process-groups/id

    @Override
    public VersionedFlowUpdateRequestEntity deleteRevertFlowVersionRequest(final String requestId) throws IOException, NiFiClientException {
        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Update request id cannot be null or blank");
        }

        return executeAction("Error deleting revert request", () -> {
            final WebTarget target = versionsTarget
                    .path("revert-requests/{id}")
                    .resolveTemplate("id", requestId);

            return getRequestBuilder(target).delete(VersionedFlowUpdateRequestEntity.class);
        });
    }
}
