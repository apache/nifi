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
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.VersionsClient;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Jersey implementation of VersionsClient.
 */
public class JerseyVersionsClient extends AbstractJerseyClient implements VersionsClient {

    private final WebTarget versionsTarget;

    public JerseyVersionsClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyVersionsClient(final WebTarget baseTarget, final Map<String,String> headers) {
        super(headers);
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
                    VersionedFlowUpdateRequestEntity.class
            );
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
}
