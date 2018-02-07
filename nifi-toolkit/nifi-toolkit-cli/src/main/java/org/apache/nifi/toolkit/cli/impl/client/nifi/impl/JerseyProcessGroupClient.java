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
import org.apache.nifi.toolkit.cli.impl.client.nifi.ProcessGroupClient;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.VariableRegistryEntity;
import org.apache.nifi.web.api.entity.VariableRegistryUpdateRequestEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Jersey implementation of ProcessGroupClient.
 */
public class JerseyProcessGroupClient extends AbstractJerseyClient implements ProcessGroupClient {

    private final WebTarget processGroupsTarget;

    public JerseyProcessGroupClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyProcessGroupClient(final WebTarget baseTarget, final Map<String,String> headers) {
        super(headers);
        this.processGroupsTarget = baseTarget.path("/process-groups");
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final String parentGroupdId, final ProcessGroupEntity entity)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (entity == null){
            throw new IllegalArgumentException("Process group entity cannot be null");
        }

        return executeAction("Error creating process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/process-groups")
                    .resolveTemplate("id", parentGroupdId);

            return getRequestBuilder(target).post(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessGroupEntity.class
            );
        });
    }

    @Override
    public ProcessGroupEntity getProcessGroup(final String processGroupId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error getting process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).get(ProcessGroupEntity.class);
        });
    }

    @Override
    public ProcessGroupEntity updateProcessGroup(final ProcessGroupEntity entity)
            throws NiFiClientException, IOException {

        if (entity == null){
            throw new IllegalArgumentException("Process group entity cannot be null");
        }

        return executeAction("Error updating process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}")
                    .resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessGroupEntity.class
            );
        });
    }

    @Override
    public VariableRegistryEntity getVariables(final String processGroupId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        return executeAction("Error getting variables for process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/variable-registry")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).get(VariableRegistryEntity.class);
        });
    }

    @Override
    public VariableRegistryUpdateRequestEntity updateVariableRegistry(
            final String processGroupId, final VariableRegistryEntity variableRegistryEntity)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (variableRegistryEntity == null) {
            throw new IllegalArgumentException("Variable registry entity cannot be null");
        }

        return executeAction("Error getting variable registry update request", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{processGroupId}/variable-registry/update-requests")
                    .resolveTemplate("processGroupId", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(variableRegistryEntity, MediaType.APPLICATION_JSON_TYPE),
                    VariableRegistryUpdateRequestEntity.class
            );
        });
    }

    @Override
    public VariableRegistryUpdateRequestEntity getVariableRegistryUpdateRequest(
            final String processGroupId, final String requestId) throws NiFiClientException, IOException {

        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Request id cannot be null or blank");
        }

        return executeAction("Error getting variable registry update request", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{processGroupId}/variable-registry/update-requests/{updateId}")
                    .resolveTemplate("processGroupId", processGroupId)
                    .resolveTemplate("updateId", requestId);

            return getRequestBuilder(target).get(VariableRegistryUpdateRequestEntity.class);
        });
    }

    @Override
    public VariableRegistryUpdateRequestEntity deleteVariableRegistryUpdateRequest(
            final String processGroupId, final String requestId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Request id cannot be null or blank");
        }

        return executeAction("Error getting variable registry update request", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{processGroupId}/variable-registry/update-requests/{updateId}")
                    .resolveTemplate("processGroupId", processGroupId)
                    .resolveTemplate("updateId", requestId);

            return getRequestBuilder(target).delete(VariableRegistryUpdateRequestEntity.class);
        });
    }
}
