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
import org.apache.nifi.toolkit.cli.impl.client.nifi.RemoteProcessGroupClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class JerseyRemoteProcessGroupClient extends AbstractJerseyClient implements RemoteProcessGroupClient {
    private final WebTarget processGroupTarget;
    private final WebTarget rpgTarget;

    public JerseyRemoteProcessGroupClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyRemoteProcessGroupClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.processGroupTarget = baseTarget.path("/process-groups/{pgId}");
        this.rpgTarget = baseTarget.path("/remote-process-groups/{id}");
    }

    @Override
    public RemoteProcessGroupEntity createRemoteProcessGroup(final String parentGroupId, final RemoteProcessGroupEntity entity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentGroupId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (entity == null) {
            throw new IllegalArgumentException("Remote Process Group entity cannot be null");
        }

        return executeAction("Error creating Remote Process Group", () -> {
            final WebTarget target = processGroupTarget
                .path("/remote-process-groups")
                .resolveTemplate("pgId", parentGroupId);

            return getRequestBuilder(target).post(
                Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                RemoteProcessGroupEntity.class
            );
        });
    }

    @Override
    public RemoteProcessGroupEntity getRemoteProcessGroup(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Remote Process Group ID cannot be null");
        }

        return executeAction("Error retrieving status of Remote Process Group", () -> {
            final WebTarget target = rpgTarget.resolveTemplate("id", id);
            return getRequestBuilder(target).get(RemoteProcessGroupEntity.class);
        });
    }

    @Override
    public RemoteProcessGroupEntity updateRemoteProcessGroup(final RemoteProcessGroupEntity entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException("Remote Process Group entity cannot be null");
        }

        return executeAction("Error updating Remote Process Group", () -> {
            final WebTarget target = rpgTarget.resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                RemoteProcessGroupEntity.class
            );
        });
    }

    @Override
    public RemoteProcessGroupEntity deleteRemoteProcessGroup(final RemoteProcessGroupEntity entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException("Remote Process Group entity cannot be null");
        }

        if (entity.getId() == null) {
            throw new IllegalArgumentException("Remote Process Group ID cannot be null");
        }

        final RevisionDTO revision = entity.getRevision();
        if (revision == null) {
            throw new IllegalArgumentException("Revision cannot be null");
        }

        return executeAction("Error deleting Remote Process Group", () -> {
            final WebTarget target = rpgTarget
                .queryParam("version", revision.getVersion())
                .queryParam("clientId", revision.getClientId())
                .resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).delete(RemoteProcessGroupEntity.class);
        });
    }

    @Override
    public RemoteProcessGroupEntity startTransmitting(final RemoteProcessGroupEntity entity) throws NiFiClientException, IOException {
        return updateTransmitting(entity, true);
    }

    @Override
    public RemoteProcessGroupEntity stopTransmitting(final RemoteProcessGroupEntity entity) throws NiFiClientException, IOException {
        return updateTransmitting(entity, false);
    }

    private RemoteProcessGroupEntity updateTransmitting(final RemoteProcessGroupEntity entity, final boolean transmitting) throws NiFiClientException, IOException {
        final RemoteProcessGroupDTO component = new RemoteProcessGroupDTO();
        component.setId(entity.getComponent().getId());
        component.setParentGroupId(entity.getComponent().getParentGroupId());
        component.setTransmitting(transmitting);

        final RemoteProcessGroupEntity transmittingEntity = new RemoteProcessGroupEntity();
        transmittingEntity.setId(entity.getId());
        transmittingEntity.setRevision(entity.getRevision());
        transmittingEntity.setComponent(component);

        return updateRemoteProcessGroup(transmittingEntity);
    }
}
