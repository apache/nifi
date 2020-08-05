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
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class CRUDJerseyClient<T extends ComponentEntity> extends AbstractJerseyClient {
    private final WebTarget creationTarget;
    private final WebTarget accessTarget;
    private final Class<T> entityType;
    private final String componentType;


    public CRUDJerseyClient(final WebTarget creationTarget, final WebTarget accessTarget, final RequestConfig requestConfig,
                            final Class<T> entityType, final String componentType) {
        super(requestConfig);
        this.entityType = entityType;
        this.componentType = componentType;

        this.creationTarget = creationTarget;
        this.accessTarget = accessTarget;
    }


    protected T createComponent(final String parentGroupdId, final T entity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (entity == null) {
            throw new IllegalArgumentException("Entity cannot be null");
        }

        return executeAction("Error creating " + componentType, () -> {
            final WebTarget target = creationTarget
                .resolveTemplate("pgId", parentGroupdId);

            return getRequestBuilder(target).post(
                Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                entityType
            );
        });
    }


    protected T getComponent(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException(componentType + " ID cannot be null");
        }

        return executeAction("Error retrieving status of " + componentType, () -> {
            final WebTarget target = accessTarget.resolveTemplate("id", id);
            return getRequestBuilder(target).get(entityType);
        });
    }


    protected T updateComponent(final T entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException(componentType + " entity cannot be null");
        }

        return executeAction("Error updating " + componentType, () -> {
            final WebTarget target = accessTarget.resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                entityType
            );
        });
    }

    public T deleteComponent(final T entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException(componentType + " entity cannot be null");
        }

        if (entity.getId() == null) {
            throw new IllegalArgumentException(componentType + " ID cannot be null");
        }

        final RevisionDTO revision = entity.getRevision();
        if (revision == null) {
            throw new IllegalArgumentException("Revision cannot be null");
        }

        return executeAction("Error deleting " + componentType, () -> {
            final WebTarget target = accessTarget
                .queryParam("version", revision.getVersion())
                .queryParam("clientId", revision.getClientId())
                .resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).delete(entityType);
        });
    }

}
