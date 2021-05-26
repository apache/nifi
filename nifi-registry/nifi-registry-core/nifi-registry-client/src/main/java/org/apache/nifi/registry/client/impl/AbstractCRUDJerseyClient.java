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
package org.apache.nifi.registry.client.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

public class AbstractCRUDJerseyClient extends AbstractJerseyClient {

    protected final WebTarget baseTarget;

    public AbstractCRUDJerseyClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.baseTarget = baseTarget;
    }

    protected <T> T get(
            String id,
            Class<T> entityType,
            String entityTypeName,
            String entityPath
    ) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException(entityTypeName + " id cannot be blank");
        }

        return executeAction("Error retrieving " + entityTypeName.toLowerCase(), () -> {
            final WebTarget target = baseTarget.path(entityPath).path(id);
            return getRequestBuilder(target).get(entityType);
        });
    }

    protected <T> T create(
            T entity,
            Class<T> entityType,
            String entityTypeName,
            String entityPath
    ) throws NiFiRegistryException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException(entityTypeName + " cannot be null");
        }

        return executeAction("Error creating " + entityTypeName.toLowerCase(), () -> {
            final WebTarget target = baseTarget.path(entityPath);

            return getRequestBuilder(target).post(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE), entityType
            );
        });
    }

    protected <T> T update(
            T entity,
            String id,
            Class<T> entityType,
            String entityTypeName,
            String entityPath
    ) throws NiFiRegistryException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException(entityTypeName + " cannot be null");
        }

        return executeAction("Error updating " + entityTypeName.toLowerCase(), () -> {
            final WebTarget target = baseTarget.path(entityPath).path(id);

            return getRequestBuilder(target).put(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE), entityType
            );
        });
    }

    protected <T> T delete(
            String id,
            RevisionInfo revisionInfo,
            Class<T> entityType,
            String entityTypeName,
            String entityPath
    ) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException(entityTypeName + " id cannot be blank");
        }

        return executeAction("Error deleting " + entityTypeName.toLowerCase(), () -> {
            WebTarget target = baseTarget.path(entityPath).path(id);
            target = addRevisionQueryParams(target, revisionInfo);

            return getRequestBuilder(target).delete(entityType);
        });
    }
}
