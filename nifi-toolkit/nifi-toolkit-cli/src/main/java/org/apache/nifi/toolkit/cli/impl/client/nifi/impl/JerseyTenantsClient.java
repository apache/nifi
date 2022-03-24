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

import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.RequestConfig;
import org.apache.nifi.toolkit.cli.impl.client.nifi.TenantsClient;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.UserGroupsEntity;
import org.apache.nifi.web.api.entity.UsersEntity;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Jersey implementation of TenantsClient.
 */
public class JerseyTenantsClient extends AbstractJerseyClient implements TenantsClient {

    private final WebTarget tenantsTarget;

    public JerseyTenantsClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyTenantsClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.tenantsTarget = baseTarget.path("/tenants");
    }

    @Override
    public UsersEntity getUsers() throws NiFiClientException, IOException {
        return executeAction("Error retrieving users", () -> {
            final WebTarget target = tenantsTarget.path("users");
            return getRequestBuilder(target).get(UsersEntity.class);
        });
    }

    @Override
    public UserEntity createUser(final UserEntity userEntity) throws NiFiClientException, IOException {
        if (userEntity == null) {
            throw new IllegalArgumentException("User entity cannot be null");
        }

        return executeAction("Error creating user", () -> {
            final WebTarget target = tenantsTarget.path("users");

            return getRequestBuilder(target).post(
                    Entity.entity(userEntity, MediaType.APPLICATION_JSON),
                    UserEntity.class
            );
        });
    }

    @Override
    public UserGroupEntity getUserGroup(final String id) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("User group id cannot be null");
        }

        return executeAction("Error retrieving user group", () -> {
            final WebTarget target = tenantsTarget
                    .path("user-groups/{id}")
                    .resolveTemplate("id", id);
            return getRequestBuilder(target).get(UserGroupEntity.class);
        });
    }

    @Override
    public UserGroupsEntity getUserGroups() throws NiFiClientException, IOException {
        return executeAction("Error retrieving user groups", () -> {
            final WebTarget target = tenantsTarget.path("user-groups");
            return getRequestBuilder(target).get(UserGroupsEntity.class);
        });
    }

    @Override
    public UserGroupEntity createUserGroup(final UserGroupEntity userGroupEntity) throws NiFiClientException, IOException {
        if (userGroupEntity == null) {
            throw new IllegalArgumentException("User group entity cannot be null");
        }

        return executeAction("Error creating user group", () -> {
            final WebTarget target = tenantsTarget.path("user-groups");

            return getRequestBuilder(target).post(
                    Entity.entity(userGroupEntity, MediaType.APPLICATION_JSON),
                    UserGroupEntity.class
            );
        });
    }

    @Override
    public UserGroupEntity updateUserGroup(UserGroupEntity userGroupEntity) throws NiFiClientException, IOException {
        if (userGroupEntity == null) {
            throw new IllegalArgumentException("User group entity cannot be null");
        }

        if (StringUtils.isBlank(userGroupEntity.getId())) {
            throw new IllegalArgumentException("User group entity must contain an id");
        }

        return executeAction("Error updating user group", () -> {
            final WebTarget target = tenantsTarget
                    .path("user-groups/{id}")
                    .resolveTemplate("id", userGroupEntity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(userGroupEntity, MediaType.APPLICATION_JSON),
                    UserGroupEntity.class
            );
        });
    }
}
