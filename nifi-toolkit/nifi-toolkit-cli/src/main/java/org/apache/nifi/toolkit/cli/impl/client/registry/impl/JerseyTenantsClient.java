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
package org.apache.nifi.toolkit.cli.impl.client.registry.impl;

import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.AbstractJerseyClient;
import org.apache.nifi.toolkit.cli.impl.client.registry.TenantsClient;
import org.apache.nifi.util.StringUtils;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JerseyTenantsClient extends AbstractJerseyClient implements TenantsClient {
    private final WebTarget tenantsTarget;

    public JerseyTenantsClient(final WebTarget baseTarget, final Map<String, String> headers) {
        super(headers);
        this.tenantsTarget = baseTarget.path("/tenants");
    }

    public JerseyTenantsClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    @Override
    public List<User> getUsers() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving users", () -> {
            final WebTarget target = tenantsTarget.path("users");
            return Arrays.asList(getRequestBuilder(target).get(User[].class));
        });
    }

    @Override
    public User getUser(final String id) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("User id cannot be null");
        }

        return executeAction("Error retrieving user", () -> {
            final WebTarget target = tenantsTarget.path("users/{id}").resolveTemplate("id", id);
            return getRequestBuilder(target).get(User.class);
        });
    }

    @Override
    public User createUser(final User user) throws NiFiRegistryException, IOException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        return executeAction("Error creating user", () -> {
            final WebTarget target = tenantsTarget.path("users");

            return getRequestBuilder(target).post(
                Entity.entity(user, MediaType.APPLICATION_JSON_TYPE), User.class
            );
        });
    }

    @Override
    public User updateUser(final User user) throws NiFiRegistryException, IOException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        return executeAction("Error updating user", () -> {
            final WebTarget target = tenantsTarget.path("users/{id}").resolveTemplate("id", user.getIdentifier());

            return getRequestBuilder(target).put(
                    Entity.entity(user, MediaType.APPLICATION_JSON_TYPE), User.class
            );
        });
    }

    @Override
    public List<UserGroup> getUserGroups() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving users", () -> {
            final WebTarget target = tenantsTarget.path("user-groups");
            return Arrays.asList(getRequestBuilder(target).get(UserGroup[].class));
        });
    }

    @Override
    public UserGroup getUserGroup(final String id) throws NiFiRegistryException, IOException {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("User group id cannot be null");
        }

        return executeAction("Error retrieving user group", () -> {
            final WebTarget target = tenantsTarget.path("user-groups/{id}").resolveTemplate("id", id);
            return getRequestBuilder(target).get(UserGroup.class);
        });
    }

    @Override
    public UserGroup createUserGroup(final UserGroup group) throws NiFiRegistryException, IOException {
        if (group == null) {
            throw new IllegalArgumentException("User group cannot be null");
        }

        return executeAction("Error creating group", () -> {
            final WebTarget target = tenantsTarget.path("user-groups");

            return getRequestBuilder(target).post(
                    Entity.entity(group, MediaType.APPLICATION_JSON_TYPE), UserGroup.class
            );
        });
    }

    @Override
    public UserGroup updateUserGroup(final UserGroup group) throws NiFiRegistryException, IOException {
        if (group == null) {
            throw new IllegalArgumentException("User group cannot be null");
        }

        return executeAction("Error creating group", () -> {
            final WebTarget target = tenantsTarget.path("user-groups/{id}").resolveTemplate("id", group.getIdentifier());

            return getRequestBuilder(target).put(
                    Entity.entity(group, MediaType.APPLICATION_JSON_TYPE), UserGroup.class
            );
        });
    }
}
