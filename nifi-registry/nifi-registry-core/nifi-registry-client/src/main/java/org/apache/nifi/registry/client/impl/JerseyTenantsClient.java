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

import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.RequestConfig;
import org.apache.nifi.registry.client.TenantsClient;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class JerseyTenantsClient extends AbstractCRUDJerseyClient implements TenantsClient {
    public static final String USER = "User";
    public static final String USERS_PATH = "users";

    public static final String USER_GROUP = "User group";
    public static final String USER_GROUPS_PATH = "user-groups";

    public JerseyTenantsClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyTenantsClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(baseTarget.path("/tenants"), requestConfig);
    }

    @Override
    public List<User> getUsers() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving users", () -> {
            final WebTarget target = baseTarget.path(USERS_PATH);
            return Arrays.asList(getRequestBuilder(target).get(User[].class));
        });
    }

    @Override
    public User getUser(final String id) throws NiFiRegistryException, IOException {
        return get(id, User.class, USER, USERS_PATH);
    }

    @Override
    public User createUser(final User user) throws NiFiRegistryException, IOException {
        return create(user, User.class, USER, USERS_PATH);
    }

    @Override
    public User updateUser(final User user) throws NiFiRegistryException, IOException {
        return update(user, user.getIdentifier(), User.class, USER, USERS_PATH);
    }

    @Override
    public User deleteUser(final String id) throws NiFiRegistryException, IOException {
        return delete(id, null, User.class, USER, USERS_PATH);
    }

    @Override
    public User deleteUser(final String id, final RevisionInfo revisionInfo) throws NiFiRegistryException, IOException {
        return delete(id, revisionInfo, User.class, USER, USERS_PATH);
    }

    @Override
    public List<UserGroup> getUserGroups() throws NiFiRegistryException, IOException {
        return executeAction("Error retrieving users", () -> {
            final WebTarget target = baseTarget.path(USER_GROUPS_PATH);
            return Arrays.asList(getRequestBuilder(target).get(UserGroup[].class));
        });
    }

    @Override
    public UserGroup getUserGroup(final String id) throws NiFiRegistryException, IOException {
        return get(id, UserGroup.class, USER_GROUP, USER_GROUPS_PATH);
    }

    @Override
    public UserGroup createUserGroup(final UserGroup group) throws NiFiRegistryException, IOException {
        return create(group, UserGroup.class, USER_GROUP, USER_GROUPS_PATH);
    }

    @Override
    public UserGroup updateUserGroup(final UserGroup group) throws NiFiRegistryException, IOException {
        return update(group, group.getIdentifier(), UserGroup.class, USER_GROUP, USER_GROUPS_PATH);
    }

    @Override
    public UserGroup deleteUserGroup(final String id) throws NiFiRegistryException, IOException {
        return delete(id, null, UserGroup.class, USER_GROUP, USER_GROUPS_PATH);
    }

    @Override
    public UserGroup deleteUserGroup(final String id, final RevisionInfo revisionInfo) throws NiFiRegistryException, IOException {
        return delete(id, revisionInfo, UserGroup.class, USER_GROUP, USER_GROUPS_PATH);
    }

}
