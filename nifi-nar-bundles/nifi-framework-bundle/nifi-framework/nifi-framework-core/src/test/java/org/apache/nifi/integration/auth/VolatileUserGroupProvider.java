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
package org.apache.nifi.integration.auth;

import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class VolatileUserGroupProvider implements UserGroupProvider {
    private final Map<String, User> users = new HashMap<>();
    private final Map<String, Group> groups = new HashMap<>();
    private final Map<User, Set<Group>> userGroupMapping = new HashMap<>();

    public void addUser(final User user) {
        this.users.put(user.getIdentifier(), user);
    }

    public void addGroup(final Group group) {
        this.groups.put(group.getIdentifier(), group);
    }

    public void addUserToGroup(final User user, final Group group) {
        userGroupMapping.computeIfAbsent(user, i -> new HashSet<>()).add(group);
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return new HashSet<>(users.values());
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        return users.get(identifier);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        return users.values().stream()
            .filter(user -> Objects.equals(identity, user.getIdentity()))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return new HashSet<>(groups.values());
    }

    @Override
    public Group getGroup(final String identifier) throws AuthorizationAccessException {
        return groups.get(identifier);
    }

    @Override
    public UserAndGroups getUserAndGroups(final String identity) throws AuthorizationAccessException {
        final User user = getUserByIdentity(identity);
        final Set<Group> groups = userGroupMapping.get(user);
        final Set<Group> groupCopy = groups == null ? Collections.emptySet() : new HashSet<>(groups);

        return new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return groupCopy;
            }
        };
    }

    @Override
    public void initialize(final UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}
