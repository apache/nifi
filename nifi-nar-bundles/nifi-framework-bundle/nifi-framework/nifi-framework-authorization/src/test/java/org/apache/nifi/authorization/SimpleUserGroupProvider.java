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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.Set;
import java.util.stream.Collectors;

public class SimpleUserGroupProvider implements UserGroupProvider {

    private final Set<User> users;
    private final Set<Group> groups;

    public SimpleUserGroupProvider(Set<User> users, Set<Group> groups) {
        this.users = users;
        this.groups = groups;
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return users;
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        return users.stream().filter(user -> user.getIdentifier().equals(identifier)).findFirst().orElse(null);
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        return users.stream().filter(user -> user.getIdentity().equals(identity)).findFirst().orElse(null);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return groups;
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        return groups.stream().filter(groups -> groups.getIdentifier().equals(identifier)).findFirst().orElse(null);
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        final User user = users.stream().filter(u -> u.getIdentity().equals(identity)).findFirst().orElse(null);
        return new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                if (user == null) {
                    return null;
                } else {
                    return groups.stream().filter(group -> group.getUsers().contains(user.getIdentifier())).collect(Collectors.toSet());
                }
            }
        };
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}