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

package org.apache.nifi.authorization.azure;

import static java.util.stream.Collectors.toMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;

public class ImmutableAzureGraphUserGroup {
    private final Set<User> users;
    private final Map<String, User> usersByObjectId;
    private final Map<String, User> usersByPrincipalName;
    private final Map<String, UserAndGroups> usersAndGroupsByUserObjectId;

    private final Set<Group> groups;
    private final Map<String, Group> groupsByObjectId;
    private final Map<String, Group> groupsByDisplayName;

    private ImmutableAzureGraphUserGroup(
        final Set<User> users,
        final Map<String, User> usersByObjectId,
        final Map<String, User> usersByPrincipalName,
        final Map<String, UserAndGroups> usersAndGroupsByUserObjectId,
        final Set<Group> groups,
        final Map<String, Group> groupsByObjectId,
        final Map<String, Group> groupsByDisplayName) {

        this.users = Collections.unmodifiableSet(users);
        this.usersByObjectId = Collections.unmodifiableMap(usersByObjectId);
        this.usersByPrincipalName = Collections.unmodifiableMap(usersByPrincipalName);
        this.usersAndGroupsByUserObjectId = Collections.unmodifiableMap(usersAndGroupsByUserObjectId);

        this.groups = Collections.unmodifiableSet(groups);
        this.groupsByObjectId = Collections.unmodifiableMap(groupsByObjectId);
        this.groupsByDisplayName = Collections.unmodifiableMap(groupsByDisplayName);
    }

    public Set<User> getUsers() {
        return users;
    }

    public User getUser(final String objectId) {
        return usersByObjectId.get(objectId);
    }

    public User getUserByPrincipalName(final String principalName) {
        return usersByPrincipalName.get(principalName);
    }

    public UserAndGroups getUserAndGroupsByUserObjectId(final String objectId) {
        return usersAndGroupsByUserObjectId.get(objectId);
    }

    public UserAndGroups getUserAndGroups(final String principalName) {
        final User user = getUserByPrincipalName(principalName);
        if (user != null) {
            final String objectId = user.getIdentifier();
            return getUserAndGroupsByUserObjectId(objectId);
        } else {
            // this covers the certificate-based authentication path
            // this path may be called when CompositeUserGroupProvider is used
            return new UserAndGroups() {
                @Override
                public User getUser() {
                    return null;
                }
                @Override
                public Set<Group> getGroups() {
                    return null;
                }
            };
        }
    }

    public Set<Group> getGroups() {
        return groups;
    }

    public Group getGroup(final String objectId) {
        return groupsByObjectId.get(objectId);
    }

    public Group getGroupByDisplayName(final String displayName) {
        return groupsByDisplayName.get(displayName);
    }

    public static ImmutableAzureGraphUserGroup newInstance(final Set<User> users, final Set<Group> groups) {
        final Map<String, User> usersByObjectId = new HashMap<>();
        final Map<String, User> usersByPrincipalName = new HashMap<>();

        users.forEach(user -> {
            usersByObjectId.put(user.getIdentifier(), user);
            usersByPrincipalName.put(user.getIdentity(), user);
        });

        final Map<String, Group> groupsByObjectId = new HashMap<>();
        final Map<String, Group> groupsByDisplayName = new HashMap<>();
        final Map<String, Set<Group>> groupsByUserObjectId =
            users.stream().collect(toMap(User::getIdentifier, user -> {
                return new HashSet<Group>();
            }));

        groups.forEach(group -> {
            groupsByObjectId.put(group.getIdentifier(), group);
            groupsByDisplayName.put(group.getName(), group);
            group.getUsers().forEach(user -> {
                groupsByUserObjectId.get(user).add(group);
            });
        });

        final Map<String, UserAndGroups> usersAndGroupsByUserObjectId =
            groupsByUserObjectId.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, e -> {
                    return new UserAndGroups() {
                        @Override public User getUser() {
                            return usersByObjectId.get(e.getKey());
                        }
                        @Override public Set<Group> getGroups() {
                            return e.getValue();
                        }
                    };
                }));

        return new ImmutableAzureGraphUserGroup(
            users,
            usersByObjectId,
            usersByPrincipalName,
            usersAndGroupsByUserObjectId,
            groups,
            groupsByObjectId,
            groupsByDisplayName
        );
    }
}
