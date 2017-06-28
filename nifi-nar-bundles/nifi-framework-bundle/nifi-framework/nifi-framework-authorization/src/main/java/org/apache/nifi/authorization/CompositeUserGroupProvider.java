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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompositeUserGroupProvider implements UserGroupProvider {

    static final String PROP_USER_GROUP_PROVIDER_PREFIX = "User Group Provider ";
    static final Pattern USER_GROUP_PROVIDER_PATTERN = Pattern.compile(PROP_USER_GROUP_PROVIDER_PREFIX + "\\S+");

    private final boolean allowEmptyProviderList;

    private UserGroupProviderLookup userGroupProviderLookup;
    private List<UserGroupProvider> userGroupProviders = new ArrayList<>(); // order matters

    public CompositeUserGroupProvider() {
        this(false);
    }

    public CompositeUserGroupProvider(boolean allowEmptyProviderList) {
        this.allowEmptyProviderList = allowEmptyProviderList;
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
            Matcher matcher = USER_GROUP_PROVIDER_PATTERN.matcher(entry.getKey());
            if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                final String userGroupProviderKey = entry.getValue();
                final UserGroupProvider userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderKey);

                if (userGroupProvider == null) {
                    throw new AuthorizerCreationException(String.format("Unable to locate the configured User Group Provider: %s", userGroupProviderKey));
                }

                userGroupProviders.add(userGroupProvider);
            }
        }

        if (!allowEmptyProviderList && userGroupProviders.isEmpty()) {
            throw new AuthorizerCreationException("At least one User Group Provider must be configured.");
        }
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        final Set<User> users = new HashSet<>();

        for (final UserGroupProvider userGroupProvider : userGroupProviders) {
            users.addAll(userGroupProvider.getUsers());
        }

        return users;
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        User user = null;

        for (final UserGroupProvider userGroupProvider : userGroupProviders) {
            user = userGroupProvider.getUser(identifier);

            if (user != null) {
                break;
            }
        }

        return user;
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        User user = null;

        for (final UserGroupProvider userGroupProvider : userGroupProviders) {
            user = userGroupProvider.getUserByIdentity(identity);

            if (user != null) {
                break;
            }
        }

        return user;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        final Set<Group> groups = new HashSet<>();

        for (final UserGroupProvider userGroupProvider : userGroupProviders) {
            groups.addAll(userGroupProvider.getGroups());
        }

        return groups;
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        Group group = null;

        for (final UserGroupProvider userGroupProvider : userGroupProviders) {
            group = userGroupProvider.getGroup(identifier);

            if (group != null) {
                break;
            }
        }

        return group;
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        UserAndGroups userAndGroups = null;

        for (final UserGroupProvider userGroupProvider : userGroupProviders) {
            userAndGroups = userGroupProvider.getUserAndGroups(identity);

            if (userAndGroups.getUser() != null) {
                break;
            }
        }

        if (userAndGroups == null) {
            // per API, returning non null with null user/groups
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
        } else {
            // a delegated provider contained a matching user
            return userAndGroups;
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
    }
}
