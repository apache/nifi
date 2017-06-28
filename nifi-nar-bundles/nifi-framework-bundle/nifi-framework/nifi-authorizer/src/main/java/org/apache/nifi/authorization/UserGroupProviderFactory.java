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
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.nar.NarCloseable;

import java.util.Set;

public final class UserGroupProviderFactory {

    public static UserGroupProvider withNarLoader(final UserGroupProvider baseUserGroupProvider) {
        if (baseUserGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider baseConfigurableUserGroupProvider = (ConfigurableUserGroupProvider) baseUserGroupProvider;
            return new ConfigurableUserGroupProvider() {
                @Override
                public User addUser(User user) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.addUser(user);
                    }
                }

                @Override
                public boolean isConfigurable(User user) {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.isConfigurable(user);
                    }
                }

                @Override
                public User updateUser(User user) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.updateUser(user);
                    }
                }

                @Override
                public User deleteUser(User user) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.deleteUser(user);
                    }
                }

                @Override
                public Group addGroup(Group group) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.addGroup(group);
                    }
                }

                @Override
                public boolean isConfigurable(Group group) {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.isConfigurable(group);
                    }
                }

                @Override
                public Group updateGroup(Group group) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.updateGroup(group);
                    }
                }

                @Override
                public Group deleteGroup(Group group) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.deleteGroup(group);
                    }
                }

                @Override
                public Set<User> getUsers() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getUsers();
                    }
                }

                @Override
                public User getUser(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getUser(identifier);
                    }
                }

                @Override
                public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getUserByIdentity(identity);
                    }
                }

                @Override
                public Set<Group> getGroups() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getGroups();
                    }
                }

                @Override
                public Group getGroup(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getGroup(identifier);
                    }
                }

                @Override
                public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getUserAndGroups(identity);
                    }
                }

                @Override
                public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableUserGroupProvider.inheritFingerprint(fingerprint);
                    }
                }

                @Override
                public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableUserGroupProvider.checkInheritability(proposedFingerprint);
                    }
                }

                @Override
                public String getFingerprint() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseConfigurableUserGroupProvider.getFingerprint();
                    }
                }

                @Override
                public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableUserGroupProvider.initialize(initializationContext);
                    }
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableUserGroupProvider.onConfigured(configurationContext);
                    }
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseConfigurableUserGroupProvider.preDestruction();
                    }
                }
            };
        } else {
            return new UserGroupProvider() {
                @Override
                public Set<User> getUsers() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseUserGroupProvider.getUsers();
                    }
                }

                @Override
                public User getUser(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseUserGroupProvider.getUser(identifier);
                    }
                }

                @Override
                public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseUserGroupProvider.getUserByIdentity(identity);
                    }
                }

                @Override
                public Set<Group> getGroups() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseUserGroupProvider.getGroups();
                    }
                }

                @Override
                public Group getGroup(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseUserGroupProvider.getGroup(identifier);
                    }
                }

                @Override
                public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseUserGroupProvider.getUserAndGroups(identity);
                    }
                }

                @Override
                public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseUserGroupProvider.initialize(initializationContext);
                    }
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseUserGroupProvider.onConfigured(configurationContext);
                    }
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseUserGroupProvider.preDestruction();
                    }
                }
            };
        }
    }

    private UserGroupProviderFactory() {}
}
