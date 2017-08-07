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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.ConfigurableAccessPolicyProvider;
import org.apache.nifi.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;

import java.util.Set;
import java.util.stream.Collectors;

public class StandardPolicyBasedAuthorizerDAO implements AccessPolicyDAO, UserGroupDAO, UserDAO {

    private final AccessPolicyProvider accessPolicyProvider;
    private final UserGroupProvider userGroupProvider;

    public StandardPolicyBasedAuthorizerDAO(final Authorizer authorizer) {
        if (AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            accessPolicyProvider = ((ManagedAuthorizer) authorizer).getAccessPolicyProvider();
        } else {
            accessPolicyProvider = new AccessPolicyProvider() {
                @Override
                public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                }

                @Override
                public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                }

                @Override
                public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                }

                @Override
                public UserGroupProvider getUserGroupProvider() {
                    return new UserGroupProvider() {
                        @Override
                        public Set<User> getUsers() throws AuthorizationAccessException {
                            throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                        }

                        @Override
                        public User getUser(String identifier) throws AuthorizationAccessException {
                            throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                        }

                        @Override
                        public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                            throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                        }

                        @Override
                        public Set<Group> getGroups() throws AuthorizationAccessException {
                            throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                        }

                        @Override
                        public Group getGroup(String identifier) throws AuthorizationAccessException {
                            throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
                        }

                        @Override
                        public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
                            throw new IllegalStateException(MSG_NON_MANAGED_AUTHORIZER);
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
                    };
                }

                @Override
                public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {

                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {

                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {

                }
            };
        }

        userGroupProvider = accessPolicyProvider.getUserGroupProvider();
    }

    private AccessPolicy findAccessPolicy(final RequestAction requestAction, final String resource) {
        return accessPolicyProvider.getAccessPolicies().stream()
                .filter(policy -> policy.getAction().equals(requestAction) && policy.getResource().equals(resource))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean supportsConfigurableAuthorizer() {
        return accessPolicyProvider instanceof ConfigurableAccessPolicyProvider;
    }

    @Override
    public boolean hasAccessPolicy(final String accessPolicyId) {
        return accessPolicyProvider.getAccessPolicy(accessPolicyId) != null;
    }

    @Override
    public AccessPolicy createAccessPolicy(final AccessPolicyDTO accessPolicyDTO) {
        if (supportsConfigurableAuthorizer()) {
            final ConfigurableAccessPolicyProvider configurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) accessPolicyProvider;
            return configurableAccessPolicyProvider.addAccessPolicy(buildAccessPolicy(accessPolicyDTO.getId(),
                    accessPolicyDTO.getResource(), RequestAction.valueOfValue(accessPolicyDTO.getAction()), accessPolicyDTO));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_POLICIES);
        }
    }

    @Override
    public AccessPolicy getAccessPolicy(final String accessPolicyId) {
        final AccessPolicy accessPolicy = accessPolicyProvider.getAccessPolicy(accessPolicyId);
        if (accessPolicy == null) {
            throw new ResourceNotFoundException(String.format("Unable to find access policy with id '%s'.", accessPolicyId));
        }
        return accessPolicy;
    }

    @Override
    public AccessPolicy getAccessPolicy(final RequestAction requestAction, final String resource) {
        return findAccessPolicy(requestAction, resource);
    }

    @Override
    public AccessPolicy getAccessPolicy(final RequestAction requestAction, final Authorizable authorizable) {
        final String resource = authorizable.getResource().getIdentifier();

        final AccessPolicy accessPolicy = findAccessPolicy(requestAction, authorizable.getResource().getIdentifier());
        if (accessPolicy == null) {
            final Authorizable parentAuthorizable = authorizable.getParentAuthorizable();
            if (parentAuthorizable == null) {
                throw new ResourceNotFoundException(String.format("Unable to find access policy for %s on %s", requestAction.toString(), resource));
            } else {
                return getAccessPolicy(requestAction, parentAuthorizable);
            }
        }

        return accessPolicy;
    }

    @Override
    public AccessPolicy updateAccessPolicy(final AccessPolicyDTO accessPolicyDTO) {
        if (supportsConfigurableAuthorizer()) {
            final ConfigurableAccessPolicyProvider configurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) accessPolicyProvider;

            final AccessPolicy currentAccessPolicy = getAccessPolicy(accessPolicyDTO.getId());
            return configurableAccessPolicyProvider.updateAccessPolicy(buildAccessPolicy(currentAccessPolicy.getIdentifier(),
                    currentAccessPolicy.getResource(), currentAccessPolicy.getAction(), accessPolicyDTO));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_POLICIES);
        }
    }

    @Override
    public AccessPolicy deleteAccessPolicy(final String accessPolicyId) {
        if (supportsConfigurableAuthorizer()) {
            final ConfigurableAccessPolicyProvider configurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) accessPolicyProvider;
            return configurableAccessPolicyProvider.deleteAccessPolicy(getAccessPolicy(accessPolicyId));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_POLICIES);
        }
    }

    private AccessPolicy buildAccessPolicy(final String identifier, final String resource, final RequestAction action, final AccessPolicyDTO accessPolicyDTO) {
        final Set<TenantEntity> userGroups = accessPolicyDTO.getUserGroups();
        final Set<TenantEntity> users = accessPolicyDTO.getUsers();
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(identifier)
                .resource(resource);
        if (userGroups != null) {
            builder.addGroups(userGroups.stream().map(ComponentEntity::getId).collect(Collectors.toSet()));
        }
        if (users != null) {
            builder.addUsers(users.stream().map(ComponentEntity::getId).collect(Collectors.toSet()));
        }
        builder.action(action);
        return builder.build();
    }

    @Override
    public boolean hasUserGroup(final String userGroupId) {
        return userGroupProvider.getGroup(userGroupId) != null;
    }

    @Override
    public Group createUserGroup(final UserGroupDTO userGroupDTO) {
        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;
            return configurableUserGroupProvider.addGroup(buildUserGroup(userGroupDTO.getId(), userGroupDTO));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    @Override
    public Group getUserGroup(final String userGroupId) {
        final Group userGroup = userGroupProvider.getGroup(userGroupId);
        if (userGroup == null) {
            throw new ResourceNotFoundException(String.format("Unable to find user group with id '%s'.", userGroupId));
        }
        return userGroup;
    }

    @Override
    public Set<Group> getUserGroupsForUser(String userId) {
        return userGroupProvider.getGroups().stream()
                .filter(g -> g.getUsers().contains(userId))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<AccessPolicy> getAccessPoliciesForUser(String userId) {
        return accessPolicyProvider.getAccessPolicies().stream()
                .filter(p -> {
                    // policy contains the user
                    if (p.getUsers().contains(userId)) {
                        return true;
                    }

                    // policy contains a group with the user
                    return !p.getGroups().stream().filter(g -> userGroupProvider.getGroup(g).getUsers().contains(userId)).collect(Collectors.toSet()).isEmpty();
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Set<AccessPolicy> getAccessPoliciesForUserGroup(String userGroupId) {
        return accessPolicyProvider.getAccessPolicies().stream()
                .filter(p -> {
                    // policy contains the user group
                    return p.getGroups().contains(userGroupId);
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Group> getUserGroups() {
        return userGroupProvider.getGroups();
    }

    @Override
    public Group updateUserGroup(final UserGroupDTO userGroupDTO) {
        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;
            return configurableUserGroupProvider.updateGroup(buildUserGroup(getUserGroup(userGroupDTO.getId()).getIdentifier(), userGroupDTO));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    @Override
    public Group deleteUserGroup(final String userGroupId) {
        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;

            final Group group = getUserGroup(userGroupId);
            final Group removedGroup = configurableUserGroupProvider.deleteGroup(group);

            // ensure the user was removed
            if (removedGroup == null) {
                throw new ResourceNotFoundException(String.format("Unable to find user group with id '%s'.", removedGroup));
            }

            // remove any references to the user group being deleted from policies if possible
            if (accessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
                for (AccessPolicy policy : accessPolicyProvider.getAccessPolicies()) {
                    final ConfigurableAccessPolicyProvider configurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) accessPolicyProvider;

                    // ensure this policy contains a reference to the user group and this policy is configurable (check proactively to prevent an exception)
                    if (policy.getGroups().contains(removedGroup.getIdentifier()) && configurableAccessPolicyProvider.isConfigurable(policy)) {
                        final AccessPolicy.Builder builder = new AccessPolicy.Builder(policy).removeGroup(removedGroup.getIdentifier());
                        configurableAccessPolicyProvider.updateAccessPolicy(builder.build());
                    }
                }
            }

            return removedGroup;
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    private Group buildUserGroup(final String identifier, final UserGroupDTO userGroupDTO) {
        final Set<TenantEntity> users = userGroupDTO.getUsers();
        final Group.Builder builder = new Group.Builder().identifier(identifier).name(userGroupDTO.getIdentity());
        if (users != null) {
            builder.addUsers(users.stream().map(ComponentEntity::getId).collect(Collectors.toSet()));
        }
        return builder.build();
    }

    @Override
    public boolean hasUser(final String userId) {
        return userGroupProvider.getUser(userId) != null;
    }

    @Override
    public User createUser(final UserDTO userDTO) {
        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;
            return configurableUserGroupProvider.addUser(buildUser(userDTO.getId(), userDTO));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    @Override
    public User getUser(final String userId) {
        final User user = userGroupProvider.getUser(userId);
        if (user == null) {
            throw new ResourceNotFoundException(String.format("Unable to find user with id '%s'.", userId));
        }
        return user;
    }

    @Override
    public Set<User> getUsers() {
        return userGroupProvider.getUsers();
    }

    @Override
    public User updateUser(final UserDTO userDTO) {
        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;
            return configurableUserGroupProvider.updateUser(buildUser(getUser(userDTO.getId()).getIdentifier(), userDTO));
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    @Override
    public User deleteUser(final String userId) {
        if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
            final ConfigurableUserGroupProvider configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;

            final User user = getUser(userId);
            final User removedUser = configurableUserGroupProvider.deleteUser(user);

            // ensure the user was removed
            if (removedUser == null) {
                throw new ResourceNotFoundException(String.format("Unable to find user with id '%s'.", userId));
            }

            // remove any references to the user being deleted from policies if possible
            if (accessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
                for (AccessPolicy policy : accessPolicyProvider.getAccessPolicies()) {
                    final ConfigurableAccessPolicyProvider configurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) accessPolicyProvider;

                    // ensure this policy contains a reference to the user and this policy is configurable (check proactively to prevent an exception)
                    if (policy.getUsers().contains(removedUser.getIdentifier()) && configurableAccessPolicyProvider.isConfigurable(policy)) {
                        final AccessPolicy.Builder builder = new AccessPolicy.Builder(policy).removeUser(removedUser.getIdentifier());
                        configurableAccessPolicyProvider.updateAccessPolicy(builder.build());
                    }
                }
            }

            return removedUser;
        } else {
            throw new IllegalStateException(MSG_NON_CONFIGURABLE_USERS);
        }
    }

    private User buildUser(final String identifier, final UserDTO userDTO) {
        final User.Builder builder = new User.Builder().identifier(identifier).identity(userDTO.getIdentity());
        return builder.build();
    }
}