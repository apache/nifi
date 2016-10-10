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

import org.apache.nifi.authorization.AbstractPolicyBasedAuthorizer;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UsersAndAccessPolicies;
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

    private final AbstractPolicyBasedAuthorizer authorizer;
    private final boolean supportsConfigurableAuthorizer;

    public StandardPolicyBasedAuthorizerDAO(final Authorizer authorizer) {
        if (authorizer instanceof AbstractPolicyBasedAuthorizer) {
            this.authorizer = (AbstractPolicyBasedAuthorizer) authorizer;
            this.supportsConfigurableAuthorizer = true;
        } else {
            this.authorizer = new AbstractPolicyBasedAuthorizer() {
                @Override
                public Group doAddGroup(final Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group getGroup(final String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group doUpdateGroup(final Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group deleteGroup(final Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Set<Group> getGroups() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User doAddUser(final User user) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User getUser(final String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User doUpdateUser(final User user) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User deleteUser(final User user) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Set<User> getUsers() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy doAddAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy deleteAccessPolicy(final AccessPolicy policy) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public void initialize(final AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
                }

                @Override
                public void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                }
            };
            this.supportsConfigurableAuthorizer = false;
        }
    }

    private AccessPolicy findAccessPolicy(final RequestAction requestAction, final String resource) {
        return authorizer.getAccessPolicies().stream()
                .filter(policy -> policy.getAction().equals(requestAction) && policy.getResource().equals(resource))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean supportsConfigurableAuthorizer() {
        return supportsConfigurableAuthorizer;
    }

    @Override
    public boolean hasAccessPolicy(final String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId) != null;
    }

    @Override
    public AccessPolicy createAccessPolicy(final AccessPolicyDTO accessPolicyDTO) {
        return authorizer.addAccessPolicy(buildAccessPolicy(accessPolicyDTO.getId(),
                accessPolicyDTO.getResource(), RequestAction.valueOfValue(accessPolicyDTO.getAction()), accessPolicyDTO));
    }

    @Override
    public AccessPolicy getAccessPolicy(final String accessPolicyId) {
        final AccessPolicy accessPolicy = authorizer.getAccessPolicy(accessPolicyId);
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
        final AccessPolicy currentAccessPolicy = getAccessPolicy(accessPolicyDTO.getId());
        return authorizer.updateAccessPolicy(buildAccessPolicy(currentAccessPolicy.getIdentifier(),
                currentAccessPolicy.getResource(), currentAccessPolicy.getAction(), accessPolicyDTO));
    }

    @Override
    public AccessPolicy deleteAccessPolicy(final String accessPolicyId) {
        return authorizer.deleteAccessPolicy(getAccessPolicy(accessPolicyId));
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
        return authorizer.getGroup(userGroupId) != null;
    }

    @Override
    public Group createUserGroup(final UserGroupDTO userGroupDTO) {
        return authorizer.addGroup(buildUserGroup(userGroupDTO.getId(), userGroupDTO));
    }

    @Override
    public Group getUserGroup(final String userGroupId) {
        final Group userGroup = authorizer.getGroup(userGroupId);
        if (userGroup == null) {
            throw new ResourceNotFoundException(String.format("Unable to find user group with id '%s'.", userGroupId));
        }
        return userGroup;
    }

    @Override
    public Set<Group> getUserGroupsForUser(String userId) {
        return authorizer.getGroups().stream()
                .filter(g -> g.getUsers().contains(userId))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<AccessPolicy> getAccessPoliciesForUser(String userId) {
        return authorizer.getAccessPolicies().stream()
                .filter(p -> {
                    // policy contains the user
                    if (p.getUsers().contains(userId)) {
                        return true;
                    }

                    // policy contains a group with the user
                    return !p.getGroups().stream().filter(g -> authorizer.getGroup(g).getUsers().contains(userId)).collect(Collectors.toSet()).isEmpty();
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Group> getUserGroups() {
        return authorizer.getGroups();
    }

    @Override
    public Group updateUserGroup(final UserGroupDTO userGroupDTO) {
        return authorizer.updateGroup(buildUserGroup(getUserGroup(userGroupDTO.getId()).getIdentifier(), userGroupDTO));
    }

    @Override
    public Group deleteUserGroup(final String userGroupId) {
        return authorizer.deleteGroup(getUserGroup(userGroupId));
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
        return authorizer.getUser(userId) != null;
    }

    @Override
    public User createUser(final UserDTO userDTO) {
        return authorizer.addUser(buildUser(userDTO.getId(), userDTO));
    }

    @Override
    public User getUser(final String userId) {
        final User user = authorizer.getUser(userId);
        if (user == null) {
            throw new ResourceNotFoundException(String.format("Unable to find user with id '%s'.", userId));
        }
        return user;
    }

    @Override
    public Set<User> getUsers() {
        return authorizer.getUsers();
    }

    @Override
    public User updateUser(final UserDTO userDTO) {
        return authorizer.updateUser(buildUser(getUser(userDTO.getId()).getIdentifier(), userDTO));
    }

    @Override
    public User deleteUser(final String userId) {
        final User user = getUser(userId);
        return authorizer.deleteUser(user);
    }

    private User buildUser(final String identifier, final UserDTO userDTO) {
        final User.Builder builder = new User.Builder().identifier(identifier).identity(userDTO.getIdentity());
        return builder.build();
    }
}