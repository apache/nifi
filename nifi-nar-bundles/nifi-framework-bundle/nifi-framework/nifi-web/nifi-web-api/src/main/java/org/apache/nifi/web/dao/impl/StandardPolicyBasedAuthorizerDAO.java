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
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;

import java.util.Set;
import java.util.stream.Collectors;

public class StandardPolicyBasedAuthorizerDAO implements AccessPolicyDAO, UserGroupDAO, UserDAO {

    private static final String MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER = "authorizer is not of type AbstractPolicyBasedAuthorizer";
    private final AbstractPolicyBasedAuthorizer authorizer;

    public StandardPolicyBasedAuthorizerDAO(final Authorizer authorizer) {
        if (authorizer instanceof AbstractPolicyBasedAuthorizer) {
            this.authorizer = (AbstractPolicyBasedAuthorizer) authorizer;
        } else {
            this.authorizer = new AbstractPolicyBasedAuthorizer() {
                @Override
                public Group addGroup(final Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group getGroup(final String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group updateGroup(final Group group) throws AuthorizationAccessException {
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
                public User addUser(final User user) throws AuthorizationAccessException {
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
                public User updateUser(final User user) throws AuthorizationAccessException {
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
                public AccessPolicy addAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
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
                public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                }
            };
        }
    }

    @Override
    public boolean hasAccessPolicy(final String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId) != null;
    }

    @Override
    public AccessPolicy createAccessPolicy(final AccessPolicyDTO accessPolicyDTO) {
        return authorizer.addAccessPolicy(buildAccessPolicy(accessPolicyDTO));
    }

    @Override
    public AccessPolicy getAccessPolicy(final String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId);
    }

    @Override
    public AccessPolicy updateAccessPolicy(final AccessPolicyDTO accessPolicyDTO) {
        return authorizer.updateAccessPolicy(buildAccessPolicy(accessPolicyDTO));
    }

    @Override
    public AccessPolicy deleteAccessPolicy(final String accessPolicyId) {
        return authorizer.deleteAccessPolicy(authorizer.getAccessPolicy(accessPolicyId));
    }

    private AccessPolicy buildAccessPolicy(final AccessPolicyDTO accessPolicyDTO) {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(accessPolicyDTO.getId())
                .addGroups(accessPolicyDTO.getUserGroups().stream().map(ComponentEntity::getId).collect(Collectors.toSet()))
                .addUsers(accessPolicyDTO.getUsers().stream().map(ComponentEntity::getId).collect(Collectors.toSet()))
                .resource(accessPolicyDTO.getResource());
        if (accessPolicyDTO.getCanRead()) {
            builder.addAction(RequestAction.READ);
        }
        if (accessPolicyDTO.getCanWrite()) {
            builder.addAction(RequestAction.WRITE);
        }
        return builder.build();
    }

    @Override
    public boolean hasUserGroup(final String userGroupId) {
        return authorizer.getGroup(userGroupId) != null;
    }

    @Override
    public Group createUserGroup(final UserGroupDTO userGroupDTO) {
        return authorizer.addGroup(buildUserGroup(userGroupDTO));
    }

    @Override
    public Group getUserGroup(final String userGroupId) {
        return authorizer.getGroup(userGroupId);
    }

    @Override
    public Group updateUserGroup(final UserGroupDTO userGroupDTO) {
        return authorizer.updateGroup(buildUserGroup(userGroupDTO));
    }

    @Override
    public Group deleteUserGroup(final String userGroupId) {
        return authorizer.deleteGroup(authorizer.getGroup(userGroupId));
    }

    private Group buildUserGroup(final UserGroupDTO userGroupDTO) {
        return new Group.Builder()
                .addUsers(userGroupDTO.getUsers().stream().map(ComponentEntity::getId).collect(Collectors.toSet()))
                .identifier(userGroupDTO.getId()).name(userGroupDTO.getName()).build();
    }

    @Override
    public boolean hasUser(final String userId) {
        return authorizer.getUser(userId) != null;
    }

    @Override
    public User createUser(final UserDTO userDTO) {
        final User user = buildUser(userDTO);
        return authorizer.addUser(user);
    }

    @Override
    public User getUser(final String userId) {
        return authorizer.getUser(userId);
    }

    @Override
    public User updateUser(final UserDTO userDTO) {
        return authorizer.updateUser(buildUser(userDTO));
    }

    @Override
    public User deleteUser(final String userId) {
        return authorizer.deleteUser(authorizer.getUser(userId));
    }

    private User buildUser(final UserDTO userDTO) {
        return new User.Builder()
                .addGroups(userDTO.getGroups().stream().map(ComponentEntity::getId).collect(Collectors.toSet()))
                .identifier(userDTO.getIdentity()).identity(userDTO.getIdentity()).build();
    }

}