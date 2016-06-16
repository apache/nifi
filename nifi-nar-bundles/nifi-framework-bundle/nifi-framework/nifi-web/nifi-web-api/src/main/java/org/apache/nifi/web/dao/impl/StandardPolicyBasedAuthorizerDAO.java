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
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;

import java.util.Set;

public class StandardPolicyBasedAuthorizerDAO implements AccessPolicyDAO, UserGroupDAO, UserDAO {

    private static final String MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER = "authorizer is not of type AbstractPolicyBasedAuthorizer";
    private final AbstractPolicyBasedAuthorizer authorizer;

    public StandardPolicyBasedAuthorizerDAO(Authorizer authorizer) {
        if (authorizer instanceof AbstractPolicyBasedAuthorizer) {
            this.authorizer = (AbstractPolicyBasedAuthorizer) authorizer;
        } else {
            this.authorizer = new AbstractPolicyBasedAuthorizer() {
                @Override
                public Group addGroup(Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group getGroup(String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group updateGroup(Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Group deleteGroup(Group group) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Set<Group> getGroups() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User addUser(User user) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User getUser(String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User updateUser(User user) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public User deleteUser(User user) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public Set<User> getUsers() throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    throw new IllegalStateException(MSG_NON_ABSTRACT_POLICY_BASED_AUTHORIZER);
                }

                @Override
                public AccessPolicy deleteAccessPolicy(AccessPolicy policy) throws AuthorizationAccessException {
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
                public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                }
            };
        }
    }

    @Override
    public boolean hasAccessPolicy(String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId) != null;
    }

    @Override
    public AccessPolicy createAccessPolicy(AccessPolicyDTO accessPolicyDTO) {
        return authorizer.addAccessPolicy(buildAccessPolicy(accessPolicyDTO));
    }

    @Override
    public AccessPolicy getAccessPolicy(String accessPolicyId) {
        return authorizer.getAccessPolicy(accessPolicyId);
    }

    @Override
    public AccessPolicy updateAccessPolicy(AccessPolicyDTO accessPolicyDTO) {
        return authorizer.updateAccessPolicy(buildAccessPolicy(accessPolicyDTO));
    }

    @Override
    public AccessPolicy deleteAccessPolicy(String accessPolicyId) {
        return authorizer.deleteAccessPolicy(authorizer.getAccessPolicy(accessPolicyId));
    }

    private AccessPolicy buildAccessPolicy(AccessPolicyDTO accessPolicyDTO) {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(accessPolicyDTO.getId()).addGroups(accessPolicyDTO.getGroups()).addUsers(accessPolicyDTO.getUsers()).resource(accessPolicyDTO.getResource());
        if (accessPolicyDTO.getCanRead()) {
            builder.addAction(RequestAction.READ);
        }
        if (accessPolicyDTO.getCanWrite()) {
            builder.addAction(RequestAction.WRITE);
        }
        return builder.build();
    }

    @Override
    public boolean hasUserGroup(String userGroupId) {
        return authorizer.getGroup(userGroupId) != null;
    }

    @Override
    public Group createUserGroup(UserGroupDTO userGroupDTO) {
        return authorizer.addGroup(buildUserGroup(userGroupDTO));
    }

    @Override
    public Group getUserGroup(String userGroupId) {
        return authorizer.getGroup(userGroupId);
    }

    @Override
    public Group updateUserGroup(UserGroupDTO userGroupDTO) {
        return authorizer.updateGroup(buildUserGroup(userGroupDTO));
    }

    @Override
    public Group deleteUserGroup(String userGroupId) {
        return authorizer.deleteGroup(authorizer.getGroup(userGroupId));
    }

    private Group buildUserGroup(UserGroupDTO userGroupDTO) {
        return new Group.Builder().addUsers(userGroupDTO.getUsers()).identifier(userGroupDTO.getId()).name(userGroupDTO.getName()).build();
    }

    @Override
    public boolean hasUser(String userId) {
        return authorizer.getUser(userId) != null;
    }

    @Override
    public User createUser(UserDTO userDTO) {
        User user = buildUser(userDTO);
        return authorizer.addUser(user);
    }

    @Override
    public User getUser(String userId) {
        return authorizer.getUser(userId);
    }

    @Override
    public User updateUser(UserDTO userDTO) {
        return authorizer.updateUser(buildUser(userDTO));
    }

    @Override
    public User deleteUser(String userId) {
        return authorizer.deleteUser(authorizer.getUser(userId));
    }

    private User buildUser(UserDTO userDTO) {
        return new User.Builder().addGroups(userDTO.getGroups()).identifier(userDTO.getIdentity()).identity(userDTO.getIdentity()).build();
    }

}