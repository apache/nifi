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

import java.util.Set;

/**
 * An Authorizer that provides management of users, groups, and policies.
 */
public abstract class AbstractPolicyBasedAuthorizer implements Authorizer {

    @Override
    public final AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
        final Set<AccessPolicy> policies = getAccessPolicies(request.getResource());

        if (policies == null || policies.isEmpty()) {
            return AuthorizationResult.resourceNotFound();
        }

        final User user = getUserByIdentity(request.getIdentity());

        if (user == null) {
            return AuthorizationResult.denied("Unknown user with identity " + request.getIdentity());
        }

        for (AccessPolicy policy : policies) {
            final boolean containsAction = policy.getActions().contains(request.getAction());
            final boolean containsUser = policy.getUsers().contains(user.getIdentifier());
            if (containsAction && (containsUser || containsGroup(user, policy)) ) {
                return AuthorizationResult.approved();
            }
        }

        return AuthorizationResult.denied();
    }


    private boolean containsGroup(final User user, final AccessPolicy policy) {
        if (user.getGroups().isEmpty() || policy.getGroups().isEmpty()) {
            return false;
        }

        for (String userGroup : user.getGroups()) {
            if (policy.getGroups().contains(userGroup)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Adds a new group.
     *
     * @param group the Group to add
     * @return the added Group
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group addGroup(Group group) throws AuthorizationAccessException;

    /**
     * Retrieves a Group by id.
     *
     * @param identifier the identifier of the Group to retrieve
     * @return the Group with the given identifier, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group getGroup(String identifier) throws AuthorizationAccessException;

    /**
     * The group represented by the provided instance will be updated based on the provided instance.
     *
     * @param group an updated group instance
     * @return the updated group instance, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group updateGroup(Group group) throws AuthorizationAccessException;

    /**
     * Deletes the given group.
     *
     * @param group the group to delete
     * @return the deleted group, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Group deleteGroup(Group group) throws AuthorizationAccessException;

    /**
     * Retrieves all groups.
     *
     * @return a list of groups
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<Group> getGroups() throws AuthorizationAccessException;


    /**
     * Adds the given user.
     *
     * @param user the user to add
     * @return the user that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User addUser(User user) throws AuthorizationAccessException;

    /**
     * Retrieves the user with the given identifier.
     *
     * @param identifier the id of the user to retrieve
     * @return the user with the given id, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User getUser(String identifier) throws AuthorizationAccessException;

    /**
     * Retrieves the user with the given identity.
     *
     * @param identity the identity of the user to retrieve
     * @return the user with the given identity, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User getUserByIdentity(String identity) throws AuthorizationAccessException;

    /**
     * The user represented by the provided instance will be updated based on the provided instance.
     *
     * @param user an updated user instance
     * @return the updated user instance, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User updateUser(User user) throws AuthorizationAccessException;

    /**
     * Deletes the given user.
     *
     * @param user the user to delete
     * @return the user that was deleted, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract User deleteUser(User user) throws AuthorizationAccessException;

    /**
     * Retrieves all users.
     *
     * @return a list of users
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<User> getUsers() throws AuthorizationAccessException;


    /**
     * Adds the given policy.
     *
     * @param accessPolicy the policy to add
     * @return the policy that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;

    /**
     * Retrieves the policy with the given identifier.
     *
     * @param identifier the id of the policy to retrieve
     * @return the policy with the given id, or null if no matching policy exists
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException;

    /**
     * The policy represented by the provided instance will be updated based on the provided instance.
     *
     * @param accessPolicy an updated policy
     * @return the updated policy, or null if no matching policy was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException;

    /**
     * Deletes the given policy.
     *
     * @param policy the policy to delete
     * @return the deleted policy, or null if no matching policy was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract AccessPolicy deleteAccessPolicy(AccessPolicy policy) throws AuthorizationAccessException;

    /**
     * Retrieves all access policies.
     *
     * @return a list of policies
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException;

    /**
     * Retrieves the access policies for the given Resource.
     *
     * @param resource the resource to retrieve policies for
     * @return a set of policies for the given resource, or an empty set if there are none
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    public abstract Set<AccessPolicy> getAccessPolicies(Resource resource) throws AuthorizationAccessException;

}
