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
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;

/**
 * Provides support for configuring Users and Groups.
 *
 * NOTE: Extensions will be called often and frequently. Because of this, if the underlying implementation needs to
 * make remote calls or expensive calculations those should probably be done asynchronously and/or cache the results.
 *
 * Additionally, extensions need to be thread safe.
 */
public interface ConfigurableUserGroupProvider extends UserGroupProvider {

    /**
     * Returns a fingerprint representing the authorizations managed by this authorizer. The fingerprint will be
     * used for comparison to determine if two policy-based authorizers represent a compatible set of users and/or groups.
     *
     * @return the fingerprint for this Authorizer
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    String getFingerprint() throws AuthorizationAccessException;

    /**
     * Parses the fingerprint and adds any users and groups to the current Authorizer.
     *
     * @param fingerprint the fingerprint that was obtained from calling getFingerprint() on another Authorizer.
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException;

    /**
     * When the fingerprints are not equal, this method will check if the proposed fingerprint is inheritable.
     * If the fingerprint is an exact match, this method will not be invoked as there is nothing to inherit.
     *
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws UninheritableAuthorizationsException if the proposed fingerprint was uninheritable
     */
    void checkInheritability(final String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException;

    /**
     * Adds the given user.
     *
     * @param user the user to add
     * @return the user that was added
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if there is already a user with the same identity
     */
    User addUser(User user) throws AuthorizationAccessException;

    /**
     * Determines whether the specified user is configurable. Provides the opportunity for a ConfigurableUserGroupProvider to prevent
     * editing of a specific user. By default, all known users are configurable.
     *
     * @param user the user
     * @return is configurable
     */
    default boolean isConfigurable(User user) {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null");
        }

        return getUser(user.getIdentifier()) != null;
    }

    /**
     * The user represented by the provided instance will be updated based on the provided instance.
     *
     * @param user an updated user instance
     * @return the updated user instance, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if there is already a user with the same identity
     */
    User updateUser(final User user) throws AuthorizationAccessException;

    /**
     * Deletes the given user.
     *
     * @param user the user to delete
     * @return the user that was deleted, or null if no matching user was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    User deleteUser(User user) throws AuthorizationAccessException;

    /**
     * Adds a new group.
     *
     * @param group the Group to add
     * @return the added Group
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if a group with the same name already exists
     */
    Group addGroup(Group group) throws AuthorizationAccessException;

    /**
     * Determines whether the specified group is configurable. Provides the opportunity for a ConfigurableUserGroupProvider to prevent
     * editing of a specific group. By default, all known groups are configurable.
     *
     * @param group the group
     * @return is configurable
     */
    default boolean isConfigurable(Group group) {
        if (group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }

        return getGroup(group.getIdentifier()) != null;
    }

    /**
     * The group represented by the provided instance will be updated based on the provided instance.
     *
     * @param group an updated group instance
     * @return the updated group instance, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     * @throws IllegalStateException if there is already a group with the same name
     */
    Group updateGroup(Group group) throws AuthorizationAccessException;

    /**
     * Deletes the given group.
     *
     * @param group the group to delete
     * @return the deleted group, or null if no matching group was found
     * @throws AuthorizationAccessException if there was an unexpected error performing the operation
     */
    Group deleteGroup(Group group) throws AuthorizationAccessException;
}
