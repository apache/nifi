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
package org.apache.nifi.web.dao;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.web.api.dto.UserGroupDTO;

import java.util.Set;

public interface UserGroupDAO {

    /**
     * @param userGroupId user group ID
     * @return Determines if the specified user group exists
     */
    boolean hasUserGroup(String userGroupId);

    /**
     * Creates a user group.
     *
     * @param userGroupDTO The user group DTO
     * @return The user group transfer object
     */
    Group createUserGroup(UserGroupDTO userGroupDTO);

    /**
     * Gets the user group with the specified ID.
     *
     * @param userGroupId The user group ID
     * @return The user group transfer object
     */
    Group getUserGroup(String userGroupId);

    /**
     * Gets the groups for the user with the specified ID.
     *
     * @param userId The user ID
     * @return The set of groups
     */
    Set<Group> getUserGroupsForUser(String userId);

    /**
     * Gets the access policies for the user with the specified ID.
     *
     * @param userId The user ID
     * @return The set of access policies
     */
    Set<AccessPolicy> getAccessPoliciesForUser(String userId);

    /**
     * Gets the access policies for the user group with the specified ID.
     *
     * @param userGroupId The user group ID
     * @return The set of access policies
     */
    Set<AccessPolicy> getAccessPoliciesForUserGroup(String userGroupId);

    /**
     * Gets all user groups.
     *
     * @return The user group transfer objects
     */
    Set<Group> getUserGroups();

    /**
     * Updates the specified user group.
     *
     * @param userGroupDTO The user group DTO
     * @return The user group transfer object
     */
    Group updateUserGroup(UserGroupDTO userGroupDTO);

    /**
     * Deletes the specified user group.
     *
     * @param userGroupId The user group ID
     * @return The user group transfer object of the deleted user group
     */
    Group deleteUserGroup(String userGroupId);

}
