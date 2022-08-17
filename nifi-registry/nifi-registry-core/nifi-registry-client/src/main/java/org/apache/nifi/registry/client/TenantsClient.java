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
package org.apache.nifi.registry.client;

import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.revision.entity.RevisionInfo;

import java.io.IOException;
import java.util.List;

public interface TenantsClient {

    /**
     * Returns all users.
     *
     * @return The list of users.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    List<User> getUsers() throws NiFiRegistryException, IOException;

    /**
     * Returns a user with a given identifier.
     *
     * @param id Identifier of the user.
     *
     * @return The user.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    User getUser(String id) throws NiFiRegistryException, IOException;

    /**
     * Creates a new user in NiFi Registry.
     *
     * @param user The new user. Note: identifier will be ignored and assigned be NiFi Registry.
     *
     * @return The created user with an assigned identifier.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    User createUser(User user) throws NiFiRegistryException, IOException;

    /**
     * Updates an existing user.
     *
     * @param user The user with the new attributes.
     *
     * @return The updated user.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    User updateUser(User user) throws NiFiRegistryException, IOException;

    /**
     * Deletes an existing user.
     *
     * @param id identifier of the user
     * @return the deleted user
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    User deleteUser(String id) throws NiFiRegistryException, IOException;

    /**
     * Deletes an existing user.
     *
     * @param id identifier of the user
     * @param revisionInfo the revision info for the user to delete
     * @return the deleted user
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    User deleteUser(String id, RevisionInfo revisionInfo) throws NiFiRegistryException, IOException;

    /**
     * Returns all user groups.
     *
     * @return The list of user groups.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    List<UserGroup> getUserGroups() throws NiFiRegistryException, IOException;

    /**
     * Returns a user group with a given identifier.
     *
     * @param id Identifier of the user group.
     *
     * @return The user group.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    UserGroup getUserGroup(String id) throws NiFiRegistryException, IOException;

    /**
     * Creates a new user group.
     *
     * @param group The user group to be created. Note: identifier will be ignored and assigned by NiFi Registry.
     *
     * @return The created user group with an assigned identifier.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    UserGroup createUserGroup(UserGroup group) throws NiFiRegistryException, IOException;

    /**
     * Updates an existing user group.
     *
     * @param group The user group with new attributes.
     *
     * @return The user group after store.
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    UserGroup updateUserGroup(UserGroup group) throws NiFiRegistryException, IOException;

    /**
     * Deletes an existing group.
     *
     * @param id identifier of the group
     * @return the deleted group
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    UserGroup deleteUserGroup(String id) throws NiFiRegistryException, IOException;

    /**
     * Deletes an existing group.
     *
     * @param id identifier of the group
     * @param revisionInfo the revision info for the group to delete
     * @return the deleted group
     *
     * @throws NiFiRegistryException Thrown in case of unsuccessful execution.
     * @throws IOException Thrown when there is an issue while communicating with NiFi Registry.
     */
    UserGroup deleteUserGroup(String id, RevisionInfo revisionInfo) throws NiFiRegistryException, IOException;

}