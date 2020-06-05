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
package org.apache.nifi.toolkit.cli.impl.client.registry;

import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.client.NiFiRegistryException;

import java.io.IOException;
import java.util.List;

/**
 * Provides API for the services might be called from registry related to tenants.
 */
public interface TenantsClient {

    /**
     * Returns all the users.
     *
     * @return The list of users in the registry.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    List<User> getUsers() throws NiFiRegistryException, IOException;

    /**
     * Returns a given user based on id.
     *
     * @param id Identifier of the user.
     *
     * @return The user.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    User getUser(String id) throws NiFiRegistryException, IOException;

    /**
     * Creates a new user in the registry.
     *
     * @param user The new user. Note: identifier will be ignored and generated.
     *
     * @return The user after store, containing it's identifier.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    User createUser(User user) throws NiFiRegistryException, IOException;

    /**
     * Updates an existing user.
     *
     * @param user The user with the new attributes.
     *
     * @return The user after store.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    User updateUser(User user) throws NiFiRegistryException, IOException;

    /**
     * Returns all the user groups.
     *
     * @return The list of user groups.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    List<UserGroup> getUserGroups() throws NiFiRegistryException, IOException;

    /**
     * Returns the given user group based on identifier.
     *
     * @param id The user group's identifier.
     *
     * @return The user group.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    UserGroup getUserGroup(String id) throws NiFiRegistryException, IOException;

    /**
     * Creates a new user group in the registry.
     *
     * @param group The user group to store. Note: identifier will be ignored and generated.
     *
     * @return The stored user group, containing id.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.@throws IOException
     */
    UserGroup createUserGroup(UserGroup group) throws NiFiRegistryException, IOException;

    /**
     * Updates an existing user group.
     *
     * @param group The user group with the new attributes.
     *
     * @return The user group after store.
     *
     * @throws NiFiRegistryException Thrown in case os unsuccessful execution.
     * @throws IOException Thrown when there is an issue with communicating the registry.
     */
    UserGroup updateUserGroup(UserGroup group) throws NiFiRegistryException, IOException;
}
