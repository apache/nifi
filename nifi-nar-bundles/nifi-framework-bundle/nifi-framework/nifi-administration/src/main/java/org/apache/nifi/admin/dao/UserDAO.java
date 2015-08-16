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
package org.apache.nifi.admin.dao;

import java.util.Date;
import java.util.Set;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;

/**
 * Defines the user data access object.
 */
public interface UserDAO {

    /**
     * Determines whether there are any PENDING user accounts.
     *
     * @return true if pending
     * @throws DataAccessException dae
     */
    Boolean hasPendingUserAccounts() throws DataAccessException;

    /**
     * Returns all users.
     *
     * @return all users
     * @throws DataAccessException dae
     */
    Set<NiFiUser> findUsers() throws DataAccessException;

    /**
     * Returns all user groups.
     *
     * @return all group names
     * @throws DataAccessException dae
     */
    Set<String> findUserGroups() throws DataAccessException;

    /**
     * Returns all users for the specified group.
     *
     * @param group group
     * @return users in group
     * @throws DataAccessException dae
     */
    Set<NiFiUser> findUsersForGroup(String group) throws DataAccessException;

    /**
     * Returns the user with the specified id.
     *
     * @param id user id
     * @return user for the given id
     * @throws DataAccessException dae
     */
    NiFiUser findUserById(String id) throws DataAccessException;

    /**
     * Returns the user with the specified DN.
     *
     * @param dn user dn
     * @return user
     */
    NiFiUser findUserByDn(String dn) throws DataAccessException;

    /**
     * Creates a new user based off the specified NiFiUser.
     *
     * @param user to create
     */
    void createUser(NiFiUser user) throws DataAccessException;

    /**
     * Updates the specified NiFiUser.
     *
     * @param user to update
     */
    void updateUser(NiFiUser user) throws DataAccessException;

    /**
     * Deletes the specified user.
     *
     * @param id user identifier
     * @throws DataAccessException dae
     */
    void deleteUser(String id) throws DataAccessException;

    /**
     * Sets the status of the specified group.
     *
     * @param group group
     * @param status status
     * @throws DataAccessException dae
     */
    void updateGroupStatus(String group, AccountStatus status) throws DataAccessException;

    /**
     * Sets the last verified time for all users in the specified group.
     *
     * @param group group
     * @param lastVerified date last verified
     * @throws DataAccessException dae
     */
    void updateGroupVerification(String group, Date lastVerified) throws DataAccessException;

    /**
     * Ungroups the specified group.
     *
     * @param group to ungroup
     * @throws DataAccessException dae
     */
    void ungroup(String group) throws DataAccessException;

}
