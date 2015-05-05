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
package org.apache.nifi.admin.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.DownloadAuthorization;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.user.NiFiUserGroup;

/**
 * Manages NiFi user accounts.
 */
public interface UserService {

    /**
     * Creates a new user account using the specified dn and justification.
     *
     * @param dn user dn
     * @param justification why the account is necessary
     * @return the created NiFiUser
     */
    NiFiUser createPendingUserAccount(String dn, String justification);

    /**
     * @return Determines if there are any PENDING user accounts present
     */
    Boolean hasPendingUserAccount();

    /**
     * @param dnChain user dn chain
     * @param attributes attributes for authorization request
     * @return Determines if the users in the dnChain are authorized to download
     * content with the specified attributes
     */
    DownloadAuthorization authorizeDownload(List<String> dnChain, Map<String, String> attributes);

    /**
     * Updates a user group using the specified group comprised of the specified
     * users. Returns all the users that are currently in the specified group.
     *
     * @param group group
     * @param userIds users
     * @param authorities auths
     * @return a user group
     */
    NiFiUserGroup updateGroup(String group, Set<String> userIds, Set<Authority> authorities);

    /**
     * Authorizes the user specified.
     *
     * @param dn user dn
     * @return the user for the given dn if found
     */
    NiFiUser checkAuthorization(String dn);

    /**
     * Deletes the user with the specified id.
     *
     * @param id user identifier
     */
    void deleteUser(String id);

    /**
     * Disables the specified users account.
     *
     * @param id user identifier
     * @return user for the given identifier
     */
    NiFiUser disable(String id);

    /**
     * Disables the specified user group.
     *
     * @param group to disable
     * @return user group
     */
    NiFiUserGroup disableGroup(String group);

    /**
     * Updates the specified user with the specified authorities.
     *
     * @param id identifier of user
     * @param authorities auths to set
     * @return the updated user
     */
    NiFiUser update(String id, Set<Authority> authorities);

    /**
     * Invalidates the specified user account.
     *
     * @param id identifier of user account to invalidate
     */
    void invalidateUserAccount(String id);

    /**
     * Invalidates the user accounts associated with the specified user group.
     *
     * @param group to invalidate user accounts on
     */
    void invalidateUserGroupAccount(String group);

    /**
     * Ungroups the specified group.
     *
     * @param group to split up
     */
    void ungroup(String group);

    /**
     * Ungroups the specified user.
     *
     * @param id user to ungroup
     */
    void ungroupUser(String id);

    /**
     * Returns a collection of all NiFiUsers.
     *
     * @return Collection of users
     */
    Collection<NiFiUser> getUsers();

    /**
     * Finds the specified user by id.
     *
     * @param id of the user
     * @return the user object
     */
    NiFiUser getUserById(String id);

    /**
     * Finds the specified user by dn.
     *
     * @param dn the user dn
     * @return the newly created user
     * @throws AdministrationException ae
     */
    NiFiUser getUserByDn(String dn);
}
