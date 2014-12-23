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
     * @param dn
     * @param justification
     * @return
     */
    NiFiUser createPendingUserAccount(String dn, String justification);

    /**
     * Determines if there are any PENDING user accounts present.
     *
     * @return
     */
    Boolean hasPendingUserAccount();

    /**
     * Determines if the users in the dnChain are authorized to download content 
     * with the specified attributes.
     * 
     * @param dnChain
     * @param attributes
     * @return 
     */
    DownloadAuthorization authorizeDownload(List<String> dnChain, Map<String, String> attributes);
    
    /**
     * Updates a user group using the specified group comprised of the specified
     * users. Returns all the users that are currently in the specified group.
     *
     * @param group
     * @param userIds
     * @param authorities
     * @return
     */
    NiFiUserGroup updateGroup(String group, Set<String> userIds, Set<Authority> authorities);

    /**
     * Authorizes the user specified.
     *
     * @param dn
     * @return
     */
    NiFiUser checkAuthorization(String dn);

    /**
     * Deletes the user with the specified id.
     *
     * @param id
     */
    void deleteUser(String id);

    /**
     * Disables the specified users account.
     *
     * @param id
     * @return
     */
    NiFiUser disable(String id);

    /**
     * Disables the specified user group.
     *
     * @param group
     * @return
     */
    NiFiUserGroup disableGroup(String group);

    /**
     * Updates the specified user with the specified authorities.
     *
     * @param id
     * @param authorities
     * @return
     */
    NiFiUser update(String id, Set<Authority> authorities);

    /**
     * Invalidates the specified user account.
     *
     * @param id
     */
    void invalidateUserAccount(String id);

    /**
     * Invalidates the user accounts associated with the specified user group.
     *
     * @param group
     */
    void invalidateUserGroupAccount(String group);

    /**
     * Ungroups the specified group.
     *
     * @param group
     */
    void ungroup(String group);

    /**
     * Ungroups the specified user.
     *
     * @param id
     */
    void ungroupUser(String id);

    /**
     * Returns a collection of all NiFiUsers.
     *
     * @return
     */
    Collection<NiFiUser> getUsers();

    /**
     * Finds the specified user by id.
     *
     * @param id
     * @return
     */
    NiFiUser getUserById(String id);

    /**
     * Finds the specified user by dn.
     *
     * @param dn
     * @return
     * @throws AdministrationException
     */
    NiFiUser getUserByDn(String dn);
}
