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

import org.apache.nifi.idp.IdpType;
import org.apache.nifi.idp.IdpUserGroup;

import java.util.List;
import java.util.Set;

/**
 * Manages IDP User Groups.
 */
public interface IdpUserGroupService {

    /**
     * Creates the given user group.
     *
     * @param userGroup the user group to create
     * @return the created user group
     */
    IdpUserGroup createUserGroup(IdpUserGroup userGroup);

    /**
     * Creates the given user groups.
     *
     * @param userGroups the user group to create
     * @return the created user group
     */
    List<IdpUserGroup> createUserGroups(List<IdpUserGroup> userGroups);

    /**
     * Gets the user groups for the given identity.
     *
     * @param identity the user identity
     * @return the list of user groups
     */
    List<IdpUserGroup> getUserGroups(String identity);

    /**
     * Deletes the user groups for the given identity.
     *
     * @param identity the user identity
     */
    void deleteUserGroups(String identity);

    /**
     * Replaces any existing groups for the given user identity with a new set specified by the set of group names.
     *
     * @param userIdentity the user identity
     * @param idpType the idp type for the groups
     * @param groupNames the group names, should already have identity mappings applied if necessary
     * @return the created groups
     */
    List<IdpUserGroup> replaceUserGroups(String userIdentity, IdpType idpType, Set<String> groupNames);

}
