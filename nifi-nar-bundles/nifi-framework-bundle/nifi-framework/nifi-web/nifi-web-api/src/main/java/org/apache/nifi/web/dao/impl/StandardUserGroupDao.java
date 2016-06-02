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
import org.apache.nifi.authorization.Group;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.dao.UserGroupDAO;

public class StandardUserGroupDao implements UserGroupDAO {

    final AbstractPolicyBasedAuthorizer authorizer;

    public StandardUserGroupDao(AbstractPolicyBasedAuthorizer authorizer) {
        this.authorizer = authorizer;
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
}
