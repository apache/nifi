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
import org.apache.nifi.authorization.User;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.dao.UserDAO;

public class StandardUserDAO implements UserDAO {

    final AbstractPolicyBasedAuthorizer authorizer;

    public StandardUserDAO(AbstractPolicyBasedAuthorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Override
    public boolean hasUser(String userId) {
        return authorizer.getUser(userId) != null;
    }

    @Override
    public User createUser(UserDTO userDTO) {
        User user = buildUser(userDTO);
        return authorizer.addUser(user);
    }

    @Override
    public User getUser(String userId) {
        return authorizer.getUser(userId);
    }

    @Override
    public User updateUser(UserDTO userDTO) {
        return authorizer.updateUser(buildUser(userDTO));
    }

    @Override
    public User deleteUser(String userId) {
        return authorizer.deleteUser(authorizer.getUser(userId));
    }

    private User buildUser(UserDTO userDTO) {
        return new User.Builder().addGroups(userDTO.getGroups()).identifier(userDTO.getIdentity()).identity(userDTO.getIdentity()).build();
    }
}
