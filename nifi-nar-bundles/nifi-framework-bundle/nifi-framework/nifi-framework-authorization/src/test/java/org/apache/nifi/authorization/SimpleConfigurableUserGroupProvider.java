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

import java.util.Set;

public class SimpleConfigurableUserGroupProvider extends SimpleUserGroupProvider implements ConfigurableUserGroupProvider {

    private final Set<User> users;
    private final Set<Group> groups;

    public SimpleConfigurableUserGroupProvider(Set<User> users, Set<Group> groups) {
        super(users, groups);

        this.users = users;
        this.groups = groups;
    }

    @Override
    public User addUser(User user) throws AuthorizationAccessException {
        users.add(user);
        return user;
    }

    @Override
    public User updateUser(User user) throws AuthorizationAccessException {
        users.remove(user);
        users.add(user);
        return user;
    }

    @Override
    public User deleteUser(User user) throws AuthorizationAccessException {
        users.remove(user);
        return user;
    }

    @Override
    public Group addGroup(Group group) throws AuthorizationAccessException {
        groups.add(group);
        return group;
    }

    @Override
    public Group updateGroup(Group group) throws AuthorizationAccessException {
        groups.remove(group);
        groups.add(group);
        return group;
    }

    @Override
    public Group deleteGroup(Group group) throws AuthorizationAccessException {
        groups.remove(group);
        return group;
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        return "fingerprint";
    }

    @Override
    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {

    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {

    }
}