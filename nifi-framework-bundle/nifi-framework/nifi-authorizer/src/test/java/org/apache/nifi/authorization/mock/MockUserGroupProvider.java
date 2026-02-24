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
package org.apache.nifi.authorization.mock;

import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

import java.util.Set;

public class MockUserGroupProvider implements UserGroupProvider {
    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return null;
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return null;
    }

    @Override
    public Group getGroup(final String identifier) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public UserAndGroups getUserAndGroups(final String identity) throws AuthorizationAccessException {
        return null;
    }

    @Override
    public void initialize(final UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {

    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {

    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {

    }
}
