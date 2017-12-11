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
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.components.PropertyValue;

import java.util.HashSet;
import java.util.Set;

public class CompositeConfigurableUserGroupProvider extends CompositeUserGroupProvider implements ConfigurableUserGroupProvider {

    static final String PROP_CONFIGURABLE_USER_GROUP_PROVIDER = "Configurable User Group Provider";

    private UserGroupProviderLookup userGroupProviderLookup;
    private ConfigurableUserGroupProvider configurableUserGroupProvider;

    public CompositeConfigurableUserGroupProvider() {
        super(true);
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();

        // initialize the CompositeUserGroupProvider
        super.initialize(initializationContext);
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final PropertyValue configurableUserGroupProviderKey = configurationContext.getProperty(PROP_CONFIGURABLE_USER_GROUP_PROVIDER);
        if (!configurableUserGroupProviderKey.isSet()) {
            throw new AuthorizerCreationException("The Configurable User Group Provider must be set.");
        }

        final UserGroupProvider userGroupProvider = userGroupProviderLookup.getUserGroupProvider(configurableUserGroupProviderKey.getValue());

        if (userGroupProvider == null) {
            throw new AuthorizerCreationException(String.format("Unable to locate the Configurable User Group Provider: %s", configurableUserGroupProviderKey));
        }

        if (!(userGroupProvider instanceof ConfigurableUserGroupProvider)) {
            throw new AuthorizerCreationException(String.format("The Configurable User Group Provider is not configurable: %s", configurableUserGroupProviderKey));
        }

        configurableUserGroupProvider = (ConfigurableUserGroupProvider) userGroupProvider;

        // configure the CompositeUserGroupProvider
        super.onConfigured(configurationContext);
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        return configurableUserGroupProvider.getFingerprint();
    }

    @Override
    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        configurableUserGroupProvider.inheritFingerprint(fingerprint);
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        configurableUserGroupProvider.checkInheritability(proposedFingerprint);
    }

    @Override
    public User addUser(User user) throws AuthorizationAccessException {
        return configurableUserGroupProvider.addUser(user);
    }

    @Override
    public boolean isConfigurable(User user) {
        return configurableUserGroupProvider.isConfigurable(user);
    }

    @Override
    public User updateUser(User user) throws AuthorizationAccessException {
        return configurableUserGroupProvider.updateUser(user);
    }

    @Override
    public User deleteUser(User user) throws AuthorizationAccessException {
        return configurableUserGroupProvider.deleteUser(user);
    }

    @Override
    public Group addGroup(Group group) throws AuthorizationAccessException {
        return configurableUserGroupProvider.addGroup(group);
    }

    @Override
    public boolean isConfigurable(Group group) {
        return configurableUserGroupProvider.isConfigurable(group);
    }

    @Override
    public Group updateGroup(Group group) throws AuthorizationAccessException {
        return configurableUserGroupProvider.updateGroup(group);
    }

    @Override
    public Group deleteGroup(Group group) throws AuthorizationAccessException {
        return configurableUserGroupProvider.deleteGroup(group);
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        final Set<User> users = new HashSet<>(configurableUserGroupProvider.getUsers());
        users.addAll(super.getUsers());
        return users;
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        User user = configurableUserGroupProvider.getUser(identifier);

        if (user == null) {
            user = super.getUser(identifier);
        }

        return user;
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        User user = configurableUserGroupProvider.getUserByIdentity(identity);

        if (user == null) {
            user = super.getUserByIdentity(identity);
        }

        return user;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        final Set<Group> groups = new HashSet<>(configurableUserGroupProvider.getGroups());
        groups.addAll(super.getGroups());
        return groups;
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        Group group = configurableUserGroupProvider.getGroup(identifier);

        if (group == null) {
            group = super.getGroup(identifier);
        }

        return group;
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        UserAndGroups userAndGroups = configurableUserGroupProvider.getUserAndGroups(identity);

        if (userAndGroups.getUser() == null) {
            userAndGroups = super.getUserAndGroups(identity);
        }

        return userAndGroups;
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        super.preDestruction();
    }
}
