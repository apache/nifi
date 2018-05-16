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

import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Provides authorizes requests to resources using policies persisted in a file.
 */
public class FileAuthorizer extends AbstractPolicyBasedAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizer.class);

    private static final String FILE_USER_GROUP_PROVIDER_ID = "file-user-group-provider";
    private static final String FILE_ACCESS_POLICY_PROVIDER_ID = "file-access-policy-provider";

    static final String PROP_LEGACY_AUTHORIZED_USERS_FILE = "Legacy Authorized Users File";

    private FileUserGroupProvider userGroupProvider = new FileUserGroupProvider();
    private FileAccessPolicyProvider accessPolicyProvider = new FileAccessPolicyProvider();

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
        // initialize the user group provider
        userGroupProvider.initialize(new UserGroupProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return FILE_USER_GROUP_PROVIDER_ID;
            }

            @Override
            public UserGroupProviderLookup getUserGroupProviderLookup() {
                return (identifier) -> null;
            }
        });

        // initialize the access policy provider
        accessPolicyProvider.initialize(new AccessPolicyProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return FILE_ACCESS_POLICY_PROVIDER_ID;
            }

            @Override
            public UserGroupProviderLookup getUserGroupProviderLookup() {
                return (identifier) -> {
                    if (FILE_USER_GROUP_PROVIDER_ID.equals(identifier)) {
                        return userGroupProvider;
                    }

                    return null;
                };
            }

            @Override
            public AccessPolicyProviderLookup getAccessPolicyProviderLookup() {
                return (identifier) ->  null;
            }
        });
    }

    @Override
    public void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final Map<String, String> configurationProperties = configurationContext.getProperties();

        // relay the relevant config
        final Map<String, String> userGroupProperties = new HashMap<>();
        if (configurationProperties.containsKey(FileUserGroupProvider.PROP_TENANTS_FILE)) {
            userGroupProperties.put(FileUserGroupProvider.PROP_TENANTS_FILE, configurationProperties.get(FileUserGroupProvider.PROP_TENANTS_FILE));
        }
        if (configurationProperties.containsKey(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)) {
            userGroupProperties.put(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE, configurationProperties.get(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE));
        }

        // relay the relevant config
        final Map<String, String> accessPolicyProperties = new HashMap<>();
        accessPolicyProperties.put(FileAccessPolicyProvider.PROP_USER_GROUP_PROVIDER, FILE_USER_GROUP_PROVIDER_ID);
        if (configurationProperties.containsKey(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE)) {
            accessPolicyProperties.put(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE, configurationProperties.get(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE));
        }
        if (configurationProperties.containsKey(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)) {
            accessPolicyProperties.put(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY, configurationProperties.get(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY));
        }
        if (configurationProperties.containsKey(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)) {
            accessPolicyProperties.put(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE, configurationProperties.get(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE));
        }

        // ensure all node identities are seeded into the user provider
        configurationProperties.forEach((property, value) -> {
            final Matcher matcher = FileAccessPolicyProvider.NODE_IDENTITY_PATTERN.matcher(property);
            if (matcher.matches()) {
                accessPolicyProperties.put(property, value);
                userGroupProperties.put(property.replace(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX, FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX), value);
            }
        });

        // ensure the initial admin is seeded into the user provider if appropriate
        if (configurationProperties.containsKey(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)) {
            int i = 0;
            while (true) {
                final String key = FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + i++;
                if (!userGroupProperties.containsKey(key)) {
                    userGroupProperties.put(key, configurationProperties.get(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY));
                    break;
                }
            }
        }

        // configure the user group provider
        userGroupProvider.onConfigured(new StandardAuthorizerConfigurationContext(FILE_USER_GROUP_PROVIDER_ID, userGroupProperties));

        // configure the access policy provider
        accessPolicyProvider.onConfigured(new StandardAuthorizerConfigurationContext(FILE_USER_GROUP_PROVIDER_ID, accessPolicyProperties));
    }

    @Override
    public void preDestruction() {

    }

    // ------------------ Groups ------------------

    @Override
    public synchronized Group doAddGroup(Group group) throws AuthorizationAccessException {
        return userGroupProvider.addGroup(group);
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        return userGroupProvider.getGroup(identifier);
    }

    @Override
    public synchronized Group doUpdateGroup(Group group) throws AuthorizationAccessException {
        return userGroupProvider.updateGroup(group);
    }

    @Override
    public synchronized Group deleteGroup(Group group) throws AuthorizationAccessException {
        return userGroupProvider.deleteGroup(group);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return userGroupProvider.getGroups();
    }

    // ------------------ Users ------------------

    @Override
    public synchronized User doAddUser(final User user) throws AuthorizationAccessException {
        return userGroupProvider.addUser(user);
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        return userGroupProvider.getUser(identifier);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        return userGroupProvider.getUserByIdentity(identity);
    }

    @Override
    public synchronized User doUpdateUser(final User user) throws AuthorizationAccessException {
        return userGroupProvider.updateUser(user);
    }

    @Override
    public synchronized User deleteUser(final User user) throws AuthorizationAccessException {
        return userGroupProvider.deleteUser(user);
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return userGroupProvider.getUsers();
    }

    // ------------------ AccessPolicies ------------------

    @Override
    public synchronized AccessPolicy doAddAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        return accessPolicyProvider.addAccessPolicy(accessPolicy);
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        return accessPolicyProvider.getAccessPolicy(identifier);
    }

    @Override
    public synchronized AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        return accessPolicyProvider.updateAccessPolicy(accessPolicy);
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        return accessPolicyProvider.deleteAccessPolicy(accessPolicy);
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return accessPolicyProvider.getAccessPolicies();
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        userGroupProvider.setNiFiProperties(properties);
        accessPolicyProvider.setNiFiProperties(properties);
    }

    @Override
    public synchronized UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
        final AuthorizationsHolder authorizationsHolder = accessPolicyProvider.getAuthorizationsHolder();
        final UserGroupHolder userGroupHolder = userGroupProvider.getUserGroupHolder();

        return new UsersAndAccessPolicies() {
            @Override
            public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) {
                return authorizationsHolder.getAccessPolicy(resourceIdentifier, action);
            }

            @Override
            public User getUser(String identity) {
                return userGroupHolder.getUser(identity);
            }

            @Override
            public Set<Group> getGroups(String userIdentity) {
                return userGroupHolder.getGroups(userIdentity);
            }
        };
    }

}
