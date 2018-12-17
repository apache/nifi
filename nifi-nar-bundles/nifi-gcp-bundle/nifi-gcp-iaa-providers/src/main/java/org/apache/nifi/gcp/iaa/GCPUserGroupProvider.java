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
package org.apache.nifi.gcp.iaa;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.admin.directory.model.Groups;
import com.google.api.services.admin.directory.model.Member;
import com.google.api.services.admin.directory.model.Members;

public class GCPUserGroupProvider implements UserGroupProvider {

    private static final Logger logger = LoggerFactory.getLogger(GCPUserGroupProvider.class);

    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final long MINIMUM_SYNC_INTERVAL_MILLISECONDS = 60_000;

    public static final String PROP_SYNC_INTERVAL = "Sync Interval";
    public static final String PROP_DOMAIN = "Domain";
    public static final String PROP_PRIVATE_KEY_PATH = "PEM Key Path";
    public static final String PROP_SERVICE_ACCOUNT_EMAIL = "Service Account Email";
    public static final String PROP_ADMIN_EMAIL = "Admin Email";

    private ScheduledExecutorService sync;
    private AtomicReference<TenantHolder> tenants = new AtomicReference<>(null);

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return tenants.get().getAllUsers();
    }

    @Override
    public User getUser(String identifier) throws AuthorizationAccessException {
        return tenants.get().getUsersById().get(identifier);
    }

    @Override
    public User getUserByIdentity(String identity) throws AuthorizationAccessException {
        return tenants.get().getUser(identity);
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return tenants.get().getAllGroups();
    }

    @Override
    public Group getGroup(String identifier) throws AuthorizationAccessException {
        return tenants.get().getGroupsById().get(identifier);
    }

    @Override
    public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
        final TenantHolder holder = tenants.get();
        return new UserAndGroups() {
            @Override
            public User getUser() {
                return holder.getUser(identity);
            }

            @Override
            public Set<Group> getGroups() {
                return holder.getGroups(identity);
            }
        };
    }

    @Override
    public void initialize(UserGroupProviderInitializationContext initializationContext) throws AuthorizerCreationException {
        sync = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            final ThreadFactory factory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = factory.newThread(r);
                thread.setName(String.format("%s (%s) - background sync thread", getClass().getSimpleName(), initializationContext.getIdentifier()));
                return thread;
            }
        });
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        try {
            final PropertyValue keyPath = configurationContext.getProperty(PROP_PRIVATE_KEY_PATH);
            final PropertyValue serviceAccount = configurationContext.getProperty(PROP_SERVICE_ACCOUNT_EMAIL);
            final PropertyValue adminAccount = configurationContext.getProperty(PROP_ADMIN_EMAIL);
            final PropertyValue domain = configurationContext.getProperty(PROP_DOMAIN);
            final PropertyValue rawSyncInterval = configurationContext.getProperty(PROP_SYNC_INTERVAL);

            if (!rawSyncInterval.isSet()) {
                throw new AuthorizerCreationException(String.format("The '%s' must be specified.", PROP_SYNC_INTERVAL));
            }
            if (!keyPath.isSet()) {
                throw new AuthorizerCreationException(String.format("The '%s' must be specified.", PROP_PRIVATE_KEY_PATH));
            }
            if (!serviceAccount.isSet()) {
                throw new AuthorizerCreationException(String.format("The '%s' must be specified.", PROP_SERVICE_ACCOUNT_EMAIL));
            }
            if (!adminAccount.isSet()) {
                throw new AuthorizerCreationException(String.format("The '%s' must be specified.", PROP_ADMIN_EMAIL));
            }
            if (!domain.isSet()) {
                throw new AuthorizerCreationException(String.format("The '%s' must be specified.", PROP_DOMAIN));
            }

            final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            final Directory service = new Directory
                    .Builder(httpTransport, JSON_FACTORY,getCredentials(httpTransport, keyPath.getValue(), serviceAccount.getValue(), adminAccount.getValue()))
                    .setApplicationName("NiFi")
                    .build();

            final long syncInterval;

            try {
                syncInterval = FormatUtils.getTimeDuration(rawSyncInterval.getValue(), TimeUnit.MILLISECONDS);
            } catch (final IllegalArgumentException iae) {
                throw new AuthorizerCreationException(String.format("The %s '%s' is not a valid time duration", PROP_SYNC_INTERVAL, rawSyncInterval.getValue()));
            }

            if (syncInterval < MINIMUM_SYNC_INTERVAL_MILLISECONDS) {
                throw new AuthorizerCreationException(String.format("The %s '%s' is below the minimum value of '%d ms'",
                        PROP_SYNC_INTERVAL, rawSyncInterval.getValue(), MINIMUM_SYNC_INTERVAL_MILLISECONDS));
            }

            // perform the initial load, tenants must be loaded as the configured UserGroupProvider is supplied
            // to the AccessPolicyProvider for granting initial permissions
            load(service, domain.getValue());

            // ensure the tenants were successfully synced
            if (tenants.get() == null) {
                throw new AuthorizerCreationException("Unable to sync users and groups.");
            }

            // schedule the background thread to load the users/groups
            sync.scheduleWithFixedDelay(() -> load(service, domain.getValue()), syncInterval, syncInterval, TimeUnit.MILLISECONDS);

        } catch (IOException | GeneralSecurityException e) {
            throw new AuthorizerCreationException(e);
        } catch (final AuthorizationAccessException e) {
            throw new AuthorizerCreationException(e);
        }
    }

    /**
     * Load the users/groups and memberships
     * @param service Service definition for Directory
     * @param domain Domain of the organization
     */
    private void load(Directory service, String domain) {
        try {
            final List<User> userList = new ArrayList<>();
            final List<Group> groupList = new ArrayList<>();

            // get the list of groups for the domain
            Groups groups = service.groups()
                    .list()
                    .setFields("groups/id,groups/name")
                    .setDomain(domain)
                    .execute();

            // for each group, get the members
            for(com.google.api.services.admin.directory.model.Group group : groups.getGroups()) {
                Members members = service.members()
                        .list(group.getId())
                        .setFields("members/id,members/email")
                        .execute();

                final Group.Builder groupBuilder = new Group.Builder().identifierGenerateFromSeed(group.getId()).name(group.getName());

                for(Member member : members.getMembers()) {
                    final User user = new User.Builder().identifierGenerateFromSeed(member.getId()).identity(member.getEmail()).build();
                    userList.add(user);
                    groupBuilder.addUser(user.getIdentifier());
                }

                groupList.add(groupBuilder.build());
            }

            tenants.set(new TenantHolder(new HashSet<>(userList), new HashSet<>(groupList)));
        } catch (Exception e) {
            logger.error("Failed to retrieve users and groups", e);
        }
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        sync.shutdown();
        try {
            if (!sync.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                logger.info("Failed to stop ldap sync thread in 10 sec. Terminating");
                sync.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Creates an authorized Credential object.
     * @param httpTransport The network HTTP Transport.
     * @param keyPath Path to the PEM file containing the private key of the service account
     * @param serviceAccount email address of the service account to use for API access
     * @param adminAccount email address of the user impersonated by the service account
     * @return An authorized Credential object.
     */
    private static Credential getCredentials(final NetHttpTransport httpTransport, String keyPath,
            String serviceAccount, String adminAccount) throws GeneralSecurityException, IOException {
        List<String> scopes = new ArrayList<String>();
        scopes.add(DirectoryScopes.ADMIN_DIRECTORY_GROUP_READONLY);
        scopes.add(DirectoryScopes.ADMIN_DIRECTORY_GROUP_MEMBER_READONLY);
        scopes.add(DirectoryScopes.ADMIN_DIRECTORY_USER_READONLY);

        return new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(JSON_FACTORY)
                .setServiceAccountId(serviceAccount)
                .setServiceAccountPrivateKeyFromPemFile(new File(keyPath))
                .setServiceAccountScopes(scopes)
                .setServiceAccountUser(adminAccount)
                .build();
    }

}
