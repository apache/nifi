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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.components.PropertyValue;
import org.junit.Test;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.admin.directory.model.Group;
import com.google.api.services.admin.directory.model.Groups;
import com.google.api.services.admin.directory.model.User;
import com.google.api.services.admin.directory.model.Users;

public class GCPUserGroupProviderIT {

    private static final String KEY_PATH = "/path/to/key.pem";
    private static final String DOMAIN = "example.com";
    private static final String ADMIN_EMAIL = "admin@example.com";
    private static final String USERACCOUNT_EMAIL = "nifi-ugprovider@serviceaccount.com";

    @Test
    public void testUserGroupProvider() throws Exception {
        final GCPUserGroupProvider provider = new GCPUserGroupProvider();
        final AuthorizerConfigurationContext context = mock(AuthorizerConfigurationContext.class);
        final UserGroupProviderInitializationContext initContext = mock(UserGroupProviderInitializationContext.class);

        when(initContext.getIdentifier()).thenReturn("Integration Test");

        final PropertyValue adminProp = mock(PropertyValue.class);
        when(adminProp.getValue()).thenReturn(ADMIN_EMAIL);
        final PropertyValue domainProp = mock(PropertyValue.class);
        when(domainProp.getValue()).thenReturn(DOMAIN);
        final PropertyValue serviceProp = mock(PropertyValue.class);
        when(serviceProp.getValue()).thenReturn(USERACCOUNT_EMAIL);
        final PropertyValue keyProp = mock(PropertyValue.class);
        when(keyProp.getValue()).thenReturn(KEY_PATH);
        final PropertyValue syncProp = mock(PropertyValue.class);
        when(syncProp.isSet()).thenReturn(true);
        when(syncProp.getValue()).thenReturn("60s");

        when(context.getProperty(GCPUserGroupProvider.PROP_ADMIN_EMAIL)).thenReturn(adminProp);
        when(context.getProperty(GCPUserGroupProvider.PROP_DOMAIN)).thenReturn(domainProp);
        when(context.getProperty(GCPUserGroupProvider.PROP_SERVICE_ACCOUNT_EMAIL)).thenReturn(serviceProp);
        when(context.getProperty(GCPUserGroupProvider.PROP_PRIVATE_KEY_PATH)).thenReturn(keyProp);
        when(context.getProperty(GCPUserGroupProvider.PROP_SYNC_INTERVAL)).thenReturn(syncProp);

        provider.initialize(initContext);
        provider.onConfigured(context);

        provider.getGroups().forEach(group -> {
            System.out.println("Group Name - " + group.getName());
            group.getUsers().forEach(user -> System.out.println("\t" + user + "=" + provider.getUser(user).getIdentity()));
        });

        System.out.println();

        provider.getUsers().forEach(user -> {
            UserAndGroups ug = provider.getUserAndGroups(user.getIdentity());
            System.out.println(ug.getUser().getIdentity());
            ug.getGroups().forEach(group -> System.out.println("\t" + group.getName()));
        });

    }

    @Test
    public void testDirectoryAccess() throws GeneralSecurityException, IOException {
        final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        final Directory service = new Directory.Builder(httpTransport, JacksonFactory.getDefaultInstance(), getCredentials(httpTransport))
                .setApplicationName("Integration Test")
                .build();

        Users users = service.users()
                .list()
                .setDomain(DOMAIN)
                .execute();
        for(User user : users.getUsers()) {
            System.out.println(user.getPrimaryEmail());
        }

        Groups groups = service.groups()
                .list()
                .setDomain(DOMAIN)
                .execute();
        for(Group group : groups.getGroups()) {
            System.out.println(group.getName());
        }
    }

    private HttpRequestInitializer getCredentials(NetHttpTransport httpTransport) throws IOException, GeneralSecurityException {
        List<String> scopes = new ArrayList<String>();
        scopes.add(DirectoryScopes.ADMIN_DIRECTORY_USER_READONLY);
        scopes.add(DirectoryScopes.ADMIN_DIRECTORY_GROUP_READONLY);
        scopes.add(DirectoryScopes.ADMIN_DIRECTORY_GROUP_MEMBER_READONLY);

        return new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(JacksonFactory.getDefaultInstance())
                .setServiceAccountId(USERACCOUNT_EMAIL)
                .setServiceAccountPrivateKeyFromPemFile(new File(KEY_PATH))
                .setServiceAccountScopes(scopes)
                .setServiceAccountUser(ADMIN_EMAIL)
                .build();
    }

}
