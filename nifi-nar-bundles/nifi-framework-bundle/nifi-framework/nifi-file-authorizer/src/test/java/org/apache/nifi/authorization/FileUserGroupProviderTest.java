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

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileUserGroupProviderTest {

    private static final String EMPTY_TENANTS_CONCISE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<tenants/>";

    private static final String EMPTY_TENANTS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<tenants>"
        + "</tenants>";

    private static final String BAD_SCHEMA_TENANTS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<tenant>"
        + "</tenant>";

    private static final String SIMPLE_TENANTS_BY_USER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<tenants>" +
            "  <users>" +
            "    <user identifier=\"user-1\" identity=\"user-1\"/>" +
            "    <user identifier=\"user-2\" identity=\"user-2\"/>" +
            "  </users>" +
            "</tenants>";

    private static final String TENANTS =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<tenants>" +
            "  <groups>" +
            "    <group identifier=\"group-1\" name=\"group-1\">" +
            "       <user identifier=\"user-1\" />" +
            "    </group>" +
            "    <group identifier=\"group-2\" name=\"group-2\">" +
            "       <user identifier=\"user-2\" />" +
            "    </group>" +
            "  </groups>" +
            "  <users>" +
            "    <user identifier=\"user-1\" identity=\"user-1\" />" +
            "    <user identifier=\"user-2\" identity=\"user-2\" />" +
            "  </users>" +
            "</tenants>";

    private NiFiProperties properties;
    private FileUserGroupProvider userGroupProvider;
    private File primaryTenants;
    private File restoreTenants;

    private AuthorizerConfigurationContext configurationContext;

    @Before
    public void setup() throws IOException {
        // primary tenants
        primaryTenants = new File("target/authorizations/users.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(primaryTenants.getParentFile());

        // restore authorizations
        restoreTenants = new File("target/restore/users.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(restoreTenants.getParentFile());

        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restoreTenants.getParentFile());

        configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE))).thenReturn(new StandardPropertyValue(null, null));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_TENANTS_FILE))).thenReturn(new StandardPropertyValue(primaryTenants.getPath(), null));
        when(configurationContext.getProperties()).then((invocation) -> {
            final Map<String, String> properties = new HashMap<>();

            final PropertyValue tenantFile = configurationContext.getProperty(FileUserGroupProvider.PROP_TENANTS_FILE);
            if (tenantFile != null) {
                properties.put(FileUserGroupProvider.PROP_TENANTS_FILE, tenantFile.getValue());
            }

            final PropertyValue legacyAuthFile = configurationContext.getProperty(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE);
            if (legacyAuthFile != null) {
                properties.put(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE, legacyAuthFile.getValue());
            }

            int i = 1;
            while (true) {
                final String key = FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + i++;
                final PropertyValue value = configurationContext.getProperty(key);
                if (value == null) {
                    break;
                } else {
                    properties.put(key, value.getValue());
                }
            }

            return properties;
        });

        userGroupProvider = new FileUserGroupProvider();
        userGroupProvider.setNiFiProperties(properties);
        userGroupProvider.initialize(null);
    }

    @After
    public void cleanup() throws Exception {
        deleteFile(primaryTenants);
        deleteFile(restoreTenants);
    }

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvided() throws Exception {
        when(configurationContext.getProperty(eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users.xml", null));

        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        // verify all users got created correctly
        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(6, users.size());

        final User user1 = userGroupProvider.getUserByIdentity("user1");
        assertNotNull(user1);

        final User user2 = userGroupProvider.getUserByIdentity("user2");
        assertNotNull(user2);

        final User user3 = userGroupProvider.getUserByIdentity("user3");
        assertNotNull(user3);

        final User user4 = userGroupProvider.getUserByIdentity("user4");
        assertNotNull(user4);

        final User user5 = userGroupProvider.getUserByIdentity("user5");
        assertNotNull(user5);

        final User user6 = userGroupProvider.getUserByIdentity("user6");
        assertNotNull(user6);

        // verify one group got created
        final Set<Group> groups = userGroupProvider.getGroups();
        assertEquals(1, groups.size());
        final Group group1 = groups.iterator().next();
        assertEquals("group1", group1.getName());
    }

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvidedWithIdentityMappings() throws Exception {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^CN=(.*?), OU=(.*?), O=(.*?), L=(.*?), ST=(.*?), C=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");

        properties = getNiFiProperties(props);
        when(properties.getRestoreDirectory()).thenReturn(restoreTenants.getParentFile());
        userGroupProvider.setNiFiProperties(properties);

        when(configurationContext.getProperty(eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users-with-dns.xml", null));

        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        final User user1 = userGroupProvider.getUserByIdentity("user1");
        assertNotNull(user1);

        final User user2 = userGroupProvider.getUserByIdentity("user2");
        assertNotNull(user2);

        final User user3 = userGroupProvider.getUserByIdentity("user3");
        assertNotNull(user3);

        final User user4 = userGroupProvider.getUserByIdentity("user4");
        assertNotNull(user4);

        final User user5 = userGroupProvider.getUserByIdentity("user5");
        assertNotNull(user5);

        final User user6 = userGroupProvider.getUserByIdentity("user6");
        assertNotNull(user6);

        // verify one group got created
        final Set<Group> groups = userGroupProvider.getGroups();
        assertEquals(1, groups.size());
        final Group group1 = groups.iterator().next();
        assertEquals("group1", group1.getName());
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenBadLegacyUsersFileProvided() throws Exception {
        when(configurationContext.getProperty(eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/does-not-exist.xml", null));

        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);
    }

    @Test
    public void testOnConfiguredWhenInitialUsersNotProvided() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(0, users.size());
    }

    @Test
    public void testOnConfiguredWhenInitialUsersProvided() throws Exception {
        final String adminIdentity = "admin-user";
        final String nodeIdentity1 = "node-identity-1";
        final String nodeIdentity2 = "node-identity-2";

        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "3")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null));

        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(3, users.size());

        assertTrue(users.contains(new User.Builder().identifierGenerateFromSeed(adminIdentity).identity(adminIdentity).build()));
        assertTrue(users.contains(new User.Builder().identifierGenerateFromSeed(nodeIdentity1).identity(nodeIdentity1).build()));
        assertTrue(users.contains(new User.Builder().identifierGenerateFromSeed(nodeIdentity2).identity(nodeIdentity2).build()));
    }

    @Test
    public void testOnConfiguredWhenTenantsExistAndInitialUsersProvided() throws Exception {
        final String adminIdentity = "admin-user";
        final String nodeIdentity1 = "node-identity-1";
        final String nodeIdentity2 = "node-identity-2";

        // despite setting initial users, they will not be loaded as the tenants file is non-empty
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "3")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null));

        writeFile(primaryTenants, SIMPLE_TENANTS_BY_USER);
        userGroupProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(2, users.size());

        assertTrue(users.contains(new User.Builder().identifier("user-1").identity("user-1").build()));
        assertTrue(users.contains(new User.Builder().identifier("user-2").identity("user-2").build()));
    }

    @Test
    public void testOnConfiguredWhenTenantsFileDoesNotExist() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(0, userGroupProvider.getUsers().size());
        assertEquals(0, userGroupProvider.getGroups().size());
    }

    @Test
    public void testOnConfiguredWhenRestoreDoesNotExist() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        assertEquals(primaryTenants.length(), restoreTenants.length());
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryDoesNotExist() throws Exception {
        writeFile(restoreTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);
    }


    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryTenantsDifferentThanRestore() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS);
        writeFile(restoreTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWithBadTenantsSchema() throws Exception {
        writeFile(primaryTenants, BAD_SCHEMA_TENANTS);
        userGroupProvider.onConfigured(configurationContext);
    }

    @Test
    public void testGetAllUsersGroupsPolicies() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = userGroupProvider.getGroups();
        assertEquals(2, groups.size());

        boolean foundGroup1 = false;
        boolean foundGroup2 = false;

        for (Group group : groups) {
            if (group.getIdentifier().equals("group-1") && group.getName().equals("group-1")
                    && group.getUsers().size() == 1 && group.getUsers().contains("user-1")) {
                foundGroup1 = true;
            } else if (group.getIdentifier().equals("group-2") && group.getName().equals("group-2")
                    && group.getUsers().size() == 1 && group.getUsers().contains("user-2")) {
                foundGroup2 = true;
            }
        }

        assertTrue(foundGroup1);
        assertTrue(foundGroup2);

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(2, users.size());

        boolean foundUser1 = false;
        boolean foundUser2 = false;

        for (User user : users) {
            if (user.getIdentifier().equals("user-1") && user.getIdentity().equals("user-1")) {
                foundUser1 = true;
            } else if (user.getIdentifier().equals("user-2") && user.getIdentity().equals("user-2")) {
                foundUser2 = true;
            }
        }

        assertTrue(foundUser1);
        assertTrue(foundUser2);
    }

    // --------------- User Tests ------------------------

    @Test
    public void testAddUser() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(0, userGroupProvider.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-1")
                .identity("user-identity-1")
                .build();

        final User addedUser = userGroupProvider.addUser(user);
        assertNotNull(addedUser);
        assertEquals(user.getIdentifier(), addedUser.getIdentifier());
        assertEquals(user.getIdentity(), addedUser.getIdentity());

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(1, users.size());
    }

    @Test
    public void testGetUserByIdentifierWhenFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        final String identifier = "user-1";
        final User user = userGroupProvider.getUser(identifier);
        assertNotNull(user);
        assertEquals(identifier, user.getIdentifier());
    }

    @Test
    public void testGetUserByIdentifierWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        final String identifier = "user-X";
        final User user = userGroupProvider.getUser(identifier);
        assertNull(user);
    }

    @Test
    public void testGetUserByIdentityWhenFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        final String identity = "user-1";
        final User user = userGroupProvider.getUserByIdentity(identity);
        assertNotNull(user);
        assertEquals(identity, user.getIdentifier());
    }

    @Test
    public void testGetUserByIdentityWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        final String identity = "user-X";
        final User user = userGroupProvider.getUserByIdentity(identity);
        assertNull(user);
    }

    @Test
    public void testDeleteUser() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        // retrieve user-1 and verify it exists
        final User user = userGroupProvider.getUser("user-1");
        assertEquals("user-1", user.getIdentifier());

        // delete user-1
        final User deletedUser = userGroupProvider.deleteUser(user);
        assertNotNull(deletedUser);
        assertEquals("user-1", deletedUser.getIdentifier());

        // should be one less user
        assertEquals(1, userGroupProvider.getUsers().size());
        assertNull(userGroupProvider.getUser(user.getIdentifier()));
    }

    @Test
    public void testDeleteUserWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        //user that doesn't exist
        final User user = new User.Builder().identifier("user-X").identity("user-identity-X").build();

        // should return null and still have 2 users because nothing was deleted
        final User deletedUser = userGroupProvider.deleteUser(user);
        assertNull(deletedUser);
        assertEquals(2, userGroupProvider.getUsers().size());
    }

    @Test
    public void testUpdateUserWhenFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-1")
                .identity("new-identity")
                .build();

        final User updatedUser = userGroupProvider.updateUser(user);
        assertNotNull(updatedUser);
        assertEquals(user.getIdentifier(), updatedUser.getIdentifier());
        assertEquals(user.getIdentity(), updatedUser.getIdentity());
    }

    @Test
    public void testUpdateUserWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-X")
                .identity("new-identity")
                .build();

        final User updatedUser = userGroupProvider.updateUser(user);
        assertNull(updatedUser);
    }

    // --------------- Group Tests ------------------------

    @Test
    public void testAddGroup() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(0, userGroupProvider.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-1")
                .name("group-name-1")
                .build();

        final Group addedGroup = userGroupProvider.addGroup(group);
        assertNotNull(addedGroup);
        assertEquals(group.getIdentifier(), addedGroup.getIdentifier());
        assertEquals(group.getName(), addedGroup.getName());
        assertEquals(0, addedGroup.getUsers().size());

        final Set<Group> groups = userGroupProvider.getGroups();
        assertEquals(1, groups.size());
    }

    @Test
    public void testAddGroupWithUser() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-XXX")
                .name("group-name-XXX")
                .addUser("user-1")
                .build();

        final Group addedGroup = userGroupProvider.addGroup(group);
        assertNotNull(addedGroup);
        assertEquals(group.getIdentifier(), addedGroup.getIdentifier());
        assertEquals(group.getName(), addedGroup.getName());
        assertEquals(1, addedGroup.getUsers().size());

        final Set<Group> groups = userGroupProvider.getGroups();
        assertEquals(3, groups.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testAddGroupWhenUserDoesNotExist() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(0, userGroupProvider.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-1")
                .name("group-name-1")
                .addUser("user1")
                .build();

        userGroupProvider.addGroup(group);
    }

    @Test
    public void testGetGroupByIdentifierWhenFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        final String identifier = "group-1";
        final Group group = userGroupProvider.getGroup(identifier);
        assertNotNull(group);
        assertEquals(identifier, group.getIdentifier());
    }

    @Test
    public void testGetGroupByIdentifierWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        final String identifier = "group-X";
        final Group group = userGroupProvider.getGroup(identifier);
        assertNull(group);
    }

    @Test
    public void testDeleteGroupWhenFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        // retrieve group-1
        final Group group = userGroupProvider.getGroup("group-1");
        assertEquals("group-1", group.getIdentifier());

        // delete group-1
        final Group deletedGroup = userGroupProvider.deleteGroup(group);
        assertNotNull(deletedGroup);
        assertEquals("group-1", deletedGroup.getIdentifier());

        // verify there is one less overall group
        assertEquals(1, userGroupProvider.getGroups().size());

        // verify we can no longer retrieve group-1 by identifier
        assertNull(userGroupProvider.getGroup(group.getIdentifier()));
    }

    @Test
    public void testDeleteGroupWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-X")
                .name("group-name-X")
                .build();

        final Group deletedGroup = userGroupProvider.deleteGroup(group);
        assertNull(deletedGroup);
        assertEquals(2, userGroupProvider.getGroups().size());
    }

    @Test
    public void testUpdateGroupWhenFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        // verify user-1 is in group-1 before the update
        final Group groupBefore = userGroupProvider.getGroup("group-1");
        assertEquals(1, groupBefore.getUsers().size());
        assertTrue(groupBefore.getUsers().contains("user-1"));

        final Group group = new Group.Builder()
                .identifier("group-1")
                .name("new-name")
                .addUser("user-2")
                .build();

        final Group updatedGroup = userGroupProvider.updateGroup(group);
        assertEquals(group.getIdentifier(), updatedGroup.getIdentifier());
        assertEquals(group.getName(), updatedGroup.getName());

        assertEquals(1, updatedGroup.getUsers().size());
        assertTrue(updatedGroup.getUsers().contains("user-2"));
    }

    @Test
    public void testUpdateGroupWhenNotFound() throws Exception {
        writeFile(primaryTenants, TENANTS);
        userGroupProvider.onConfigured(configurationContext);
        assertEquals(2, userGroupProvider.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-X")
                .name("group-X")
                .build();

        final Group updatedGroup = userGroupProvider.updateGroup(group);
        assertNull(updatedGroup);
        assertEquals(2, userGroupProvider.getGroups().size());
    }

    private static void writeFile(final File file, final String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(bytes);
        }
    }

    private static boolean deleteFile(final File file) {
        if (file.isDirectory()) {
            FileUtils.deleteFilesInDir(file, null, null, true, true);
        }
        return FileUtils.deleteFile(file, null, 10);
    }

    private NiFiProperties getNiFiProperties(final Properties properties) {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getPropertyKeys()).thenReturn(properties.stringPropertyNames());

        when(nifiProperties.getProperty(anyString())).then(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                return properties.getProperty((String)invocationOnMock.getArguments()[0]);
            }
        });
        return nifiProperties;
    }

}
