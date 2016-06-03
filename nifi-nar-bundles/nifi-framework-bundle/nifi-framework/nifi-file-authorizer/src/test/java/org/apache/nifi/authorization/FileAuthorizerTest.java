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
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileAuthorizerTest {

    private static final String EMPTY_AUTHORIZATIONS_CONCISE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<authorizations/>";

    private static final String EMPTY_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<authorizations>"
        + "</authorizations>";

    private static final String BAD_SCHEMA_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<authorization>"
        + "</authorization>";

    private static final String SIMPLE_AUTHORIZATION_BY_USER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<authorizations>" +
            "  <users>" +
            "    <user identifier=\"user-1\" identity=\"user-1\"/>" +
            "    <user identifier=\"user-2\" identity=\"user-2\"/>" +
            "  </users>" +
            "  <policies>" +
            "      <policy identifier=\"policy-1\" resource=\"/flow\" action=\"R\">" +
            "        <user identifier=\"user-1\" />" +
            "      </policy>" +
            "  </policies>" +
            "</authorizations>";

    private static final String AUTHORIZATIONS =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<authorizations>" +
            "  <groups>" +
            "    <group identifier=\"group-1\" name=\"group-1\" />" +
            "    <group identifier=\"group-2\" name=\"group-2\" />" +
            "  </groups>" +
            "  <users>" +
            "    <user identifier=\"user-1\" identity=\"user-1\">" +
            "      <group identifier=\"group-1\" />" +
            "      <group identifier=\"group-2\" />" +
            "    </user>\n" +
            "    <user identifier=\"user-2\" identity=\"user-2\" />" +
            "  </users>" +
            "  <policies>" +
            "      <policy identifier=\"policy-1\" resource=\"/flow\" action=\"RW\">" +
                    "  <group identifier=\"group-1\" />" +
                    "  <group identifier=\"group-2\" />" +
                    "  <user identifier=\"user-1\" />" +
            "      </policy>" +
            "      <policy identifier=\"policy-2\" resource=\"/flow\" action=\"RW\">" +
            "        <user identifier=\"user-2\" />" +
            "      </policy>" +
            "  </policies>" +
            "</authorizations>";

    private static final String UPDATED_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<resources>"
            + "<resource identifier=\"/flow\">"
                + "<authorization identity=\"user-1\" action=\"RW\"/>"
            + "</resource>"
        + "</resources>";

    private FileAuthorizer authorizer;
    private File primary;
    private File restore;

    private AuthorizerConfigurationContext configurationContext;

    @Before
    public void setup() throws IOException {
        // primary authorizations
        primary = new File("target/primary/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(primary.getParentFile());

        // restore authorizations
        restore = new File("target/restore/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(restore.getParentFile());

        final NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restore.getParentFile());

        configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(Mockito.eq("Authorizations File"))).thenReturn(new StandardPropertyValue(primary.getPath(), null));

        authorizer = new FileAuthorizer();
        authorizer.setNiFiProperties(properties);
        authorizer.initialize(null);
    }

    @After
    public void cleanup() throws Exception {
        deleteFile(primary);
        deleteFile(restore);
    }

    @Test
    public void testOnConfiguredWhenInitialAdminProvided() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(Mockito.eq("Initial Admin Identity")))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(4, policies.size());
    }

    @Test
    public void testOnConfiguredWhenInitialAdminNotProvided() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(0, users.size());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(0, policies.size());
    }

    @Test
    public void testOnConfiguredWhenRestoreDoesNotExist() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        assertEquals(primary.length(), restore.length());
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryDoesNotExist() throws Exception {
        writeAuthorizationsFile(restore, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryDifferentThanRestore() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        writeAuthorizationsFile(restore, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWithBadSchema() throws Exception {
        writeAuthorizationsFile(primary, BAD_SCHEMA_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
    }

    @Test
    public void testAuthorizedUserAction() throws Exception {
        writeAuthorizationsFile(primary, SIMPLE_AUTHORIZATION_BY_USER);
        authorizer.onConfigured(configurationContext);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getFlowResource())
                .identity("user-1")
                .anonymous(false)
                .accessAttempt(true)
                .action(RequestAction.READ)
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        assertTrue(Result.Approved.equals(result.getResult()));
    }

    @Test
    public void testUnauthorizedUser() throws Exception {
        writeAuthorizationsFile(primary, SIMPLE_AUTHORIZATION_BY_USER);
        authorizer.onConfigured(configurationContext);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getFlowResource())
                .identity("user-2")
                .anonymous(false)
                .accessAttempt(true)
                .action(RequestAction.READ)
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        assertFalse(Result.Approved.equals(result.getResult()));
    }

    @Test
    public void testUnauthorizedAction() throws Exception {
        writeAuthorizationsFile(primary, SIMPLE_AUTHORIZATION_BY_USER);
        authorizer.onConfigured(configurationContext);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getFlowResource())
                .identity("user-1")
                .anonymous(false)
                .accessAttempt(true)
                .action(RequestAction.WRITE)
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        assertFalse(Result.Approved.equals(result.getResult()));
    }

    @Test
    public void testGetAllUsersGroupsPolicies() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);

        final Set<Group> groups = authorizer.getGroups();
        assertEquals(2, groups.size());

        boolean foundGroup1 = false;
        boolean foundGroup2 = false;

        for (Group group : groups) {
            if (group.getIdentifier().equals("group-1") && group.getName().equals("group-1")
                    && group.getUsers().size() == 1 && group.getUsers().contains("user-1")) {
                foundGroup1 = true;
            } else if (group.getIdentifier().equals("group-2") && group.getName().equals("group-2")
                    && group.getUsers().size() == 1 && group.getUsers().contains("user-1")) {
                foundGroup2 = true;
            }
        }

        assertTrue(foundGroup1);
        assertTrue(foundGroup2);

        final Set<User> users = authorizer.getUsers();
        assertEquals(2, users.size());

        boolean foundUser1 = false;
        boolean foundUser2 = false;

        for (User user : users) {
            if (user.getIdentifier().equals("user-1") && user.getIdentity().equals("user-1")
                    && user.getGroups().size() == 2 && user.getGroups().contains("group-1")
                    && user.getGroups().contains("group-2")) {
                foundUser1 = true;
            } else if (user.getIdentifier().equals("user-2") && user.getIdentity().equals("user-2")
                    && user.getGroups().size() == 0) {
                foundUser2 = true;
            }
        }

        assertTrue(foundUser1);
        assertTrue(foundUser2);

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(2, policies.size());

        boolean foundPolicy1 = false;
        boolean foundPolicy2 = false;

        for (AccessPolicy policy : policies) {
            if (policy.getIdentifier().equals("policy-1")
                    && policy.getResource().equals("/flow")
                    && policy.getActions().size() == 2
                    && policy.getActions().contains(RequestAction.READ)
                    && policy.getActions().contains(RequestAction.WRITE)
                    && policy.getGroups().size() == 2
                    && policy.getGroups().contains("group-1")
                    && policy.getGroups().contains("group-2")
                    && policy.getUsers().size() == 1
                    && policy.getUsers().contains("user-1")) {
                foundPolicy1 = true;
            } else if (policy.getIdentifier().equals("policy-2")
                    && policy.getResource().equals("/flow")
                    && policy.getActions().size() == 2
                    && policy.getActions().contains(RequestAction.READ)
                    && policy.getActions().contains(RequestAction.WRITE)
                    && policy.getGroups().size() == 0
                    && policy.getUsers().size() == 1
                    && policy.getUsers().contains("user-2")) {
                foundPolicy2 = true;
            }
        }

        assertTrue(foundPolicy1);
        assertTrue(foundPolicy2);
    }

    // --------------- User Tests ------------------------

    @Test
    public void testAddUser() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-1")
                .identity("user-identity-1")
                .addGroup("group1")
                .addGroup("group2")
                .build();

        final User addedUser = authorizer.addUser(user);
        assertNotNull(addedUser);
        assertEquals(user.getIdentifier(), addedUser.getIdentifier());
        assertEquals(user.getIdentity(), addedUser.getIdentity());
        assertEquals(2, addedUser.getGroups().size());
        assertTrue(addedUser.getGroups().contains("group1"));
        assertTrue(addedUser.getGroups().contains("group2"));

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());
    }

    @Test
    public void testGetUserByIdentifierWhenFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identifier = "user-1";
        final User user = authorizer.getUser(identifier);
        assertNotNull(user);
        assertEquals(identifier, user.getIdentifier());
    }

    @Test
    public void testGetUserByIdentifierWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identifier = "user-X";
        final User user = authorizer.getUser(identifier);
        assertNull(user);
    }

    @Test
    public void testGetUserByIdentityWhenFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identity = "user-1";
        final User user = authorizer.getUserByIdentity(identity);
        assertNotNull(user);
        assertEquals(identity, user.getIdentifier());
    }

    @Test
    public void testGetUserByIdentityWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identity = "user-X";
        final User user = authorizer.getUserByIdentity(identity);
        assertNull(user);
    }

    @Test
    public void testDeleteUser() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        // retrieve user-1 and verify it exsits
        final User user = authorizer.getUser("user-1");
        assertEquals("user-1", user.getIdentifier());

        final AccessPolicy policy1 = authorizer.getAccessPolicy("policy-1");
        assertTrue(policy1.getUsers().contains("user-1"));

        // delete user-1
        final User deletedUser = authorizer.deleteUser(user);
        assertNotNull(deletedUser);
        assertEquals("user-1", deletedUser.getIdentifier());

        // should be one less user
        assertEquals(1, authorizer.getUsers().size());
        assertNull(authorizer.getUser(user.getIdentifier()));

        // verify policy-1 no longer has a reference to user-1
        final AccessPolicy updatedPolicy1 = authorizer.getAccessPolicy("policy-1");
        assertFalse(updatedPolicy1.getUsers().contains("user-1"));
    }

    @Test
    public void testDeleteUserWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        //user that doesn't exist
        final User user = new User.Builder().identifier("user-X").identity("user-identity-X").build();

        // should return null and still have 2 users because nothing was deleted
        final User deletedUser = authorizer.deleteUser(user);
        assertNull(deletedUser);
        assertEquals(2, authorizer.getUsers().size());
    }

    @Test
    public void testUpdateUserWhenFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-1")
                .identity("new-identity")
                .addGroup("new-group")
                .build();

        final User updatedUser = authorizer.updateUser(user);
        assertNotNull(updatedUser);
        assertEquals(user.getIdentifier(), updatedUser.getIdentifier());
        assertEquals(user.getIdentity(), updatedUser.getIdentity());
        assertEquals(1, updatedUser.getGroups().size());
        assertTrue(updatedUser.getGroups().contains("new-group"));
    }

    @Test
    public void testUpdateUserWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-X")
                .identity("new-identity")
                .addGroup("new-group")
                .build();

        final User updatedUser = authorizer.updateUser(user);
        assertNull(updatedUser);
    }

    // --------------- Group Tests ------------------------

    @Test
    public void testAddGroup() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-1")
                .name("group-name-1")
                .addUser("user1") // should be ignored
                .build();

        final Group addedGroup = authorizer.addGroup(group);
        assertNotNull(addedGroup);
        assertEquals(group.getIdentifier(), addedGroup.getIdentifier());
        assertEquals(group.getName(), addedGroup.getName());
        assertEquals(0, addedGroup.getUsers().size());

        final Set<Group> groups = authorizer.getGroups();
        assertEquals(1, groups.size());
    }

    @Test
    public void testGetGroupByIdentifierWhenFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final String identifier = "group-1";
        final Group group = authorizer.getGroup(identifier);
        assertNotNull(group);
        assertEquals(identifier, group.getIdentifier());
    }

    @Test
    public void testGetGroupByIdentifierWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final String identifier = "group-X";
        final Group group = authorizer.getGroup(identifier);
        assertNull(group);
    }

    @Test
    public void testDeleteGroupWhenFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        // retrieve user-1 and verify its in group-1
        final User user1 = authorizer.getUser("user-1");
        assertNotNull(user1);
        assertEquals(2, user1.getGroups().size());
        assertTrue(user1.getGroups().contains("group-1"));

        final AccessPolicy policy1 = authorizer.getAccessPolicy("policy-1");
        assertTrue(policy1.getGroups().contains("group-1"));

        // retrieve group-1
        final Group group = authorizer.getGroup("group-1");
        assertEquals("group-1", group.getIdentifier());

        // delete group-1
        final Group deletedGroup = authorizer.deleteGroup(group);
        assertNotNull(deletedGroup);
        assertEquals("group-1", deletedGroup.getIdentifier());

        // verify there is one less overall group
        assertEquals(1, authorizer.getGroups().size());

        // verify we can no longer retrieve group-1 by identifier
        assertNull(authorizer.getGroup(group.getIdentifier()));

        // verify user-1 is no longer in group-1
        final User updatedUser1 = authorizer.getUser("user-1");
        assertNotNull(updatedUser1);
        assertEquals(1, updatedUser1.getGroups().size());
        assertFalse(updatedUser1.getGroups().contains("group-1"));

        // verify group-1 is no longer in policy-1
        final AccessPolicy updatedPolicy1 = authorizer.getAccessPolicy("policy-1");
        assertFalse(updatedPolicy1.getGroups().contains("group-1"));
    }

    @Test
    public void testDeleteGroupWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-X")
                .name("group-name-X")
                .build();

        final Group deletedGroup = authorizer.deleteGroup(group);
        assertNull(deletedGroup);
        assertEquals(2, authorizer.getGroups().size());
    }

    @Test
    public void testUpdateGroupWhenFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-1")
                .name("new-name")
                .build();

        final Group updatedGroup = authorizer.updateGroup(group);
        assertEquals(group.getIdentifier(), updatedGroup.getIdentifier());
        assertEquals(group.getName(), updatedGroup.getName());
    }

    @Test
    public void testUpdateGroupWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-X")
                .name("group-X")
                .build();

        final Group updatedGroup = authorizer.updateGroup(group);
        assertNull(updatedGroup);
        assertEquals(2, authorizer.getGroups().size());
    }

    // --------------- AccessPolicy Tests ------------------------

    @Test
    public void testAddAccessPolicy() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());

        final AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-1")
                .addUser("user-1")
                .addGroup("group-1")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy1 = authorizer.addAccessPolicy(policy1);
        assertNotNull(returnedPolicy1);
        assertEquals(policy1.getIdentifier(), returnedPolicy1.getIdentifier());
        assertEquals(policy1.getResource(), returnedPolicy1.getResource());
        assertEquals(policy1.getUsers(), returnedPolicy1.getUsers());
        assertEquals(policy1.getGroups(), returnedPolicy1.getGroups());
        assertEquals(policy1.getActions(), returnedPolicy1.getActions());

        assertEquals(1, authorizer.getAccessPolicies().size());

        // second policy for the same resource
        final AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-2")
                .resource("resource-1")
                .addUser("user-1")
                .addGroup("group-1")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy2 = authorizer.addAccessPolicy(policy2);
        assertNotNull(returnedPolicy2);
        assertEquals(2, authorizer.getAccessPolicies().size());
    }

    @Test
    public void testAddAccessPolicyWithEmptyUsersAndGroups() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());

        final AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-1")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy1 = authorizer.addAccessPolicy(policy1);
        assertNotNull(returnedPolicy1);
        assertEquals(policy1.getIdentifier(), returnedPolicy1.getIdentifier());
        assertEquals(policy1.getResource(), returnedPolicy1.getResource());
        assertEquals(policy1.getUsers(), returnedPolicy1.getUsers());
        assertEquals(policy1.getGroups(), returnedPolicy1.getGroups());
        assertEquals(policy1.getActions(), returnedPolicy1.getActions());

        assertEquals(1, authorizer.getAccessPolicies().size());
    }

    @Test
    public void testGetAccessPolicy() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = authorizer.getAccessPolicy("policy-1");
        assertNotNull(policy);
        assertEquals("policy-1", policy.getIdentifier());
        assertEquals("/flow", policy.getResource());

        assertEquals(2, policy.getActions().size());
        assertTrue(policy.getActions().contains(RequestAction.WRITE));
        assertTrue(policy.getActions().contains(RequestAction.READ));

        assertEquals(1, policy.getUsers().size());
        assertTrue(policy.getUsers().contains("user-1"));

        assertEquals(2, policy.getGroups().size());
        assertTrue(policy.getGroups().contains("group-1"));
        assertTrue(policy.getGroups().contains("group-2"));
    }

    @Test
    public void testGetAccessPolicyWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = authorizer.getAccessPolicy("policy-X");
        assertNull(policy);
    }

    @Test
    public void testUpdateAccessPolicy() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy updateAccessPolicy = authorizer.updateAccessPolicy(policy);
        assertNotNull(updateAccessPolicy);
        assertEquals("policy-1", updateAccessPolicy.getIdentifier());
        assertEquals("resource-A", updateAccessPolicy.getResource());

        assertEquals(1, updateAccessPolicy.getUsers().size());
        assertTrue(updateAccessPolicy.getUsers().contains("user-A"));

        assertEquals(1, updateAccessPolicy.getGroups().size());
        assertTrue(updateAccessPolicy.getGroups().contains("group-A"));

        assertEquals(1, updateAccessPolicy.getActions().size());
        assertTrue(updateAccessPolicy.getActions().contains(RequestAction.READ));
    }

    @Test
    public void testUpdateAccessPolicyWhenResourceNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-XXX")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy updateAccessPolicy = authorizer.updateAccessPolicy(policy);
        assertNull(updateAccessPolicy);
    }

    @Test
    public void testDeleteAccessPolicy() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy deletedAccessPolicy = authorizer.deleteAccessPolicy(policy);
        assertNotNull(deletedAccessPolicy);
        assertEquals(policy.getIdentifier(), deletedAccessPolicy.getIdentifier());

        // should have one less policy, and get by policy id should return null
        assertEquals(1, authorizer.getAccessPolicies().size());
        assertNull(authorizer.getAccessPolicy(policy.getIdentifier()));
    }

    @Test
    public void testDeleteAccessPolicyWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-XXX")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .addAction(RequestAction.READ)
                .build();

        final AccessPolicy deletedAccessPolicy = authorizer.deleteAccessPolicy(policy);
        assertNull(deletedAccessPolicy);
    }

    private static void writeAuthorizationsFile(final File file, final String content) throws Exception {
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
}
