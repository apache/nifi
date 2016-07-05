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
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
            "  <policies>" +
            "      <policy identifier=\"policy-1\" resource=\"/flow\" action=\"R\">" +
                    "  <group identifier=\"group-1\" />" +
                    "  <group identifier=\"group-2\" />" +
                    "  <user identifier=\"user-1\" />" +
            "      </policy>" +
            "      <policy identifier=\"policy-2\" resource=\"/flow\" action=\"W\">" +
            "        <user identifier=\"user-2\" />" +
            "      </policy>" +
            "  </policies>" +
            "</authorizations>";

    // This is the root group id from the flow.xml.gz in src/test/resources
    private static final String ROOT_GROUP_ID = "e530e14c-adcf-41c2-b5d6-d9a59ba8765c";

    private NiFiProperties properties;
    private FileAuthorizer authorizer;
    private File primary;
    private File restore;
    private File flow;

    private AuthorizerConfigurationContext configurationContext;

    @Before
    public void setup() throws IOException {
        // primary authorizations
        primary = new File("target/primary/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(primary.getParentFile());

        // restore authorizations
        restore = new File("target/restore/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(restore.getParentFile());

        flow = new File("src/test/resources/flow.xml.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flow.getParentFile());

        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restore.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flow);

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
    public void testOnConfiguredWhenLegacyUsersFileProvidedWithOverlappingRoles() throws Exception {
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users-multirole.xml", null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        UsersAndAccessPolicies usersAndAccessPolicies = authorizer.getUsersAndAccessPolicies();
        assertEquals(1, usersAndAccessPolicies.getAccessPolicies(ResourceType.Flow.getValue()).size());
        assertEquals(2, usersAndAccessPolicies.getAccessPolicies(ResourceType.Controller.getValue()).size());
        assertEquals(1, usersAndAccessPolicies.getAccessPolicies(ResourceType.System.getValue()).size());
        assertEquals(2, usersAndAccessPolicies.getAccessPolicies(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).size());
    }

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvided() throws Exception {
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users.xml", null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        // verify all users got created correctly
        final Set<User> users = authorizer.getUsers();
        assertEquals(6, users.size());

        final User user1 = authorizer.getUserByIdentity("user1");
        assertNotNull(user1);

        final User user2 = authorizer.getUserByIdentity("user2");
        assertNotNull(user2);

        final User user3 = authorizer.getUserByIdentity("user3");
        assertNotNull(user3);

        final User user4 = authorizer.getUserByIdentity("user4");
        assertNotNull(user4);

        final User user5 = authorizer.getUserByIdentity("user5");
        assertNotNull(user5);

        final User user6 = authorizer.getUserByIdentity("user6");
        assertNotNull(user6);

        // verify one group got created
        final Set<Group> groups = authorizer.getGroups();
        assertEquals(1, groups.size());
        assertEquals("group1", groups.iterator().next().getName());

        // verify more than one policy got created
        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertTrue(policies.size() > 0);

        // verify user1's policies
        final Map<String,Set<RequestAction>> user1Policies = getResourceActions(policies, user1);
        assertEquals(4, user1Policies.size());

        assertTrue(user1Policies.containsKey(ResourceType.Flow.getValue()));
        assertEquals(1, user1Policies.get(ResourceType.Flow.getValue()).size());
        assertTrue(user1Policies.get(ResourceType.Flow.getValue()).contains(RequestAction.READ));

        assertTrue(user1Policies.containsKey(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID));
        assertEquals(1, user1Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).size());
        assertTrue(user1Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).contains(RequestAction.READ));

        // verify user2's policies
        final Map<String,Set<RequestAction>> user2Policies = getResourceActions(policies, user2);
        assertEquals(2, user2Policies.size());

        assertTrue(user2Policies.containsKey(ResourceType.Provenance.getValue()));
        assertEquals(1, user2Policies.get(ResourceType.Provenance.getValue()).size());
        assertTrue(user2Policies.get(ResourceType.Provenance.getValue()).contains(RequestAction.READ));

        // verify user3's policies
        final Map<String,Set<RequestAction>> user3Policies = getResourceActions(policies, user3);
        assertEquals(4, user3Policies.size());

        assertTrue(user3Policies.containsKey(ResourceType.Flow.getValue()));
        assertEquals(1, user3Policies.get(ResourceType.Flow.getValue()).size());
        assertTrue(user3Policies.get(ResourceType.Flow.getValue()).contains(RequestAction.READ));

        assertTrue(user3Policies.containsKey(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID));
        assertEquals(2, user3Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).size());
        assertTrue(user3Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).contains(RequestAction.WRITE));

        // verify user4's policies
        final Map<String,Set<RequestAction>> user4Policies = getResourceActions(policies, user4);
        assertEquals(5, user4Policies.size());

        assertTrue(user4Policies.containsKey(ResourceType.Flow.getValue()));
        assertEquals(1, user4Policies.get(ResourceType.Flow.getValue()).size());
        assertTrue(user4Policies.get(ResourceType.Flow.getValue()).contains(RequestAction.READ));

        assertTrue(user4Policies.containsKey(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID));
        assertEquals(1, user4Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).size());
        assertTrue(user4Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).contains(RequestAction.READ));

        assertTrue(user4Policies.containsKey(ResourceType.Tenant.getValue()));
        assertEquals(2, user4Policies.get(ResourceType.Tenant.getValue()).size());
        assertTrue(user4Policies.get(ResourceType.Tenant.getValue()).contains(RequestAction.WRITE));

        assertTrue(user4Policies.containsKey(ResourceType.Policy.getValue()));
        assertEquals(2, user4Policies.get(ResourceType.Policy.getValue()).size());
        assertTrue(user4Policies.get(ResourceType.Policy.getValue()).contains(RequestAction.WRITE));

        // verify user5's policies
        final Map<String,Set<RequestAction>> user5Policies = getResourceActions(policies, user5);
        assertEquals(1, user5Policies.size());

        assertTrue(user5Policies.containsKey(ResourceType.Proxy.getValue()));
        assertEquals(2, user5Policies.get(ResourceType.Proxy.getValue()).size());
        assertTrue(user5Policies.get(ResourceType.Proxy.getValue()).contains(RequestAction.WRITE));

        // verify user6's policies
        final Map<String,Set<RequestAction>> user6Policies = getResourceActions(policies, user6);
        assertEquals(2, user6Policies.size());

        assertTrue(user6Policies.containsKey(ResourceType.SiteToSite.getValue()));
        assertEquals(2, user6Policies.get(ResourceType.SiteToSite.getValue()).size());
        assertTrue(user6Policies.get(ResourceType.SiteToSite.getValue()).contains(RequestAction.WRITE));
    }

    private Map<String,Set<RequestAction>> getResourceActions(final Set<AccessPolicy> policies, final User user) {
        Map<String,Set<RequestAction>> resourceActionMap = new HashMap<>();

        for (AccessPolicy accessPolicy : policies) {
            if (accessPolicy.getUsers().contains(user.getIdentifier())) {
                Set<RequestAction> actions = resourceActionMap.get(accessPolicy.getResource());
                if (actions == null) {
                    actions = new HashSet<>();
                    resourceActionMap.put(accessPolicy.getResource(), actions);
                }
                actions.add(accessPolicy.getAction());
            }
        }

        return resourceActionMap;
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenBadLegacyUsersFileProvided() throws Exception {
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/does-not-exist.xml", null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenInitialAdminAndLegacyUsersProvided() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users.xml", null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
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
    public void testOnConfiguredWhenInitialAdminProvided() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(4, policies.size());

        final String rootGroupResource = ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID;

        boolean foundRootGroupPolicy = false;
        for (AccessPolicy policy : policies) {
            if (policy.getResource().equals(rootGroupResource)) {
                foundRootGroupPolicy = true;
                break;
            }
        }

        assertTrue(foundRootGroupPolicy);
    }

    @Test
    public void testOnConfiguredWhenInitialAdminProvidedAndNoFlowExists() throws Exception {
        // setup NiFi properties to return a file that does not exist
        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restore.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(new File("src/test/resources/does-not-exist.xml.gz"));
        authorizer.setNiFiProperties(properties);

        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(3, policies.size());

        final String rootGroupResource = ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID;

        boolean foundRootGroupPolicy = false;
        for (AccessPolicy policy : policies) {
            if (policy.getResource().equals(rootGroupResource)) {
                foundRootGroupPolicy = true;
                break;
            }
        }

        assertFalse(foundRootGroupPolicy);
    }

    @Test
    public void testOnConfiguredWhenInitialAdminProvidedAndFlowIsNull() throws Exception {
        // setup NiFi properties to return a file that does not exist
        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restore.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(null);
        authorizer.setNiFiProperties(properties);

        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(3, policies.size());

        final String rootGroupResource = ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID;

        boolean foundRootGroupPolicy = false;
        for (AccessPolicy policy : policies) {
            if (policy.getResource().equals(rootGroupResource)) {
                foundRootGroupPolicy = true;
                break;
            }
        }

        assertFalse(foundRootGroupPolicy);
    }

    @Test
    public void testOnConfiguredWhenAuthorizationsFileDoesNotExist() {
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());
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
                    && group.getUsers().size() == 1 && group.getUsers().contains("user-2")) {
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
            if (user.getIdentifier().equals("user-1") && user.getIdentity().equals("user-1")) {
                foundUser1 = true;
            } else if (user.getIdentifier().equals("user-2") && user.getIdentity().equals("user-2")) {
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
                    && policy.getAction() == RequestAction.READ
                    && policy.getGroups().size() == 2
                    && policy.getGroups().contains("group-1")
                    && policy.getGroups().contains("group-2")
                    && policy.getUsers().size() == 1
                    && policy.getUsers().contains("user-1")) {
                foundPolicy1 = true;
            } else if (policy.getIdentifier().equals("policy-2")
                    && policy.getResource().equals("/flow")
                    && policy.getAction() == RequestAction.WRITE
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
                .build();

        final User addedUser = authorizer.addUser(user);
        assertNotNull(addedUser);
        assertEquals(user.getIdentifier(), addedUser.getIdentifier());
        assertEquals(user.getIdentity(), addedUser.getIdentity());

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
                .build();

        final User updatedUser = authorizer.updateUser(user);
        assertNotNull(updatedUser);
        assertEquals(user.getIdentifier(), updatedUser.getIdentifier());
        assertEquals(user.getIdentity(), updatedUser.getIdentity());
    }

    @Test
    public void testUpdateUserWhenNotFound() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final User user = new User.Builder()
                .identifier("user-X")
                .identity("new-identity")
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
    public void testAddGroupWithUser() throws Exception {
        writeAuthorizationsFile(primary, AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-XXX")
                .name("group-name-XXX")
                .addUser("user-1")
                .build();

        final Group addedGroup = authorizer.addGroup(group);
        assertNotNull(addedGroup);
        assertEquals(group.getIdentifier(), addedGroup.getIdentifier());
        assertEquals(group.getName(), addedGroup.getName());
        assertEquals(1, addedGroup.getUsers().size());

        final Set<Group> groups = authorizer.getGroups();
        assertEquals(3, groups.size());
    }


    @Test(expected = IllegalStateException.class)
    public void testAddGroupWhenUserDoesNotExist() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getGroups().size());

        final Group group = new Group.Builder()
                .identifier("group-id-1")
                .name("group-name-1")
                .addUser("user1")
                .build();

        authorizer.addGroup(group);
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

        // verify user-1 is in group-1 before the update
        final Group groupBefore = authorizer.getGroup("group-1");
        assertEquals(1, groupBefore.getUsers().size());
        assertTrue(groupBefore.getUsers().contains("user-1"));

        final Group group = new Group.Builder()
                .identifier("group-1")
                .name("new-name")
                .addUser("user-2")
                .build();

        final Group updatedGroup = authorizer.updateGroup(group);
        assertEquals(group.getIdentifier(), updatedGroup.getIdentifier());
        assertEquals(group.getName(), updatedGroup.getName());

        assertEquals(1, updatedGroup.getUsers().size());
        assertTrue(updatedGroup.getUsers().contains("user-2"));
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
                .action(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy1 = authorizer.addAccessPolicy(policy1);
        assertNotNull(returnedPolicy1);
        assertEquals(policy1.getIdentifier(), returnedPolicy1.getIdentifier());
        assertEquals(policy1.getResource(), returnedPolicy1.getResource());
        assertEquals(policy1.getUsers(), returnedPolicy1.getUsers());
        assertEquals(policy1.getGroups(), returnedPolicy1.getGroups());
        assertEquals(policy1.getAction(), returnedPolicy1.getAction());

        assertEquals(1, authorizer.getAccessPolicies().size());

        // second policy for the same resource
        final AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-2")
                .resource("resource-1")
                .addUser("user-1")
                .addGroup("group-1")
                .action(RequestAction.READ)
                .build();

        try {
            final AccessPolicy returnedPolicy2 = authorizer.addAccessPolicy(policy2);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {
        }

        assertEquals(1, authorizer.getAccessPolicies().size());
    }

    @Test
    public void testAddAccessPolicyWithEmptyUsersAndGroups() throws Exception {
        writeAuthorizationsFile(primary, EMPTY_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());

        final AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-1")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy1 = authorizer.addAccessPolicy(policy1);
        assertNotNull(returnedPolicy1);
        assertEquals(policy1.getIdentifier(), returnedPolicy1.getIdentifier());
        assertEquals(policy1.getResource(), returnedPolicy1.getResource());
        assertEquals(policy1.getUsers(), returnedPolicy1.getUsers());
        assertEquals(policy1.getGroups(), returnedPolicy1.getGroups());
        assertEquals(policy1.getAction(), returnedPolicy1.getAction());

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

        assertEquals(RequestAction.READ, policy.getAction());

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
                .action(RequestAction.READ)
                .build();

        final AccessPolicy updateAccessPolicy = authorizer.updateAccessPolicy(policy);
        assertNotNull(updateAccessPolicy);
        assertEquals("policy-1", updateAccessPolicy.getIdentifier());
        assertEquals("/flow", updateAccessPolicy.getResource());

        assertEquals(1, updateAccessPolicy.getUsers().size());
        assertTrue(updateAccessPolicy.getUsers().contains("user-A"));

        assertEquals(1, updateAccessPolicy.getGroups().size());
        assertTrue(updateAccessPolicy.getGroups().contains("group-A"));

        assertEquals(RequestAction.READ, updateAccessPolicy.getAction());
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
                .action(RequestAction.READ)
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
                .action(RequestAction.READ)
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
                .action(RequestAction.READ)
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
