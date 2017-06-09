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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileAuthorizerTest {

    private static final String EMPTY_AUTHORIZATIONS_CONCISE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<authorizations/>";

    private static final String EMPTY_TENANTS_CONCISE =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<tenants/>";

    private static final String EMPTY_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<authorizations>"
        + "</authorizations>";

    private static final String EMPTY_TENANTS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<tenants>"
        + "</tenants>";

    private static final String BAD_SCHEMA_AUTHORIZATIONS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<authorization>"
        + "</authorization>";

    private static final String BAD_SCHEMA_TENANTS =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<tenant>"
        + "</tenant>";

    private static final String SIMPLE_AUTHORIZATION_BY_USER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<authorizations>" +
            "  <policies>" +
            "      <policy identifier=\"policy-1\" resource=\"/flow\" action=\"R\">" +
            "        <user identifier=\"user-1\" />" +
            "      </policy>" +
            "  </policies>" +
            "</authorizations>";

    private static final String SIMPLE_TENANTS_BY_USER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<tenants>" +
            "  <users>" +
            "    <user identifier=\"user-1\" identity=\"user-1\"/>" +
            "    <user identifier=\"user-2\" identity=\"user-2\"/>" +
            "  </users>" +
            "</tenants>";

    private static final String AUTHORIZATIONS =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
            "<authorizations>" +
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

    private static final String TENANTS_FOR_ADMIN_AND_NODES =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                    "<tenants>" +
                    "  <users>" +
                    "    <user identifier=\"admin-user\" identity=\"admin-user\"/>" +
                    "    <user identifier=\"node1\" identity=\"node1\"/>" +
                    "    <user identifier=\"node2\" identity=\"node2\"/>" +
                    "  </users>" +
                    "</tenants>";

    // This is the root group id from the flow.xml.gz in src/test/resources
    private static final String ROOT_GROUP_ID = "e530e14c-adcf-41c2-b5d6-d9a59ba8765c";

    private NiFiProperties properties;
    private FileAuthorizer authorizer;
    private File primaryAuthorizations;
    private File primaryTenants;
    private File restoreAuthorizations;
    private File restoreTenants;
    private File flow;
    private File flowNoPorts;
    private File flowWithDns;

    private AuthorizerConfigurationContext configurationContext;

    @Before
    public void setup() throws IOException {
        // primary authorizations
        primaryAuthorizations = new File("target/authorizations/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(primaryAuthorizations.getParentFile());

        // primary tenants
        primaryTenants = new File("target/authorizations/users.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(primaryTenants.getParentFile());

        // restore authorizations
        restoreAuthorizations = new File("target/restore/authorizations.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(restoreAuthorizations.getParentFile());

        // restore authorizations
        restoreTenants = new File("target/restore/users.xml");
        FileUtils.ensureDirectoryExistAndCanAccess(restoreTenants.getParentFile());

        flow = new File("src/test/resources/flow.xml.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flow.getParentFile());

        flowNoPorts = new File("src/test/resources/flow-no-ports.xml.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flowNoPorts.getParentFile());

        flowWithDns = new File("src/test/resources/flow-with-dns.xml.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flowWithDns.getParentFile());

        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flow);

        configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE))).thenReturn(new StandardPropertyValue(primaryAuthorizations.getPath(), null));
        when(configurationContext.getProperty(Mockito.eq(FileUserGroupProvider.PROP_TENANTS_FILE))).thenReturn(new StandardPropertyValue(primaryTenants.getPath(), null));
        when(configurationContext.getProperties()).then((invocation) -> {
            final Map<String, String> properties = new HashMap<>();

            final PropertyValue authFile = configurationContext.getProperty(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE);
            if (authFile != null) {
                properties.put(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE, authFile.getValue());
            }

            final PropertyValue tenantFile = configurationContext.getProperty(FileUserGroupProvider.PROP_TENANTS_FILE);
            if (tenantFile != null) {
                properties.put(FileUserGroupProvider.PROP_TENANTS_FILE, tenantFile.getValue());
            }

            final PropertyValue legacyAuthFile = configurationContext.getProperty(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE);
            if (legacyAuthFile != null) {
                properties.put(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE, legacyAuthFile.getValue());
            }

            final PropertyValue initialAdmin = configurationContext.getProperty(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY);
            if (initialAdmin != null) {
                properties.put(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY, initialAdmin.getValue());
            }

            int i = 1;
            while (true) {
                final String key = FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + i++;
                final PropertyValue value = configurationContext.getProperty(key);
                if (value == null) {
                    break;
                } else {
                    properties.put(key, value.getValue());
                }
            }

            return properties;
        });

        authorizer = new FileAuthorizer();
        authorizer.setNiFiProperties(properties);
        authorizer.initialize(null);
    }

    @After
    public void cleanup() throws Exception {
        deleteFile(primaryAuthorizations);
        deleteFile(primaryTenants);
        deleteFile(restoreAuthorizations);
        deleteFile(restoreTenants);
    }

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvidedWithOverlappingRoles() throws Exception {
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users-multirole.xml", null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        UsersAndAccessPolicies usersAndAccessPolicies = authorizer.getUsersAndAccessPolicies();
        assertNotNull(usersAndAccessPolicies.getAccessPolicy(ResourceType.Flow.getValue(), RequestAction.READ));
        assertNotNull(usersAndAccessPolicies.getAccessPolicy(ResourceType.Controller.getValue(), RequestAction.READ));
        assertNotNull(usersAndAccessPolicies.getAccessPolicy(ResourceType.Controller.getValue(), RequestAction.WRITE));
        assertNotNull(usersAndAccessPolicies.getAccessPolicy(ResourceType.System.getValue(), RequestAction.READ));
        assertNotNull(usersAndAccessPolicies.getAccessPolicy(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID, RequestAction.READ));
        assertNotNull(usersAndAccessPolicies.getAccessPolicy(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID, RequestAction.WRITE));
    }

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvidedAndFlowHasNoPorts() throws Exception {
        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flowNoPorts);

        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users.xml", null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        boolean foundDataTransferPolicy = false;
        for (AccessPolicy policy : authorizer.getAccessPolicies()) {
            if (policy.getResource().contains(ResourceType.DataTransfer.name())) {
                foundDataTransferPolicy = true;
                break;
            }
        }

        assertFalse(foundDataTransferPolicy);
    }

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvided() throws Exception {
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users.xml", null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
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
        final Group group1 = groups.iterator().next();
        assertEquals("group1", group1.getName());

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
        assertEquals(6, user3Policies.size());

        assertTrue(user3Policies.containsKey(ResourceType.Flow.getValue()));
        assertEquals(1, user3Policies.get(ResourceType.Flow.getValue()).size());
        assertTrue(user3Policies.get(ResourceType.Flow.getValue()).contains(RequestAction.READ));

        assertTrue(user3Policies.containsKey(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID));
        assertEquals(2, user3Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).size());
        assertTrue(user3Policies.get(ResourceType.ProcessGroup.getValue() + "/" + ROOT_GROUP_ID).contains(RequestAction.WRITE));

        // verify user4's policies
        final Map<String,Set<RequestAction>> user4Policies = getResourceActions(policies, user4);
        assertEquals(6, user4Policies.size());

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
        assertEquals(2, user5Policies.size());

        assertTrue(user5Policies.containsKey(ResourceType.Proxy.getValue()));
        assertEquals(1, user5Policies.get(ResourceType.Proxy.getValue()).size());
        assertTrue(user5Policies.get(ResourceType.Proxy.getValue()).contains(RequestAction.WRITE));

        // verify user6's policies
        final Map<String,Set<RequestAction>> user6Policies = getResourceActions(policies, user6);
        assertEquals(3, user6Policies.size());

        assertTrue(user6Policies.containsKey(ResourceType.SiteToSite.getValue()));
        assertEquals(1, user6Policies.get(ResourceType.SiteToSite.getValue()).size());
        assertTrue(user6Policies.get(ResourceType.SiteToSite.getValue()).contains(RequestAction.READ));

        final Resource inputPortResource = ResourceFactory.getDataTransferResource(
                ResourceFactory.getComponentResource(ResourceType.InputPort, "2f7d1606-b090-4be7-a592-a5b70fb55531", "TCP Input"));
        final AccessPolicy inputPortPolicy = authorizer.getUsersAndAccessPolicies().getAccessPolicy(inputPortResource.getIdentifier(), RequestAction.WRITE);
        assertNotNull(inputPortPolicy);
        assertEquals(1, inputPortPolicy.getUsers().size());
        assertTrue(inputPortPolicy.getUsers().contains(user6.getIdentifier()));
        assertEquals(1, inputPortPolicy.getGroups().size());
        assertTrue(inputPortPolicy.getGroups().contains(group1.getIdentifier()));

        final Resource outputPortResource = ResourceFactory.getDataTransferResource(
                ResourceFactory.getComponentResource(ResourceType.OutputPort, "2f7d1606-b090-4be7-a592-a5b70fb55532", "TCP Output"));
        final AccessPolicy outputPortPolicy = authorizer.getUsersAndAccessPolicies().getAccessPolicy(outputPortResource.getIdentifier(), RequestAction.WRITE);
        assertNotNull(outputPortPolicy);
        assertEquals(1, outputPortPolicy.getUsers().size());
        assertTrue(outputPortPolicy.getUsers().contains(user4.getIdentifier()));
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

    @Test
    public void testOnConfiguredWhenLegacyUsersFileProvidedWithIdentityMappings() throws Exception {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^CN=(.*?), OU=(.*?), O=(.*?), L=(.*?), ST=(.*?), C=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");

        properties = getNiFiProperties(props);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flowWithDns);
        authorizer.setNiFiProperties(properties);

        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users-with-dns.xml", null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

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
        final Group group1 = groups.iterator().next();
        assertEquals("group1", group1.getName());

        final Resource inputPortResource = ResourceFactory.getDataTransferResource(
                ResourceFactory.getComponentResource(ResourceType.InputPort, "2f7d1606-b090-4be7-a592-a5b70fb55531", "TCP Input"));
        final AccessPolicy inputPortPolicy = authorizer.getUsersAndAccessPolicies().getAccessPolicy(inputPortResource.getIdentifier(), RequestAction.WRITE);
        assertNotNull(inputPortPolicy);
        assertEquals(1, inputPortPolicy.getUsers().size());
        assertTrue(inputPortPolicy.getUsers().contains(user6.getIdentifier()));
        assertEquals(1, inputPortPolicy.getGroups().size());
        assertTrue(inputPortPolicy.getGroups().contains(group1.getIdentifier()));

        final Resource outputPortResource = ResourceFactory.getDataTransferResource(
                ResourceFactory.getComponentResource(ResourceType.OutputPort, "2f7d1606-b090-4be7-a592-a5b70fb55532", "TCP Output"));
        final AccessPolicy outputPortPolicy = authorizer.getUsersAndAccessPolicies().getAccessPolicy(outputPortResource.getIdentifier(), RequestAction.WRITE);
        assertNotNull(outputPortPolicy);
        assertEquals(1, outputPortPolicy.getUsers().size());
        assertTrue(outputPortPolicy.getUsers().contains(user4.getIdentifier()));
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenBadLegacyUsersFileProvided() throws Exception {
        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/does-not-exist.xml", null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenInitialAdminAndLegacyUsersProvided() throws Exception {
        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        when(configurationContext.getProperty(Mockito.eq(FileAuthorizer.PROP_LEGACY_AUTHORIZED_USERS_FILE)))
                .thenReturn(new StandardPropertyValue("src/test/resources/authorized-users.xml", null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test
    public void testOnConfiguredWhenInitialAdminNotProvided() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(0, users.size());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(0, policies.size());
    }

    @Test
    public void testOnConfiguredWhenInitialAdminProvided() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(12, policies.size());

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
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(new File("src/test/resources/does-not-exist.xml.gz"));
        authorizer.setNiFiProperties(properties);

        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(8, policies.size());

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
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(null);
        authorizer.setNiFiProperties(properties);

        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = authorizer.getAccessPolicies();
        assertEquals(8, policies.size());

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
    public void testOnConfiguredWhenInitialAdminProvidedWithIdentityMapping() throws Exception {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^CN=(.*?), OU=(.*?), O=(.*?), L=(.*?), ST=(.*?), C=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1_$2_$3");

        properties = getNiFiProperties(props);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flow);
        authorizer.setNiFiProperties(properties);

        final String adminIdentity = "CN=localhost, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US";
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        final Set<User> users = authorizer.getUsers();
        assertEquals(1, users.size());

        final User adminUser = users.iterator().next();
        assertEquals("localhost_Apache NiFi_Apache", adminUser.getIdentity());
    }

    @Test
    public void testOnConfiguredWhenNodeIdentitiesProvided() throws Exception {
        final String adminIdentity = "admin-user";
        final String nodeIdentity1 = "node1";
        final String nodeIdentity2 = "node2";

        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        User adminUser = authorizer.getUserByIdentity(adminIdentity);
        assertNotNull(adminUser);

        User nodeUser1 = authorizer.getUserByIdentity(nodeIdentity1);
        assertNotNull(nodeUser1);

        User nodeUser2 = authorizer.getUserByIdentity(nodeIdentity2);
        assertNotNull(nodeUser2);

        AccessPolicy proxyWritePolicy = authorizer.getUsersAndAccessPolicies().getAccessPolicy(ResourceType.Proxy.getValue(), RequestAction.WRITE);

        assertNotNull(proxyWritePolicy);
        assertTrue(proxyWritePolicy.getUsers().contains(nodeUser1.getIdentifier()));
        assertTrue(proxyWritePolicy.getUsers().contains(nodeUser2.getIdentifier()));
    }

    @Test
    public void testOnConfiguredWhenNodeIdentitiesProvidedAndUsersAlreadyExist() throws Exception {
        final String adminIdentity = "admin-user";
        final String nodeIdentity1 = "node1";
        final String nodeIdentity2 = "node2";

        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, TENANTS_FOR_ADMIN_AND_NODES);
        authorizer.onConfigured(configurationContext);

        assertEquals(3, authorizer.getUsers().size());

        User adminUser = authorizer.getUserByIdentity(adminIdentity);
        assertNotNull(adminUser);

        User nodeUser1 = authorizer.getUserByIdentity(nodeIdentity1);
        assertNotNull(nodeUser1);

        User nodeUser2 = authorizer.getUserByIdentity(nodeIdentity2);
        assertNotNull(nodeUser2);

        AccessPolicy proxyWritePolicy = authorizer.getUsersAndAccessPolicies().getAccessPolicy(ResourceType.Proxy.getValue(), RequestAction.WRITE);

        assertNotNull(proxyWritePolicy);
        assertTrue(proxyWritePolicy.getUsers().contains(nodeUser1.getIdentifier()));
        assertTrue(proxyWritePolicy.getUsers().contains(nodeUser2.getIdentifier()));
    }

    @Test
    public void testOnConfiguredWhenNodeIdentitiesProvidedWithIdentityMappings() throws Exception {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^CN=(.*?), OU=(.*?), O=(.*?), L=(.*?), ST=(.*?), C=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");

        properties = getNiFiProperties(props);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flow);
        authorizer.setNiFiProperties(properties);

        final String adminIdentity = "CN=user1, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US";
        final String nodeIdentity1 = "CN=node1, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US";
        final String nodeIdentity2 = "CN=node2, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US";

        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        User adminUser = authorizer.getUserByIdentity("user1");
        assertNotNull(adminUser);

        User nodeUser1 = authorizer.getUserByIdentity("node1");
        assertNotNull(nodeUser1);

        User nodeUser2 = authorizer.getUserByIdentity("node2");
        assertNotNull(nodeUser2);
    }

    @Test
    public void testOnConfiguredWhenTenantsAndAuthorizationsFileDoesNotExist() {
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());
        assertEquals(0, authorizer.getUsers().size());
        assertEquals(0, authorizer.getGroups().size());
    }

    @Test
    public void testOnConfiguredWhenAuthorizationsFileDoesNotExist() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());
        assertEquals(0, authorizer.getUsers().size());
        assertEquals(0, authorizer.getGroups().size());
    }

    @Test
    public void testOnConfiguredWhenTenantsFileDoesNotExist() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
        assertEquals(0, authorizer.getAccessPolicies().size());
        assertEquals(0, authorizer.getUsers().size());
        assertEquals(0, authorizer.getGroups().size());
    }

    @Test
    public void testOnConfiguredWhenRestoreDoesNotExist() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);

        assertEquals(primaryAuthorizations.length(), restoreAuthorizations.length());
        assertEquals(primaryTenants.length(), restoreTenants.length());
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryDoesNotExist() throws Exception {
        writeFile(restoreAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(restoreTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryAuthorizationsDifferentThanRestore() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(restoreAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWhenPrimaryTenantsDifferentThanRestore() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS);
        writeFile(restoreTenants, EMPTY_TENANTS_CONCISE);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWithBadAuthorizationsSchema() throws Exception {
        writeFile(primaryAuthorizations, BAD_SCHEMA_AUTHORIZATIONS);
        authorizer.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testOnConfiguredWithBadTenantsSchema() throws Exception {
        writeFile(primaryTenants, BAD_SCHEMA_TENANTS);
        authorizer.onConfigured(configurationContext);
    }

    @Test
    public void testAuthorizedUserAction() throws Exception {
        writeFile(primaryAuthorizations, SIMPLE_AUTHORIZATION_BY_USER);
        writeFile(primaryTenants, SIMPLE_TENANTS_BY_USER);
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
        writeFile(primaryAuthorizations, SIMPLE_AUTHORIZATION_BY_USER);
        writeFile(primaryTenants, SIMPLE_TENANTS_BY_USER);
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
        writeFile(primaryAuthorizations, SIMPLE_AUTHORIZATION_BY_USER);
        writeFile(primaryTenants, SIMPLE_TENANTS_BY_USER);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identifier = "user-1";
        final User user = authorizer.getUser(identifier);
        assertNotNull(user);
        assertEquals(identifier, user.getIdentifier());
    }

    @Test
    public void testGetUserByIdentifierWhenNotFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identifier = "user-X";
        final User user = authorizer.getUser(identifier);
        assertNull(user);
    }

    @Test
    public void testGetUserByIdentityWhenFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identity = "user-1";
        final User user = authorizer.getUserByIdentity(identity);
        assertNotNull(user);
        assertEquals(identity, user.getIdentifier());
    }

    @Test
    public void testGetUserByIdentityWhenNotFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        final String identity = "user-X";
        final User user = authorizer.getUserByIdentity(identity);
        assertNull(user);
    }

    @Test
    public void testDeleteUser() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getUsers().size());

        // retrieve user-1 and verify it exists
        final User user = authorizer.getUser("user-1");
        assertEquals("user-1", user.getIdentifier());

        // delete user-1
        final User deletedUser = authorizer.deleteUser(user);
        assertNotNull(deletedUser);
        assertEquals("user-1", deletedUser.getIdentifier());

        // should be one less user
        assertEquals(1, authorizer.getUsers().size());
        assertNull(authorizer.getUser(user.getIdentifier()));
    }

    @Test
    public void testDeleteUserWhenNotFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final String identifier = "group-1";
        final Group group = authorizer.getGroup(identifier);
        assertNotNull(group);
        assertEquals(identifier, group.getIdentifier());
    }

    @Test
    public void testGetGroupByIdentifierWhenNotFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

        final String identifier = "group-X";
        final Group group = authorizer.getGroup(identifier);
        assertNull(group);
    }

    @Test
    public void testDeleteGroupWhenFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getGroups().size());

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
    }

    @Test
    public void testDeleteGroupWhenNotFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);
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

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(authorizer);
        final ConfigurableAccessPolicyProvider accessPolicyProviderWithChecks = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        try {
            final AccessPolicy returnedPolicy2 = accessPolicyProviderWithChecks.addAccessPolicy(policy2);
            fail("Should have thrown exception");
        } catch (Exception e) {
        }

        assertEquals(1, authorizer.getAccessPolicies().size());
    }

    @Test
    public void testAddAccessPolicyWithEmptyUsersAndGroups() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
        authorizer.onConfigured(configurationContext);
        assertEquals(2, authorizer.getAccessPolicies().size());

        final AccessPolicy policy = authorizer.getAccessPolicy("policy-X");
        assertNull(policy);
    }

    @Test
    public void testUpdateAccessPolicy() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);
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
