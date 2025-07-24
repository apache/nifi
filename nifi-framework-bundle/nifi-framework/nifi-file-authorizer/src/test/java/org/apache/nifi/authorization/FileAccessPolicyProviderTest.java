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
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileAccessPolicyProviderTest {

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

    private static final String TENANTS_FOR_ADMIN_AND_NODE_GROUP =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                    "<tenants>" +
                    "  <groups>" +
                    "    <group identifier=\"cluster-nodes\" name=\"Cluster Nodes\">" +
                    "       <user identifier=\"node1\" />" +
                    "       <user identifier=\"node2\" />" +
                    "    </group>" +
                    "  </groups>" +
                    "  <users>" +
                    "    <user identifier=\"admin-user\" identity=\"admin-user\"/>" +
                    "    <user identifier=\"node1\" identity=\"node1\"/>" +
                    "    <user identifier=\"node2\" identity=\"node2\"/>" +
                    "  </users>" +
                    "</tenants>";

    // This is the root group id from the flow.xml.gz in src/test/resources
    private static final String ROOT_GROUP_ID = "e530e14c-adcf-41c2-b5d6-d9a59ba8765c";

    private NiFiProperties properties;
    private FileAccessPolicyProvider accessPolicyProvider;
    private FileUserGroupProvider userGroupProvider;
    private File primaryAuthorizations;
    private File primaryTenants;
    private File restoreAuthorizations;
    private File restoreTenants;
    private File flow;

    private AuthorizerConfigurationContext configurationContext;

    @BeforeEach
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

        flow = new File("src/test/resources/flow.json.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flow.getParentFile());

        File flowNoPorts = new File("src/test/resources/flow-no-ports.json.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flowNoPorts.getParentFile());

        File flowWithDns = new File("src/test/resources/flow-with-dns.json.gz");
        FileUtils.ensureDirectoryExistAndCanAccess(flowWithDns.getParentFile());

        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flow);

        userGroupProvider = new FileUserGroupProvider();
        userGroupProvider.setNiFiProperties(properties);
        userGroupProvider.initialize(null);

        // this same configuration is being used for both the user group provider and the access policy provider
        configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_AUTHORIZATIONS_FILE))).thenReturn(new StandardPropertyValue(primaryAuthorizations.getPath(), null,
            ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_TENANTS_FILE))).thenReturn(new StandardPropertyValue(primaryTenants.getPath(), null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY))).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP))).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_USER_GROUP_PROVIDER))).thenReturn(new StandardPropertyValue("user-group-provider", null,
            ParameterLookup.EMPTY));
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

            final PropertyValue initialAdmin = configurationContext.getProperty(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY);
            if (initialAdmin != null) {
                properties.put(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY, initialAdmin.getValue());
            }

            final PropertyValue initialAdminGroup = configurationContext.getProperty(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP);
            if (initialAdminGroup != null) {
                properties.put(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP, initialAdminGroup.getValue());
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

            i = 1;
            while (true) {
                final String key = FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + i++;
                final PropertyValue value = configurationContext.getProperty(key);
                if (value == null) {
                    break;
                } else {
                    properties.put(key, value.getValue());
                }
            }

            i = 1;
            while (true) {
                final String key = FileUserGroupProvider.PROP_INITIAL_GROUP_IDENTITY_PREFIX + i++;
                final PropertyValue value = configurationContext.getProperty(key);
                if (value == null) {
                    break;
                } else {
                    properties.put(key, value.getValue());
                }
            }

            // ensure the initial admin is seeded into the user provider if appropriate
            if (properties.containsKey(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)) {
                i = 0;
                while (true) {
                    final String key = FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + i++;
                    if (!properties.containsKey(key)) {
                        properties.put(key, properties.get(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY));
                        break;
                    }
                }
            }

            // ensure the initial admin group is seeded into the user provider if appropriate
            if (properties.containsKey(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP)) {
                i = 0;
                while (true) {
                    final String key = FileUserGroupProvider.PROP_INITIAL_GROUP_IDENTITY_PREFIX + i++;
                    if (!properties.containsKey(key)) {
                        properties.put(key, properties.get(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP));
                        break;
                    }
                }
            }

            return properties;
        });

        final AccessPolicyProviderInitializationContext initializationContext = mock(AccessPolicyProviderInitializationContext.class);
        when(initializationContext.getUserGroupProviderLookup()).thenReturn(identifier -> userGroupProvider);

        accessPolicyProvider = new FileAccessPolicyProvider();
        accessPolicyProvider.setNiFiProperties(properties);
        accessPolicyProvider.initialize(initializationContext);
    }

    @AfterEach
    public void cleanup() {
        deleteFile(primaryAuthorizations);
        deleteFile(primaryTenants);
        deleteFile(restoreAuthorizations);
        deleteFile(restoreTenants);
    }

    @Test
    public void testOnConfiguredWhenInitialAdminNotProvided() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
        assertEquals(0, policies.size());
    }

    @Test
    public void testOnConfiguredWhenInitialAdminProvided() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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
        when(properties.getFlowConfigurationFile()).thenReturn(new File("src/test/resources/does-not-exist.json.gz"));

        userGroupProvider.setNiFiProperties(properties);
        accessPolicyProvider.setNiFiProperties(properties);

        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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

        userGroupProvider.setNiFiProperties(properties);
        accessPolicyProvider.setNiFiProperties(properties);

        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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
    public void testOnConfiguredWhenInitialAdminGroupProvided() throws Exception {
        final String adminGroupName = "admin-group";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP)))
                .thenReturn(new StandardPropertyValue(adminGroupName, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<Group> groups = userGroupProvider.getGroups();
        final Group adminGroup = groups.iterator().next();
        assertEquals(adminGroupName, adminGroup.getName());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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
    public void testOnConfiguredWhenInitialAdminGroupProvidedAndNoFlowExists() throws Exception {
        // setup NiFi properties to return a file that does not exist
        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(new File("src/test/resources/does-not-exist.json.gz"));

        userGroupProvider.setNiFiProperties(properties);
        accessPolicyProvider.setNiFiProperties(properties);

        final String adminGroupName = "admin-group";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP)))
                .thenReturn(new StandardPropertyValue(adminGroupName, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<Group> groups = userGroupProvider.getGroups();
        final Group adminGroup = groups.iterator().next();
        assertEquals(adminGroupName, adminGroup.getName());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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
    public void testOnConfiguredWhenInitialAdminGroupProvidedAndFlowIsNull() throws Exception {
        // setup NiFi properties to return a file that does not exist
        properties = mock(NiFiProperties.class);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(null);

        userGroupProvider.setNiFiProperties(properties);
        accessPolicyProvider.setNiFiProperties(properties);

        final String adminGroupName = "admin-group";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP)))
                .thenReturn(new StandardPropertyValue(adminGroupName, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<Group> groups = userGroupProvider.getGroups();
        final Group adminGroup = groups.iterator().next();
        assertEquals(adminGroupName, adminGroup.getName());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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
    public void testOnConfiguredWhenBothInitialAdminAndInitialAdminGroupProvided() throws Exception {
        final String adminIdentity = "admin-user";
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));

        final String adminGroupName = "admin-group";
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_GROUP)))
                .thenReturn(new StandardPropertyValue(adminGroupName, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        final User adminUser = users.iterator().next();
        assertEquals(adminIdentity, adminUser.getIdentity());

        final Set<Group> groups = userGroupProvider.getGroups();
        final Group adminGroup = groups.iterator().next();
        assertEquals(adminGroupName, adminGroup.getName());
        assertEquals(Set.of(adminUser.getIdentifier()), adminGroup.getUsers());

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
        assertEquals(12, policies.size());
        // admin user is a member of admin group; no need to grant access right to the user itself
        final Set<String> usersWithPolicies = policies.stream()
                .flatMap(policy -> policy.getUsers().stream())
                .collect(Collectors.toUnmodifiableSet());
        assertEquals(Collections.emptySet(), usersWithPolicies);
    }

    @Test
    public void testOnConfiguredWhenInitialAdminProvidedWithIdentityMapping() throws Exception {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^CN=(.*?), OU=(.*?), O=(.*?), L=(.*?), ST=(.*?), C=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1_$2_$3");

        properties = getNiFiProperties(props);
        when(properties.getRestoreDirectory()).thenReturn(restoreAuthorizations.getParentFile());
        when(properties.getFlowConfigurationFile()).thenReturn(flow);

        userGroupProvider.setNiFiProperties(properties);
        accessPolicyProvider.setNiFiProperties(properties);

        final String adminIdentity = "CN=localhost, OU=Apache NiFi, O=Apache, L=Santa Monica, ST=CA, C=US";
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<User> users = userGroupProvider.getUsers();
        final User adminUser = users.iterator().next();
        assertEquals("localhost_Apache NiFi_Apache", adminUser.getIdentity());
    }

    @Test
    public void testOnConfiguredWhenNodeIdentitiesProvided() throws Exception {
        final String adminIdentity = "admin-user";
        final String nodeIdentity1 = "node1";
        final String nodeIdentity2 = "node2";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null, ParameterLookup.EMPTY));

        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "3")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        User nodeUser1 = userGroupProvider.getUserByIdentity(nodeIdentity1);
        User nodeUser2 = userGroupProvider.getUserByIdentity(nodeIdentity2);

        AccessPolicy proxyWritePolicy = accessPolicyProvider.getAccessPolicy(ResourceType.Proxy.getValue(), RequestAction.WRITE);

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
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(Mockito.eq(FileAccessPolicyProvider.PROP_NODE_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null, ParameterLookup.EMPTY));

        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "1")))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "2")))
                .thenReturn(new StandardPropertyValue(nodeIdentity1, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileUserGroupProvider.PROP_INITIAL_USER_IDENTITY_PREFIX + "3")))
                .thenReturn(new StandardPropertyValue(nodeIdentity2, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, TENANTS_FOR_ADMIN_AND_NODES);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        User nodeUser1 = userGroupProvider.getUserByIdentity(nodeIdentity1);
        User nodeUser2 = userGroupProvider.getUserByIdentity(nodeIdentity2);

        AccessPolicy proxyWritePolicy = accessPolicyProvider.getAccessPolicy(ResourceType.Proxy.getValue(), RequestAction.WRITE);

        assertNotNull(proxyWritePolicy);
        assertTrue(proxyWritePolicy.getUsers().contains(nodeUser1.getIdentifier()));
        assertTrue(proxyWritePolicy.getUsers().contains(nodeUser2.getIdentifier()));
    }

    @Test
    public void testOnConfiguredWhenNodeGroupProvided() throws Exception {
        final String adminIdentity = "admin-user";
        final String nodeGroupName = "Cluster Nodes";
        final String nodeGroupIdentifier = "cluster-nodes";
        final String nodeIdentity1 = "node1";
        final String nodeIdentity2 = "node2";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
                .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_NODE_GROUP_NAME)))
                .thenReturn(new StandardPropertyValue(nodeGroupName, null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, TENANTS_FOR_ADMIN_AND_NODE_GROUP);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertNotNull(userGroupProvider.getUserByIdentity(nodeIdentity1));
        assertNotNull(userGroupProvider.getUserByIdentity(nodeIdentity2));

        AccessPolicy proxyWritePolicy = accessPolicyProvider.getAccessPolicy(ResourceType.Proxy.getValue(), RequestAction.WRITE);

        assertNotNull(proxyWritePolicy);
        assertTrue(proxyWritePolicy.getGroups().contains(nodeGroupIdentifier));
    }

    @Test
    public void testOnConfiguredWhenNodeGroupEmpty() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
            .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_NODE_GROUP_NAME)))
            .thenReturn(new StandardPropertyValue("", null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, TENANTS_FOR_ADMIN_AND_NODE_GROUP);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertNull(accessPolicyProvider.getAccessPolicy(ResourceType.Proxy.getValue(), RequestAction.WRITE));
    }

    @Test
    public void testOnConfiguredWhenNodeGroupDoesNotExist() throws Exception {
        final String adminIdentity = "admin-user";

        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_INITIAL_ADMIN_IDENTITY)))
            .thenReturn(new StandardPropertyValue(adminIdentity, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(eq(FileAccessPolicyProvider.PROP_NODE_GROUP_NAME)))
            .thenReturn(new StandardPropertyValue("nonexistent", null, ParameterLookup.EMPTY));

        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, TENANTS_FOR_ADMIN_AND_NODE_GROUP);

        userGroupProvider.onConfigured(configurationContext);

        assertThrows(AuthorizerCreationException.class,
                () -> accessPolicyProvider.onConfigured(configurationContext));
    }

    @Test
    public void testOnConfiguredWhenTenantsAndAuthorizationsFileDoesNotExist() {
        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);
        assertEquals(0, accessPolicyProvider.getAccessPolicies().size());
    }

    @Test
    public void testOnConfiguredWhenAuthorizationsFileDoesNotExist() throws Exception {
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);
        assertEquals(0, accessPolicyProvider.getAccessPolicies().size());
    }

    @Test
    public void testOnConfiguredWhenTenantsFileDoesNotExist() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);
        assertEquals(0, accessPolicyProvider.getAccessPolicies().size());
    }

    @Test
    public void testOnConfiguredWhenRestoreDoesNotExist() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        writeFile(primaryTenants, EMPTY_TENANTS_CONCISE);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(primaryAuthorizations.length(), restoreAuthorizations.length());
        assertEquals(primaryTenants.length(), restoreTenants.length());
    }

    @Test
    public void testOnConfiguredWhenPrimaryDoesNotExist() throws Exception {
        writeFile(restoreAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        assertThrows(AuthorizerCreationException.class, () ->
                accessPolicyProvider.onConfigured(configurationContext));
    }

    @Test
    public void testOnConfiguredWhenPrimaryAuthorizationsDifferentThanRestore() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(restoreAuthorizations, EMPTY_AUTHORIZATIONS_CONCISE);
        userGroupProvider.onConfigured(configurationContext);

        assertThrows(AuthorizerCreationException.class,
                () -> accessPolicyProvider.onConfigured(configurationContext));
    }

    @Test
    public void testOnConfiguredWithBadAuthorizationsSchema() throws Exception {
        writeFile(primaryAuthorizations, BAD_SCHEMA_AUTHORIZATIONS);
        userGroupProvider.onConfigured(configurationContext);

        assertThrows(AuthorizerCreationException.class,
                () -> accessPolicyProvider.onConfigured(configurationContext));
    }

    @Test
    public void testGetAllUsersGroupsPolicies() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        final Set<AccessPolicy> policies = accessPolicyProvider.getAccessPolicies();
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
                    && policy.getGroups().isEmpty()
                    && policy.getUsers().size() == 1
                    && policy.getUsers().contains("user-2")) {
                foundPolicy2 = true;
            }
        }

        assertTrue(foundPolicy1);
        assertTrue(foundPolicy2);
    }

    // --------------- AccessPolicy Tests ------------------------

    @Test
    public void testAddAccessPolicy() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(0, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-1")
                .addUser("user-1")
                .addGroup("group-1")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy1 = accessPolicyProvider.addAccessPolicy(policy1);
        assertNotNull(returnedPolicy1);
        assertEquals(policy1.getIdentifier(), returnedPolicy1.getIdentifier());
        assertEquals(policy1.getResource(), returnedPolicy1.getResource());
        assertEquals(policy1.getUsers(), returnedPolicy1.getUsers());
        assertEquals(policy1.getGroups(), returnedPolicy1.getGroups());
        assertEquals(policy1.getAction(), returnedPolicy1.getAction());

        assertEquals(1, accessPolicyProvider.getAccessPolicies().size());
    }

    @Test
    public void testAddAccessPolicyWithEmptyUsersAndGroups() throws Exception {
        writeFile(primaryAuthorizations, EMPTY_AUTHORIZATIONS);
        writeFile(primaryTenants, EMPTY_TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(0, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-1")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy returnedPolicy1 = accessPolicyProvider.addAccessPolicy(policy1);
        assertNotNull(returnedPolicy1);
        assertEquals(policy1.getIdentifier(), returnedPolicy1.getIdentifier());
        assertEquals(policy1.getResource(), returnedPolicy1.getResource());
        assertEquals(policy1.getUsers(), returnedPolicy1.getUsers());
        assertEquals(policy1.getGroups(), returnedPolicy1.getGroups());
        assertEquals(policy1.getAction(), returnedPolicy1.getAction());

        assertEquals(1, accessPolicyProvider.getAccessPolicies().size());
    }

    @Test
    public void testGetAccessPolicy() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(2, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy = accessPolicyProvider.getAccessPolicy("policy-1");
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

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(2, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy = accessPolicyProvider.getAccessPolicy("policy-X");
        assertNull(policy);
    }

    @Test
    public void testUpdateAccessPolicy() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(2, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy updateAccessPolicy = accessPolicyProvider.updateAccessPolicy(policy);
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

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(2, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-XXX")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy updateAccessPolicy = accessPolicyProvider.updateAccessPolicy(policy);
        assertNull(updateAccessPolicy);
    }

    @Test
    public void testDeleteAccessPolicy() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(2, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-1")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy deletedAccessPolicy = accessPolicyProvider.deleteAccessPolicy(policy);
        assertNotNull(deletedAccessPolicy);
        assertEquals(policy.getIdentifier(), deletedAccessPolicy.getIdentifier());

        // should have one less policy, and get by policy id should return null
        assertEquals(1, accessPolicyProvider.getAccessPolicies().size());
        assertNull(accessPolicyProvider.getAccessPolicy(policy.getIdentifier()));
    }

    @Test
    public void testDeleteAccessPolicyWhenNotFound() throws Exception {
        writeFile(primaryAuthorizations, AUTHORIZATIONS);
        writeFile(primaryTenants, TENANTS);

        userGroupProvider.onConfigured(configurationContext);
        accessPolicyProvider.onConfigured(configurationContext);

        assertEquals(2, accessPolicyProvider.getAccessPolicies().size());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("policy-XXX")
                .resource("resource-A")
                .addUser("user-A")
                .addGroup("group-A")
                .action(RequestAction.READ)
                .build();

        final AccessPolicy deletedAccessPolicy = accessPolicyProvider.deleteAccessPolicy(policy);
        assertNull(deletedAccessPolicy);
    }

    private static void writeFile(final File file, final String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(bytes);
        }
    }

    private static void deleteFile(final File file) {
        if (file.isDirectory()) {
            FileUtils.deleteFilesInDir(file, null, null, true, true);
        }
        FileUtils.deleteFile(file, null, 10);
    }

    private NiFiProperties getNiFiProperties(final Properties properties) {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getPropertyKeys()).thenReturn(properties.stringPropertyNames());

        when(nifiProperties.getProperty(anyString())).then((Answer<String>) invocationOnMock -> properties.getProperty((String) invocationOnMock.getArguments()[0]));
        return nifiProperties;
    }

}
