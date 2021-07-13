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
package org.apache.nifi.registry.security.authorization.database;

import org.apache.nifi.registry.db.DatabaseBaseTest;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authorization.AbstractConfigurableAccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.AuthorizerInitializationContext;
import org.apache.nifi.registry.security.authorization.ConfigurableAccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.security.authorization.UserGroupProviderLookup;
import org.apache.nifi.registry.security.authorization.resource.ResourceFactory;
import org.apache.nifi.registry.security.authorization.util.AccessPolicyProviderUtils;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.identity.DefaultIdentityMapper;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.util.StandardPropertyValue;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Collections;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDatabaseAccessPolicyProvider extends DatabaseBaseTest {

    private static final String UGP_IDENTIFIER = "mock-user-group-provider";

    private static final User ADMIN_USER = new User.Builder().identifierGenerateRandom().identity("admin").build();
    private static final User ADMIN_USER2 = new User.Builder().identifierGenerateRandom().identity("admin2").build();

    private static final User NIFI1_USER = new User.Builder().identifierGenerateRandom().identity("nifi1").build();
    private static final User NIFI2_USER = new User.Builder().identifierGenerateRandom().identity("nifi2").build();
    private static final User NIFI3_USER = new User.Builder().identifierGenerateRandom().identity("nifi3").build();

    private static final Set<String> NIFI_IDENTITIES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                    NIFI1_USER.getIdentity(), NIFI2_USER.getIdentity(), NIFI3_USER.getIdentity())));

    private static final Group NIFI_GROUP = new Group.Builder().identifierGenerateRandom().name("nifi-nodes").build();
    private static final Group OTHERS_GROUP = new Group.Builder().identifierGenerateRandom().name("other-members").build();

    @Autowired
    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;
    private NiFiRegistryProperties properties;
    private IdentityMapper identityMapper;

    private UserGroupProviderLookup userGroupProviderLookup;
    private UserGroupProvider userGroupProvider;

    // Class under test
    private ConfigurableAccessPolicyProvider policyProvider;

    @Before
    public void setup() {
        properties = new NiFiRegistryProperties();
        identityMapper = new DefaultIdentityMapper(properties);
        jdbcTemplate = new JdbcTemplate(dataSource);

        userGroupProvider = mock(UserGroupProvider.class);
        when(userGroupProvider.getUserByIdentity(ADMIN_USER.getIdentity())).thenReturn(ADMIN_USER);
        when(userGroupProvider.getUserByIdentity(ADMIN_USER2.getIdentity())).thenReturn(ADMIN_USER2);
        when(userGroupProvider.getUserByIdentity(NIFI1_USER.getIdentity())).thenReturn(NIFI1_USER);
        when(userGroupProvider.getUserByIdentity(NIFI2_USER.getIdentity())).thenReturn(NIFI2_USER);
        when(userGroupProvider.getUserByIdentity(NIFI3_USER.getIdentity())).thenReturn(NIFI3_USER);
        when(userGroupProvider.getGroups()).thenReturn(Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(OTHERS_GROUP, NIFI_GROUP))));

        userGroupProviderLookup = mock(UserGroupProviderLookup.class);
        when(userGroupProviderLookup.getUserGroupProvider(UGP_IDENTIFIER)).thenReturn(userGroupProvider);

        final AuthorizerInitializationContext initializationContext = mock(AuthorizerInitializationContext.class);
        when(initializationContext.getUserGroupProviderLookup()).thenReturn(userGroupProviderLookup);

        final DatabaseAccessPolicyProvider databaseProvider = new DatabaseAccessPolicyProvider();
        databaseProvider.setDataSource(dataSource);
        databaseProvider.setIdentityMapper(identityMapper);
        databaseProvider.initialize(initializationContext);

        policyProvider = databaseProvider;
    }

    private void configure(final String initialAdmin, final Set<String> nifiIdentifies) {
        configure(initialAdmin, nifiIdentifies, null);
    }

    /**
     * Helper method to call onConfigured with a configuration context.
     *
     * @param initialAdmin the initial admin identity to put in the context, or null
     * @param nifiIdentifies the nifi identities to put in the context, or null
     * @param nifiGroupName the name of the nifi group
     */
    private void configure(final String initialAdmin, final Set<String> nifiIdentifies, final String nifiGroupName) {
        final Map<String,String> properties = new HashMap<>();
        properties.put(AbstractConfigurableAccessPolicyProvider.PROP_USER_GROUP_PROVIDER, UGP_IDENTIFIER);

        if (initialAdmin != null) {
            properties.put(AccessPolicyProviderUtils.PROP_INITIAL_ADMIN_IDENTITY, initialAdmin);
        }

        if (nifiIdentifies != null) {
            int i = 1;
            for (final String nifiIdentity : nifiIdentifies) {
                properties.put(AccessPolicyProviderUtils.PROP_NIFI_IDENTITY_PREFIX + i++, nifiIdentity);
            }
        }

        if (nifiGroupName != null) {
            properties.put(AccessPolicyProviderUtils.PROP_NIFI_GROUP_NAME, nifiGroupName);
        }

        final AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperties()).thenReturn(properties);
        when(configurationContext.getProperty(AbstractConfigurableAccessPolicyProvider.PROP_USER_GROUP_PROVIDER))
                .thenReturn(new StandardPropertyValue(UGP_IDENTIFIER));
        when(configurationContext.getProperty(AccessPolicyProviderUtils.PROP_INITIAL_ADMIN_IDENTITY))
                .thenReturn(new StandardPropertyValue(initialAdmin));
        when(configurationContext.getProperty(AccessPolicyProviderUtils.PROP_NIFI_GROUP_NAME))
                .thenReturn(new StandardPropertyValue(nifiGroupName));
        policyProvider.onConfigured(configurationContext);
    }

    private void configure() {
        configure(null, null, null);
    }

    // -- Helper methods for accessing the DB outside of the provider

    private int getPolicyCount() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM APP_POLICY", Integer.class);
    }

    private void createPolicy(final String identifier, final String resource, final RequestAction action) {
        final String policySql = "INSERT INTO APP_POLICY(IDENTIFIER, RESOURCE, ACTION) VALUES (?, ?, ?)";
        final int rowsUpdated = jdbcTemplate.update(policySql, identifier, resource, action.toString());
        assertEquals(1, rowsUpdated);
    }

    private void addUserToPolicy(final String policyIdentifier, final String userIdentifier) {
        final String policyUserSql = "INSERT INTO APP_POLICY_USER(POLICY_IDENTIFIER, USER_IDENTIFIER) VALUES (?, ?)";
        final int rowsUpdated = jdbcTemplate.update(policyUserSql, policyIdentifier, userIdentifier);
        assertEquals(1, rowsUpdated);
    }

    private void addGroupToPolicy(final String policyIdentifier, final String groupIdentifier) {
        final String policyGroupSql = "INSERT INTO APP_POLICY_GROUP(POLICY_IDENTIFIER, GROUP_IDENTIFIER) VALUES (?, ?)";
        final int rowsUpdated = jdbcTemplate.update(policyGroupSql, policyIdentifier, groupIdentifier);
        assertEquals(1, rowsUpdated);
    }

    // -- Test onConfigured

    @Test
    public void testOnConfiguredCreatesInitialPolicies() {
        // verify no policies in DB
        assertEquals(0, getPolicyCount());

        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES, NIFI_GROUP.getName());

        // verify policies got created for admin and NiFi identities
        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertEquals(18, policies.size());
    }

    @Test
    public void testOnConfiguredWhenOnlyInitialAdmin() {
        // verify no policies in DB
        assertEquals(0, getPolicyCount());

        configure(ADMIN_USER.getIdentity(), null);

        // verify policies got created for admin and NiFi identities
        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertEquals(18, policies.size());

        // verify each policy only has the initial admin
        policies.forEach(p -> {
            assertNotNull(p.getUsers());
            assertEquals(1, p.getUsers().size());
            assertTrue(p.getUsers().contains(ADMIN_USER.getIdentifier()));
        });
    }

    @Test
    public void testOnConfiguredWhenOnlyInitialNiFiIdentities() {
        // verify no policies in DB
        assertEquals(0, getPolicyCount());

        configure(null, NIFI_IDENTITIES);

        // verify policies got created for admin and NiFi identities
        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertEquals(4, policies.size());

        // verify each policy only has the initial admin
        policies.forEach(p -> {
            assertNotNull(p.getUsers());
            assertEquals(3, p.getUsers().size());
            assertFalse(p.getUsers().contains(ADMIN_USER.getIdentifier()));
        });
    }

    @Test
    public void testOnConfiguredWhenOnlyNiFiGroupName() {
        // verify no policies in DB
        assertEquals(0, getPolicyCount());

        configure(null, null, NIFI_GROUP.getName());

        // verify policies got created for admin and NiFi identities
        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertEquals(4, policies.size());

        // verify each policy only has the initial admin
        policies.forEach(p -> {
            assertNotNull(p.getGroups());
            assertEquals(1, p.getGroups().size());
            assertTrue(p.getGroups().contains(NIFI_GROUP.getIdentifier()));
        });
    }

    @Test
    public void testOnConfiguredStillCreatesInitialPoliciesWhenPoliciesAlreadyExist() {
        // create one policy
        createPolicy("policy1", "/foo", RequestAction.READ);
        assertEquals(1, getPolicyCount());

        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);
        assertTrue(getPolicyCount() > 1);
    }

    @Test
    public void testOnConfiguredWhenChangingInitialAdmin() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);
        configure("admin2", NIFI_IDENTITIES);

        final AccessPolicy readBuckets = policyProvider.getAccessPolicy(
                ResourceFactory.getBucketsResource().getIdentifier(), RequestAction.READ);
        assertNotNull(readBuckets);
        assertEquals(5, readBuckets.getUsers().size());
        assertTrue(readBuckets.getUsers().contains(ADMIN_USER.getIdentifier()));
        assertTrue(readBuckets.getUsers().contains(ADMIN_USER2.getIdentifier()));
        assertTrue(readBuckets.getUsers().contains(NIFI1_USER.getIdentifier()));
        assertTrue(readBuckets.getUsers().contains(NIFI2_USER.getIdentifier()));
        assertTrue(readBuckets.getUsers().contains(NIFI3_USER.getIdentifier()));
    }

    @Test
    public void testOnConfiguredAppliesIdentityMappings() {
        final Properties props = new Properties();
        // Set up an identity mapping for kerberos principals
        props.setProperty("nifi.registry.security.identity.mapping.pattern.kerb", "^(.*?)@(.*?)$");
        props.setProperty("nifi.registry.security.identity.mapping.value.kerb", "$1");
        properties = new NiFiRegistryProperties(props);

        identityMapper = new DefaultIdentityMapper(properties);
        ((DatabaseAccessPolicyProvider)policyProvider).setIdentityMapper(identityMapper);

        // Call configure with full admin identity, should get mapped to just 'admin' before looking up user
        configure("admin@HDF.COM", null);

        // verify policies got created for admin and NiFi identities
        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertEquals(18, policies.size());

        // verify each policy only has the initial admin
        policies.forEach(p -> {
            assertNotNull(p.getUsers());
            assertEquals(1, p.getUsers().size());
            assertTrue(p.getUsers().contains(ADMIN_USER.getIdentifier()));
        });
    }

    @Test
    public void testOnConfiguredAppliesGroupMappings() {
        final Properties props = new Properties();
        // Set up an identity mapping for kerberos principals
        props.setProperty("nifi.registry.security.group.mapping.pattern.anyGroup", "^(.*)$");
        props.setProperty("nifi.registry.security.group.mapping.value.anyGroup", "$1");
        props.setProperty("nifi.registry.security.group.mapping.transform.anyGroup", "LOWER");
        properties = new NiFiRegistryProperties(props);

        identityMapper = new DefaultIdentityMapper(properties);
        ((DatabaseAccessPolicyProvider)policyProvider).setIdentityMapper(identityMapper);

        // Call configure with NiFi Group in all uppercase, should get mapped to lower case
        configure(null, null, NIFI_GROUP.getName().toUpperCase());

        // verify policies got created for admin and NiFi identities
        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertEquals(4, policies.size());

        // verify each policy only has the initial admin
        policies.forEach(p -> {
            assertNotNull(p.getGroups());
            assertEquals(1, p.getGroups().size());
            assertTrue(p.getGroups().contains(NIFI_GROUP.getIdentifier()));
        });
    }

    @Test(expected = SecurityProviderCreationException.class)
    public void testOnConfiguredWhenInitialAdminNotFound() {
        configure("does-not-exist", null);
    }

    @Test(expected = SecurityProviderCreationException.class)
    public void testOnConfiguredWhenNiFiIdentityNotFound() {
        configure(null, Collections.singleton("does-not-exist"));
    }

    // -- Test AccessPolicy methods

    @Test
    public void testAddAccessPolicy() {
        configure();
        assertEquals(0, getPolicyCount());

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .resource("/buckets")
                .action(RequestAction.READ)
                .addUser("user1")
                .addGroup("group1")
                .build();

        final AccessPolicy createdPolicy = policyProvider.addAccessPolicy(policy);
        assertNotNull(createdPolicy);

        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy(policy.getIdentifier());
        verifyPoliciesEqual(policy, retrievedPolicy);
    }

    @Test
    public void testGetPolicyByIdentifierWhenExists() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertTrue(policies.size() > 0);

        final AccessPolicy existingPolicy = policies.stream().findFirst().get();
        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy(existingPolicy.getIdentifier());
        verifyPoliciesEqual(existingPolicy, retrievedPolicy);
    }

    @Test
    public void testGetPolicyByIdentifierWhenDoesNotExist() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy("does-not-exist");
        assertNull(retrievedPolicy);
    }

    @Test
    public void testGetPolicyByResourceAndActionWhenExists() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final Set<AccessPolicy> policies = policyProvider.getAccessPolicies();
        assertNotNull(policies);
        assertTrue(policies.size() > 0);

        final AccessPolicy existingPolicy = policies.stream().findFirst().get();
        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy(existingPolicy.getResource(), existingPolicy.getAction());
        verifyPoliciesEqual(existingPolicy, retrievedPolicy);
    }

    @Test
    public void testGetPolicyByResourceAndActionWhenDoesNotExist() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy("does-not-exist", RequestAction.READ);
        assertNull(retrievedPolicy);
    }

    @Test
    public void testUpdatePolicyWhenExists() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final AccessPolicy readBucketsPolicy = policyProvider.getAccessPolicy(
                ResourceFactory.getBucketsResource().getIdentifier(), RequestAction.READ);
        assertNotNull(readBucketsPolicy);
        assertEquals(4, readBucketsPolicy.getUsers().size());
        assertEquals(0, readBucketsPolicy.getGroups().size());

        final AccessPolicy updatedPolicy = new AccessPolicy.Builder(readBucketsPolicy)
                .addUser("user1")
                .addGroup("group1")
                .build();

        final AccessPolicy returnedPolicy = policyProvider.updateAccessPolicy(updatedPolicy);
        assertNotNull(returnedPolicy);

        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy(readBucketsPolicy.getIdentifier());
        verifyPoliciesEqual(updatedPolicy, retrievedPolicy);
    }

    @Test
    public void testUpdatePolicyWhenDoesNotExist() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .resource("/foo")
                .action(RequestAction.READ)
                .addUser("user1")
                .addGroup("group1")
                .build();

        final AccessPolicy returnedPolicy = policyProvider.updateAccessPolicy(policy);
        assertNull(returnedPolicy);

        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy(policy.getIdentifier());
        assertNull(retrievedPolicy);
    }

    @Test
    public void testDeletePolicyWhenExists() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final AccessPolicy readBucketsPolicy = policyProvider.getAccessPolicy(
                ResourceFactory.getBucketsResource().getIdentifier(), RequestAction.READ);
        assertNotNull(readBucketsPolicy);
        assertEquals(4, readBucketsPolicy.getUsers().size());
        assertEquals(0, readBucketsPolicy.getGroups().size());

        final AccessPolicy deletedPolicy = policyProvider.deleteAccessPolicy(readBucketsPolicy);
        assertNotNull(deletedPolicy);

        final AccessPolicy retrievedPolicy = policyProvider.getAccessPolicy(readBucketsPolicy.getIdentifier());
        assertNull(retrievedPolicy);
    }

    @Test
    public void testDeletePolicyWhenDoesNotExist() {
        configure(ADMIN_USER.getIdentity(), NIFI_IDENTITIES);

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .resource("/foo")
                .action(RequestAction.READ)
                .addUser("user1")
                .addGroup("group1")
                .build();

        final AccessPolicy deletedPolicy = policyProvider.deleteAccessPolicy(policy);
        assertNull(deletedPolicy);
    }

    private void verifyPoliciesEqual(final AccessPolicy policy1, final AccessPolicy policy2) {
        assertNotNull(policy1);
        assertNotNull(policy2);
        assertEquals(policy1.getIdentifier(), policy2.getIdentifier());
        assertEquals(policy1.getResource(), policy2.getResource());
        assertEquals(policy1.getAction(), policy2.getAction());
        assertEquals(policy1.getUsers().size(), policy2.getUsers().size());
        assertEquals(policy1.getGroups().size(), policy2.getGroups().size());
    }

}
