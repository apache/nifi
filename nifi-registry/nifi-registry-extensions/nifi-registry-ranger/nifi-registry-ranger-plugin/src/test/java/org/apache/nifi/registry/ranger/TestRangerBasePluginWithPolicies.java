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
package org.apache.nifi.registry.ranger;

import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.UserAndGroups;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.security.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRangerBasePluginWithPolicies {

    @Test
    public void testPoliciesWithoutUserGroupProvider() {
        final String user1 = "user-1";
        final String group1 = "group-1";

        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("READ")).collect(Collectors.toList()));
        policy1Item.setUsers(Stream.of(user1).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final String resourceIdentifier2 = "/resource-2";
        RangerPolicy.RangerPolicyResource resource2 = new RangerPolicy.RangerPolicyResource(resourceIdentifier2);

        final Map<String, RangerPolicy.RangerPolicyResource> policy2Resources = new HashMap<>();
        policy2Resources.put(resourceIdentifier2, resource2);

        final RangerPolicy.RangerPolicyItem policy2Item = new RangerPolicy.RangerPolicyItem();
        policy2Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("READ"), new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));
        policy2Item.setGroups(Stream.of(group1).collect(Collectors.toList()));

        final RangerPolicy policy2 = new RangerPolicy();
        policy2.setResources(policy2Resources);
        policy2.setPolicyItems(Stream.of(policy2Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);
        policies.add(policy2);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the two ranger policies converted into 3 nifi-registry access policies
        final Set<AccessPolicy> accessPolicies = pluginWithPolicies.getAccessPolicies();
        assertEquals(3, accessPolicies.size());

        // resource 1 -> read but no write
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.READ));

        // read
        final AccessPolicy readResource1 = pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.READ);
        assertNotNull(readResource1);
        assertTrue(accessPolicies.contains(readResource1));
        assertTrue(readResource1.equals(pluginWithPolicies.getAccessPolicy(readResource1.getIdentifier())));
        assertEquals(1, readResource1.getUsers().size());
        assertTrue(readResource1.getUsers().contains(new User.Builder().identifierGenerateFromSeed(user1).identity(user1).build().getIdentifier()));
        assertTrue(readResource1.getGroups().isEmpty());

        // but no write
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));

        // resource 2 -> read and write
        assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier2, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier2, RequestAction.READ));

        // read
        final AccessPolicy readResource2 = pluginWithPolicies.getAccessPolicy(resourceIdentifier2, RequestAction.READ);
        assertNotNull(readResource2);
        assertTrue(accessPolicies.contains(readResource2));
        assertTrue(readResource2.equals(pluginWithPolicies.getAccessPolicy(readResource2.getIdentifier())));
        assertTrue(readResource2.getUsers().isEmpty());
        assertEquals(1, readResource2.getGroups().size());
        assertTrue(readResource2.getGroups().contains(new Group.Builder().identifierGenerateFromSeed(group1).name(group1).build().getIdentifier()));

        // and write
        final AccessPolicy writeResource2 = pluginWithPolicies.getAccessPolicy(resourceIdentifier2, RequestAction.READ);
        assertNotNull(writeResource2);
        assertTrue(accessPolicies.contains(writeResource2));
        assertTrue(writeResource2.equals(pluginWithPolicies.getAccessPolicy(writeResource2.getIdentifier())));
        assertTrue(writeResource2.getUsers().isEmpty());
        assertEquals(1, writeResource2.getGroups().size());
        assertTrue(writeResource2.getGroups().contains(new Group.Builder().identifierGenerateFromSeed(group1).name(group1).build().getIdentifier()));

        // resource 3 -> no read or write
        assertFalse(pluginWithPolicies.doesPolicyExist("resource-3", RequestAction.WRITE));
        assertFalse(pluginWithPolicies.doesPolicyExist("resource-3", RequestAction.READ));

        // no read or write
        assertNull(pluginWithPolicies.getAccessPolicy("resource-3", RequestAction.WRITE));
        assertNull(pluginWithPolicies.getAccessPolicy("resource-3", RequestAction.READ));
    }

    @Test
    public void testNoPolicies() {
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");

        assertFalse(pluginWithPolicies.doesPolicyExist("non-existent-resource", RequestAction.READ));
        assertTrue(pluginWithPolicies.getAccessPolicies().isEmpty());
        assertNull(pluginWithPolicies.getAccessPolicy("non-existent-identifier"));
        assertNull(pluginWithPolicies.getAccessPolicy("non-existent-resource", RequestAction.READ));
    }

    @Test
    public void testDisabledPolicy() {
        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("READ")).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setIsEnabled(false);
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the policy was skipped
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.READ));
        assertTrue(pluginWithPolicies.getAccessPolicies().isEmpty());
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.READ));
    }

    @Test
    public void testMissingResourceValue() {
        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource();

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the policy was skipped
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.getAccessPolicies().isEmpty());
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));
    }

    @Test
    public void testWildcardResourceValue() {
        final String resourceIdentifier1 = "*";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the policy was skipped
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.getAccessPolicies().isEmpty());
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));
    }

    @Test
    public void testExcludesPolicy() {
        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);
        resource1.setIsExcludes(true);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the policy was skipped
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.getAccessPolicies().isEmpty());
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));
    }

    @Test
    public void testRecursivePolicy() {
        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);
        resource1.setIsRecursive(true);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the policy was skipped
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.getAccessPolicies().isEmpty());
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));
    }

    @Test
    public void testDelegateAdmin() {
        final String user1 = "user-1";

        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("READ"), new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));
        policy1Item.setUsers(Stream.of(user1).collect(Collectors.toList()));
        policy1Item.setDelegateAdmin(true);

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry");
        pluginWithPolicies.setPolicies(servicePolicies);

        assertEquals(4, pluginWithPolicies.getAccessPolicies().size());
        assertNotNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.READ));
        assertNotNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));
        assertNotNull(pluginWithPolicies.getAccessPolicy("/policies" + resourceIdentifier1, RequestAction.READ));
        assertNotNull(pluginWithPolicies.getAccessPolicy("/policies" + resourceIdentifier1, RequestAction.WRITE));
    }

    @Test
    public void testPoliciesWithUserGroupProvider() {
        final String user1 = "user-1"; // unknown according to user group provider
        final String user2 = "user-2"; // known according to user group provider
        final String group1 = "group-1"; // unknown according to user group provider
        final String group2 = "group-2"; // known according to user group provider

        final UserGroupProvider userGroupProvider = new UserGroupProvider() {
            @Override
            public Set<User> getUsers() throws AuthorizationAccessException {
                return Stream.of(new User.Builder().identifierGenerateFromSeed(user2).identity(user2).build()).collect(Collectors.toSet());
            }

            @Override
            public User getUser(String identifier) throws AuthorizationAccessException {
                final User u2 = new User.Builder().identifierGenerateFromSeed(user2).identity(user2).build();
                if (u2.getIdentifier().equals(identifier)) {
                    return u2;
                } else {
                    return null;
                }
            }

            @Override
            public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                if (user2.equals(identity)) {
                    return new User.Builder().identifierGenerateFromSeed(user2).identity(user2).build();
                } else {
                    return null;
                }
            }

            @Override
            public Set<Group> getGroups() throws AuthorizationAccessException {
                return Stream.of(new Group.Builder().identifierGenerateFromSeed(group2).name(group2).build()).collect(Collectors.toSet());
            }

            @Override
            public Group getGroup(String identifier) throws AuthorizationAccessException {
                final Group g2 = new Group.Builder().identifierGenerateFromSeed(group2).name(group2).build();
                if (g2.getIdentifier().equals(identifier)) {
                    return g2;
                } else {
                    return null;
                }
            }

            @Override
            public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
                if (user2.equals(identity)) {
                    return new UserAndGroups() {
                        @Override
                        public User getUser() {
                            return new User.Builder().identifierGenerateFromSeed(user2).identity(user2).build();
                        }

                        @Override
                        public Set<Group> getGroups() {
                            return Collections.EMPTY_SET;
                        }
                    };
                } else {
                    return null;
                }
            }

            @Override
            public void initialize(UserGroupProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void preDestruction() throws SecurityProviderCreationException {
            }
        };

        final String resourceIdentifier1 = "/resource-1";
        RangerPolicy.RangerPolicyResource resource1 = new RangerPolicy.RangerPolicyResource(resourceIdentifier1);

        final Map<String, RangerPolicy.RangerPolicyResource> policy1Resources = new HashMap<>();
        policy1Resources.put(resourceIdentifier1, resource1);

        final RangerPolicy.RangerPolicyItem policy1Item = new RangerPolicy.RangerPolicyItem();
        policy1Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("READ")).collect(Collectors.toList()));
        policy1Item.setUsers(Stream.of(user1).collect(Collectors.toList()));
        policy1Item.setGroups(Stream.of(group2).collect(Collectors.toList()));

        final RangerPolicy policy1 = new RangerPolicy();
        policy1.setResources(policy1Resources);
        policy1.setPolicyItems(Stream.of(policy1Item).collect(Collectors.toList()));

        final String resourceIdentifier2 = "/resource-2";
        RangerPolicy.RangerPolicyResource resource2 = new RangerPolicy.RangerPolicyResource(resourceIdentifier2);

        final Map<String, RangerPolicy.RangerPolicyResource> policy2Resources = new HashMap<>();
        policy2Resources.put(resourceIdentifier2, resource2);

        final RangerPolicy.RangerPolicyItem policy2Item = new RangerPolicy.RangerPolicyItem();
        policy2Item.setAccesses(Stream.of(new RangerPolicy.RangerPolicyItemAccess("READ"), new RangerPolicy.RangerPolicyItemAccess("WRITE")).collect(Collectors.toList()));
        policy2Item.setUsers(Stream.of(user2).collect(Collectors.toList()));
        policy2Item.setGroups(Stream.of(group1).collect(Collectors.toList()));

        final RangerPolicy policy2 = new RangerPolicy();
        policy2.setResources(policy2Resources);
        policy2.setPolicyItems(Stream.of(policy2Item).collect(Collectors.toList()));

        final List<RangerPolicy> policies = new ArrayList<>();
        policies.add(policy1);
        policies.add(policy2);

        final RangerServiceDef serviceDef = new RangerServiceDef();
        serviceDef.setName("nifi-registry");

        final ServicePolicies servicePolicies = new ServicePolicies();
        servicePolicies.setPolicies(policies);
        servicePolicies.setServiceDef(serviceDef);

        // set all the policies in the plugin
        final RangerBasePluginWithPolicies pluginWithPolicies = new RangerBasePluginWithPolicies("nifi-registry", "nifi-registry", userGroupProvider);
        pluginWithPolicies.setPolicies(servicePolicies);

        // ensure the two ranger policies converted into 3 nifi-registry access policies
        final Set<AccessPolicy> accessPolicies = pluginWithPolicies.getAccessPolicies();
        assertEquals(3, accessPolicies.size());

        // resource 1 -> read but no write
        assertFalse(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier1, RequestAction.READ));

        // read
        final AccessPolicy readResource1 = pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.READ);
        assertNotNull(readResource1);
        assertTrue(accessPolicies.contains(readResource1));
        assertTrue(readResource1.equals(pluginWithPolicies.getAccessPolicy(readResource1.getIdentifier())));
        assertTrue(readResource1.getUsers().isEmpty());
        assertEquals(1, readResource1.getGroups().size());
        assertTrue(readResource1.getGroups().contains(new Group.Builder().identifierGenerateFromSeed(group2).name(group2).build().getIdentifier()));

        // but no write
        assertNull(pluginWithPolicies.getAccessPolicy(resourceIdentifier1, RequestAction.WRITE));

        // resource 2 -> read and write
        assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier2, RequestAction.WRITE));
        assertTrue(pluginWithPolicies.doesPolicyExist(resourceIdentifier2, RequestAction.READ));

        // read
        final AccessPolicy readResource2 = pluginWithPolicies.getAccessPolicy(resourceIdentifier2, RequestAction.READ);
        assertNotNull(readResource2);
        assertTrue(accessPolicies.contains(readResource2));
        assertTrue(readResource2.equals(pluginWithPolicies.getAccessPolicy(readResource2.getIdentifier())));
        assertEquals(1, readResource2.getUsers().size());
        assertTrue(readResource2.getUsers().contains(new User.Builder().identifierGenerateFromSeed(user2).identity(user2).build().getIdentifier()));
        assertTrue(readResource2.getGroups().isEmpty());

        // and write
        final AccessPolicy writeResource2 = pluginWithPolicies.getAccessPolicy(resourceIdentifier2, RequestAction.READ);
        assertNotNull(writeResource2);
        assertTrue(accessPolicies.contains(writeResource2));
        assertTrue(writeResource2.equals(pluginWithPolicies.getAccessPolicy(writeResource2.getIdentifier())));
        assertEquals(1, writeResource2.getUsers().size());
        assertTrue(writeResource2.getUsers().contains(new User.Builder().identifierGenerateFromSeed(user2).identity(user2).build().getIdentifier()));
        assertTrue(writeResource2.getGroups().isEmpty());
    }
}
