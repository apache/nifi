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

import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizerFactoryTest {

    @Test
    public void testOnConfiguredWhenPoliciesWithSameResourceAndAction() {
        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").build();

        AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-id-1")
                .resource("resource1")
                .action(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .build();

        AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-id-2")
                .resource("resource1")
                .action(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .build();

        Set<AccessPolicy> policies = new LinkedHashSet<>();
        policies.add(policy1);
        policies.add(policy2);

        Set<User> users = new LinkedHashSet<>();
        users.add(user1);

        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);
        Authorizer authorizer = AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer(new HashSet<>(), users, policies));

        assertThrows(AuthorizerCreationException.class, () -> authorizer.onConfigured(context));
    }

    @Test
    public void testOnConfiguredWhenUsersWithSameIdentity() {
        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").build();
        User user2 = new User.Builder().identifier("user-id-2").identity("user-1").build();

        Set<User> users = new LinkedHashSet<>();
        users.add(user1);
        users.add(user2);

        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);
        Authorizer authorizer = AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer(new HashSet<>(), users, new HashSet<>()));

        assertThrows(AuthorizerCreationException.class, () -> authorizer.onConfigured(context));
    }

    @Test
    public void testOnConfiguredWhenGroupsWithSameName() {
        Group group1 = new Group.Builder().identifier("group-id-1").name("group-1").build();
        Group group2 = new Group.Builder().identifier("group-id-2").name("group-1").build();

        Set<Group> groups = new LinkedHashSet<>();
        groups.add(group1);
        groups.add(group2);

        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);
        Authorizer authorizer = AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer(groups, new HashSet<>(), new HashSet<>()));

        assertThrows(AuthorizerCreationException.class, () -> authorizer.onConfigured(context));
    }

    @Test
    public void testAddPoliciesWithSameResourceAndAction() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").build();
        userGroupProvider.addUser(user1);

        AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-id-1")
                .resource("resource1")
                .action(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .build();
        accessPolicyProvider.addAccessPolicy(policy1);

        AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-id-2")
                .resource("resource1")
                .action(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .build();

        assertThrows(IllegalStateException.class,
                () -> accessPolicyProvider.addAccessPolicy(policy2));
    }

    @Test
    public void testAddUsersWithSameIdentity() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").build();
        userGroupProvider.addUser(user1);

        User user2 = new User.Builder().identifier("user-id-2").identity("user-1").build();

        assertThrows(IllegalStateException.class,
                () -> userGroupProvider.addUser(user2));
    }

    @Test
    public void testAddGroupsWithSameName() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        Group group1 = new Group.Builder().identifier("group-id-1").name("group-1").build();
        userGroupProvider.addGroup(group1);

        Group group2 = new Group.Builder().identifier("group-id-2").name("group-1").build();

        assertThrows(IllegalStateException.class,
                () -> userGroupProvider.addGroup(group2));
    }

    @Test
    public void testAddUsersWithSameIdentityAsGroupName() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        Group group1 = new Group.Builder().identifier("group-id-1").name("abc").build();
        userGroupProvider.addGroup(group1);

        User user = new User.Builder().identifier("user-id-2").identity("abc").build();
        userGroupProvider.addUser(user);
    }

    @Test
    public void testAddGroupWithSameNameAsUserIdentity() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        User user = new User.Builder().identifier("user-id-2").identity("abc").build();
        userGroupProvider.addUser(user);

        Group group1 = new Group.Builder().identifier("group-id-1").name("abc").build();
        userGroupProvider.addGroup(group1);
    }

    @Test
    public void testUpdateUserWithSameIdentity() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        User user1 = new User.Builder().identifier("user-id-1").identity("abc").build();
        userGroupProvider.addUser(user1);

        User user2 = new User.Builder().identifier("user-id-2").identity("xyz").build();
        userGroupProvider.addUser(user2);

        User user1Updated = new User.Builder().identifier("user-id-1").identity("xyz").build();

        assertThrows(IllegalStateException.class,
                () -> userGroupProvider.updateUser(user1Updated));
    }

    @Test
    public void testUpdateGroupWithSameName() {
        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);

        final ManagedAuthorizer managedAuthorizer = (ManagedAuthorizer) AuthorizerFactory.installIntegrityChecks(new MockPolicyBasedAuthorizer());
        managedAuthorizer.onConfigured(context);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = (ConfigurableAccessPolicyProvider) managedAuthorizer.getAccessPolicyProvider();
        final ConfigurableUserGroupProvider userGroupProvider = (ConfigurableUserGroupProvider) accessPolicyProvider.getUserGroupProvider();

        Group group1 = new Group.Builder().identifier("group-id-1").name("abc").build();
        userGroupProvider.addGroup(group1);

        Group group2 = new Group.Builder().identifier("group-id-2").name("xyz").build();
        userGroupProvider.addGroup(group2);

        Group group1Updated = new Group.Builder().identifier("group-id-1").name("xyz").build();

        assertThrows(IllegalStateException.class,
                () -> userGroupProvider.updateGroup(group1Updated));
    }

    @Test
    public void testAuditInvoked() {
        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").build();

        AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-id-1")
                .resource("resource1")
                .action(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .build();

        Set<AccessPolicy> policies = new LinkedHashSet<>();
        policies.add(policy1);

        Set<User> users = new LinkedHashSet<>();
        users.add(user1);

        final MockPolicyBasedAuthorizer mockAuthorizer = new MockPolicyBasedAuthorizer(new HashSet<>(), users, policies);

        AuthorizerConfigurationContext context = Mockito.mock(AuthorizerConfigurationContext.class);
        Authorizer authorizer = AuthorizerFactory.installIntegrityChecks(mockAuthorizer);
        authorizer.onConfigured(context);

        assertInstanceOf(AuthorizationAuditor.class, authorizer);

        final AuthorizationRequest accessAttempt = new AuthorizationRequest.Builder()
                .resource(new MockResource("resource1", "Resource 1"))
                .identity("user-1")
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        final AuthorizationResult accessAttemptResult = authorizer.authorize(accessAttempt);

        assertEquals(Result.Approved, accessAttemptResult.getResult());
        assertTrue(mockAuthorizer.isAudited(accessAttempt));

        final AuthorizationRequest nonAccessAttempt = new AuthorizationRequest.Builder()
                .resource(new MockResource("resource1", "Resource 1"))
                .identity("user-1")
                .accessAttempt(false)
                .action(RequestAction.READ)
                .anonymous(false)
                .build();

        final AuthorizationResult nonAccessAttempResult = authorizer.authorize(nonAccessAttempt);

        assertEquals(Result.Approved, nonAccessAttempResult.getResult());
        assertFalse(mockAuthorizer.isAudited(nonAccessAttempt));
    }

    /**
     * Resource implementation for testing.
     */
    private static class MockResource implements Resource {

        private final String identifier;
        private final String name;

        public MockResource(String identifier, String name) {
            this.identifier = identifier;
            this.name = name;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getSafeDescription() {
            return name;
        }
    }
}