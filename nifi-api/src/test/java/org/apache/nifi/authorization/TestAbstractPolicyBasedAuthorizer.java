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

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class TestAbstractPolicyBasedAuthorizer {

    static final Resource TEST_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "1";
        }

        @Override
        public String getName() {
            return "resource1";
        }
    };

    @Test
    public void testApproveBasedOnUser() {
        AbstractPolicyBasedAuthorizer authorizer = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        UsersAndAccessPolicies usersAndAccessPolicies = Mockito.mock(UsersAndAccessPolicies.class);
        when(authorizer.getUsersAndAccessPolicies()).thenReturn(usersAndAccessPolicies);

        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";

        final Set<AccessPolicy> policiesForResource = new HashSet<>();
        policiesForResource.add(new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addUser(userIdentifier)
                .addAction(RequestAction.READ)
                .build());

        when(usersAndAccessPolicies.getAccessPolicies(TEST_RESOURCE.getIdentifier())).thenReturn(policiesForResource);

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .build();

        when(usersAndAccessPolicies.getUser(userIdentity)).thenReturn(user);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        assertEquals(AuthorizationResult.approved(), authorizer.authorize(request));
    }

    @Test
    public void testApprovedBasedOnGroup() {
        AbstractPolicyBasedAuthorizer authorizer = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        UsersAndAccessPolicies usersAndAccessPolicies = Mockito.mock(UsersAndAccessPolicies.class);
        when(authorizer.getUsersAndAccessPolicies()).thenReturn(usersAndAccessPolicies);

        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";
        final String groupIdentifier = "groupIdentifier1";

        final Set<AccessPolicy> policiesForResource = new HashSet<>();
        policiesForResource.add(new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addGroup(groupIdentifier)
                .addAction(RequestAction.READ)
                .build());

        when(usersAndAccessPolicies.getAccessPolicies(TEST_RESOURCE.getIdentifier())).thenReturn(policiesForResource);

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .addGroup(groupIdentifier)
                .build();

        when(usersAndAccessPolicies.getUser(userIdentity)).thenReturn(user);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        assertEquals(AuthorizationResult.approved(), authorizer.authorize(request));
    }

    @Test
    public void testDeny() {
        AbstractPolicyBasedAuthorizer authorizer = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        UsersAndAccessPolicies usersAndAccessPolicies = Mockito.mock(UsersAndAccessPolicies.class);
        when(authorizer.getUsersAndAccessPolicies()).thenReturn(usersAndAccessPolicies);

        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";

        final Set<AccessPolicy> policiesForResource = new HashSet<>();
        policiesForResource.add(new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addUser("NOT_USER_1")
                .addAction(RequestAction.READ)
                .build());

        when(usersAndAccessPolicies.getAccessPolicies(TEST_RESOURCE.getIdentifier())).thenReturn(policiesForResource);

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .build();

        when(usersAndAccessPolicies.getUser(userIdentity)).thenReturn(user);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        assertEquals(AuthorizationResult.denied().getResult(), authorizer.authorize(request).getResult());
    }

    @Test
    public void testResourceNotFound() {
        AbstractPolicyBasedAuthorizer authorizer = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        UsersAndAccessPolicies usersAndAccessPolicies = Mockito.mock(UsersAndAccessPolicies.class);
        when(authorizer.getUsersAndAccessPolicies()).thenReturn(usersAndAccessPolicies);

        when(usersAndAccessPolicies.getAccessPolicies(TEST_RESOURCE.getIdentifier())).thenReturn(new HashSet<>());

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity("userIdentity")
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        assertEquals(AuthorizationResult.resourceNotFound(), authorizer.authorize(request));
    }

    @Test
    public void testGetFingerprint() {
        // create the users, groups, and policies

        Group group1 = new Group.Builder().identifier("group-id-1").name("group-1").build();
        Group group2 = new Group.Builder().identifier("group-id-2").name("group-2").build();

        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").addGroup(group1.getIdentifier()).build();
        User user2 = new User.Builder().identifier("user-id-2").identity("user-2").addGroup(group2.getIdentifier()).build();

        AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-id-1")
                .resource("resource1")
                .addAction(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .addUser(user2.getIdentifier())
                .build();

        AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-id-2")
                .resource("resource2")
                .addAction(RequestAction.READ)
                .addAction(RequestAction.WRITE)
                .addGroup(group1.getIdentifier())
                .addGroup(group2.getIdentifier())
                .addUser(user1.getIdentifier())
                .addUser(user2.getIdentifier())
                .build();

        // create the first Authorizer

        Set<Group> groups1 = new LinkedHashSet<>();
        groups1.add(group1);
        groups1.add(group2);

        Set<User> users1 = new LinkedHashSet<>();
        users1.add(user1);
        users1.add(user2);

        Set<AccessPolicy> policies1 = new LinkedHashSet<>();
        policies1.add(policy1);
        policies1.add(policy2);

        AbstractPolicyBasedAuthorizer authorizer1 = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        when(authorizer1.getGroups()).thenReturn(groups1);
        when(authorizer1.getUsers()).thenReturn(users1);
        when(authorizer1.getAccessPolicies()).thenReturn(policies1);

        // create the second Authorizer

        Set<Group> groups2 = new LinkedHashSet<>();
        groups2.add(group2);
        groups2.add(group1);

        Set<User> users2 = new LinkedHashSet<>();
        users2.add(user2);
        users2.add(user1);

        Set<AccessPolicy> policies2 = new LinkedHashSet<>();
        policies2.add(policy2);
        policies2.add(policy1);

        AbstractPolicyBasedAuthorizer authorizer2 = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        when(authorizer2.getGroups()).thenReturn(groups2);
        when(authorizer2.getUsers()).thenReturn(users2);
        when(authorizer2.getAccessPolicies()).thenReturn(policies2);

        // compare the fingerprints

        assertEquals(authorizer1.getFingerprint(), authorizer2.getFingerprint());

        System.out.println(authorizer1.getFingerprint());
    }

    @Test
    public void testInheritFingerprint() {
        Group group1 = new Group.Builder().identifier("group-id-1").name("group-1").build();
        Group group2 = new Group.Builder().identifier("group-id-2").name("group-2").build();

        User user1 = new User.Builder().identifier("user-id-1").identity("user-1").addGroup(group1.getIdentifier()).build();
        User user2 = new User.Builder().identifier("user-id-2").identity("user-2").build();

        AccessPolicy policy1 = new AccessPolicy.Builder()
                .identifier("policy-id-1")
                .resource("resource1")
                .addAction(RequestAction.READ)
                .addUser(user1.getIdentifier())
                .addUser(user2.getIdentifier())
                .build();

        AccessPolicy policy2 = new AccessPolicy.Builder()
                .identifier("policy-id-2")
                .resource("resource2")
                .addAction(RequestAction.READ)
                .addAction(RequestAction.WRITE)
                .addGroup(group1.getIdentifier())
                .addGroup(group2.getIdentifier())
                .addUser(user1.getIdentifier())
                .addUser(user2.getIdentifier())
                .build();

        // create the first Authorizer

        Set<Group> groups1 = new LinkedHashSet<>();
        groups1.add(group1);
        groups1.add(group2);

        Set<User> users1 = new LinkedHashSet<>();
        users1.add(user1);
        users1.add(user2);

        Set<AccessPolicy> policies1 = new LinkedHashSet<>();
        policies1.add(policy1);
        policies1.add(policy2);

        AbstractPolicyBasedAuthorizer authorizer1 = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        when(authorizer1.getGroups()).thenReturn(groups1);
        when(authorizer1.getUsers()).thenReturn(users1);
        when(authorizer1.getAccessPolicies()).thenReturn(policies1);

        final String fingerprint1 = authorizer1.getFingerprint();

        // make a second authorizer using the memory-backed implementation so we can inherit the fingerprint
        // and then compute a new fingerprint to compare them
        AbstractPolicyBasedAuthorizer authorizer2 = new MemoryPolicyBasedAuthorizer();
        authorizer2.inheritFingerprint(fingerprint1);

        // computer the fingerprint of the second authorizer and it should be the same as the first
        final String fingerprint2 = authorizer2.getFingerprint();
        assertEquals(fingerprint1, fingerprint2);

        // all the sets should be equal now after inheriting
        assertEquals(authorizer1.getUsers(), authorizer2.getUsers());
        assertEquals(authorizer1.getGroups(), authorizer2.getGroups());
        assertEquals(authorizer1.getAccessPolicies(), authorizer2.getAccessPolicies());
    }

    @Test
    public void testEmptyAuthorizer() {
        AbstractPolicyBasedAuthorizer authorizer = Mockito.mock(AbstractPolicyBasedAuthorizer.class);
        when(authorizer.getGroups()).thenReturn(new HashSet<Group>());
        when(authorizer.getUsers()).thenReturn(new HashSet<User>());
        when(authorizer.getAccessPolicies()).thenReturn(new HashSet<AccessPolicy>());

        final String fingerprint = authorizer.getFingerprint();
        Assert.assertNotNull(fingerprint);
        Assert.assertTrue(fingerprint.length() > 0);
    }

    /**
     * An AbstractPolicyBasedAuthorizer that stores everything in memory.
     */
    private static final class MemoryPolicyBasedAuthorizer extends AbstractPolicyBasedAuthorizer {

        private Set<Group> groups = new HashSet<>();
        private Set<User> users = new HashSet<>();
        private Set<AccessPolicy> policies = new HashSet<>();

        @Override
        public Group addGroup(Group group) throws AuthorizationAccessException {
            groups.add(group);
            return group;
        }

        @Override
        public Group getGroup(String identifier) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Group updateGroup(Group group) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Group deleteGroup(Group group) throws AuthorizationAccessException {
            groups.remove(group);
            return group;
        }

        @Override
        public Set<Group> getGroups() throws AuthorizationAccessException {
            return groups;
        }

        @Override
        public User addUser(User user) throws AuthorizationAccessException {
            users.add(user);
            return user;
        }

        @Override
        public User getUser(String identifier) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public User getUserByIdentity(String identity) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public User updateUser(User user) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public User deleteUser(User user) throws AuthorizationAccessException {
            users.remove(user);
            return user;
        }

        @Override
        public Set<User> getUsers() throws AuthorizationAccessException {
            return users;
        }

        @Override
        public AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
            policies.add(accessPolicy);
            return accessPolicy;
        }

        @Override
        public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessPolicy deleteAccessPolicy(AccessPolicy policy) throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
            return policies;
        }

        @Override
        public UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {

        }

        @Override
        public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {

        }

        @Override
        public void preDestruction() throws AuthorizerDestructionException {

        }
    }

}
