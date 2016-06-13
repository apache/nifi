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

import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashSet;
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

}
