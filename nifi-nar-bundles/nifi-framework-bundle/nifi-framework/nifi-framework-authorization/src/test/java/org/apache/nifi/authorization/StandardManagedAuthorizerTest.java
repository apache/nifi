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
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandardManagedAuthorizerTest {

    private static final String EMPTY_FINGERPRINT = "<?xml version=\"1.0\" ?>"
            + "<managedAuthorizations>"
                + "<accessPolicyProvider></accessPolicyProvider>"
                + "<userGroupProvider></userGroupProvider>"
            + "</managedAuthorizations>";

    private static final String NON_EMPTY_FINGERPRINT = "<?xml version=\"1.0\" ?>"
            + "<managedAuthorizations>"
                + "<accessPolicyProvider>"
                    + "&lt;accessPolicies&gt;"
                        + "&lt;policy identifier=\"policy-id-1\" resource=\"resource2\" actions=\"READ\"&gt;"
                            + "&lt;policyUser identifier=\"user-id-1\"&gt;&lt;/policyUser&gt;"
                            + "&lt;policyGroup identifier=\"group-id-1\"&gt;&lt;/policyGroup&gt;"
                        + "&lt;/policy&gt;"
                    + "&lt;/accessPolicies&gt;"
                + "</accessPolicyProvider>"
                + "<userGroupProvider>"
                    + "&lt;tenants&gt;"
                        + "&lt;user identifier=\"user-id-1\" identity=\"user-1\"&gt;&lt;/user&gt;"
                        + "&lt;group identifier=\"group-id-1\" name=\"group-1\"&gt;"
                            + "&lt;groupUser identifier=\"user-id-1\"&gt;&lt;/groupUser&gt;"
                        + "&lt;/group&gt;"
                    + "&lt;/tenants&gt;"
                + "</userGroupProvider>"
            + "</managedAuthorizations>";

    private static final String ACCESS_POLICY_FINGERPRINT =
            "<accessPolicies>"
                + "<policy identifier=\"policy-id-1\" resource=\"resource2\" actions=\"READ\">"
                    + "<policyUser identifier=\"user-id-1\"></policyUser>"
                    + "<policyGroup identifier=\"group-id-1\"></policyGroup>"
                + "</policy>"
            + "</accessPolicies>";

    private static final String TENANT_FINGERPRINT =
            "<tenants>"
                + "<user identifier=\"user-id-1\" identity=\"user-1\"></user>"
                + "<group identifier=\"group-id-1\" name=\"group-1\">"
                    + "<groupUser identifier=\"user-id-1\"></groupUser>"
                + "</group>"
            + "</tenants>";

    private static final Resource TEST_RESOURCE = new Resource() {
        @Override
        public String getIdentifier() {
            return "1";
        }

        @Override
        public String getName() {
            return "resource1";
        }

        @Override
        public String getSafeDescription() {
            return "description1";
        }
    };

    @Test(expected = AuthorizerCreationException.class)
    public void testNullAccessPolicyProvider() throws Exception {
        getStandardManagedAuthorizer(null);
    }

    @Test
    public void testEmptyFingerPrint() throws Exception {
        final UserGroupProvider userGroupProvider = mock(UserGroupProvider.class);

        final AccessPolicyProvider accessPolicyProvider = mock(AccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        Assert.assertEquals(EMPTY_FINGERPRINT, managedAuthorizer.getFingerprint());
    }

    @Test
    public void testNonEmptyFingerPrint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getFingerprint()).thenReturn(TENANT_FINGERPRINT);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getFingerprint()).thenReturn(ACCESS_POLICY_FINGERPRINT);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        Assert.assertEquals(NON_EMPTY_FINGERPRINT, managedAuthorizer.getFingerprint());
    }

    @Test
    public void testInheritEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        managedAuthorizer.inheritFingerprint(EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(0)).inheritFingerprint(anyString());
        verify(accessPolicyProvider, times(0)).inheritFingerprint(anyString());
    }

    @Test(expected = AuthorizationAccessException.class)
    public void testInheritInvalidFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        managedAuthorizer.inheritFingerprint("not a valid fingerprint");
    }

    @Test
    public void testInheritNonEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        managedAuthorizer.inheritFingerprint(NON_EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(1)).inheritFingerprint(TENANT_FINGERPRINT);
        verify(accessPolicyProvider, times(1)).inheritFingerprint(ACCESS_POLICY_FINGERPRINT);
    }

    @Test
    public void testCheckInheritEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        managedAuthorizer.checkInheritability(EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(0)).inheritFingerprint(anyString());
        verify(accessPolicyProvider, times(0)).inheritFingerprint(anyString());
    }

    @Test(expected = AuthorizationAccessException.class)
    public void testCheckInheritInvalidFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        managedAuthorizer.checkInheritability("not a valid fingerprint");
    }

    @Test
    public void testCheckInheritNonEmptyFingerprint() throws Exception {
        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        managedAuthorizer.checkInheritability(NON_EMPTY_FINGERPRINT);

        verify(userGroupProvider, times(1)).checkInheritability(TENANT_FINGERPRINT);
        verify(accessPolicyProvider, times(1)).checkInheritability(ACCESS_POLICY_FINGERPRINT);
    }

    @Test
    public void testAuthorizationByUser() throws Exception {
        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .build();

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addUser(userIdentifier)
                .action(RequestAction.READ)
                .build();

        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getUserAndGroups(userIdentity)).thenReturn(new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return Collections.EMPTY_SET;
            }
        });

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getAccessPolicy(TEST_RESOURCE.getIdentifier(), RequestAction.READ)).thenReturn(policy);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        assertEquals(AuthorizationResult.approved(), managedAuthorizer.authorize(request));
    }

    @Test
    public void testAuthorizationByGroup() throws Exception {
        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";
        final String groupIdentifier = "groupIdentifier1";

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .build();

        final Group group = new Group.Builder()
                .identifier(groupIdentifier)
                .name(groupIdentifier)
                .addUser(user.getIdentifier())
                .build();

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addGroup(groupIdentifier)
                .action(RequestAction.READ)
                .build();

        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getUserAndGroups(userIdentity)).thenReturn(new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return Stream.of(group).collect(Collectors.toSet());
            }
        });

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getAccessPolicy(TEST_RESOURCE.getIdentifier(), RequestAction.READ)).thenReturn(policy);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        assertEquals(AuthorizationResult.approved(), managedAuthorizer.authorize(request));
    }

    @Test
    public void testResourceNotFound() throws Exception {
        final String userIdentity = "userIdentity1";

        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getAccessPolicy(TEST_RESOURCE.getIdentifier(), RequestAction.READ)).thenReturn(null);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        assertEquals(AuthorizationResult.resourceNotFound(), managedAuthorizer.authorize(request));
    }

    @Test
    public void testUnauthorizedDueToUnknownUser() throws Exception {
        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";
        final String notUser1Identity = "not userIdentity1";

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .build();

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addUser(userIdentifier)
                .action(RequestAction.READ)
                .build();

        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getUserAndGroups(notUser1Identity)).thenReturn(new UserAndGroups() {
            @Override
            public User getUser() {
                return null;
            }

            @Override
            public Set<Group> getGroups() {
                return Collections.EMPTY_SET;
            }
        });

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getAccessPolicy(TEST_RESOURCE.getIdentifier(), RequestAction.READ)).thenReturn(policy);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(notUser1Identity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        assertTrue(AuthorizationResult.denied().getResult().equals(managedAuthorizer.authorize(request).getResult()));
    }

    @Test
    public void testUnauthorizedDueToLackOfPermission() throws Exception {
        final String userIdentifier = "userIdentifier1";
        final String userIdentity = "userIdentity1";

        final User user = new User.Builder()
                .identity(userIdentity)
                .identifier(userIdentifier)
                .build();

        final AccessPolicy policy = new AccessPolicy.Builder()
                .identifier("1")
                .resource(TEST_RESOURCE.getIdentifier())
                .addUser("userIdentity2")
                .action(RequestAction.READ)
                .build();

        final ConfigurableUserGroupProvider userGroupProvider = mock(ConfigurableUserGroupProvider.class);
        when(userGroupProvider.getUserAndGroups(userIdentity)).thenReturn(new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return Collections.EMPTY_SET;
            }
        });

        final ConfigurableAccessPolicyProvider accessPolicyProvider = mock(ConfigurableAccessPolicyProvider.class);
        when(accessPolicyProvider.getAccessPolicy(TEST_RESOURCE.getIdentifier(), RequestAction.READ)).thenReturn(policy);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(userIdentity)
                .resource(TEST_RESOURCE)
                .action(RequestAction.READ)
                .accessAttempt(true)
                .anonymous(false)
                .build();

        final StandardManagedAuthorizer managedAuthorizer = getStandardManagedAuthorizer(accessPolicyProvider);
        assertTrue(AuthorizationResult.denied().getResult().equals(managedAuthorizer.authorize(request).getResult()));
    }

    private StandardManagedAuthorizer getStandardManagedAuthorizer(final AccessPolicyProvider accessPolicyProvider) {
        final StandardManagedAuthorizer managedAuthorizer = new StandardManagedAuthorizer();

        final AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty("Access Policy Provider")).thenReturn(new StandardPropertyValue("access-policy-provider", null));

        final AccessPolicyProviderLookup accessPolicyProviderLookup = mock(AccessPolicyProviderLookup.class);
        when(accessPolicyProviderLookup.getAccessPolicyProvider("access-policy-provider")).thenReturn(accessPolicyProvider);

        final AuthorizerInitializationContext initializationContext = mock(AuthorizerInitializationContext.class);
        when(initializationContext.getAccessPolicyProviderLookup()).thenReturn(accessPolicyProviderLookup);

        managedAuthorizer.initialize(initializationContext);
        managedAuthorizer.onConfigured(configurationContext);

        return managedAuthorizer;
    }
}