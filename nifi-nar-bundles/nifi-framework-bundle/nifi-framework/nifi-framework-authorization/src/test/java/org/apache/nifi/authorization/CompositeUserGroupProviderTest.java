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

import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.junit.Test;

import static org.apache.nifi.authorization.CompositeUserGroupProvider.PROP_USER_GROUP_PROVIDER_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeUserGroupProviderTest extends CompositeUserGroupProviderTestBase {

    @Test(expected = AuthorizerCreationException.class)
    public void testNoConfiguredProviders() throws Exception {
        initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null);
    }

    @Test
    public void testNoConfiguredProvidersAllowed() throws Exception {
        initCompositeUserGroupProvider(new CompositeUserGroupProvider(true), null, null);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testUnknownProvider() throws Exception {
        // initialization
        final UserGroupProviderInitializationContext initializationContext = mock(UserGroupProviderInitializationContext.class);
        when(initializationContext.getUserGroupProviderLookup()).thenReturn(new UserGroupProviderLookup() {
            @Override
            public UserGroupProvider getUserGroupProvider(String identifier) {
                return null;
            }
        });

        // configuration
        final AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(eq(PROP_USER_GROUP_PROVIDER_PREFIX + "1"))).thenReturn(new StandardPropertyValue(String.valueOf("1"), null, ParameterLookup.EMPTY));
        mockProperties(configurationContext);

        final CompositeUserGroupProvider compositeUserGroupProvider = new CompositeUserGroupProvider();
        compositeUserGroupProvider.initialize(initializationContext);
        compositeUserGroupProvider.onConfigured(configurationContext);
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testDuplicateProviders() throws Exception {
        UserGroupProvider duplicatedUserGroupProvider = getUserGroupProviderOne();
        initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null, duplicatedUserGroupProvider, duplicatedUserGroupProvider);
    }

    @Test
    public void testOneProvider() throws Exception {
        final UserGroupProvider userGroupProvider = initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null, getUserGroupProviderOne());

        // users and groups
        assertEquals(2, userGroupProvider.getUsers().size());
        assertEquals(1, userGroupProvider.getGroups().size());

        // unknown
        assertNull(userGroupProvider.getUser(NOT_A_REAL_USER_IDENTIFIER));
        assertNull(userGroupProvider.getUserByIdentity(NOT_A_REAL_USER_IDENTITY));

        final UserAndGroups unknownUserAndGroups = userGroupProvider.getUserAndGroups(NOT_A_REAL_USER_IDENTITY);
        assertNotNull(unknownUserAndGroups);
        assertNull(unknownUserAndGroups.getUser());
        assertNull(unknownUserAndGroups.getGroups());

        // providers
        testUserGroupProviderOne(userGroupProvider);
    }

    @Test
    public void testMultipleProviders() throws Exception {
        final UserGroupProvider userGroupProvider = initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null,
                getUserGroupProviderOne(), getUserGroupProviderTwo());

        // users and groups
        assertEquals(3, userGroupProvider.getUsers().size());
        assertEquals(2, userGroupProvider.getGroups().size());

        // unknown
        assertNull(userGroupProvider.getUser(NOT_A_REAL_USER_IDENTIFIER));
        assertNull(userGroupProvider.getUserByIdentity(NOT_A_REAL_USER_IDENTITY));

        final UserAndGroups unknownUserAndGroups = userGroupProvider.getUserAndGroups(NOT_A_REAL_USER_IDENTITY);
        assertNotNull(unknownUserAndGroups);
        assertNull(unknownUserAndGroups.getUser());
        assertNull(unknownUserAndGroups.getGroups());

        // providers
        testUserGroupProviderOne(userGroupProvider);
        testUserGroupProviderTwo(userGroupProvider);
    }

    @Test
    public void testMultipleProvidersWithConflictingUsers() throws Exception {
        final UserGroupProvider userGroupProvider = initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null,
                getUserGroupProviderOne(), getUserGroupProviderTwo(), getConflictingUserGroupProvider());

        // users and groups
        assertEquals(4, userGroupProvider.getUsers().size());
        assertEquals(2, userGroupProvider.getGroups().size());

        // unknown
        assertNull(userGroupProvider.getUser(NOT_A_REAL_USER_IDENTIFIER));
        assertNull(userGroupProvider.getUserByIdentity(NOT_A_REAL_USER_IDENTITY));

        final UserAndGroups unknownUserAndGroups = userGroupProvider.getUserAndGroups(NOT_A_REAL_USER_IDENTITY);
        assertNotNull(unknownUserAndGroups);
        assertNull(unknownUserAndGroups.getUser());
        assertNull(unknownUserAndGroups.getGroups());

        // providers
        testUserGroupProviderTwo(userGroupProvider);

        try {
            testUserGroupProviderOne(userGroupProvider);
            assertTrue("Should never get here as we expect the line above to throw an exception", false);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertTrue(e.getMessage().contains(USER_1_IDENTITY));
        }

        try {
            testConflictingUserGroupProvider(userGroupProvider);
            assertTrue("Should never get here as we expect the line above to throw an exception", false);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertTrue(e.getMessage().contains(USER_1_IDENTITY));
        }
    }

    @Test
    public void testMultipleProvidersWithCollaboratingUserGroupProvider() throws Exception {
        final UserGroupProvider userGroupProvider = initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null,
                getUserGroupProviderOne(), getUserGroupProviderTwo(), getCollaboratingUserGroupProvider());

        // users and groups
        assertEquals(4, userGroupProvider.getUsers().size());
        assertEquals(2, userGroupProvider.getGroups().size());

        // unknown
        assertNull(userGroupProvider.getUser(NOT_A_REAL_USER_IDENTIFIER));
        assertNull(userGroupProvider.getUserByIdentity(NOT_A_REAL_USER_IDENTITY));

        final UserAndGroups unknownUserAndGroups = userGroupProvider.getUserAndGroups(NOT_A_REAL_USER_IDENTITY);
        assertNotNull(unknownUserAndGroups);
        assertNull(unknownUserAndGroups.getUser());
        assertNull(unknownUserAndGroups.getGroups());

        // providers
        testUserGroupProviderTwo(userGroupProvider);

        final UserAndGroups user1AndGroups = userGroupProvider.getUserAndGroups(USER_1_IDENTITY);
        assertNotNull(user1AndGroups);
        assertNotNull(user1AndGroups.getUser());
        assertEquals(2, user1AndGroups.getGroups().size()); // from UGP1 and CollaboratingUGP
    }
}