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
import org.apache.nifi.parameter.ParameterLookup;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.authorization.CompositeUserGroupProvider.PROP_USER_GROUP_PROVIDER_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeUserGroupProviderTest extends CompositeUserGroupProviderTestBase {

    @Test
    public void testNoConfiguredProviders() {
        assertThrows(AuthorizerCreationException.class, () -> {
            initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null);
        });
    }

    @Test
    public void testNoConfiguredProvidersAllowed() {
        initCompositeUserGroupProvider(new CompositeUserGroupProvider(true), null, null);
    }

    @Test
    public void testUnknownProvider() {
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

        assertThrows(AuthorizerCreationException.class, () -> {
            final CompositeUserGroupProvider compositeUserGroupProvider = new CompositeUserGroupProvider();
            compositeUserGroupProvider.initialize(initializationContext);
            compositeUserGroupProvider.onConfigured(configurationContext);
        });
    }

    @Test
    public void testDuplicateProviders() {
        UserGroupProvider duplicatedUserGroupProvider = getUserGroupProviderOne();
        assertThrows(AuthorizerCreationException.class, () -> {
            initCompositeUserGroupProvider(new CompositeUserGroupProvider(), null, null, duplicatedUserGroupProvider, duplicatedUserGroupProvider);
        });
    }

    @Test
    public void testOneProvider() {
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
    public void testMultipleProviders() {
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
    public void testMultipleProvidersWithConflictingUsers() {
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

        Exception exception = assertThrows(IllegalStateException.class, () -> {
            testUserGroupProviderOne(userGroupProvider);
        });
        assertTrue(exception.getMessage().contains(USER_1_IDENTITY));

        exception = assertThrows(IllegalStateException.class, () -> {
           testConflictingUserGroupProvider(userGroupProvider);
        });
        assertTrue(exception.getMessage().contains(USER_1_IDENTITY));
    }

    @Test
    public void testMultipleProvidersWithCollaboratingUserGroupProvider() {
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