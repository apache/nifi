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
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.authorization.CompositeConfigurableUserGroupProvider.PROP_CONFIGURABLE_USER_GROUP_PROVIDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeConfigurableUserGroupProviderTest extends CompositeUserGroupProviderTest {

    public static final String USER_5_IDENTIFIER = "user-identifier-5";
    public static final String USER_5_IDENTITY = "user-identity-5";

    public static final String CONFIGURABLE_USER_GROUP_PROVIDER = "configurable-user-group-provider";
    public static final String NOT_CONFIGURABLE_USER_GROUP_PROVIDER = "not-configurable-user-group-provider";

    @Test(expected = AuthorizerCreationException.class)
    public void testNoConfigurableUserGroupProviderSpecified() throws Exception {
        initCompositeUserGroupProvider(new CompositeConfigurableUserGroupProvider(), null, configurationContext -> {
            when(configurationContext.getProperty(PROP_CONFIGURABLE_USER_GROUP_PROVIDER)).thenReturn(new StandardPropertyValue(null, null));
        });
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testUnknownConfigurableUserGroupProvider() throws Exception {
        initCompositeUserGroupProvider(new CompositeConfigurableUserGroupProvider(), null, configurationContext -> {
            when(configurationContext.getProperty(PROP_CONFIGURABLE_USER_GROUP_PROVIDER)).thenReturn(new StandardPropertyValue("unknown-user-group-provider", null));
        });
    }

    @Test(expected = AuthorizerCreationException.class)
    public void testNonConfigurableUserGroupProvider() throws Exception {
        initCompositeUserGroupProvider(new CompositeConfigurableUserGroupProvider(), lookup -> {
            when(lookup.getUserGroupProvider(eq(NOT_CONFIGURABLE_USER_GROUP_PROVIDER))).thenReturn(mock(UserGroupProvider.class));
        }, configurationContext -> {
            when(configurationContext.getProperty(PROP_CONFIGURABLE_USER_GROUP_PROVIDER)).thenReturn(new StandardPropertyValue(NOT_CONFIGURABLE_USER_GROUP_PROVIDER, null));
        });
    }

    @Test
    public void testConfigurableUserGroupProviderOnly() throws Exception {
        final UserGroupProvider userGroupProvider = initCompositeUserGroupProvider(new CompositeConfigurableUserGroupProvider(), lookup -> {
            when(lookup.getUserGroupProvider(eq(CONFIGURABLE_USER_GROUP_PROVIDER))).thenReturn(getConfigurableUserGroupProvider());
        }, configurationContext -> {
            when(configurationContext.getProperty(PROP_CONFIGURABLE_USER_GROUP_PROVIDER)).thenReturn(new StandardPropertyValue(CONFIGURABLE_USER_GROUP_PROVIDER, null));
        });

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
        testConfigurableUserGroupProvider(userGroupProvider);
    }

    @Test
    public void testConfigurableUserGroupProviderWithConflictingUserGroupProvider() throws Exception {
        final UserGroupProvider userGroupProvider = initCompositeUserGroupProvider(new CompositeConfigurableUserGroupProvider(), lookup -> {
            when(lookup.getUserGroupProvider(eq(CONFIGURABLE_USER_GROUP_PROVIDER))).thenReturn(getConfigurableUserGroupProvider());
        }, configurationContext -> {
            when(configurationContext.getProperty(PROP_CONFIGURABLE_USER_GROUP_PROVIDER)).thenReturn(new StandardPropertyValue(CONFIGURABLE_USER_GROUP_PROVIDER, null));
        }, getConflictingUserGroupProvider());

        // users and groups
        assertEquals(3, userGroupProvider.getUsers().size());
        assertEquals(1, userGroupProvider.getGroups().size());

        // unknown
        assertNull(userGroupProvider.getUser(NOT_A_REAL_USER_IDENTIFIER));
        assertNull(userGroupProvider.getUserByIdentity(NOT_A_REAL_USER_IDENTITY));

        final UserAndGroups unknownUserAndGroups = userGroupProvider.getUserAndGroups(NOT_A_REAL_USER_IDENTITY);
        assertNotNull(unknownUserAndGroups);
        assertNull(unknownUserAndGroups.getUser());
        assertNull(unknownUserAndGroups.getGroups());

        // providers
        testConfigurableUserGroupProvider(userGroupProvider);
        testConflictingUserGroupProvider(userGroupProvider);
    }

    private UserGroupProvider getConfigurableUserGroupProvider() {
        final Set<User> users = Stream.of(
                new User.Builder().identifier(USER_1_IDENTIFIER).identity(USER_1_IDENTITY).build(),
                new User.Builder().identifier(USER_5_IDENTIFIER).identity(USER_5_IDENTITY).build()
        ).collect(Collectors.toSet());

        final Set<Group> groups = Stream.of(
                new Group.Builder().identifier(GROUP_2_IDENTIFIER).name(GROUP_2_NAME).addUser(USER_1_IDENTIFIER).build()
        ).collect(Collectors.toSet());

        return new SimpleConfigurableUserGroupProvider(users, groups);
    }

    private void testConfigurableUserGroupProvider(final UserGroupProvider userGroupProvider) {
        assertNotNull(userGroupProvider.getUser(USER_1_IDENTIFIER));
        assertNotNull(userGroupProvider.getUserByIdentity(USER_1_IDENTITY));

        final UserAndGroups user1AndGroups = userGroupProvider.getUserAndGroups(USER_1_IDENTITY);
        assertNotNull(user1AndGroups);
        assertNotNull(user1AndGroups.getUser());
        assertEquals(1, user1AndGroups.getGroups().size());

        assertNotNull(userGroupProvider.getUser(USER_5_IDENTIFIER));
        assertNotNull(userGroupProvider.getUserByIdentity(USER_5_IDENTITY));

        final UserAndGroups user5AndGroups = userGroupProvider.getUserAndGroups(USER_5_IDENTITY);
        assertNotNull(user5AndGroups);
        assertNotNull(user5AndGroups.getUser());
        assertTrue(user5AndGroups.getGroups().isEmpty());

        assertNotNull(userGroupProvider.getGroup(GROUP_2_IDENTIFIER));
        assertEquals(1, userGroupProvider.getGroup(GROUP_2_IDENTIFIER).getUsers().size());
    }
}