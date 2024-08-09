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
package org.apache.nifi.ldap.tenants;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.ldap.LdapAuthenticationStrategy;
import org.apache.nifi.ldap.ReferralStrategy;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.security.ldap.server.UnboundIdContainer;

import java.util.Properties;
import java.util.Set;

import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_AUTHENTICATION_STRATEGY;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_CONNECT_TIMEOUT;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_MEMBERSHIP_ENFORCE_CASE_SENSITIVITY;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_MEMBER_ATTRIBUTE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_MEMBER_REFERENCED_USER_ATTRIBUTE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_NAME_ATTRIBUTE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_OBJECT_CLASS;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_SEARCH_BASE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_SEARCH_FILTER;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_GROUP_SEARCH_SCOPE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_MANAGER_DN;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_MANAGER_PASSWORD;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_PAGE_SIZE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_READ_TIMEOUT;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_REFERRAL_STRATEGY;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_SYNC_INTERVAL;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_URL;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_GROUP_ATTRIBUTE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_GROUP_REFERENCED_GROUP_ATTRIBUTE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_IDENTITY_ATTRIBUTE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_OBJECT_CLASS;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_SEARCH_BASE;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_SEARCH_FILTER;
import static org.apache.nifi.ldap.tenants.LdapUserGroupProvider.PROP_USER_SEARCH_SCOPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LdapUserGroupProviderTest {

    private static final String USER_SEARCH_BASE = "ou=users,o=nifi";
    private static final String GROUP_SEARCH_BASE = "ou=groups,o=nifi";

    private LdapUserGroupProvider ldapUserGroupProvider;
    private Integer serverPort;
    private UnboundIdContainer server;

    @BeforeEach
    public void setup() {
        server = new UnboundIdContainer("o=nifi", "classpath:nifi-example.ldif");
        server.setApplicationContext(new GenericApplicationContext());
        server.setPort(0);
        server.afterPropertiesSet();
        serverPort = server.getPort();
        final UserGroupProviderInitializationContext initializationContext = mock(UserGroupProviderInitializationContext.class);
        when(initializationContext.getIdentifier()).thenReturn("identifier");

        ldapUserGroupProvider = new LdapUserGroupProvider();
        ldapUserGroupProvider.setNiFiProperties(getNiFiProperties(new Properties()));
        ldapUserGroupProvider.initialize(initializationContext);
    }

    @AfterEach
    public void tearDown() {
        if (server != null && server.isRunning()) {
            server.destroy();
        }
    }

    @Test
    public void testNoSearchBasesSpecified() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, null);
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testUserSearchBaseSpecifiedButNoUserObjectClass() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_OBJECT_CLASS)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testUserSearchBaseSpecifiedButNoUserSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testInvalidUserSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue("not-valid", null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testSearchUsersWithNoIdentityAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("cn=User 1,ou=users,o=nifi"));
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersWithUidIdentityAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("user1"));
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersWithCnIdentityAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("User 1"));
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersObjectSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(SearchScope.OBJECT.name(), null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertTrue(ldapUserGroupProvider.getUsers().isEmpty());
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersSubtreeSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration("o=nifi", null);
        when(configurationContext.getProperty(PROP_USER_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(SearchScope.SUBTREE.name(), null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(9, ldapUserGroupProvider.getUsers().size());
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersWithFilter() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_SEARCH_FILTER)).thenReturn(new StandardPropertyValue("(uid=user1)", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(1, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("user1"));
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersWithPaging() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_PAGE_SIZE)).thenReturn(new StandardPropertyValue("1", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchUsersWithGroupingNoGroupName() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of memberof
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertEquals(3, ldapUserGroupProvider.getGroups().size());

        final UserAndGroups user4AndGroups = ldapUserGroupProvider.getUserAndGroups("user4");
        assertNotNull(user4AndGroups.getUser());
        assertEquals(1, user4AndGroups.getGroups().size());
        assertEquals("cn=team1,ou=groups,o=nifi", user4AndGroups.getGroups().iterator().next().getName());

        final UserAndGroups user7AndGroups = ldapUserGroupProvider.getUserAndGroups("user7");
        assertNotNull(user7AndGroups.getUser());
        assertEquals(1, user7AndGroups.getGroups().size());
        assertEquals("cn=team2,ou=groups,o=nifi", user7AndGroups.getGroups().iterator().next().getName());

        final UserAndGroups user8AndGroups = ldapUserGroupProvider.getUserAndGroups("user8");
        assertNotNull(user8AndGroups.getUser());
        assertEquals(1, user8AndGroups.getGroups().size());
        assertEquals("cn=Team2,ou=groups,o=nifi", user8AndGroups.getGroups().iterator().next().getName());
    }

    @Test
    public void testGetGroupByName() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of memberof
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertEquals(2, ldapUserGroupProvider.getGroups().size());

        final Group group = ldapUserGroupProvider.getGroupByName("team1");
        assertNotNull(group);
        assertEquals("team1", group.getName());
    }

    @Test
    public void testSearchUsersWithGroupingAndGroupName() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of memberof
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());
        assertEquals(2, ldapUserGroupProvider.getGroups().size());

        final UserAndGroups userAndGroups = ldapUserGroupProvider.getUserAndGroups("user4");
        assertNotNull(userAndGroups.getUser());
        assertEquals(1, userAndGroups.getGroups().size());
        assertEquals("team1", userAndGroups.getGroups().iterator().next().getName());
    }

    @Test
    public void testSearchGroupsWithoutMemberAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testGroupSearchBaseSpecifiedButNoGroupObjectClass() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testUserSearchBaseSpecifiedButNoGroupSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testInvalidGroupSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue("not-valid", null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testSearchGroupsWithNoNameAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());
        assertEquals(1, groups.stream().filter(group -> "cn=admins,ou=groups,o=nifi".equals(group.getName())).count());
    }

    @Test
    public void testSearchGroupsWithNoNameAttributeCaptureGroupBlank() {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^cn=User( )\\d,ou=.*?,o=.*?$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");
        props.setProperty("nifi.security.identity.mapping.transform.dn1", "UPPER");

        final NiFiProperties properties = getNiFiProperties(props);
        ldapUserGroupProvider.setNiFiProperties(properties);

        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
        final String message = exception.getMessage();
        assertTrue(message.contains("o=nifi"), "Distinguished Name not found");
    }

    @Test
    public void testSearchGroupsWithPaging() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_PAGE_SIZE)).thenReturn(new StandardPropertyValue("1", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(5, ldapUserGroupProvider.getGroups().size());
    }

    @Test
    public void testSearchGroupsObjectSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(SearchScope.OBJECT.name(), null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertTrue(ldapUserGroupProvider.getUsers().isEmpty());
        assertTrue(ldapUserGroupProvider.getGroups().isEmpty());
    }

    @Test
    public void testSearchGroupsSubtreeSearchScope() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, "o=nifi");
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(SearchScope.SUBTREE.name(), null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(5, ldapUserGroupProvider.getGroups().size());
    }

    @Test
    public void testSearchGroupsWithNameAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group admins = groups.stream().filter(group -> "admins".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(admins);
        assertFalse(admins.getUsers().isEmpty());
        assertEquals(1, admins.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                        user -> "cn=User 1,ou=users,o=nifi".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchGroupsWithNoNameAndUserIdentityUidAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group admins = groups.stream().filter(group -> "cn=admins,ou=groups,o=nifi".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(admins);
        assertFalse(admins.getUsers().isEmpty());
        assertEquals(1, admins.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchGroupsWithNameAndUserIdentityCnAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group admins = groups.stream().filter(group -> "admins".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(admins);
        assertFalse(admins.getUsers().isEmpty());
        assertEquals(1, admins.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "User 1".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchGroupsWithFilter() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_FILTER)).thenReturn(new StandardPropertyValue("(cn=admins)", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(1, groups.size());
        assertEquals(1, groups.stream().filter(group -> "cn=admins,ou=groups,o=nifi".equals(group.getName())).count());
    }

    @Test
    public void testSearchUsersAndGroupsNoMembership() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());
        groups.forEach(group -> assertTrue(group.getUsers().isEmpty()));
    }

    @Test
    public void testSearchUsersAndGroupsMembershipThroughUsers() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of memberof
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group team1 = groups.stream().filter(group -> "team1".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team1);
        assertEquals(2, team1.getUsers().size());
        assertEquals(2, team1.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user4".equals(user.getIdentity()) || "user5".equals(user.getIdentity())).count());

        final Group team2 = groups.stream().filter(group -> "team2".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team2);
        assertEquals(2, team2.getUsers().size());
        assertEquals(2, team2.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user6".equals(user.getIdentity()) || "user7".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchUsersAndGroupsMembershipThroughGroups() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group admins = groups.stream().filter(group -> "admins".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(admins);
        assertEquals(2, admins.getUsers().size());
        assertEquals(2, admins.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity()) || "user3".equals(user.getIdentity())).count());

        final Group readOnly = groups.stream().filter(group -> "read-only".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(readOnly);
        assertEquals(1, readOnly.getUsers().size());
        assertEquals(1, readOnly.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user2".equals(user.getIdentity())).count());

        final Group team1 = groups.stream().filter(group -> "team1".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team1);
        assertEquals(1, team1.getUsers().size());
        assertEquals(1, team1.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity())).count());

        final Group team2 = groups.stream().filter(group -> "team2".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team2);
        assertEquals(1, team2.getUsers().size());
        assertEquals(1, team2.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchUsersAndGroupsMembershipThroughUsersAndGroups() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of memberof
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group admins = groups.stream().filter(group -> "admins".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(admins);
        assertEquals(2, admins.getUsers().size());
        assertEquals(2, admins.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity()) || "user3".equals(user.getIdentity())).count());

        final Group readOnly = groups.stream().filter(group -> "read-only".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(readOnly);
        assertEquals(1, readOnly.getUsers().size());
        assertEquals(1, readOnly.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user2".equals(user.getIdentity())).count());

        final Group team1 = groups.stream().filter(group -> "team1".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team1);
        assertEquals(3, team1.getUsers().size());
        assertEquals(3, team1.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity()) || "user4".equals(user.getIdentity()) || "user5".equals(user.getIdentity())).count());

        final Group team2 = groups.stream().filter(group -> "team2".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team2);
        assertEquals(3, team2.getUsers().size());
        assertEquals(3, team2.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity()) || "user6".equals(user.getIdentity()) || "user7".equals(user.getIdentity())).count());
    }

    @Test
    public void testUserIdentityMapping() {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^cn=(.*?),o=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");

        final NiFiProperties properties = getNiFiProperties(props);
        ldapUserGroupProvider.setNiFiProperties(properties);

        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_SEARCH_FILTER)).thenReturn(new StandardPropertyValue("(uid=user1)", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(1, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("User 1,ou=users"));
    }

    @Test
    public void testUserIdentityMappingWithTransforms() {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^cn=(.*?),ou=(.*?),o=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");
        props.setProperty("nifi.security.identity.mapping.transform.dn1", "UPPER");

        final NiFiProperties properties = getNiFiProperties(props);
        ldapUserGroupProvider.setNiFiProperties(properties);

        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, null);
        when(configurationContext.getProperty(PROP_USER_SEARCH_FILTER)).thenReturn(new StandardPropertyValue("(uid=user1)", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(1, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("USER 1"));
    }

    @Test
    public void testUserIdentityAndGroupMappingWithTransforms() {
        final Properties props = new Properties();
        props.setProperty("nifi.security.identity.mapping.pattern.dn1", "^cn=(.*?),ou=(.*?),o=(.*?)$");
        props.setProperty("nifi.security.identity.mapping.value.dn1", "$1");
        props.setProperty("nifi.security.identity.mapping.transform.dn1", "UPPER");
        props.setProperty("nifi.security.group.mapping.pattern.dn1", "^cn=(.*?),ou=(.*?),o=(.*?)$");
        props.setProperty("nifi.security.group.mapping.value.dn1", "$1");
        props.setProperty("nifi.security.group.mapping.transform.dn1", "UPPER");

        final NiFiProperties properties = getNiFiProperties(props);
        ldapUserGroupProvider.setNiFiProperties(properties);

        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_SEARCH_FILTER)).thenReturn(new StandardPropertyValue("(uid=user1)", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_FILTER)).thenReturn(new StandardPropertyValue("(cn=admins)", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(1, ldapUserGroupProvider.getUsers().size());
        assertNotNull(ldapUserGroupProvider.getUserByIdentity("USER 1"));

        assertEquals(1, ldapUserGroupProvider.getGroups().size());
        assertEquals("ADMINS", ldapUserGroupProvider.getGroups().iterator().next().getName());
    }

    @Test
    public void testReferencedGroupAttributeWithoutGroupSearchBase() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration("ou=users-2,o=nifi", null);
        when(configurationContext.getProperty(PROP_USER_GROUP_REFERENCED_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testReferencedGroupWithoutDefiningReferencedAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration("ou=users-2,o=nifi", "ou=groups-2,o=nifi");
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("room", null, ParameterLookup.EMPTY)); // using room due to reqs of groupOfNames
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of member
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("room", null, ParameterLookup.EMPTY)); // using room due to reqs of groupOfNames
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(1, groups.size());

        final Group team3 = groups.stream().filter(group -> "team3".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team3);
        assertTrue(team3.getUsers().isEmpty());
    }

    @Test
    public void testReferencedGroupUsingReferencedAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration("ou=users-2,o=nifi", "ou=groups-2,o=nifi");
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of member
        when(configurationContext.getProperty(PROP_USER_GROUP_REFERENCED_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("room", null, ParameterLookup.EMPTY)); // using room because groupOfNames requires a member
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(1, groups.size());

        final Group team3 = groups.stream().filter(group -> "team3".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team3);
        assertEquals(1, team3.getUsers().size());
        assertEquals(1, team3.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user9".equals(user.getIdentity())).count());
    }

    @Test
    public void testReferencedUserWithoutUserSearchBase() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(null, "ou=groups-2,o=nifi");
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_REFERENCED_USER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        assertThrows(AuthorizerCreationException.class, () -> ldapUserGroupProvider.onConfigured(configurationContext));
    }

    @Test
    public void testReferencedUserWithoutDefiningReferencedAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration("ou=users-2,o=nifi", "ou=groups-2,o=nifi");
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("room", null, ParameterLookup.EMPTY)); // using room due to reqs of groupOfNames
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of member
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(1, groups.size());

        final Group team3 = groups.stream().filter(group -> "team3".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team3);
        assertTrue(team3.getUsers().isEmpty());
    }

    @Test
    public void testReferencedUserUsingReferencedAttribute() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration("ou=users-2,o=nifi", "ou=groups-2,o=nifi");
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("sn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("room", null, ParameterLookup.EMPTY)); // using room due to reqs of groupOfNames
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of member
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        // does not need to be the same as user id attr
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_REFERENCED_USER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(1, groups.size());

        final Group team3 = groups.stream().filter(group -> "team3".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team3);
        assertEquals(1, team3.getUsers().size());
        assertEquals(1, team3.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "User9".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchUsersAndGroupsMembershipThroughGroupsCaseInsensitive() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBERSHIP_ENFORCE_CASE_SENSITIVITY)).thenReturn(new StandardPropertyValue("false", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group team4 = groups.stream().filter(group -> "team4".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team4);
        assertEquals(2, team4.getUsers().size());
        assertEquals(1, team4.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity())).count());
        assertEquals(1, team4.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user2".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchUsersAndGroupsMembershipThroughGroupsCaseSensitive() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue("member", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group team4 = groups.stream().filter(group -> "team4".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team4);
        assertEquals(1, team4.getUsers().size());
        assertEquals(1, team4.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user1".equals(user.getIdentity())).count());
    }

    @Test
    public void testSearchUsersAndGroupsMembershipThroughUsersCaseInsensitive() {
        final AuthorizerConfigurationContext configurationContext = getBaseConfiguration(USER_SEARCH_BASE, GROUP_SEARCH_BASE);
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue("uid", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue("description", null, ParameterLookup.EMPTY)); // using description in lieu of memberof
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue("cn", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBERSHIP_ENFORCE_CASE_SENSITIVITY)).thenReturn(new StandardPropertyValue("false", null, ParameterLookup.EMPTY));
        ldapUserGroupProvider.onConfigured(configurationContext);

        assertEquals(8, ldapUserGroupProvider.getUsers().size());

        final Set<Group> groups = ldapUserGroupProvider.getGroups();
        assertEquals(5, groups.size());

        final Group team1 = groups.stream().filter(group -> "team1".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team1);
        assertEquals(2, team1.getUsers().size());
        assertEquals(2, team1.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user4".equals(user.getIdentity()) || "user5".equals(user.getIdentity())).count());

        final Group team2 = groups.stream().filter(group -> "team2".equals(group.getName())).findFirst().orElse(null);
        assertNotNull(team2);
        assertEquals(3, team2.getUsers().size());
        assertEquals(3, team2.getUsers().stream().map(
                userIdentifier -> ldapUserGroupProvider.getUser(userIdentifier)).filter(
                user -> "user6".equals(user.getIdentity()) || "user7".equals(user.getIdentity()) || "user8".equals(user.getIdentity())).count());
    }

    private AuthorizerConfigurationContext getBaseConfiguration(final String userSearchBase, final String groupSearchBase) {
        final AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperty(PROP_URL)).thenReturn(new StandardPropertyValue("ldap://127.0.0.1:" + serverPort, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_CONNECT_TIMEOUT)).thenReturn(new StandardPropertyValue("30 secs", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_READ_TIMEOUT)).thenReturn(new StandardPropertyValue("30 secs", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_REFERRAL_STRATEGY)).thenReturn(new StandardPropertyValue(ReferralStrategy.FOLLOW.name(), null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_PAGE_SIZE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_SYNC_INTERVAL)).thenReturn(new StandardPropertyValue("30 mins", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBERSHIP_ENFORCE_CASE_SENSITIVITY)).thenReturn(new StandardPropertyValue("true", null, ParameterLookup.EMPTY));

        when(configurationContext.getProperty(PROP_AUTHENTICATION_STRATEGY)).thenReturn(new StandardPropertyValue(LdapAuthenticationStrategy.SIMPLE.name(), null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_MANAGER_DN)).thenReturn(new StandardPropertyValue("uid=admin,ou=system", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_MANAGER_PASSWORD)).thenReturn(new StandardPropertyValue("secret", null, ParameterLookup.EMPTY));

        when(configurationContext.getProperty(PROP_USER_SEARCH_BASE)).thenReturn(new StandardPropertyValue(userSearchBase, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("person", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(SearchScope.ONE_LEVEL.name(), null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_SEARCH_FILTER)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_IDENTITY_ATTRIBUTE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_USER_GROUP_REFERENCED_GROUP_ATTRIBUTE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));

        when(configurationContext.getProperty(PROP_GROUP_SEARCH_BASE)).thenReturn(new StandardPropertyValue(groupSearchBase, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_OBJECT_CLASS)).thenReturn(new StandardPropertyValue("groupOfNames", null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_SCOPE)).thenReturn(new StandardPropertyValue(SearchScope.ONE_LEVEL.name(), null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_SEARCH_FILTER)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_NAME_ATTRIBUTE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_ATTRIBUTE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));
        when(configurationContext.getProperty(PROP_GROUP_MEMBER_REFERENCED_USER_ATTRIBUTE)).thenReturn(new StandardPropertyValue(null, null, ParameterLookup.EMPTY));

        return configurationContext;
    }

    private NiFiProperties getNiFiProperties(final Properties properties) {
        final NiFiProperties nifiProperties = Mockito.mock(NiFiProperties.class);
        when(nifiProperties.getPropertyKeys()).thenReturn(properties.stringPropertyNames());
        when(nifiProperties.getProperty(anyString())).then(invocationOnMock -> properties.getProperty((String) invocationOnMock.getArguments()[0]));
        return nifiProperties;
    }
}
