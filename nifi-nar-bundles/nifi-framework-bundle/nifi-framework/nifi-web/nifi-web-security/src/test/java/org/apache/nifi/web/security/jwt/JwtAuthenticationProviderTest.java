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
package org.apache.nifi.web.security.jwt;

import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserAndGroups;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.idp.IdpUserGroup;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtAuthenticationProviderTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final static int EXPIRATION_MILLIS = 60000;
    private final static String CLIENT_ADDRESS = "127.0.0.1";
    private final static String ADMIN_IDENTITY = "nifiadmin";
    private final static String REALMED_ADMIN_KERBEROS_IDENTITY = "nifiadmin@nifi.apache.org";

    private final static String UNKNOWN_TOKEN = "eyJhbGciOiJIUzI1NiJ9" +
            ".eyJzdWIiOiJ1bmtub3duX3Rva2VuIiwiaXNzIjoiS2VyYmVyb3NQcm9" +
            "2aWRlciIsImF1ZCI6IktlcmJlcm9zUHJvdmlkZXIiLCJwcmVmZXJyZWR" +
            "fdXNlcm5hbWUiOiJ1bmtub3duX3Rva2VuIiwia2lkIjoxLCJleHAiOjE" +
            "2OTI0NTQ2NjcsImlhdCI6MTU5MjQxMTQ2N30.PpOGx3Ul5ydokOOuzKd" +
            "aRKv1kxy6Q4jGy7rBPU8PqxY";

    private NiFiProperties properties;


    private JwtService jwtService;
    private Authorizer authorizer;
    private IdpUserGroupService idpUserGroupService;

    private JwtAuthenticationProvider jwtAuthenticationProvider;

    @Before
    public void setUp() throws Exception {
        TestKeyService keyService = new TestKeyService();
        jwtService = new JwtService(keyService);
        idpUserGroupService = mock(IdpUserGroupService.class);
        authorizer = mock(Authorizer.class);

        // Set up Kerberos identity mappings
        Properties props = new Properties();
        props.put(properties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX, "^(.*?)@(.*?)$");
        props.put(properties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX, "$1");
        properties = new NiFiProperties(props);

        jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtService, properties, authorizer, idpUserGroupService);
    }

    @Test
    public void testAdminIdentityAndTokenIsValid() throws Exception {
        // Arrange
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(ADMIN_IDENTITY,
                                             EXPIRATION_MILLIS,
                                      "MockIdentityProvider");
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(token, CLIENT_ADDRESS);

        when(idpUserGroupService.getUserGroups(ADMIN_IDENTITY)).thenReturn(Collections.emptyList());

        // Act
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();

        // Assert
        assertEquals(ADMIN_IDENTITY, details.getUsername());
    }

    @Test
    public void testKerberosRealmedIdentityAndTokenIsValid() throws Exception {
        // Arrange
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(REALMED_ADMIN_KERBEROS_IDENTITY,
                        EXPIRATION_MILLIS,
                        "MockIdentityProvider");
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(token, CLIENT_ADDRESS);

        when(idpUserGroupService.getUserGroups(ADMIN_IDENTITY)).thenReturn(Collections.emptyList());

        // Act
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();

        // Assert
        // Check we now have the mapped identity
        assertEquals(ADMIN_IDENTITY, details.getUsername());
    }

    @Test
    public void testFailToAuthenticateWithUnknownToken() throws Exception {
        // Arrange
        expectedException.expect(InvalidAuthenticationException.class);
        expectedException.expectMessage("Unable to validate the access token.");

        // Generate a token with a known token
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(ADMIN_IDENTITY,
                        EXPIRATION_MILLIS,
                        "MockIdentityProvider");
        jwtService.generateSignedToken(loginAuthenticationToken);

        when(idpUserGroupService.getUserGroups(ADMIN_IDENTITY)).thenReturn(Collections.emptyList());

        // Act
        // Try to  authenticate with an unknown token
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(UNKNOWN_TOKEN, CLIENT_ADDRESS);
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);

        // Assert
        // Expect exception
    }

    @Test
    public void testIdpUserGroupsPresent() {
        // Arrange
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(ADMIN_IDENTITY,
                        EXPIRATION_MILLIS,
                        "MockIdentityProvider");
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(token, CLIENT_ADDRESS);

        final String groupName1 = "group1";
        final IdpUserGroup idpUserGroup1 = createIdpUserGroup(1, ADMIN_IDENTITY, groupName1, IdpType.SAML);

        final String groupName2 = "group2";
        final IdpUserGroup idpUserGroup2 = createIdpUserGroup(2, ADMIN_IDENTITY, groupName2, IdpType.SAML);

        when(idpUserGroupService.getUserGroups(ADMIN_IDENTITY)).thenReturn(Arrays.asList(idpUserGroup1, idpUserGroup2));

        // Act
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();

        // Assert details username is correct
        assertEquals(ADMIN_IDENTITY, details.getUsername());

        final NiFiUser returnedUser = details.getNiFiUser();
        assertNotNull(returnedUser);

        // Assert user-group-provider groups is empty
        assertNull(returnedUser.getGroups());

        // Assert identity-provider groups is correct
        assertEquals(2, returnedUser.getIdentityProviderGroups().size());
        assertTrue(returnedUser.getIdentityProviderGroups().contains(groupName1));
        assertTrue(returnedUser.getIdentityProviderGroups().contains(groupName2));

        // Assert combined groups has only idp groups
        assertEquals(2, returnedUser.getAllGroups().size());
        assertTrue(returnedUser.getAllGroups().contains(groupName1));
        assertTrue(returnedUser.getAllGroups().contains(groupName2));
    }

    @Test
    public void testCombineUserGroupProviderGroupsAndIdpUserGroups() {
        // setup IdpUserGroupService...

        final String groupName1 = "group1";
        final IdpUserGroup idpUserGroup1 = createIdpUserGroup(1, ADMIN_IDENTITY, groupName1, IdpType.SAML);

        final String groupName2 = "group2";
        final IdpUserGroup idpUserGroup2 = createIdpUserGroup(2, ADMIN_IDENTITY, groupName2, IdpType.SAML);

        idpUserGroupService = mock(IdpUserGroupService.class);
        when(idpUserGroupService.getUserGroups(ADMIN_IDENTITY)).thenReturn(Arrays.asList(idpUserGroup1, idpUserGroup2));

        // setup ManagedAuthorizer...
        final String groupName3 = "group3";
        final Group group3 = new Group.Builder().identifierGenerateRandom().name(groupName3).build();

        final UserGroupProvider userGroupProvider = mock(UserGroupProvider.class);
        when(userGroupProvider.getUserAndGroups(ADMIN_IDENTITY)).thenReturn(new UserAndGroups() {
            @Override
            public User getUser() {
                return new User.Builder().identifier(ADMIN_IDENTITY).identity(ADMIN_IDENTITY).build();
            }

            @Override
            public Set<Group> getGroups() {
                return Collections.singleton(group3);
            }
        });

        final AccessPolicyProvider accessPolicyProvider = mock(AccessPolicyProvider.class);
        when(accessPolicyProvider.getUserGroupProvider()).thenReturn(userGroupProvider);

        final ManagedAuthorizer managedAuthorizer = mock(ManagedAuthorizer.class);
        when(managedAuthorizer.getAccessPolicyProvider()).thenReturn(accessPolicyProvider);

        jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtService, properties, managedAuthorizer, idpUserGroupService);

        // Arrange
        LoginAuthenticationToken loginAuthenticationToken =
                new LoginAuthenticationToken(ADMIN_IDENTITY,
                        EXPIRATION_MILLIS,
                        "MockIdentityProvider");
        String token = jwtService.generateSignedToken(loginAuthenticationToken);
        final JwtAuthenticationRequestToken request = new JwtAuthenticationRequestToken(token, CLIENT_ADDRESS);

        // Act
        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) jwtAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();

        // Assert details username is correct
        assertEquals(ADMIN_IDENTITY, details.getUsername());

        final NiFiUser returnedUser = details.getNiFiUser();
        assertNotNull(returnedUser);

        // Assert user-group-provider groups are correct
        assertEquals(1, returnedUser.getGroups().size());
        assertTrue(returnedUser.getGroups().contains(groupName3));

        // Assert identity-provider groups are correct
        assertEquals(2, returnedUser.getIdentityProviderGroups().size());
        assertTrue(returnedUser.getIdentityProviderGroups().contains(groupName1));
        assertTrue(returnedUser.getIdentityProviderGroups().contains(groupName2));

        // Assert combined groups are correct
        assertEquals(3, returnedUser.getAllGroups().size());
        assertTrue(returnedUser.getAllGroups().contains(groupName1));
        assertTrue(returnedUser.getAllGroups().contains(groupName2));
        assertTrue(returnedUser.getAllGroups().contains(groupName3));
    }

    private IdpUserGroup createIdpUserGroup(int id, String identity, String groupName, IdpType idpType) {
        final IdpUserGroup userGroup = new IdpUserGroup();
        userGroup.setId(id);
        userGroup.setIdentity(identity);
        userGroup.setGroupName(groupName);
        userGroup.setType(idpType);
        return userGroup;
    }

}