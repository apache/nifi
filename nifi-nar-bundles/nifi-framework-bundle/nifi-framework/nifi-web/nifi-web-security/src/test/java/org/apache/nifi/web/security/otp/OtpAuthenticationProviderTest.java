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
package org.apache.nifi.web.security.otp;

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
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OtpAuthenticationProviderTest {

    private final static String UI_EXTENSION_AUTHENTICATED_USER = "ui-extension-token-authenticated-user";
    private final static String UI_EXTENSION_TOKEN = "ui-extension-token";

    private final static String DOWNLOAD_AUTHENTICATED_USER = "download-token-authenticated-user";
    private final static String DOWNLOAD_TOKEN = "download-token";

    private OtpService otpService;
    private OtpAuthenticationProvider otpAuthenticationProvider;
    private NiFiProperties nifiProperties;
    private IdpUserGroupService idpUserGroupService;

    @Before
    public void setUp() throws Exception {
        otpService = mock(OtpService.class);
        doAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String downloadToken = (String) args[0];

                if (DOWNLOAD_TOKEN.equals(downloadToken)) {
                    return DOWNLOAD_AUTHENTICATED_USER;
                }

                throw new OtpAuthenticationException("Invalid token");
            }
        }).when(otpService).getAuthenticationFromDownloadToken(anyString());
        doAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String uiExtensionToken = (String) args[0];

                if (UI_EXTENSION_TOKEN.equals(uiExtensionToken)) {
                    return UI_EXTENSION_AUTHENTICATED_USER;
                }

                throw new OtpAuthenticationException("Invalid token");
            }
        }).when(otpService).getAuthenticationFromUiExtensionToken(anyString());

        idpUserGroupService = mock(IdpUserGroupService.class);

        otpAuthenticationProvider = new OtpAuthenticationProvider(
                otpService, mock(NiFiProperties.class), mock(Authorizer.class), idpUserGroupService);
    }

    @Test
    public void testUiExtensionPath() throws Exception {
        when(idpUserGroupService.getUserGroups(anyString())).thenReturn(Collections.emptyList());

        final OtpAuthenticationRequestToken request = new OtpAuthenticationRequestToken(UI_EXTENSION_TOKEN, false, null);

        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) otpAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();
        assertEquals(UI_EXTENSION_AUTHENTICATED_USER, details.getUsername());
        assertNotNull(details.getNiFiUser());

        assertNull(details.getNiFiUser().getGroups());

        assertNotNull(details.getNiFiUser().getIdentityProviderGroups());
        assertEquals(0, details.getNiFiUser().getIdentityProviderGroups().size());

        assertNotNull(details.getNiFiUser().getAllGroups());
        assertEquals(0, details.getNiFiUser().getAllGroups().size());

        verify(otpService, times(1)).getAuthenticationFromUiExtensionToken(UI_EXTENSION_TOKEN);
        verify(otpService, never()).getAuthenticationFromDownloadToken(anyString());
    }

    @Test
    public void testDownload() throws Exception {
        when(idpUserGroupService.getUserGroups(anyString())).thenReturn(Collections.emptyList());

        final OtpAuthenticationRequestToken request = new OtpAuthenticationRequestToken(DOWNLOAD_TOKEN, true, null);

        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) otpAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, details.getUsername());
        assertNotNull(details.getNiFiUser());

        assertNull(details.getNiFiUser().getGroups());

        assertNotNull(details.getNiFiUser().getIdentityProviderGroups());
        assertEquals(0, details.getNiFiUser().getIdentityProviderGroups().size());

        assertNotNull(details.getNiFiUser().getAllGroups());
        assertEquals(0, details.getNiFiUser().getAllGroups().size());

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

    @Test
    public void testWhenIdpUserGroupsArePresent() {
        final String groupName1 = "group1";
        final IdpUserGroup idpUserGroup1 = createIdpUserGroup(1, DOWNLOAD_AUTHENTICATED_USER, groupName1, IdpType.SAML);

        final String groupName2 = "group2";
        final IdpUserGroup idpUserGroup2 = createIdpUserGroup(2, DOWNLOAD_AUTHENTICATED_USER, groupName2, IdpType.SAML);

        when(idpUserGroupService.getUserGroups(DOWNLOAD_AUTHENTICATED_USER)).thenReturn(Arrays.asList(idpUserGroup1, idpUserGroup2));

        final OtpAuthenticationRequestToken request = new OtpAuthenticationRequestToken(DOWNLOAD_TOKEN, true, null);

        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) otpAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, details.getUsername());

        final NiFiUser returnedUser = details.getNiFiUser();
        assertNotNull(returnedUser);

        // user-group-provider groups are null
        assertNull(returnedUser.getGroups());

        // identity-provider group contain the two groups
        assertNotNull(returnedUser.getIdentityProviderGroups());
        assertEquals(2, returnedUser.getIdentityProviderGroups().size());
        assertTrue(returnedUser.getIdentityProviderGroups().contains(groupName1));
        assertTrue(returnedUser.getIdentityProviderGroups().contains(groupName1));

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
    }

    @Test
    public void testWhenUserGroupProviderGroupsAndIdpUserGroupsArePresent() {
        // setup idp user group service...

        final String groupName1 = "group1";
        final IdpUserGroup idpUserGroup1 = createIdpUserGroup(1, DOWNLOAD_AUTHENTICATED_USER, groupName1, IdpType.SAML);

        final String groupName2 = "group2";
        final IdpUserGroup idpUserGroup2 = createIdpUserGroup(2, DOWNLOAD_AUTHENTICATED_USER, groupName2, IdpType.SAML);

        when(idpUserGroupService.getUserGroups(DOWNLOAD_AUTHENTICATED_USER)).thenReturn(Arrays.asList(idpUserGroup1, idpUserGroup2));

        // setup managed authorizer...

        final String groupName3 = "group3";
        final Group group3 = new Group.Builder().identifierGenerateRandom().name(groupName3).build();

        final UserGroupProvider userGroupProvider = mock(UserGroupProvider.class);
        when(userGroupProvider.getUserAndGroups(DOWNLOAD_AUTHENTICATED_USER)).thenReturn(new UserAndGroups() {
            @Override
            public User getUser() {
                return new User.Builder().identifier(DOWNLOAD_AUTHENTICATED_USER).identity(DOWNLOAD_AUTHENTICATED_USER).build();
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

        // create OTP auth provider using mocks from above

        otpAuthenticationProvider = new OtpAuthenticationProvider(
                otpService, mock(NiFiProperties.class), managedAuthorizer, idpUserGroupService);

        // test...

        final OtpAuthenticationRequestToken request = new OtpAuthenticationRequestToken(DOWNLOAD_TOKEN, true, null);

        final NiFiAuthenticationToken result = (NiFiAuthenticationToken) otpAuthenticationProvider.authenticate(request);
        final NiFiUserDetails details = (NiFiUserDetails) result.getPrincipal();
        assertEquals(DOWNLOAD_AUTHENTICATED_USER, details.getUsername());

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

        verify(otpService, never()).getAuthenticationFromUiExtensionToken(anyString());
        verify(otpService, times(1)).getAuthenticationFromDownloadToken(DOWNLOAD_TOKEN);
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
