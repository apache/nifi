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
package org.apache.nifi.admin.service.impl;

import org.junit.Ignore;

/**
 *
 */
@Ignore
public class NiFiAuthorizationServiceTest {

//    private static final String UNKNOWN_USER_IN_CACHE_DN = "unknown-user-in-cache-dn";
//    private static final String PENDING_USER_DN = "pending-user-dn";
//    private static final String DISABLED_USER_DN = "disabled-user-dn";
//    private static final String UNKNOWN_USER_IN_IDENTITY_PROVIDER_DN = "unknown-user-in-identity-provider-dn";
//    private static final String ACCESS_EXCEPTION_IN_IDENTITY_PROVIDER_DN = "access-exception-in-identity-provider-dn";
//    private static final String UNABLE_TO_UPDATE_CACHE_DN = "unable-to-update-cache-dn";
//    private static final String VERIFICATION_REQUIRED_DN = "verification-required-dn";
//    private static final String VERIFICATION_NOT_REQUIRED_DN = "verification-not-required-dn";
//    private static final String NEW_USER_DN = "new-user-dn";
//    
//    private UserService userService;
//    private AuthorityProvider authorityProvider;
//    private UserDAO userDAO;
//
//    @Before
//    public void setup() throws Exception {
//        // mock the web security properties
//        NiFiProperties properties = Mockito.mock(NiFiProperties.class);
//        Mockito.when(properties.getSupportNewAccountRequests()).thenReturn(Boolean.TRUE);
//        Mockito.when(properties.getUserCredentialCacheDurationSeconds()).thenReturn(60);
//
//        // mock the authority provider
//
//        // mock the admin service
//        userDAO = Mockito.mock(UserDAO.class);
//        Mockito.doAnswer(new Answer() {
//
//            @Override
//            public Object answer(InvocationOnMock invocation) throws Throwable {
//                Object[] args = invocation.getArguments();
//                String dn = (String) args[0];
//
//                NiFiUser user = null;
//                switch (dn) {
//                    case PENDING_USER_DN:
//                        user = new NiFiUser();
//                        user.setDn(dn);
//                        user.setStatus(AccountStatus.PENDING);
//                        break;
//                    case DISABLED_USER_DN:
//                        user = new NiFiUser();
//                        user.setDn(dn);
//                        user.setStatus(AccountStatus.DISABLED);
//                        break;
//                    case UNKNOWN_USER_IN_IDENTITY_PROVIDER_DN:
//                    case UNABLE_TO_UPDATE_CACHE_DN:
//                    case ACCESS_EXCEPTION_IN_IDENTITY_PROVIDER_DN:
//                        user = new NiFiUser();
//                        user.setDn(dn);
//                        user.setStatus(AccountStatus.ACTIVE);
//                        break;
//                    case VERIFICATION_REQUIRED_DN: {
//                        Calendar calendar = Calendar.getInstance();
//                        calendar.add(Calendar.SECOND, -65);
//                        user = new NiFiUser();
//                        user.setDn(dn);
//                        user.setStatus(AccountStatus.ACTIVE);
//                        user.setLastVerified(calendar.getTime());
//                        user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_ADMIN, Authority.ROLE_DFM));
//                        break;
//                    }
//                    case VERIFICATION_NOT_REQUIRED_DN: {
//                        Calendar calendar = Calendar.getInstance();
//                        calendar.add(Calendar.SECOND, -5);
//                        user = new NiFiUser();
//                        user.setDn(dn);
//                        user.setStatus(AccountStatus.ACTIVE);
//                        user.setLastVerified(calendar.getTime());
//                        user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_ADMIN, Authority.ROLE_DFM));
//                        break;
//                    }
//                }
//                return user;
//            }
//        }).when(userDAO).getUser(Mockito.anyString());
//        Mockito.doAnswer(new Answer() {
//
//            @Override
//            public Object answer(InvocationOnMock invocation) throws Throwable {
//                Object[] args = invocation.getArguments();
//                NiFiUser user = (NiFiUser) args[0];
//
//                if (UNABLE_TO_UPDATE_CACHE_DN.equals(user.getDn())) {
//                    throw new AdministrationException();
//                }
//                return user;
//            }
//        }).when(userDAO).updateUser(Mockito.any(NiFiUser.class));
//        Mockito.doNothing().when(userDAO).createUser(Mockito.any(NiFiUser.class));
//
//        // mock the authority provider
//        authorityProvider = Mockito.mock(AuthorityProvider.class);
//        Mockito.doAnswer(new Answer() {
//
//            @Override
//            public Object answer(InvocationOnMock invocation) throws Throwable {
//                Object[] args = invocation.getArguments();
//                String dn = (String) args[0];
//
//                boolean hasDn = false;
//                if (VERIFICATION_REQUIRED_DN.equals(dn) || NEW_USER_DN.equals(dn)) {
//                    hasDn = true;
//                }
//                return hasDn;
//            }
//        }).when(authorityProvider).doesDnExist(Mockito.anyString());
//        Mockito.doAnswer(new Answer() {
//
//            @Override
//            public Object answer(InvocationOnMock invocation) throws Throwable {
//                Object[] args = invocation.getArguments();
//                String dn = (String) args[0];
//
//                Set<String> authorities = null;
//                switch (dn) {
//                    case VERIFICATION_REQUIRED_DN:
//                    case NEW_USER_DN:
//                        authorities = new HashSet<>();
//                        authorities.add("ROLE_MONITOR");
//                        break;
//                    case DISABLED_USER_DN:
//                        throw new UnknownIdentityException("Unable to find user");
//                }
//                return authorities;
//            }
//        }).when(authorityProvider).getAuthorities(Mockito.anyString());
//
//        // create an instance of the authorization service
//        userService = new UserServiceImpl();
//        ((UserServiceImpl) userService).setAuthorityProvider(authorityProvider);
//        ((UserServiceImpl) userService).set(authorityProvider);
//        
////        authorizationService.setIdentityProvider(identityProvider);
////        authorizationService.setAuthorityProvider(authorityProvider);
////        authorizationService.setProperties(properties);
//    }
//
//    /**
//     * Ensures the authorization service correctly handles users who are
//     * unknown.
//     *
//     * @throws Exception
//     */
//    @Test(expected = org.springframework.security.core.userdetails.UsernameNotFoundException.class)
//    public void testUnknownUserInCache() throws Exception {
//        authorizationService.loadUserByUsername(WebUtils.formatProxyDn(UNKNOWN_USER_IN_CACHE_DN));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles users whose accounts
//     * are PENDING.
//     *
//     * @throws Exception
//     */
//    @Test(expected = nifi.admin.service.AccountPendingException.class)
//    public void testPendingUser() throws Exception {
//        authorizationService.loadUserByUsername(WebUtils.formatProxyDn(PENDING_USER_DN));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles users whose accounts
//     * are DISABLED.
//     *
//     * @throws Exception
//     */
//    @Test(expected = org.springframework.security.authentication.DisabledException.class)
//    public void testDisabledUser() throws Exception {
//        authorizationService.loadUserByUsername(WebUtils.formatProxyDn(DISABLED_USER_DN));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles users whose are in
//     * the cache but have been removed from the identity provider.
//     *
//     * @throws Exception
//     */
//    @Test(expected = org.springframework.security.authentication.DisabledException.class)
//    public void testUnknownUserInIdentityProvider() throws Exception {
//        authorizationService.loadUserByUsername(WebUtils.formatProxyDn(UNKNOWN_USER_IN_IDENTITY_PROVIDER_DN));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles cases when the cache
//     * is unable to be updated.
//     *
//     * @throws Exception
//     */
//    @Test(expected = org.springframework.security.authentication.AuthenticationServiceException.class)
//    public void testUnableToUpdateCache() throws Exception {
//        authorizationService.loadUserByUsername(WebUtils.formatProxyDn(UNABLE_TO_UPDATE_CACHE_DN));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles cases when the
//     * identity provider has an access exception.
//     *
//     * @throws Exception
//     */
//    @Test(expected = org.springframework.security.authentication.AuthenticationServiceException.class)
//    public void testUnableToAccessIdentity() throws Exception {
//        authorizationService.loadUserByUsername(WebUtils.formatProxyDn(ACCESS_EXCEPTION_IN_IDENTITY_PROVIDER_DN));
//    }
//
//    /**
//     * Ensures that user authorities are properly loaded from the authority
//     * provider.
//     *
//     * @throws Exception
//     */
//    @Test
//    public void testVerificationRequiredUser() throws Exception {
//        NiFiUserDetails userDetails = (NiFiUserDetails) authorizationService.loadUserByUsername(WebUtils.formatProxyDn(VERIFICATION_REQUIRED_DN));
//        NiFiUser user = userDetails.getNiFiUser();
//        Mockito.verify(authorityProvider).getAuthorities(VERIFICATION_REQUIRED_DN);
//
//        // ensure the user details
//        Assert.assertEquals(VERIFICATION_REQUIRED_DN, user.getDn());
//        Assert.assertEquals(1, user.getAuthorities().size());
//        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_MONITOR));
//    }
//
//    /**
//     * Ensures that user authorities are not loaded when the cache is still
//     * valid.
//     *
//     * @throws Exception
//     */
//    @Test
//    public void testVerificationNotRequiredUser() throws Exception {
//        NiFiUserDetails userDetails = (NiFiUserDetails) authorizationService.loadUserByUsername(WebUtils.formatProxyDn(VERIFICATION_NOT_REQUIRED_DN));
//        NiFiUser user = userDetails.getNiFiUser();
//        Mockito.verify(authorityProvider, Mockito.never()).getAuthorities(VERIFICATION_NOT_REQUIRED_DN);
//
//        // ensure the user details
//        Assert.assertEquals(VERIFICATION_NOT_REQUIRED_DN, user.getDn());
//        Assert.assertEquals(2, user.getAuthorities().size());
//        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_ADMIN));
//        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_DFM));
//    }
//
//    /**
//     * Ensures that new users are automatically created when the authority
//     * provider has their authorities.
//     *
//     * @throws Exception
//     */
//    @Test
//    public void testNewUser() throws Exception {
//        NiFiUserDetails userDetails = (NiFiUserDetails) authorizationService.loadUserByUsername(WebUtils.formatProxyDn(NEW_USER_DN));
//        NiFiUser user = userDetails.getNiFiUser();
//        Mockito.verify(authorityProvider).getAuthorities(NEW_USER_DN);
//
//        // ensure the user details
//        Assert.assertEquals(NEW_USER_DN, user.getDn());
//        Assert.assertEquals(1, user.getAuthorities().size());
//        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_MONITOR));
//    }
}
