///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.nifi.web.security.authorization;
//
//import org.apache.nifi.admin.service.AccountDisabledException;
//import org.apache.nifi.admin.service.AccountNotFoundException;
//import org.apache.nifi.admin.service.AccountPendingException;
//import org.apache.nifi.admin.service.AdministrationException;
//import org.apache.nifi.admin.service.UserService;
//import org.apache.nifi.authorization.Authority;
//import org.apache.nifi.user.NiFiUser;
//import org.apache.nifi.util.NiFiProperties;
//import org.apache.nifi.web.security.ProxiedEntitiesUtils;
//import org.apache.nifi.web.security.UntrustedProxyException;
//import org.apache.nifi.web.security.user.NiFiUserDetails;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mockito;
//import org.mockito.invocation.InvocationOnMock;
//import org.mockito.stubbing.Answer;
//import org.springframework.security.authentication.AccountStatusException;
//import org.springframework.security.authentication.AuthenticationServiceException;
//import org.springframework.security.core.userdetails.UsernameNotFoundException;
//
///**
// * Test case for NiFiAuthorizationService.
// */
//public class NiFiAuthorizationServiceTest {
//
//    private static final String USER = "user";
//    private static final String PROXY = "proxy";
//    private static final String PROXY_PROXY = "proxy-proxy";
//    private static final String USER_NOT_FOUND = "user-not-found";
//    private static final String USER_DISABLED = "user-disabled";
//    private static final String USER_PENDING = "user-pending";
//    private static final String USER_ADMIN_EXCEPTION = "user-admin-exception";
//    private static final String PROXY_NOT_FOUND = "proxy-not-found";
//
//    private NiFiAuthorizationService authorizationService;
//    private UserService userService;
//
//    @Before
//    public void setup() throws Exception {
//        // mock the web security properties
//        final NiFiProperties properties = Mockito.mock(NiFiProperties.class);
//        Mockito.when(properties.getSupportNewAccountRequests()).thenReturn(Boolean.TRUE);
//
//        userService = Mockito.mock(UserService.class);
//        Mockito.doReturn(null).when(userService).createPendingUserAccount(Mockito.anyString(), Mockito.anyString());
//        Mockito.doAnswer(new Answer() {
//            @Override
//            public Object answer(InvocationOnMock invocation) throws Throwable {
//                Object[] args = invocation.getArguments();
//                String dn = (String) args[0];
//
//                if (null != dn) {
//                    switch (dn) {
//                        case USER_NOT_FOUND:
//                        case PROXY_NOT_FOUND:
//                            throw new AccountNotFoundException("");
//                        case USER_DISABLED:
//                            throw new AccountDisabledException("");
//                        case USER_PENDING:
//                            throw new AccountPendingException("");
//                        case USER_ADMIN_EXCEPTION:
//                            throw new AdministrationException();
//                        case USER:
//                            final NiFiUser monitor = new NiFiUser();
//                            monitor.setDn(dn);
//                            monitor.getAuthorities().add(Authority.ROLE_MONITOR);
//                            return monitor;
//                        case PROXY:
//                        case PROXY_PROXY:
//                            final NiFiUser proxy = new NiFiUser();
//                            proxy.setDn(dn);
//                            proxy.getAuthorities().add(Authority.ROLE_PROXY);
//                            return proxy;
//                    }
//                }
//
//                return null;
//            }
//        }).when(userService).checkAuthorization(Mockito.anyString());
//
//        // create the authorization service
//        authorizationService = new NiFiAuthorizationService();
//        authorizationService.setProperties(properties);
//        authorizationService.setUserService(userService);
//    }
//
//    /**
//     * Ensures the authorization service correctly handles users invalid dn
//     * chain.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = UntrustedProxyException.class)
//    public void testInvalidDnChain() throws Exception {
//        authorizationService.loadUserByUsername(USER);
//    }
//
//    /**
//     * Ensures the authorization service correctly handles account not found.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = UsernameNotFoundException.class)
//    public void testAccountNotFound() throws Exception {
//        authorizationService.loadUserByUsername(ProxiedEntitiesUtils.formatProxyDn(USER_NOT_FOUND));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles account disabled.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = AccountStatusException.class)
//    public void testAccountDisabled() throws Exception {
//        authorizationService.loadUserByUsername(ProxiedEntitiesUtils.formatProxyDn(USER_DISABLED));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles account pending.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = AccountStatusException.class)
//    public void testAccountPending() throws Exception {
//        authorizationService.loadUserByUsername(ProxiedEntitiesUtils.formatProxyDn(USER_PENDING));
//    }
//
//    /**
//     * Ensures the authorization service correctly handles account
//     * administration exception.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = AuthenticationServiceException.class)
//    public void testAccountAdminException() throws Exception {
//        authorizationService.loadUserByUsername(ProxiedEntitiesUtils.formatProxyDn(USER_ADMIN_EXCEPTION));
//    }
//
//    /**
//     * Tests the case when there is no proxy.
//     *
//     * @throws Exception ex
//     */
//    @Test
//    public void testNoProxy() throws Exception {
//        final NiFiUserDetails details = (NiFiUserDetails) authorizationService.loadUserByUsername(ProxiedEntitiesUtils.formatProxyDn(USER));
//        final NiFiUser user = details.getNiFiUser();
//
//        Assert.assertEquals(USER, user.getDn());
//        Assert.assertNull(user.getChain());
//    }
//
//    /**
//     * Tests the case when the proxy does not have ROLE_PROXY.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = UntrustedProxyException.class)
//    public void testInvalidProxy() throws Exception {
//        final String dnChain = ProxiedEntitiesUtils.formatProxyDn(USER) + ProxiedEntitiesUtils.formatProxyDn(USER);
//        authorizationService.loadUserByUsername(dnChain);
//    }
//
//    /**
//     * Ensures the authorization service correctly handles proxy not found by
//     * attempting to create an account request for the proxy.
//     *
//     * @throws Exception ex
//     */
//    @Test(expected = UsernameNotFoundException.class)
//    public void testProxyNotFound() throws Exception {
//        try {
//            final String dnChain = ProxiedEntitiesUtils.formatProxyDn(USER) + ProxiedEntitiesUtils.formatProxyDn(PROXY_NOT_FOUND);
//            authorizationService.loadUserByUsername(ProxiedEntitiesUtils.formatProxyDn(dnChain));
//        } finally {
//            Mockito.verify(userService).createPendingUserAccount(Mockito.eq(PROXY_NOT_FOUND), Mockito.anyString());
//        }
//    }
//
//    /**
//     * Tests the case when there is a proxy.
//     *
//     * @throws Exception ex
//     */
//    @Test
//    public void testProxy() throws Exception {
//        final String dnChain = ProxiedEntitiesUtils.formatProxyDn(USER) + ProxiedEntitiesUtils.formatProxyDn(PROXY);
//        final NiFiUserDetails details = (NiFiUserDetails) authorizationService.loadUserByUsername(dnChain);
//        final NiFiUser user = details.getNiFiUser();
//
//        // verify the user
//        Assert.assertEquals(USER, user.getDn());
//        Assert.assertNotNull(user.getChain());
//
//        // get the proxy
//        final NiFiUser proxy = user.getChain();
//
//        // verify the proxy
//        Assert.assertEquals(PROXY, proxy.getDn());
//        Assert.assertNull(proxy.getChain());
//    }
//
//    /**
//     * Tests the case when there is are multiple proxies.
//     *
//     * @throws Exception ex
//     */
//    @Test
//    public void testProxyProxy() throws Exception {
//        final String dnChain = ProxiedEntitiesUtils.formatProxyDn(USER) + ProxiedEntitiesUtils.formatProxyDn(PROXY) + ProxiedEntitiesUtils.formatProxyDn(PROXY_PROXY);
//        final NiFiUserDetails details = (NiFiUserDetails) authorizationService.loadUserByUsername(dnChain);
//        final NiFiUser user = details.getNiFiUser();
//
//        // verify the user
//        Assert.assertEquals(USER, user.getDn());
//        Assert.assertNotNull(user.getChain());
//
//        // get the proxy
//        NiFiUser proxy = user.getChain();
//
//        // verify the proxy
//        Assert.assertEquals(PROXY, proxy.getDn());
//        Assert.assertNotNull(proxy.getChain());
//
//        // get the proxies proxy
//        proxy = proxy.getChain();
//
//        // verify the proxies proxy
//        Assert.assertEquals(PROXY_PROXY, proxy.getDn());
//        Assert.assertNull(proxy.getChain());
//    }
//}
