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
package org.apache.nifi.admin.service.action;

import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountDisabledException;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AccountPendingException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 */
public class AuthorizeUserActionTest {

    private static final String USER_ID_6 = "6";
    private static final String USER_ID_7 = "7";
    private static final String USER_ID_8 = "8";
    private static final String USER_ID_9 = "9";
    private static final String USER_ID_10 = "10";
    private static final String USER_ID_11 = "11";

    private static final String USER_IDENTITY_1 = "authority access exception while searching for user";
    private static final String USER_IDENTITY_2 = "unknown user";
    private static final String USER_IDENTITY_3 = "user removed after checking existence";
    private static final String USER_IDENTITY_4 = "access exception getting authorities";
    private static final String USER_IDENTITY_5 = "error creating user account";
    private static final String USER_IDENTITY_6 = "create user general sequence";
    private static final String USER_IDENTITY_7 = "existing user requires verification";
    private static final String USER_IDENTITY_8 = "existing user does not require verification";
    private static final String USER_IDENTITY_9 = "existing pending user";
    private static final String USER_IDENTITY_10 = "existing disabled user";
    private static final String USER_IDENTITY_11 = "existing user is now unknown in the authority provider";

    private DAOFactory daoFactory;
    private UserDAO userDao;
    private AuthorityDAO authorityDao;
    private AuthorityProvider authorityProvider;

    @Before
    public void setup() throws Exception {
        // mock the user dao
        userDao = Mockito.mock(UserDAO.class);
        Mockito.doAnswer(new Answer<NiFiUser>() {
            @Override
            public NiFiUser answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String id = (String) args[0];

                NiFiUser user = null;
                if (USER_ID_7.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_7);
                    user.setIdentity(USER_IDENTITY_7);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                } else if (USER_ID_8.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_8);
                    user.setIdentity(USER_IDENTITY_8);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                    user.setLastVerified(new Date());
                } else if (USER_ID_11.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_11);
                    user.setIdentity(USER_IDENTITY_11);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                    user.setStatus(AccountStatus.ACTIVE);
                }

                return user;
            }
        }).when(userDao).findUserById(Mockito.anyString());
        Mockito.doAnswer(new Answer<NiFiUser>() {
            @Override
            public NiFiUser answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];

                NiFiUser user = null;
                switch (dn) {
                    case USER_IDENTITY_7:
                        user = new NiFiUser();
                        user.setId(USER_ID_7);
                        user.setIdentity(USER_IDENTITY_7);
                        user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                        break;
                    case USER_IDENTITY_8:
                        user = new NiFiUser();
                        user.setId(USER_ID_8);
                        user.setIdentity(USER_IDENTITY_8);
                        user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                        user.setLastVerified(new Date());
                        break;
                    case USER_IDENTITY_9:
                        user = new NiFiUser();
                        user.setId(USER_ID_9);
                        user.setIdentity(USER_IDENTITY_9);
                        user.setStatus(AccountStatus.PENDING);
                        break;
                    case USER_IDENTITY_10:
                        user = new NiFiUser();
                        user.setId(USER_ID_10);
                        user.setIdentity(USER_IDENTITY_10);
                        user.setStatus(AccountStatus.DISABLED);
                        break;
                    case USER_IDENTITY_11:
                        user = new NiFiUser();
                        user.setId(USER_ID_11);
                        user.setIdentity(USER_IDENTITY_11);
                        user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                        user.setStatus(AccountStatus.ACTIVE);
                        break;
                }

                return user;
            }
        }).when(userDao).findUserByDn(Mockito.anyString());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                NiFiUser user = (NiFiUser) args[0];
                switch (user.getIdentity()) {
                    case USER_IDENTITY_5:
                        throw new DataAccessException();
                    case USER_IDENTITY_6:
                        user.setId(USER_ID_6);
                        break;
                }

                // do nothing
                return null;
            }
        }).when(userDao).createUser(Mockito.any(NiFiUser.class));
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                NiFiUser user = (NiFiUser) args[0];

                // do nothing
                return null;
            }
        }).when(userDao).updateUser(Mockito.any(NiFiUser.class));

        // mock the authority dao
        authorityDao = Mockito.mock(AuthorityDAO.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Set<Authority> authorities = (Set<Authority>) args[0];
                String id = (String) args[1];

                // do nothing
                return null;
            }
        }).when(authorityDao).createAuthorities(Mockito.anySetOf(Authority.class), Mockito.anyString());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Set<Authority> authorities = (Set<Authority>) args[0];
                String id = (String) args[1];

                // do nothing
                return null;
            }
        }).when(authorityDao).deleteAuthorities(Mockito.anySetOf(Authority.class), Mockito.anyString());

        // mock the dao factory
        daoFactory = Mockito.mock(DAOFactory.class);
        Mockito.when(daoFactory.getUserDAO()).thenReturn(userDao);
        Mockito.when(daoFactory.getAuthorityDAO()).thenReturn(authorityDao);

        // mock the authority provider
        authorityProvider = Mockito.mock(AuthorityProvider.class);
        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];
                switch (dn) {
                    case USER_IDENTITY_1:
                        throw new AuthorityAccessException(StringUtils.EMPTY);
                    case USER_IDENTITY_2:
                        return false;
                }

                return true;
            }
        }).when(authorityProvider).doesDnExist(Mockito.anyString());
        Mockito.doAnswer(new Answer<Set<Authority>>() {
            @Override
            public Set<Authority> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];
                Set<Authority> authorities = EnumSet.noneOf(Authority.class);
                switch (dn) {
                    case USER_IDENTITY_3:
                        throw new UnknownIdentityException(StringUtils.EMPTY);
                    case USER_IDENTITY_4:
                        throw new AuthorityAccessException(StringUtils.EMPTY);
                    case USER_IDENTITY_6:
                        authorities.add(Authority.ROLE_MONITOR);
                        break;
                    case USER_IDENTITY_7:
                        authorities.add(Authority.ROLE_DFM);
                        break;
                    case USER_IDENTITY_9:
                        throw new UnknownIdentityException(StringUtils.EMPTY);
                    case USER_IDENTITY_10:
                        throw new UnknownIdentityException(StringUtils.EMPTY);
                    case USER_IDENTITY_11:
                        throw new UnknownIdentityException(StringUtils.EMPTY);
                }

                return authorities;
            }
        }).when(authorityProvider).getAuthorities(Mockito.anyString());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];
                Set<Authority> authorites = (Set<Authority>) args[1];

                // do nothing
                return null;
            }
        }).when(authorityProvider).setAuthorities(Mockito.anyString(), Mockito.anySet());
    }

    /**
     * Tests AuthorityAccessException in doesDnExist.
     *
     * @throws Exception ex
     */
    @Test(expected = AdministrationException.class)
    public void testAuthorityAccessExceptionInDoesDnExist() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_1, 0);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Test unknown user in the authority provider.
     *
     * @throws Exception ex
     */
    @Test(expected = AccountNotFoundException.class)
    public void testUnknownUser() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_2, 0);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Test a user thats been removed after checking their existence.
     *
     * @throws Exception ex
     */
    @Test(expected = AccountNotFoundException.class)
    public void testUserRemovedAfterCheckingExistence() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_3, 0);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Testing AuthorityAccessException when getting authorities.
     *
     * @throws Exception ex
     */
    @Test(expected = AdministrationException.class)
    public void testAuthorityAccessException() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_4, 0);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Testing DataAccessException while creating user accounts.
     *
     * @throws Exception ex
     */
    @Test(expected = DataAccessException.class)
    public void testErrorCreatingUserAccount() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_5, 0);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests the general case when a user account is created.
     *
     * @throws Exception ex
     */
    @Test
    public void testAccountCreation() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_6, 0);
        NiFiUser user = authorizeUser.execute(daoFactory, authorityProvider);

        // verify the user
        Assert.assertEquals(USER_IDENTITY_6, user.getIdentity());
        Assert.assertEquals(1, user.getAuthorities().size());
        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_MONITOR));

        // verify interaction with dao and provider
        Mockito.verify(userDao, Mockito.times(1)).createUser(user);
    }

    /**
     * Tests the general case when there is an existing user account that
     * requires verification.
     *
     * @throws Exception ex
     */
    @Test
    public void testExistingUserRequiresVerification() throws Exception {
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_7, 0);
        NiFiUser user = authorizeUser.execute(daoFactory, authorityProvider);

        // verify the user
        Assert.assertEquals(USER_IDENTITY_7, user.getIdentity());
        Assert.assertEquals(1, user.getAuthorities().size());
        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_DFM));

        // verify interaction with dao and provider
        Mockito.verify(userDao, Mockito.times(1)).updateUser(user);
        Mockito.verify(authorityDao, Mockito.times(1)).createAuthorities(EnumSet.of(Authority.ROLE_DFM), USER_ID_7);
    }

    /**
     * Tests the general case when there is an existing user account that does
     * not require verification.
     *
     * @throws Exception ex
     */
    @Test
    public void testExistingUserNoVerification() throws Exception {
        // disabling verification by passing in a large cache duration
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_8, Integer.MAX_VALUE);
        NiFiUser user = authorizeUser.execute(daoFactory, authorityProvider);

        // verify the user
        Assert.assertEquals(USER_IDENTITY_8, user.getIdentity());
        Assert.assertEquals(1, user.getAuthorities().size());
        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_MONITOR));

        // verify interaction with dao and provider
        Mockito.verify(userDao, Mockito.times(1)).updateUser(user);
        Mockito.verify(authorityDao, Mockito.never()).createAuthorities(Mockito.anySet(), Mockito.eq(USER_ID_8));
        Mockito.verify(authorityDao, Mockito.never()).deleteAuthorities(Mockito.anySet(), Mockito.eq(USER_ID_8));
    }

    /**
     * Tests existing users whose accounts are in a pending status.
     *
     * @throws Exception ex
     */
    @Test(expected = AccountPendingException.class)
    public void testExistingPendingUser() throws Exception {
        // disabling verification by passing in a large cache duration
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_9, Integer.MAX_VALUE);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests existing users whose accounts are in a disabled status.
     *
     * @throws Exception ex
     */
    @Test(expected = AccountDisabledException.class)
    public void testExistingDisabledUser() throws Exception {
        // disabling verification by passing in a large cache duration
        AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_10, Integer.MAX_VALUE);
        authorizeUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests the general case where there is an active user that has been
     * removed from the authority provider.
     *
     * @throws Exception ex
     */
    @Test
    public void testExistingActiveUserNotFoundInProvider() throws Exception {
        try {
            AuthorizeUserAction authorizeUser = new AuthorizeUserAction(USER_IDENTITY_11, 0);
            authorizeUser.execute(daoFactory, authorityProvider);

            Assert.fail();
        } catch (AccountDisabledException ade) {
            ArgumentCaptor<NiFiUser> user = ArgumentCaptor.forClass(NiFiUser.class);

            // verify interaction with dao
            Mockito.verify(userDao, Mockito.times(1)).updateUser(user.capture());

            // verify user
            Assert.assertEquals(AccountStatus.DISABLED, user.getValue().getStatus());
        }
    }
}
