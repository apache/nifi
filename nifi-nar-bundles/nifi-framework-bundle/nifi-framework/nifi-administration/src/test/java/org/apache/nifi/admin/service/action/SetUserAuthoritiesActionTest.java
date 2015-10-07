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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test case for SetUserAuthoritiesAction.
 */
public class SetUserAuthoritiesActionTest {

    private static final String USER_ID_1 = "1";
    private static final String USER_ID_2 = "2";
    private static final String USER_ID_3 = "3";

    private static final String USER_IDENTITY_2 = "user 2";
    private static final String USER_IDENTITY_3 = "user 3";

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
                if (USER_ID_1.equals(id)) {
                    // leave user uninitialized
                } else if (USER_ID_2.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_2);
                    user.setIdentity(USER_IDENTITY_2);
                } else if (USER_ID_3.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_3);
                    user.setIdentity(USER_IDENTITY_3);
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
                if (USER_IDENTITY_3.equals(dn)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_3);
                    user.setIdentity(USER_IDENTITY_3);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                    user.setStatus(AccountStatus.ACTIVE);
                }
                return user;
            }
        }).when(userDao).findUserByDn(Mockito.anyString());
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
        Mockito.doAnswer(new Answer<Set<Authority>>() {
            @Override
            public Set<Authority> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];

                Set<Authority> authorities = EnumSet.noneOf(Authority.class);
                if (USER_IDENTITY_3.equals(dn)) {
                    authorities.add(Authority.ROLE_DFM);
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

                if (USER_IDENTITY_2.equals(dn)) {
                    throw new AuthorityAccessException(StringUtils.EMPTY);
                }

                // do nothing
                return null;
            }
        }).when(authorityProvider).setAuthorities(Mockito.anyString(), Mockito.anySet());
    }

    /**
     * Test activating an unknown user account. User accounts are unknown then
     * there is no pending account for the user.
     *
     * @throws Exception ex
     */
    @Test(expected = AccountNotFoundException.class)
    public void testUnknownUser() throws Exception {
        UpdateUserAction setUserAuthorities = new UpdateUserAction(USER_ID_1, Collections.EMPTY_SET);
        setUserAuthorities.execute(daoFactory, authorityProvider);
    }

    /**
     * Testing case then an AuthorityAccessException occurs while setting a
     * users authorities.
     *
     * @throws Exception ex
     */
    @Test(expected = AdministrationException.class)
    public void testAuthorityAccessException() throws Exception {
        UpdateUserAction setUserAuthorities = new UpdateUserAction(USER_ID_2, Collections.EMPTY_SET);
        setUserAuthorities.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests general case of setting user authorities.
     *
     * @throws Exception ex
     */
    @Test
    public void testSetAuthorities() throws Exception {
        UpdateUserAction setUserAuthorities = new UpdateUserAction(USER_ID_3, EnumSet.of(Authority.ROLE_ADMIN));
        NiFiUser user = setUserAuthorities.execute(daoFactory, authorityProvider);

        // verify user
        Assert.assertEquals(USER_ID_3, user.getId());
        Assert.assertEquals(1, user.getAuthorities().size());
        Assert.assertTrue(user.getAuthorities().contains(Authority.ROLE_ADMIN));

        // verify interaction with dao
        Mockito.verify(userDao, Mockito.times(1)).updateUser(user);
        Mockito.verify(authorityDao, Mockito.times(1)).createAuthorities(EnumSet.of(Authority.ROLE_ADMIN), USER_ID_3);

        Set<Authority> authoritiesAddedToProvider = EnumSet.of(Authority.ROLE_ADMIN);

        // verify interaction with provider
        Mockito.verify(authorityProvider, Mockito.times(1)).setAuthorities(USER_IDENTITY_3, authoritiesAddedToProvider);
    }
}
