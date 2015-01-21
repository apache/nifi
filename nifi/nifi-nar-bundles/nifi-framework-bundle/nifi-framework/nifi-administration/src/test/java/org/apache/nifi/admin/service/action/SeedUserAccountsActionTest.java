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

import org.apache.nifi.admin.service.action.SeedUserAccountsAction;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 */
public class SeedUserAccountsActionTest {

    private static final String USER_ID_1 = "1";
    private static final String USER_ID_2 = "2";
    private static final String USER_ID_3 = "3";
    private static final String USER_ID_4 = "4";

    private static final String USER_DN_1 = "user dn 1 - active user - remove monitor and operator, add dfm";
    private static final String USER_DN_2 = "user dn 2 - active user - no action";
    private static final String USER_DN_3 = "user dn 3 - pending user - add operator";
    private static final String USER_DN_4 = "user dn 4 - new user - add monitor";

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
                    user = new NiFiUser();
                    user.setId(USER_ID_1);
                    user.setDn(USER_DN_1);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                    user.setStatus(AccountStatus.ACTIVE);
                } else if (USER_ID_2.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_2);
                    user.setDn(USER_DN_2);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_ADMIN));
                    user.setStatus(AccountStatus.ACTIVE);
                } else if (USER_ID_3.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_3);
                    user.setDn(USER_DN_3);
                    user.setStatus(AccountStatus.PENDING);
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
                if (USER_DN_1.equals(dn)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_1);
                    user.setDn(USER_DN_1);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_MONITOR));
                    user.setStatus(AccountStatus.ACTIVE);
                } else if (USER_DN_2.equals(dn)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_2);
                    user.setDn(USER_DN_2);
                    user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_ADMIN));
                    user.setStatus(AccountStatus.ACTIVE);
                } else if (USER_DN_3.equals(dn)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_3);
                    user.setDn(USER_DN_3);
                    user.setStatus(AccountStatus.PENDING);
                }
                return user;
            }
        }).when(userDao).findUserByDn(Mockito.anyString());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                NiFiUser user = (NiFiUser) args[0];

                if (USER_DN_4.equals(user.getDn())) {
                    user.setId(USER_ID_4);
                }

                return null;
            }
        }).when(userDao).createUser(Mockito.any(NiFiUser.class));

        // mock the authority dao
        authorityDao = Mockito.mock(AuthorityDAO.class);

        // mock the authority provider
        authorityProvider = Mockito.mock(AuthorityProvider.class);
        Mockito.doAnswer(new Answer<Set<String>>() {
            @Override
            public Set<String> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Authority role = (Authority) args[0];

                Set<String> users = new HashSet<>();
                if (Authority.ROLE_DFM.equals(role)) {
                    users.add(USER_DN_1);
                } else if (Authority.ROLE_ADMIN.equals(role)) {
                    users.add(USER_DN_2);
                } else if (Authority.ROLE_PROXY.equals(role)) {
                    users.add(USER_DN_3);
                } else if (Authority.ROLE_MONITOR.equals(role)) {
                    users.add(USER_DN_4);
                }
                return users;
            }
        }).when(authorityProvider).getUsers(Mockito.any(Authority.class));
        Mockito.doAnswer(new Answer<Set<Authority>>() {
            @Override
            public Set<Authority> answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];

                Set<Authority> authorities = EnumSet.noneOf(Authority.class);
                switch (dn) {
                    case USER_DN_1:
                        authorities.add(Authority.ROLE_DFM);
                        break;
                    case USER_DN_2:
                        authorities.add(Authority.ROLE_ADMIN);
                        break;
                    case USER_DN_3:
                        authorities.add(Authority.ROLE_PROXY);
                        break;
                    case USER_DN_4:
                        authorities.add(Authority.ROLE_MONITOR);
                        break;
                }
                return authorities;
            }
        }).when(authorityProvider).getAuthorities(Mockito.anyString());

        // mock the dao factory
        daoFactory = Mockito.mock(DAOFactory.class);
        Mockito.when(daoFactory.getUserDAO()).thenReturn(userDao);
        Mockito.when(daoFactory.getAuthorityDAO()).thenReturn(authorityDao);
    }

    /**
     * Tests seeding the user accounts.
     *
     * @throws Exception
     */
    @Test
    public void testSeedUsers() throws Exception {
        SeedUserAccountsAction seedUserAccounts = new SeedUserAccountsAction();
        seedUserAccounts.execute(daoFactory, authorityProvider);

        // matcher for user 1
        Matcher<NiFiUser> matchesUser1 = new ArgumentMatcher<NiFiUser>() {
            @Override
            public boolean matches(Object argument) {
                NiFiUser user = (NiFiUser) argument;
                return USER_ID_1.equals(user.getId());
            }
        };

        // verify user 1 - active existing user - remove monitor, operator, add dfm
        Mockito.verify(userDao, Mockito.times(1)).updateUser(Mockito.argThat(matchesUser1));
        Mockito.verify(userDao, Mockito.never()).createUser(Mockito.argThat(matchesUser1));
        Mockito.verify(authorityDao, Mockito.times(1)).createAuthorities(EnumSet.of(Authority.ROLE_DFM), USER_ID_1);

        // matcher for user 2
        Matcher<NiFiUser> matchesUser2 = new ArgumentMatcher<NiFiUser>() {
            @Override
            public boolean matches(Object argument) {
                NiFiUser user = (NiFiUser) argument;
                return USER_ID_2.equals(user.getId());
            }
        };

        // verify user 2 - active existing user - no actions
        Mockito.verify(userDao, Mockito.times(1)).updateUser(Mockito.argThat(matchesUser2));
        Mockito.verify(userDao, Mockito.never()).createUser(Mockito.argThat(matchesUser2));
        Mockito.verify(authorityDao, Mockito.never()).createAuthorities(Mockito.anySet(), Mockito.eq(USER_ID_2));
        Mockito.verify(authorityDao, Mockito.never()).deleteAuthorities(Mockito.anySet(), Mockito.eq(USER_ID_2));

        // matchers for user 3
        Matcher<NiFiUser> matchesPendingUser3 = new ArgumentMatcher<NiFiUser>() {
            @Override
            public boolean matches(Object argument) {
                NiFiUser user = (NiFiUser) argument;
                return USER_ID_3.equals(user.getId()) && AccountStatus.ACTIVE.equals(user.getStatus());
            }
        };
        Matcher<NiFiUser> matchesUser3 = new ArgumentMatcher<NiFiUser>() {
            @Override
            public boolean matches(Object argument) {
                NiFiUser user = (NiFiUser) argument;
                return USER_ID_3.equals(user.getId());
            }
        };

        // verify user 3 - pending user - add operator
        Mockito.verify(userDao, Mockito.times(1)).updateUser(Mockito.argThat(matchesPendingUser3));
        Mockito.verify(userDao, Mockito.never()).createUser(Mockito.argThat(matchesUser3));
        Mockito.verify(authorityDao, Mockito.times(1)).createAuthorities(EnumSet.of(Authority.ROLE_PROXY), USER_ID_3);
        Mockito.verify(authorityDao, Mockito.never()).deleteAuthorities(Mockito.anySet(), Mockito.eq(USER_ID_3));

        // matcher for user 4
        Matcher<NiFiUser> matchesUser4 = new ArgumentMatcher<NiFiUser>() {
            @Override
            public boolean matches(Object argument) {
                NiFiUser user = (NiFiUser) argument;
                return USER_ID_4.equals(user.getId());
            }
        };

        // verify user 4 - new user - add monitor
        Mockito.verify(userDao, Mockito.never()).updateUser(Mockito.argThat(matchesUser4));
        Mockito.verify(userDao, Mockito.times(1)).createUser(Mockito.argThat(matchesUser4));
        Mockito.verify(authorityDao, Mockito.times(1)).createAuthorities(EnumSet.of(Authority.ROLE_MONITOR), USER_ID_4);
        Mockito.verify(authorityDao, Mockito.never()).deleteAuthorities(Mockito.anySet(), Mockito.eq(USER_ID_4));
    }
}
