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

import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AdministrationException;
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
 *
 */
public class DisableUserActionTest {

    private static final String USER_ID_1 = "1";
    private static final String USER_ID_2 = "2";
    private static final String USER_ID_3 = "3";
    private static final String USER_ID_4 = "4";

    private static final String USER_DN_3 = "authority access exception";
    private static final String USER_DN_4 = "general disable user case";

    private DAOFactory daoFactory;
    private UserDAO userDao;
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
                    user.setId(id);
                } else if (USER_ID_3.equals(id)) {
                    user = new NiFiUser();
                    user.setId(id);
                    user.setDn(USER_DN_3);
                } else if (USER_ID_4.equals(id)) {
                    user = new NiFiUser();
                    user.setId(id);
                    user.setDn(USER_DN_4);
                    user.setStatus(AccountStatus.ACTIVE);
                }
                return user;
            }
        }).when(userDao).findUserById(Mockito.anyString());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                NiFiUser user = (NiFiUser) args[0];

                if (USER_ID_2.equals(user.getId())) {
                    throw new DataAccessException(StringUtils.EMPTY);
                }

                // do nothing
                return null;
            }
        }).when(userDao).updateUser(Mockito.any(NiFiUser.class));

        // mock the dao factory
        daoFactory = Mockito.mock(DAOFactory.class);
        Mockito.when(daoFactory.getUserDAO()).thenReturn(userDao);

        // mock the authority provider
        authorityProvider = Mockito.mock(AuthorityProvider.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];

                if (USER_DN_3.equals(dn)) {
                    throw new AuthorityAccessException(StringUtils.EMPTY);
                }

                // do nothing
                return null;
            }
        }).when(authorityProvider).revokeUser(Mockito.anyString());
    }

    /**
     * Tests the case when the user account is unknown.
     *
     * @throws Exception
     */
    @Test(expected = AccountNotFoundException.class)
    public void testUnknownUserAccount() throws Exception {
        DisableUserAction disableUser = new DisableUserAction(USER_ID_1);
        disableUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests the case when a DataAccessException is thrown by the userDao.
     *
     * @throws Exception
     */
    @Test(expected = DataAccessException.class)
    public void testDataAccessExceptionInUserDao() throws Exception {
        DisableUserAction disableUser = new DisableUserAction(USER_ID_2);
        disableUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests the case when a AuthorityAccessException is thrown by the provider.
     *
     * @throws Exception
     */
    @Test(expected = AdministrationException.class)
    public void testAuthorityAccessExceptionInProvider() throws Exception {
        DisableUserAction disableUser = new DisableUserAction(USER_ID_3);
        disableUser.execute(daoFactory, authorityProvider);
    }

    /**
     * Tests the general case when the user is disabled.
     *
     * @throws Exception
     */
    @Test
    public void testDisableUser() throws Exception {
        DisableUserAction disableUser = new DisableUserAction(USER_ID_4);
        NiFiUser user = disableUser.execute(daoFactory, authorityProvider);

        // verify the user
        Assert.assertEquals(USER_ID_4, user.getId());
        Assert.assertEquals(USER_DN_4, user.getDn());
        Assert.assertEquals(AccountStatus.DISABLED, user.getStatus());

        // verify the interaction with the dao and provider
        Mockito.verify(userDao, Mockito.times(1)).updateUser(user);
        Mockito.verify(authorityProvider, Mockito.times(1)).revokeUser(USER_DN_4);
    }
}
