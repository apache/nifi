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

import java.util.EnumSet;
import java.util.Set;
import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test cases for creating a user.
 */
public class CreateUserActionTest {

    private String USER_ID_2 = "2";
    private String USER_ID_3 = "3";

    private String USER_DN_1 = "data access exception when creating user";
    private String USER_DN_3 = "general create user case";

    private DAOFactory daoFactory;
    private UserDAO userDao;
    private AuthorityDAO authorityDao;

    @Before
    public void setup() throws Exception {
        // mock the user dao
        userDao = Mockito.mock(UserDAO.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                NiFiUser user = (NiFiUser) args[0];

                if (USER_DN_1.equals(user.getDn())) {
                    throw new DataAccessException();
                } else if (USER_DN_3.equals(user.getDn())) {
                    user.setId(USER_ID_3);
                }

                // do nothing
                return null;
            }
        }).when(userDao).createUser(Mockito.any(NiFiUser.class));

        // mock the authority dao
        authorityDao = Mockito.mock(AuthorityDAO.class);
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Set<Authority> authorities = (Set<Authority>) args[0];
                String id = (String) args[1];

                if (USER_ID_2.equals(id)) {
                    throw new DataAccessException(StringUtils.EMPTY);
                }

                // do nothing
                return null;
            }
        }).when(authorityDao).createAuthorities(Mockito.anySetOf(Authority.class), Mockito.anyString());

        // mock the dao factory
        daoFactory = Mockito.mock(DAOFactory.class);
        Mockito.when(daoFactory.getUserDAO()).thenReturn(userDao);
        Mockito.when(daoFactory.getAuthorityDAO()).thenReturn(authorityDao);
    }

    /**
     * Tests DataAccessExceptions that occur while creating user accounts.
     *
     * @throws Exception
     */
    @Test(expected = DataAccessException.class)
    public void testExceptionCreatingUser() throws Exception {
        NiFiUser user = new NiFiUser();
        user.setDn(USER_DN_1);

        CreateUserAction createUser = new CreateUserAction(user);
        createUser.execute(daoFactory, null);
    }

    /**
     * Tests DataAccessExceptions that occur while create user authorities.
     *
     * @throws Exception
     */
    @Test(expected = DataAccessException.class)
    public void testExceptionCreatingAuthoroties() throws Exception {
        NiFiUser user = new NiFiUser();
        user.setId(USER_ID_2);

        CreateUserAction createUser = new CreateUserAction(user);
        createUser.execute(daoFactory, null);
    }

    /**
     * General case for creating a user.
     *
     * @throws Exception
     */
    @Test
    public void testCreateUserAccount() throws Exception {
        NiFiUser user = new NiFiUser();
        user.setDn(USER_DN_3);
        user.getAuthorities().addAll(EnumSet.of(Authority.ROLE_DFM, Authority.ROLE_ADMIN));

        CreateUserAction createUser = new CreateUserAction(user);
        createUser.execute(daoFactory, null);

        // verify the user
        Assert.assertEquals(USER_ID_3, user.getId());

        // verify interaction with dao
        Mockito.verify(userDao, Mockito.times(1)).createUser(user);
        Mockito.verify(authorityDao, Mockito.times(1)).createAuthorities(user.getAuthorities(), USER_ID_3);
    }
}
