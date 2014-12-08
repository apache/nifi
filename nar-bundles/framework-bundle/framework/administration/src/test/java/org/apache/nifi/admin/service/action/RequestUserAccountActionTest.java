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
 * Test case for RequestUserAccountAction.
 */
public class RequestUserAccountActionTest {

    private static final String USER_ID_3 = "3";

    private static final String USER_DN_1 = "existing user account dn";
    private static final String USER_DN_2 = "data access exception";
    private static final String USER_DN_3 = "new account request";

    private DAOFactory daoFactory;
    private UserDAO userDao;

    @Before
    public void setup() throws Exception {
        // mock the user dao
        userDao = Mockito.mock(UserDAO.class);
        Mockito.doAnswer(new Answer<NiFiUser>() {
            @Override
            public NiFiUser answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String dn = (String) args[0];

                NiFiUser user = null;
                if (USER_DN_1.equals(dn)) {
                    user = new NiFiUser();
                }
                return user;
            }
        }).when(userDao).findUserByDn(Mockito.anyString());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                NiFiUser user = (NiFiUser) args[0];
                switch (user.getDn()) {
                    case USER_DN_2:
                        throw new DataAccessException();
                    case USER_DN_3:
                        user.setId(USER_ID_3);
                        break;
                }

                // do nothing
                return null;
            }
        }).when(userDao).createUser(Mockito.any(NiFiUser.class));

        // mock the dao factory
        daoFactory = Mockito.mock(DAOFactory.class);
        Mockito.when(daoFactory.getUserDAO()).thenReturn(userDao);
    }

    /**
     * Tests when a user account already exists.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingAccount() throws Exception {
        RequestUserAccountAction requestUserAccount = new RequestUserAccountAction(USER_DN_1, StringUtils.EMPTY);
        requestUserAccount.execute(daoFactory, null);
    }

    /**
     * Tests when a DataAccessException occurs while saving the new account
     * request.
     *
     * @throws Exception
     */
    @Test(expected = DataAccessException.class)
    public void testDataAccessException() throws Exception {
        RequestUserAccountAction requestUserAccount = new RequestUserAccountAction(USER_DN_2, StringUtils.EMPTY);
        requestUserAccount.execute(daoFactory, null);
    }

    /**
     * Tests the general case for requesting a new user account.
     *
     * @throws Exception
     */
    @Test
    public void testRequestUserAccountAction() throws Exception {
        RequestUserAccountAction requestUserAccount = new RequestUserAccountAction(USER_DN_3, StringUtils.EMPTY);
        NiFiUser user = requestUserAccount.execute(daoFactory, null);

        // verfiy the user
        Assert.assertEquals(USER_ID_3, user.getId());
        Assert.assertEquals(USER_DN_3, user.getDn());
        Assert.assertEquals(AccountStatus.PENDING, user.getStatus());

        // verify interaction with dao
        Mockito.verify(userDao, Mockito.times(1)).createUser(user);
    }
}
