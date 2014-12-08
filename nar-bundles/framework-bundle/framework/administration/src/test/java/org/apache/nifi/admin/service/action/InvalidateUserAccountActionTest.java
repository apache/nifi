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
import org.junit.Assert;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test case for InvalidateUserAccountAction.
 */
public class InvalidateUserAccountActionTest {

    private static final String USER_ID_1 = "1";
    private static final String USER_ID_2 = "2";
    private static final String USER_ID_3 = "3";

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
                String id = (String) args[0];

                NiFiUser user = null;
                if (USER_ID_1.equals(id)) {
                    // leave uninitialized
                } else if (USER_ID_2.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_2);
                } else if (USER_ID_3.equals(id)) {
                    user = new NiFiUser();
                    user.setId(USER_ID_3);
                    user.setLastVerified(new Date());
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
    }

    /**
     * Tests when the user account cannot be found.
     *
     * @throws Exception
     */
    @Test(expected = AccountNotFoundException.class)
    public void testAccountNotFoundException() throws Exception {
        InvalidateUserAccountAction invalidateUserAccount = new InvalidateUserAccountAction(USER_ID_1);
        invalidateUserAccount.execute(daoFactory, null);
    }

    /**
     * Tests when a data access exception occurs when updating the user record.
     *
     * @throws Exception
     */
    @Test(expected = DataAccessException.class)
    public void testDataAccessException() throws Exception {
        InvalidateUserAccountAction invalidateUserAccount = new InvalidateUserAccountAction(USER_ID_2);
        invalidateUserAccount.execute(daoFactory, null);
    }

    /**
     * Tests the general case of invalidating a user.
     *
     * @throws Exception
     */
    @Test
    public void testInvalidateUser() throws Exception {
        InvalidateUserAccountAction invalidateUserAccount = new InvalidateUserAccountAction(USER_ID_3);
        invalidateUserAccount.execute(daoFactory, null);

        // verify the interaction with the dao
        ArgumentCaptor<NiFiUser> userCaptor = ArgumentCaptor.forClass(NiFiUser.class);
        Mockito.verify(userDao, Mockito.times(1)).updateUser(userCaptor.capture());

        // verify the user
        NiFiUser user = userCaptor.getValue();
        Assert.assertEquals(USER_ID_3, user.getId());
        Assert.assertNull(user.getLastVerified());
    }
}
