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
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;

/**
 *
 */
public class RequestUserAccountAction implements AdministrationAction<NiFiUser> {

    private final String identity;
    private final String justification;

    public RequestUserAccountAction(String identity, String justification) {
        this.identity = identity;
        this.justification = justification;
    }

    @Override
    public NiFiUser execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        UserDAO userDao = daoFactory.getUserDAO();

        // determine if this user already exists
        NiFiUser user = userDao.findUserByDn(identity);
        if (user != null) {
            throw new IllegalArgumentException(String.format("User account for %s already exists.", identity));
        }

        // create the user
        user = new NiFiUser();
        user.setIdentity(identity);
        user.setUserName(CertificateUtils.extractUsername(identity));
        user.setJustification(justification);
        user.setStatus(AccountStatus.PENDING);

        // update user timestamps
        Date now = new Date();
        user.setCreation(now);

        // create the new user account
        userDao.createUser(user);

        return user;
    }
}
