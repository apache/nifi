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
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DisableUserAction implements AdministrationAction<NiFiUser> {

    private static final Logger logger = LoggerFactory.getLogger(DisableUserAction.class);

    private final String id;

    public DisableUserAction(String id) {
        this.id = id;
    }

    @Override
    public NiFiUser execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        UserDAO userDao = daoFactory.getUserDAO();

        // get the user
        NiFiUser user = userDao.findUserById(id);

        // ensure the user exists
        if (user == null) {
            throw new AccountNotFoundException(String.format("Unable to find account with ID %s.", id));
        }

        // update the account
        user.setStatus(AccountStatus.DISABLED);
        user.setUserGroup(null);

        // update the user locally
        userDao.updateUser(user);

        try {
            // revoke the user in the authority provider
            authorityProvider.revokeUser(user.getIdentity());
        } catch (UnknownIdentityException uie) {
            // user identity is not known
            logger.info(String.format("User %s has already been removed from the authority provider.", user.getIdentity()));
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(String.format("Unable to revoke user '%s': %s", user.getIdentity(), aae.getMessage()), aae);
        }

        return user;
    }
}
