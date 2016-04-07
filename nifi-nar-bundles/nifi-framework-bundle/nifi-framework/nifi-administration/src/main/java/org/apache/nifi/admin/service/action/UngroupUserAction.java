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
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.NiFiUser;

/**
 *
 */
public class UngroupUserAction extends AbstractUserAction<Void> {

    private final String userId;

    public UngroupUserAction(String userId) {
        this.userId = userId;
    }

    @Override
    public Void execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) {
        final UserDAO userDao = daoFactory.getUserDAO();

        // get the user in question
        final NiFiUser user = userDao.findUserById(userId);

        // ensure the user exists
        if (user == null) {
            throw new AccountNotFoundException(String.format("Unable to find account with ID %s.", userId));
        }

        // set the user group
        user.setUserGroup(null);

        // update the user locally
        userDao.updateUser(user);

        try {
            // update the authority provider
            authorityProvider.ungroupUser(user.getIdentity());
        } catch (UnknownIdentityException uie) {
            throw new AccountNotFoundException(String.format("Unable to ungroup user '%s': %s", user.getIdentity(), uie.getMessage()), uie);
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(String.format("Unable to ungroup user '%s': %s", user.getIdentity(), aae.getMessage()), aae);
        }

        return null;
    }

}
