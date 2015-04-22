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

import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;

/**
 *
 */
public class DeleteUserAction implements AdministrationAction<Void> {

    private final String userId;

    /**
     * Creates a new transactions for deleting the specified user.
     *
     * @param userId user identifier
     */
    public DeleteUserAction(String userId) {
        this.userId = userId;
    }

    @Override
    public Void execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        final AuthorityDAO authorityDAO = daoFactory.getAuthorityDAO();
        final UserDAO userDAO = daoFactory.getUserDAO();

        // find the user and ensure they are currently revoked
        final NiFiUser user = userDAO.findUserById(userId);

        // ensure the user was found
        if (user == null) {
            throw new AccountNotFoundException(String.format("Unable to find account with ID %s.", userId));
        }

        // ensure the user is in the appropriate state
        if (AccountStatus.ACTIVE.equals(user.getStatus())) {
            throw new IllegalStateException(String.format("An active user cannot be removed. Revoke user access before attempting to remove."));
        }

        // remove the user and their authorities
        authorityDAO.deleteAuthorities(userId);
        userDAO.deleteUser(userId);

        return null;
    }
}
