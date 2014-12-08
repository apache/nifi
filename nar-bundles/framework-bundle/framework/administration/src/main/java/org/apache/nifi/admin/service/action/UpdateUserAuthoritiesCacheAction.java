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

import java.util.Set;
import org.apache.nifi.admin.dao.AuthorityDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Updates a NiFiUser's authorities. Prior to invoking this action, the user's
 * authorities should be set according to the business logic of the service in
 * question. This should not be invoked directly when attempting to set user
 * authorities as the authorityProvider is not called from this action.
 */
public class UpdateUserAuthoritiesCacheAction extends AbstractUserAction<Void> {

    private final NiFiUser user;

    public UpdateUserAuthoritiesCacheAction(NiFiUser user) {
        this.user = user;
    }

    @Override
    public Void execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        UserDAO userDao = daoFactory.getUserDAO();
        AuthorityDAO authorityDao = daoFactory.getAuthorityDAO();

        // get the user
        NiFiUser currentUser = userDao.findUserById(user.getId());

        // ensure the user exists
        if (currentUser == null) {
            throw new AccountNotFoundException(String.format("Unable to find account with ID %s.", user.getId()));
        }

        // determine what authorities need to be added/removed
        Set<Authority> authorities = user.getAuthorities();
        Set<Authority> authoritiesToAdd = determineAuthoritiesToAdd(currentUser, authorities);
        Set<Authority> authoritiesToRemove = determineAuthoritiesToRemove(currentUser, authorities);

        // update the user authorities locally
        if (CollectionUtils.isNotEmpty(authoritiesToAdd)) {
            authorityDao.createAuthorities(authoritiesToAdd, user.getId());
        }
        if (CollectionUtils.isNotEmpty(authoritiesToRemove)) {
            authorityDao.deleteAuthorities(authoritiesToRemove, user.getId());
        }

        return null;
    }

}
