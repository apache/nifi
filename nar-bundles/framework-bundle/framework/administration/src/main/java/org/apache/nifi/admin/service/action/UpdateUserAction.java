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
import java.util.Set;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets user authorities.
 */
public class UpdateUserAction extends AbstractUserAction<NiFiUser> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateUserAction.class);

    private final String id;
    private final Set<Authority> authorities;

    public UpdateUserAction(String id, Set<Authority> authorities) {
        this.id = id;
        this.authorities = authorities;
    }

    @Override
    public NiFiUser execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException, AdministrationException {
        UserDAO userDao = daoFactory.getUserDAO();

        // get the user
        NiFiUser user = userDao.findUserById(id);

        // ensure the user exists
        if (user == null) {
            throw new AccountNotFoundException(String.format("Unable to find account with ID %s.", id));
        }

        // determine whether this users exists
        boolean doesDnExist = false;
        try {
            doesDnExist = authorityProvider.doesDnExist(user.getDn());
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(String.format("Unable to access authority details: %s", aae.getMessage()), aae);
        }

        // if the user already doesn't exist, add them
        if (!doesDnExist) {
            try {
                // add the account account and group if necessary
                authorityProvider.addUser(user.getDn(), user.getUserGroup());
            } catch (final IdentityAlreadyExistsException iaee) {
                logger.warn(String.format("User '%s' already exists in the authority provider.  Continuing with user update.", user.getDn()));
            } catch (AuthorityAccessException aae) {
                throw new AdministrationException(String.format("Unable to access authorities for '%s': %s", user.getDn(), aae.getMessage()), aae);
            }
        }

        try {
            // update the authority provider as approprivate
            authorityProvider.setAuthorities(user.getDn(), authorities);
        } catch (UnknownIdentityException uie) {
            throw new AccountNotFoundException(String.format("Unable to modify authorities for '%s': %s.", user.getDn(), uie.getMessage()), uie);
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(String.format("Unable to access authorities for '%s': %s.", user.getDn(), aae.getMessage()), aae);
        }

        try {
            // get the user group
            user.setUserGroup(authorityProvider.getGroupForUser(user.getDn()));
        } catch (UnknownIdentityException uie) {
            throw new AccountNotFoundException(String.format("Unable to determine the group for '%s': %s.", user.getDn(), uie.getMessage()), uie);
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(String.format("Unable to access the group for '%s': %s.", user.getDn(), aae.getMessage()), aae);
        }

        // since all the authorities were updated accordingly, set the authorities
        user.getAuthorities().clear();
        user.getAuthorities().addAll(authorities);

        // update the users status in case they were previously pending or disabled
        user.setStatus(AccountStatus.ACTIVE);

        // update the users last verified time - this timestamp shouldn't be recorded
        // until the both the user's authorities and group have been synced
        Date now = new Date();
        user.setLastVerified(now);

        // persist the user's updates
        UpdateUserCacheAction updateUser = new UpdateUserCacheAction(user);
        updateUser.execute(daoFactory, authorityProvider);

        // persist the user's authorities
        UpdateUserAuthoritiesCacheAction updateUserAuthorities = new UpdateUserAuthoritiesCacheAction(user);
        updateUserAuthorities.execute(daoFactory, authorityProvider);

        // return the user
        return user;
    }
}
