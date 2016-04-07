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

import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seeds the user accounts. This action is performed at start up because it
 * takes the users specified in the authority provider and makes them available
 * to be seen in the UI. This happens because the UI loads the users from the
 * cache. Without pre loading the users, the table in the UI would only show a
 * given user once they have visited the application.
 */
public class SeedUserAccountsAction extends AbstractUserAction<Void> {

    private static final Logger logger = LoggerFactory.getLogger(SeedUserAccountsAction.class);

    @Override
    public Void execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        UserDAO userDao = daoFactory.getUserDAO();
        Set<String> authorizedIdentities = new HashSet<>();

        // get the current user cache
        final Set<NiFiUser> existingUsers;
        try {
            existingUsers = userDao.findUsers();
        } catch (Exception e) {
            // unable to access local cache... start up failure
            logger.error(String.format("Unable to get existing user base. Cannot proceed until these users can be "
                    + "verified against the current authority provider: %s", e));
            throw new AdministrationException(e);
        }

        try {
            // all users for all roles
            for (final Authority authority : Authority.values()) {
                authorizedIdentities.addAll(authorityProvider.getUsers(authority));
            }
        } catch (AuthorityAccessException aae) {
            // unable to access the authority provider... honor the cache
            logger.warn("Unable to access authority provider due to " + aae);
            return null;
        }

        final Set<NiFiUser> accountsToRevoke = new HashSet<>(existingUsers);

        // persist the users
        for (String identity : authorizedIdentities) {
            NiFiUser user = null;
            try {
                // locate the user for this dn
                user = userDao.findUserByDn(identity);
                boolean newAccount = false;

                // if the user does not exist, create a new account
                if (user == null) {
                    logger.info(String.format("Creating user account: %s", identity));
                    newAccount = true;

                    // create the user
                    user = new NiFiUser();
                    user.setIdentity(identity);
                    user.setUserName(CertificateUtils.extractUsername(identity));
                    user.setJustification("User details specified by authority provider.");
                } else {
                    logger.info(String.format("User account already created: %s. Updating authorities...", identity));
                }

                // verify the account
                verifyAccount(authorityProvider, user);

                // persist the account accordingly
                if (newAccount) {
                    CreateUserAction createUser = new CreateUserAction(user);
                    createUser.execute(daoFactory, authorityProvider);
                } else {
                    // this is not a new user and we have just verified their
                    // account, do not revoke...
                    accountsToRevoke.remove(user);

                    // persist the user
                    UpdateUserCacheAction updateUser = new UpdateUserCacheAction(user);
                    updateUser.execute(daoFactory, authorityProvider);

                    // persist the user's authorities
                    UpdateUserAuthoritiesCacheAction updateUserAuthorities = new UpdateUserAuthoritiesCacheAction(user);
                    updateUserAuthorities.execute(daoFactory, authorityProvider);
                }
            } catch (DataAccessException dae) {
                if (user != null) {
                    logger.warn(String.format("Unable to access account details in local cache for user %s: %s", user, dae.getMessage()));
                } else {
                    logger.warn(String.format("Unable to access account details in local cache: %s", dae.getMessage()));
                }
            } catch (UnknownIdentityException uie) {
                if (user != null) {
                    logger.warn(String.format("Unable to find account details in authority provider for user %s: %s", user, uie.getMessage()));
                } else {
                    logger.warn(String.format("Unable to find account details in authority provider: %s", uie.getMessage()));
                }
            } catch (AuthorityAccessException aae) {
                logger.warn("Unable to access authority provider due to " + aae);

                // unable to access authority provider for this user, honor the cache for now
                accountsToRevoke.remove(user);
            }
        }

        // remove all users that are no longer in the provider
        for (final NiFiUser user : accountsToRevoke) {
            // allow pending requests to remain...
            if (AccountStatus.PENDING.equals(user.getStatus())) {
                continue;
            }

            try {
                logger.info(String.format("User not authorized with configured provider: %s. Disabling account...", user.getIdentity()));

                // disable the account and reset its last verified timestamp since it was not found
                // in the current configured authority provider
                user.setStatus(AccountStatus.DISABLED);
                user.setLastVerified(null);

                // update the user record
                UpdateUserCacheAction updateUser = new UpdateUserCacheAction(user);
                updateUser.execute(daoFactory, authorityProvider);
            } catch (final Exception e) {
                // unable to revoke access for someone we know is not authorized... fail start up
                logger.error(String.format("Unable to revoke access for user %s that is no longer authorized: %s", user, e));
                throw new AdministrationException(e);
            }
        }

        return null;
    }
}
