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

import java.util.Calendar;
import java.util.Date;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountDisabledException;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AccountPendingException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;

/**
 *
 */
public class AuthorizeUserAction extends AbstractUserAction<NiFiUser> {

    private final String identity;
    private final int cacheDurationSeconds;

    public AuthorizeUserAction(String identity, int cacheDurationSeconds) {
        this.identity = identity;
        this.cacheDurationSeconds = cacheDurationSeconds;
    }

    @Override
    public NiFiUser execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        UserDAO userDao = daoFactory.getUserDAO();

        // get the user
        NiFiUser user = userDao.findUserByDn(identity);

        // verify the user was found
        if (user == null) {
            // determine whether this users exists
            boolean doesDnExist = false;
            try {
                doesDnExist = authorityProvider.doesDnExist(identity);
            } catch (AuthorityAccessException aae) {
                throw new AdministrationException(String.format("Unable to access authority details: %s", aae.getMessage()), aae);
            }

            // if the authority provider has the details for this user, create the account
            if (doesDnExist) {
                // create the user
                user = new NiFiUser();
                user.setIdentity(identity);
                user.setUserName(CertificateUtils.extractUsername(identity));
                user.setJustification("User details specified by authority provider.");

                try {
                    // verify the users account
                    verifyAccount(authorityProvider, user);

                    // get the date used for verification
                    Date now = user.getLastVerified();

                    // update the last accessed field
                    user.setLastAccessed(now);
                    user.setCreation(now);

                    // create the new user account
                    CreateUserAction createUser = new CreateUserAction(user);
                    createUser.execute(daoFactory, authorityProvider);
                } catch (UnknownIdentityException uie) {
                    // strange since the provider just reported this dn existed but handleing anyways...
                    throw new AccountNotFoundException(String.format("Unable to verify access for %s.", identity));
                } catch (AuthorityAccessException aae) {
                    throw new AdministrationException(String.format("Unable to access authority details: %s", aae.getMessage()), aae);
                }
            } else {
                throw new AccountNotFoundException(String.format("Unable to verify access for %s.", identity));
            }
        } else {
            Throwable providerError = null;

            // verify the users account if necessary
            if (isAccountVerificationRequired(user)) {
                try {
                    // verify the users account
                    verifyAccount(authorityProvider, user);

                    // update the last accessed field
                    user.setLastAccessed(user.getLastVerified());
                } catch (UnknownIdentityException uie) {
                    // check the account status before attempting to update the account - depending on the account
                    // status we might not need to update the account
                    checkAccountStatus(user);

                    // the user is currently active and they were not found in the providers - disable the account...
                    user.setStatus(AccountStatus.DISABLED);

                    // record the exception
                    providerError = uie;
                } catch (AuthorityAccessException aae) {
                    throw new AdministrationException(String.format("Unable to access authority details: %s", aae.getMessage()), aae);
                }
            } else {
                // verfiy the users account status before allowing access.
                checkAccountStatus(user);

                // update the users last accessed time
                user.setLastAccessed(new Date());
            }

            // persist the user's updates
            UpdateUserCacheAction updateUser = new UpdateUserCacheAction(user);
            updateUser.execute(daoFactory, authorityProvider);

            // persist the user's authorities
            UpdateUserAuthoritiesCacheAction updateUserAuthorities = new UpdateUserAuthoritiesCacheAction(user);
            updateUserAuthorities.execute(daoFactory, authorityProvider);

            if (providerError != null) {
                throw new AccountDisabledException(String.format("User credentials for %s were not found. This account has been disabled.", user.getIdentity()), providerError);
            }
        }

        return user;
    }

    /**
     * @return Determines if account verification is required
     */
    private boolean isAccountVerificationRequired(NiFiUser user) {
        // accounts that have never been verified obviously needs to be re-verified
        if (user.getLastVerified() == null) {
            return true;
        }

        // create a calendar and substract the threshold - anything
        // before this time will need to be re-verified
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, -cacheDurationSeconds);

        return user.getLastVerified().before(calendar.getTime());
    }

    /**
     * Checks the account status of the specified user.
     *
     * @param user to check
     */
    private void checkAccountStatus(NiFiUser user) {
        if (AccountStatus.DISABLED.equals(user.getStatus())) {
            throw new AccountDisabledException(String.format("The account for %s has been disabled.", user.getIdentity()));
        } else if (AccountStatus.PENDING.equals(user.getStatus())) {
            throw new AccountPendingException(String.format("The account for %s is currently pending approval.", user.getIdentity()));
        }
    }
}
