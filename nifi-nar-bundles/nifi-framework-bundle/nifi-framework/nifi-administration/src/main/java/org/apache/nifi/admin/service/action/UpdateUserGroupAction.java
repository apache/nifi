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
import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.admin.dao.DataAccessException;
import org.apache.nifi.admin.dao.UserDAO;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates all NiFiUser authorities in a specified group.
 */
public class UpdateUserGroupAction extends AbstractUserAction<Void> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateUserGroupAction.class);

    private final String group;
    private final Set<String> userIds;
    private final Set<Authority> authorities;

    public UpdateUserGroupAction(String group, Set<String> userIds, Set<Authority> authorities) {
        this.group = group;
        this.userIds = userIds;
        this.authorities = authorities;
    }

    @Override
    public Void execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        if (userIds == null && authorities == null) {
            throw new IllegalArgumentException("Must specify user Ids or authorities.");
        }

        UserDAO userDao = daoFactory.getUserDAO();

        // record the new users being added to this group
        final Set<NiFiUser> newUsers = new HashSet<>();
        final Set<String> newUserIdentities = new HashSet<>();

        // if the user ids have been specified we need to create/update a group using the specified group name
        if (userIds != null) {
            if (userIds.isEmpty()) {
                throw new IllegalArgumentException("When creating a group, at least one user id must be specified.");
            }

            // going to create a group using the specified user ids
            for (final String userId : userIds) {
                // get the user in question
                final NiFiUser user = userDao.findUserById(userId);

                // ensure the user exists
                if (user == null) {
                    throw new AccountNotFoundException(String.format("Unable to find account with ID %s.", userId));
                }

                try {
                    // if the user is unknown to the authority provider we cannot continue
                    if (!authorityProvider.doesDnExist(user.getIdentity()) || AccountStatus.DISABLED.equals(user.getStatus())) {
                        throw new IllegalStateException(String.format("Unable to group these users because access for '%s' is not %s.", user.getIdentity(), AccountStatus.ACTIVE.toString()));
                    }

                    // record the user being added to this group
                    newUsers.add(user);
                    newUserIdentities.add(user.getIdentity());
                } catch (final AuthorityAccessException aae) {
                    throw new AdministrationException(String.format("Unable to access authority details: %s", aae.getMessage()), aae);
                }
            }

            try {
                // update the authority provider
                authorityProvider.setUsersGroup(newUserIdentities, group);
            } catch (UnknownIdentityException uie) {
                throw new AccountNotFoundException(String.format("Unable to set user group '%s': %s", StringUtils.join(newUserIdentities, ", "), uie.getMessage()), uie);
            } catch (AuthorityAccessException aae) {
                throw new AdministrationException(String.format("Unable to set user group '%s': %s", StringUtils.join(newUserIdentities, ", "), aae.getMessage()), aae);
            }
        }

        // get all the users that need to be updated
        final Set<NiFiUser> users = new HashSet<>(userDao.findUsersForGroup(group));
        users.addAll(newUsers);

        // ensure the user exists
        if (users.isEmpty()) {
            throw new AccountNotFoundException(String.format("Unable to find user accounts with group id %s.", group));
        }

        // update each user in this group
        for (final NiFiUser user : users) {
            // if there are new authorities set them, otherwise refresh them according to the provider
            if (authorities != null) {
                try {
                    // update the authority provider as approprivate
                    authorityProvider.setAuthorities(user.getIdentity(), authorities);

                    // since all the authorities were updated accordingly, set the authorities
                    user.getAuthorities().clear();
                    user.getAuthorities().addAll(authorities);
                } catch (UnknownIdentityException uie) {
                    throw new AccountNotFoundException(String.format("Unable to modify authorities for '%s': %s.", user.getIdentity(), uie.getMessage()), uie);
                } catch (AuthorityAccessException aae) {
                    throw new AdministrationException(String.format("Unable to access authorities for '%s': %s.", user.getIdentity(), aae.getMessage()), aae);
                }
            } else {
                try {
                    // refresh the authorities according to the provider
                    user.getAuthorities().clear();
                    user.getAuthorities().addAll(authorityProvider.getAuthorities(user.getIdentity()));
                } catch (UnknownIdentityException uie) {
                    throw new AccountNotFoundException(String.format("Unable to determine the authorities for '%s': %s.", user.getIdentity(), uie.getMessage()), uie);
                } catch (AuthorityAccessException aae) {
                    throw new AdministrationException(String.format("Unable to access authorities for '%s': %s.", user.getIdentity(), aae.getMessage()), aae);
                }
            }

            try {
                // get the user group
                user.setUserGroup(authorityProvider.getGroupForUser(user.getIdentity()));
            } catch (UnknownIdentityException uie) {
                throw new AccountNotFoundException(String.format("Unable to determine the group for '%s': %s.", user.getIdentity(), uie.getMessage()), uie);
            } catch (AuthorityAccessException aae) {
                throw new AdministrationException(String.format("Unable to access the group for '%s': %s.", user.getIdentity(), aae.getMessage()), aae);
            }

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
        }

        return null;
    }
}
