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
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DisableUserGroupAction implements AdministrationAction<NiFiUserGroup> {

    private static final Logger logger = LoggerFactory.getLogger(DisableUserGroupAction.class);

    private final String group;

    public DisableUserGroupAction(final String group) {
        this.group = group;
    }

    @Override
    public NiFiUserGroup execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        final NiFiUserGroup userGroup = new NiFiUserGroup();

        final UserDAO userDao = daoFactory.getUserDAO();

        // update the user group locally
        userDao.updateGroupStatus(group, AccountStatus.DISABLED);

        // populate the group details
        userGroup.setGroup(group);
        userGroup.setUsers(userDao.findUsersForGroup(group));

        try {
            // revoke the user in the authority provider
            authorityProvider.revokeGroup(group);
        } catch (UnknownIdentityException uie) {
            // user identity is not known
            logger.info(String.format("User group %s has already been removed from the authority provider.", group));
        } catch (AuthorityAccessException aae) {
            throw new AdministrationException(String.format("Unable to revoke user group '%s': %s", group, aae.getMessage()), aae);
        }

        return userGroup;
    }
}
