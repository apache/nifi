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
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.user.NiFiUser;

/**
 * Updates a NiFiUser. This will not update the user authorities, they must be
 * updated with the UpdateUserAuthoritiesAction.
 */
public class UpdateUserCacheAction extends AbstractUserAction<Void> {

    private final NiFiUser user;

    public UpdateUserCacheAction(NiFiUser user) {
        this.user = user;
    }

    @Override
    public Void execute(DAOFactory daoFactory, AuthorityProvider authorityProvider) throws DataAccessException {
        UserDAO userDao = daoFactory.getUserDAO();

        // update the user
        userDao.updateUser(user);

        return null;
    }

}
