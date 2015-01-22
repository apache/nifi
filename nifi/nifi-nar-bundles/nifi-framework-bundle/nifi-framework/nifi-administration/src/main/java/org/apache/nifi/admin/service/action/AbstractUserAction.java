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
import java.util.EnumSet;
import java.util.Set;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.user.AccountStatus;
import org.apache.nifi.user.NiFiUser;

/**
 *
 * @param <T>
 */
public abstract class AbstractUserAction<T> implements AdministrationAction<T> {

    /**
     * Determines the authorities that need to be added to the specified user.
     *
     * @param user
     * @param authorities
     * @return
     */
    protected Set<Authority> determineAuthoritiesToAdd(NiFiUser user, Set<Authority> authorities) {
        // not using copyOf since authorities may be empty and copyOf can throw an IllegalArgumentException when empty
        Set<Authority> authoritiesToAdd = EnumSet.noneOf(Authority.class);
        authoritiesToAdd.addAll(authorities);

        // identify the authorities that need to be inserted
        authoritiesToAdd.removeAll(user.getAuthorities());

        // return the desired authorities
        return authoritiesToAdd;
    }

    /**
     * Determines the authorities that need to be removed from the specified
     * user.
     *
     * @param user
     * @param authorities
     * @return
     */
    protected Set<Authority> determineAuthoritiesToRemove(NiFiUser user, Set<Authority> authorities) {
        Set<Authority> authoritiesToRemove = EnumSet.copyOf(user.getAuthorities());

        // identify the authorities that need to be removed
        authoritiesToRemove.removeAll(authorities);

        // return the desired authorities
        return authoritiesToRemove;
    }

    /**
     * Verifies the specified users account. Includes obtaining the authorities
     * and group according to the specified authority provider.
     *
     * @param authorityProvider
     * @param user
     */
    protected void verifyAccount(AuthorityProvider authorityProvider, NiFiUser user) {
        // load the roles for the user
        Set<Authority> authorities = authorityProvider.getAuthorities(user.getDn());

        // update the user's authorities
        user.getAuthorities().clear();
        user.getAuthorities().addAll(authorities);

        // get the user group
        user.setUserGroup(authorityProvider.getGroupForUser(user.getDn()));

        // update the users status in case they were previously pending or disabled
        user.setStatus(AccountStatus.ACTIVE);

        // update the users last verified time - this timestampt shouldn't be record
        // until the both the user's authorities and group have been synced
        Date now = new Date();
        user.setLastVerified(now);
    }

}
