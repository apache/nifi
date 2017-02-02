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
package org.apache.nifi.authorization.user;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Utility methods for retrieving information about the current application user.
 *
 */
public final class NiFiUserUtils {

    /**
     * Returns the current NiFiUser or null if the current user is not a NiFiUser.
     *
     * @return user
     */
    public static NiFiUser getNiFiUser() {
        NiFiUser user = null;

        // obtain the principal in the current authentication
        final SecurityContext context = SecurityContextHolder.getContext();
        final Authentication authentication = context.getAuthentication();
        if (authentication != null) {
            Object principal = authentication.getPrincipal();
            if (principal instanceof NiFiUserDetails) {
                user = ((NiFiUserDetails) principal).getNiFiUser();
            }
        }

        return user;
    }

    public static String getNiFiUserIdentity() {
        // get the nifi user to extract the username
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            return "unknown";
        } else {
            return user.getIdentity();
        }
    }

    /**
     * Builds the proxy chain for the specified user.
     *
     * @param user The current user
     * @return The proxy chain for that user in List form
     */
    public static List<String> buildProxiedEntitiesChain(final NiFiUser user) {
        // calculate the dn chain
        final List<String> proxyChain = new ArrayList<>();

        // build the dn chain
        NiFiUser chainedUser = user;
        while (chainedUser != null) {
            // add the entry for this user
            if (chainedUser.isAnonymous()) {
                // use an empty string to represent an anonymous user in the proxy entities chain
                proxyChain.add(StringUtils.EMPTY);
            } else {
                proxyChain.add(chainedUser.getIdentity());
            }

            // go to the next user in the chain
            chainedUser = chainedUser.getChain();
        }

        return proxyChain;
    }
}
