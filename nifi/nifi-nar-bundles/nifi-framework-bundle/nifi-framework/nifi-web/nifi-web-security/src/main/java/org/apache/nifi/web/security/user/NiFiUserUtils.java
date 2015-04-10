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
package org.apache.nifi.web.security.user;

import java.util.HashSet;
import java.util.Set;
import org.apache.nifi.user.NiFiUser;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Utility methods for retrieving information about the current application
 * user.
 *
 * @author unattributed
 */
public final class NiFiUserUtils {

    /**
     * Return the authorities for the current user.
     *
     * @return
     */
    public static Set<String> getAuthorities() {
        Set<GrantedAuthority> grantedAuthorities = new HashSet<>();

        SecurityContext context = SecurityContextHolder.getContext();
        if (context != null) {
            Authentication authentication = context.getAuthentication();
            if (authentication != null) {
                // get the authorities for the user of the current request
                grantedAuthorities.addAll(authentication.getAuthorities());
            }
        }

        // convert to a list of authorities
        Set<String> authorities = new HashSet<>(grantedAuthorities.size());
        for (GrantedAuthority grantedAuthority : grantedAuthorities) {
            authorities.add(grantedAuthority.getAuthority());
        }

        return authorities;
    }

    /**
     * Returns the current NiFiUser or null if the current user is not a
     * NiFiUser.
     *
     * @return
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
    
    public static String getNiFiUserName() {
        // get the nifi user to extract the username
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            return "unknown";
        } else {
            return user.getUserName();
        }
    }
}
