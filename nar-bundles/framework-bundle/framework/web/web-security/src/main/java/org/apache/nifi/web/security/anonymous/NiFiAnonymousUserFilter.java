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
package org.apache.nifi.web.security.anonymous;

import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

/**
 * Custom AnonymouseAuthenticationFilter used to grant additional authorities
 * depending on the current operating mode.
 */
public class NiFiAnonymousUserFilter extends AnonymousAuthenticationFilter {

    private static final Logger anonymousUserFilterLogger = LoggerFactory.getLogger(NiFiAnonymousUserFilter.class);

    private static final String ANONYMOUS_KEY = "anonymousNifiKey";

    private NiFiProperties properties;
    private UserService userService;

    public NiFiAnonymousUserFilter() {
        super(ANONYMOUS_KEY);
    }

    @Override
    protected Authentication createAuthentication(HttpServletRequest request) {
        Authentication authentication;
        try {
            // load the anonymous user from the database
            NiFiUser user = userService.getUserByDn(NiFiUser.ANONYMOUS_USER_DN);
            NiFiUserDetails userDetails = new NiFiUserDetails(user);

            // get the granted authorities
            List<GrantedAuthority> authorities = new ArrayList<>(userDetails.getAuthorities());
            authentication = new AnonymousAuthenticationToken(ANONYMOUS_KEY, userDetails, authorities);
        } catch (AdministrationException ase) {
            // record the issue
            anonymousUserFilterLogger.warn("Unable to load anonymous user from accounts database: " + ase.getMessage());
            if (anonymousUserFilterLogger.isDebugEnabled()) {
                anonymousUserFilterLogger.warn(StringUtils.EMPTY, ase);
            }

            // defer to the base implementation
            authentication = super.createAuthentication(request);
        }
        return authentication;
    }

    /**
     * Only supports anonymous users for non-secure requests or one way ssl.
     *
     * @param request
     * @return
     */
    @Override
    protected boolean applyAnonymousForThisRequest(HttpServletRequest request) {
        // anonymous for non secure requests
        if ("http".equalsIgnoreCase(request.getScheme())) {
            return true;
        }

        return !properties.getNeedClientAuth();
    }

    /* setters */
    public void setUserService(UserService userService) {
        this.userService = userService;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
