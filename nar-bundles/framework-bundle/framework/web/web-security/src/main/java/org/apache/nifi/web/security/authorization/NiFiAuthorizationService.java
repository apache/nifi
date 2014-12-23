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
package org.apache.nifi.web.security.authorization;

import java.util.Deque;
import java.util.Iterator;
import org.apache.nifi.admin.service.AccountDisabledException;
import org.apache.nifi.admin.service.AccountNotFoundException;
import org.apache.nifi.admin.service.AccountPendingException;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.web.security.DnUtils;
import org.apache.nifi.user.NiFiUser;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.security.authentication.AccountStatusException;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

/**
 * UserDetailsService that will verify user identity and grant user authorities.
 */
public class NiFiAuthorizationService implements UserDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(NiFiAuthorizationService.class);

    private UserService userService;
    private NiFiProperties properties;

    /**
     * Loads the user details for the specified dn. Method must be synchronized
     * since multiple requests from the same user may be sent simultaneously.
     * Since we don't want to run the account verification process multiple for
     * the same user, we treat each request atomically.
     *
     * @param rawProxyChain
     * @return
     * @throws UsernameNotFoundException
     * @throws org.springframework.dao.DataAccessException
     */
    @Override
    public synchronized UserDetails loadUserByUsername(String rawProxyChain) throws UsernameNotFoundException, DataAccessException {
        NiFiUserDetails userDetails = null;
        final Deque<String> dnList = DnUtils.tokenizeProxyChain(rawProxyChain);

        // ensure valid input
        if (dnList.size() == 0) {
            logger.warn("Malformed proxy chain: " + rawProxyChain);
            throw new UntrustedProxyException("Malformed proxy chain.");
        }

        NiFiUser proxy = null;
        
        // process each part of the proxy chain
        for (final Iterator<String> dnIter = dnList.iterator(); dnIter.hasNext();) {
            final String dn = dnIter.next();

            // if there is another dn after this one, this dn is a proxy for the request
            if (dnIter.hasNext()) {
                try {
                    // get the user details for the proxy
                    final NiFiUserDetails proxyDetails = getNiFiUserDetails(dn);
                    final NiFiUser user = proxyDetails.getNiFiUser();

                    // verify the proxy has the appropriate role
                    if (!user.getAuthorities().contains(Authority.ROLE_PROXY)) {
                        logger.warn(String.format("Proxy '%s' must have '%s' authority. Current authorities: %s", dn, Authority.ROLE_PROXY.toString(), StringUtils.join(user.getAuthorities(), ", ")));
                        throw new UntrustedProxyException(String.format("Untrusted proxy '%s' must be authorized with '%s'.", dn, Authority.ROLE_PROXY.toString()));
                    }
                    
                    // if we've already encountered a proxy, update the chain
                    if (proxy != null) {
                        user.setChain(proxy);
                    }
                    
                    // record this user as the proxy for the next user in the chain
                    proxy = user;
                } catch (UsernameNotFoundException unfe) {
                    // if this proxy is a new user, conditionally create a new account automatically
                    if (properties.getSupportNewAccountRequests()) {
                        try {
                            logger.warn(String.format("Automatic account request generated for unknown proxy: %s", dn));

                            // attempt to create a new user account for the proxying client
                            userService.createPendingUserAccount(dn, "Automatic account request generated for unknown proxy.");

                            // propagate the exception to return the appropriate response
                            throw new UsernameNotFoundException(String.format("An account request was generated for the proxy '%s'.", dn));
                        } catch (AdministrationException ae) {
                            throw new AuthenticationServiceException(String.format("Unable to create an account request for '%s': %s", dn, ae.getMessage()), ae);
                        }
                    } else {
                        logger.warn(String.format("Untrusted proxy '%s' must be authorized with '%s' authority: %s", dn, Authority.ROLE_PROXY.toString(), unfe.getMessage()));
                        throw new UntrustedProxyException(String.format("Untrusted proxy '%s' must be authorized with '%s'.", dn, Authority.ROLE_PROXY.toString()));
                    }
                } catch (AuthenticationException ae) {
                    logger.warn(String.format("Untrusted proxy '%s' must be authorized with '%s' authority: %s", dn, Authority.ROLE_PROXY.toString(), ae.getMessage()));
                    throw new UntrustedProxyException(String.format("Untrusted proxy '%s' must be authorized with '%s'.", dn, Authority.ROLE_PROXY.toString()));
                }
            } else {
                userDetails = getNiFiUserDetails(dn);
                
                // if we've already encountered a proxy, update the chain
                if (proxy != null) {
                    final NiFiUser user = userDetails.getNiFiUser();
                    user.setChain(proxy);
                }
            }
        }

        return userDetails;
    }

    /**
     * Loads the user details for the specified dn.
     *
     * @param dn
     * @return
     */
    private NiFiUserDetails getNiFiUserDetails(String dn) {
        try {
            NiFiUser user = userService.checkAuthorization(dn);
            return new NiFiUserDetails(user);
        } catch (AdministrationException ase) {
            throw new AuthenticationServiceException(String.format("An error occurred while accessing the user credentials for '%s': %s", dn, ase.getMessage()), ase);
        } catch (AccountDisabledException | AccountPendingException e) {
            throw new AccountStatusException(e.getMessage(), e) {
            };
        } catch (AccountNotFoundException anfe) {
            throw new UsernameNotFoundException(anfe.getMessage());
        }
    }

    /* setters */
    public void setUserService(UserService userService) {
        this.userService = userService;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
