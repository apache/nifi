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
package org.apache.nifi.web.security;

import org.apache.nifi.web.security.token.NewAccountAuthenticationRequestToken;
import org.apache.nifi.web.security.token.NewAccountAuthenticationToken;
import org.apache.nifi.web.security.token.NiFiAuthenticationRequestToken;
import org.apache.nifi.web.security.token.NiFiAuthorizationToken;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

/**
 *
 */
public class NiFiAuthenticationProvider implements AuthenticationProvider {

    private final AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService;

    public NiFiAuthenticationProvider(final AuthenticationUserDetailsService<NiFiAuthenticationRequestToken> userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        final NiFiAuthenticationRequestToken request = (NiFiAuthenticationRequestToken) authentication;

        try {
            // defer to the nifi user details service to authorize the user
            final UserDetails userDetails = userDetailsService.loadUserDetails(request);

            // build an authentication for accesing nifi
            final NiFiAuthorizationToken result = new NiFiAuthorizationToken(userDetails);
            result.setDetails(request.getDetails());
            return result;
        } catch (final UsernameNotFoundException unfe) {
            // if the authentication request is for a new account and it could not be authorized because the user was not found,
            // return the token so the new account could be created. this must go here toe nsure that any proxies have been authorized
            if (isNewAccountAuthenticationToken(request)) {
                return new NewAccountAuthenticationToken(((NewAccountAuthenticationRequestToken) authentication).getNewAccountRequest());
            } else {
                throw unfe;
            }
        }
    }

    private boolean isNewAccountAuthenticationToken(final Authentication authentication) {
        return NewAccountAuthenticationRequestToken.class.isAssignableFrom(authentication.getClass());
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return NiFiAuthenticationRequestToken.class.isAssignableFrom(authentication);
    }

}
