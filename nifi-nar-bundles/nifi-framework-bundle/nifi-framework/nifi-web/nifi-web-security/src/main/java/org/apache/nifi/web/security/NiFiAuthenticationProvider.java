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

import org.apache.nifi.web.security.token.NewAccountAuthorizationRequestToken;
import org.apache.nifi.web.security.token.NewAccountAuthorizationToken;
import org.apache.nifi.web.security.token.NiFiAuthorizationRequestToken;
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

    private final AuthenticationUserDetailsService<NiFiAuthorizationRequestToken> userDetailsService;

    public NiFiAuthenticationProvider(final AuthenticationUserDetailsService<NiFiAuthorizationRequestToken> userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        final NiFiAuthorizationRequestToken request = (NiFiAuthorizationRequestToken) authentication;

        try {
            // defer to the nifi user details service to authorize the user
            final UserDetails userDetails = userDetailsService.loadUserDetails(request);

            // build a token for accesing nifi
            final NiFiAuthorizationToken result = new NiFiAuthorizationToken(userDetails);
            result.setDetails(request.getDetails());
            return result;
        } catch (final UsernameNotFoundException unfe) {
            // if the authorization request is for a new account and it could not be authorized because the user was not found,
            // return the token so the new account could be created. this must go here to ensure that any proxies have been authorized
            if (isNewAccountAuthenticationToken(request)) {
                return new NewAccountAuthorizationToken(((NewAccountAuthorizationRequestToken) authentication).getNewAccountRequest());
            } else {
                throw unfe;
            }
        }
    }

    private boolean isNewAccountAuthenticationToken(final Authentication authentication) {
        return NewAccountAuthorizationRequestToken.class.isAssignableFrom(authentication.getClass());
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return NiFiAuthorizationRequestToken.class.isAssignableFrom(authentication);
    }

}
