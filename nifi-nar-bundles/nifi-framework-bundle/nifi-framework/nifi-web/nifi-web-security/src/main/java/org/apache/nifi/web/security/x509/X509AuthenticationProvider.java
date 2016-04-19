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
package org.apache.nifi.web.security.x509;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.apache.nifi.web.security.user.NiFiUserDetails;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 *
 */
public class X509AuthenticationProvider implements AuthenticationProvider {

    private X509IdentityProvider certificateIdentityProvider;

    public X509AuthenticationProvider(X509IdentityProvider certificateIdentityProvider) {
        this.certificateIdentityProvider = certificateIdentityProvider;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        final X509AuthenticationRequestToken request = (X509AuthenticationRequestToken) authentication;

        // attempt to authenticate if certificates were found
        final AuthenticationResponse authenticationResponse;
        try {
            authenticationResponse = certificateIdentityProvider.authenticate(request.getCertificates());
        } catch (final IllegalArgumentException iae) {
            throw new InvalidAuthenticationException(iae.getMessage(), iae);
        }

        if (StringUtils.isBlank(request.getProxiedEntitiesChain())) {
            return new NiFiAuthenticationToken(new NiFiUserDetails(new NiFiUser(authenticationResponse.getIdentity(), authenticationResponse.getUsername(), null)));
        } else {
            // build the entire proxy chain if applicable - <end-user><proxy1><proxy2>
            final List<String> proxyChain = new ArrayList<>(ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(request.getProxiedEntitiesChain()));
            proxyChain.add(authenticationResponse.getIdentity());

            // add the chain as appropriate to each proxy
            NiFiUser proxy = null;
            for (final ListIterator<String> chainIter = proxyChain.listIterator(proxyChain.size()); chainIter.hasPrevious();) {
                proxy = new NiFiUser(chainIter.previous(), proxy);
            }

            return new NiFiAuthenticationToken(new NiFiUserDetails(proxy));
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return X509AuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
