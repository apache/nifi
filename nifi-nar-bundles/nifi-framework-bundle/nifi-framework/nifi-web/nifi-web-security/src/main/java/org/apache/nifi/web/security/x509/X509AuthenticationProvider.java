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
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
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
    private Authorizer authorizer;

    public X509AuthenticationProvider(final X509IdentityProvider certificateIdentityProvider, final Authorizer authorizer) {
        this.certificateIdentityProvider = certificateIdentityProvider;
        this.authorizer = authorizer;
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
            return new NiFiAuthenticationToken(new NiFiUserDetails(new StandardNiFiUser(authenticationResponse.getIdentity())));
        } else {
            // build the entire proxy chain if applicable - <end-user><proxy1><proxy2>
            final List<String> proxyChain = new ArrayList<>(ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(request.getProxiedEntitiesChain()));
            proxyChain.add(authenticationResponse.getIdentity());

            // add the chain as appropriate to each proxy
            NiFiUser proxy = null;
            for (final ListIterator<String> chainIter = proxyChain.listIterator(proxyChain.size()); chainIter.hasPrevious();) {
                final String identity = chainIter.previous();

                if (chainIter.hasPrevious()) {
                    // authorize this proxy in order to authenticate this user
                    final AuthorizationRequest proxyAuthorizationRequest = new AuthorizationRequest.Builder()
                        .identity(identity)
                        .anonymous(false)
                        .accessAttempt(true)
                        .action(RequestAction.WRITE)
                        .resource(ResourceFactory.getProxyResource())
                        .build();

                    final AuthorizationResult proxyAuthorizationResult = authorizer.authorize(proxyAuthorizationRequest);
                    if (!Result.Approved.equals(proxyAuthorizationResult.getResult())) {
                        throw new UntrustedProxyException(String.format("Untrusted proxy %s", identity));
                    }
                }

                proxy = new StandardNiFiUser(chainIter.previous(), proxy);
            }

            return new NiFiAuthenticationToken(new NiFiUserDetails(proxy));
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return X509AuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
