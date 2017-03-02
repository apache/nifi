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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationProvider;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/**
 *
 */
public class X509AuthenticationProvider extends NiFiAuthenticationProvider {

    private X509IdentityProvider certificateIdentityProvider;
    private Authorizer authorizer;

    public X509AuthenticationProvider(final X509IdentityProvider certificateIdentityProvider, final Authorizer authorizer, final NiFiProperties nifiProperties) {
        super(nifiProperties);
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
            final String mappedIdentity = mapIdentity(authenticationResponse.getIdentity());
            return new NiFiAuthenticationToken(new NiFiUserDetails(new StandardNiFiUser(mappedIdentity, request.getClientAddress())));
        } else {
            // build the entire proxy chain if applicable - <end-user><proxy1><proxy2>
            final List<String> proxyChain = new ArrayList<>(ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(request.getProxiedEntitiesChain()));
            proxyChain.add(authenticationResponse.getIdentity());

            // add the chain as appropriate to each proxy
            NiFiUser proxy = null;
            for (final ListIterator<String> chainIter = proxyChain.listIterator(proxyChain.size()); chainIter.hasPrevious(); ) {
                String identity = chainIter.previous();

                // determine if the user is anonymous
                final boolean isAnonymous = StringUtils.isBlank(identity);
                if (isAnonymous) {
                    identity = StandardNiFiUser.ANONYMOUS_IDENTITY;
                } else {
                    identity = mapIdentity(identity);
                }

                if (chainIter.hasPrevious()) {
                    // authorize this proxy in order to authenticate this user
                    final AuthorizationRequest proxyAuthorizationRequest = new AuthorizationRequest.Builder()
                            .identity(identity)
                            .anonymous(isAnonymous)
                            .accessAttempt(true)
                            .action(RequestAction.WRITE)
                            .resource(ResourceFactory.getProxyResource())
                            .userContext(proxy == null ? getUserContext(request) : null) // only set the context for the real user
                            .build();

                    final AuthorizationResult proxyAuthorizationResult = authorizer.authorize(proxyAuthorizationRequest);
                    if (!Result.Approved.equals(proxyAuthorizationResult.getResult())) {
                        throw new UntrustedProxyException(String.format("Untrusted proxy %s", identity));
                    }
                }

                // Only set the client address for user making the request because we don't know the client address of the proxies
                String clientAddress = (proxy == null) ? request.getClientAddress() : null;
                proxy = createUser(identity, proxy, clientAddress, isAnonymous);
            }

            return new NiFiAuthenticationToken(new NiFiUserDetails(proxy));
        }
    }

    /**
     * Returns a regular user populated with the provided values, or if the user should be anonymous, a well-formed instance of the anonymous user with the provided values.
     *
     * @param identity      the user's identity
     * @param chain         the proxied entities
     * @param clientAddress the requesting IP address
     * @param isAnonymous   if true, an anonymous user will be returned (identity will be ignored)
     * @return the populated user
     */
    protected static NiFiUser createUser(String identity, NiFiUser chain, String clientAddress, boolean isAnonymous) {
        if (isAnonymous) {
            return StandardNiFiUser.populateAnonymousUser(chain, clientAddress);
        } else {
            return new StandardNiFiUser(identity, chain, clientAddress);
        }
    }

    private Map<String, String> getUserContext(final X509AuthenticationRequestToken request) {
        final Map<String, String> userContext;
        if (!StringUtils.isBlank(request.getClientAddress())) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), request.getClientAddress());
        } else {
            userContext = null;
        }
        return userContext;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return X509AuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
