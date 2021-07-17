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
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.InvalidAuthenticationException;
import org.apache.nifi.web.security.NiFiAuthenticationProvider;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.UntrustedProxyException;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 *
 */
public class X509AuthenticationProvider extends NiFiAuthenticationProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(X509AuthenticationProvider.class);

    private static final Authorizable PROXY_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getProxyResource();
        }
    };

    private X509IdentityProvider certificateIdentityProvider;
    private Authorizer authorizer;
    final NiFiProperties properties;

    public X509AuthenticationProvider(final X509IdentityProvider certificateIdentityProvider, final Authorizer authorizer, final NiFiProperties nifiProperties) {
        super(nifiProperties, authorizer);
        this.certificateIdentityProvider = certificateIdentityProvider;
        this.authorizer = authorizer;
        this.properties = nifiProperties;
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
            return new NiFiAuthenticationToken(new NiFiUserDetails(new Builder().identity(mappedIdentity).groups(getUserGroups(mappedIdentity)).clientAddress(request.getClientAddress()).build()));
        } else {
            // get the idp groups for the end-user that were sent over in the X-ProxiedEntityGroups header
            final Set<String> endUserIdpGroups = ProxiedEntitiesUtils.tokenizeProxiedEntityGroups(request.getProxiedEntityGroups());

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
                    // prevent anonymous users unless it's been explicitly configured
                    if (!properties.isAnonymousAuthenticationAllowed()) {
                        throw new InvalidAuthenticationException("Anonymous authentication has not been configured.");
                    }

                    identity = StandardNiFiUser.ANONYMOUS_IDENTITY;
                } else {
                    identity = mapIdentity(identity);
                }

                // get the groups from any configured UserGroupProviders
                final Set<String> groups = getUserGroups(identity);

                // only the end-user can have these groups so any other entity in the chain gets an empty set
                final Set<String> idpGroups = chainIter.hasPrevious() ? Collections.emptySet() : endUserIdpGroups;

                // Only set the client address for client making the request because we don't know the clientAddress of the proxied entities
                String clientAddress = (proxy == null) ? request.getClientAddress() : null;
                proxy = createUser(identity, groups, idpGroups, proxy, clientAddress, isAnonymous);

                if (chainIter.hasPrevious()) {
                    try {
                        PROXY_AUTHORIZABLE.authorize(authorizer, RequestAction.WRITE, proxy);
                    } catch (final AccessDeniedException e) {
                        throw new UntrustedProxyException(String.format("Untrusted proxy %s", identity));
                    }
                }
            }

            if (LOGGER.isTraceEnabled()) {
                logProxyChain(proxy);
            }

            return new NiFiAuthenticationToken(new NiFiUserDetails(proxy));
        }
    }

    private void logProxyChain(final NiFiUser chain) {
        final StringBuilder builder = new StringBuilder("\n== Proxy Entity Chain ==");
        NiFiUser user = chain;
        while (user != null) {
            builder.append("\nIdentity: ")
                    .append(user.getIdentity())
                    .append(" , IDP Groups: ")
                    .append(StringUtils.join(user.getIdentityProviderGroups()));
            user = user.getChain();
        }
        builder.append("\n============");
        LOGGER.trace(builder.toString());
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
    protected static NiFiUser createUser(String identity, Set<String> groups, Set<String> idpGroups, NiFiUser chain, String clientAddress, boolean isAnonymous) {
        if (isAnonymous) {
            return StandardNiFiUser.populateAnonymousUser(chain, clientAddress);
        } else {
            return new Builder().identity(identity).groups(groups).identityProviderGroups(idpGroups).chain(chain).clientAddress(clientAddress).build();
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return X509AuthenticationRequestToken.class.isAssignableFrom(authentication);
    }
}
