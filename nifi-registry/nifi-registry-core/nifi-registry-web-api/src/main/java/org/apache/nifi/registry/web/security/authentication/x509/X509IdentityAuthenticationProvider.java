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
package org.apache.nifi.registry.web.security.authentication.x509;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.apache.nifi.registry.security.authorization.user.NiFiUserDetails;
import org.apache.nifi.registry.security.authorization.user.StandardNiFiUser;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.security.util.ProxiedEntitiesUtils;
import org.apache.nifi.registry.web.security.authentication.AuthenticationRequestToken;
import org.apache.nifi.registry.web.security.authentication.AuthenticationSuccessToken;
import org.apache.nifi.registry.web.security.authentication.IdentityAuthenticationProvider;

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public class X509IdentityAuthenticationProvider extends IdentityAuthenticationProvider {

    public X509IdentityAuthenticationProvider(Authorizer authorizer, IdentityProvider identityProvider, IdentityMapper identityMapper) {
        super(authorizer, identityProvider, identityMapper);
    }

    @Override
    protected AuthenticationSuccessToken buildAuthenticatedToken(
            AuthenticationRequestToken requestToken,
            AuthenticationResponse response) {

        final AuthenticationRequest authenticationRequest = requestToken.getAuthenticationRequest();

        final Object requestDetails = authenticationRequest.getDetails();
        if (requestDetails == null || !(requestDetails instanceof X509AuthenticationRequestDetails)) {
            throw new IllegalStateException("Invalid request details specified");
        }

        final X509AuthenticationRequestDetails x509RequestDetails = (X509AuthenticationRequestDetails) authenticationRequest.getDetails();

        final String proxiedEntitiesChain = x509RequestDetails.getProxiedEntitiesChain();
        if (StringUtils.isBlank(proxiedEntitiesChain)) {
            return super.buildAuthenticatedToken(requestToken, response);
        }

        // build the entire proxy chain if applicable - <end-user><proxy1><proxy2>
        final List<String> proxyChain = ProxiedEntitiesUtils.tokenizeProxiedEntitiesChain(proxiedEntitiesChain);
        proxyChain.add(response.getIdentity());

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

            final Set<String> groups = getUserGroups(identity);

            // Only set the client address for client making the request because we don't know the clientAddress of the proxied entities
            String clientAddress = (proxy == null) ? requestToken.getClientAddress() : null;
            proxy = createUser(identity, groups, proxy, clientAddress, isAnonymous);
        }

        // Defer authorization of proxy until later in FrameworkAuthorizer

        return new AuthenticationSuccessToken(new NiFiUserDetails(proxy));
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
    private static NiFiUser createUser(String identity, Set<String> groups, NiFiUser chain, String clientAddress, boolean isAnonymous) {
        if (isAnonymous) {
            return StandardNiFiUser.populateAnonymousUser(chain, clientAddress);
        } else {
            return new StandardNiFiUser.Builder().identity(identity).groups(groups).chain(chain).clientAddress(clientAddress).build();
        }
    }

}
