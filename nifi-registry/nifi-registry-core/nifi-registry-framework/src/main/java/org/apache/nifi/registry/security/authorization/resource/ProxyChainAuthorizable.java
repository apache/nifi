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
package org.apache.nifi.registry.security.authorization.resource;

import org.apache.nifi.registry.security.authorization.AuthorizationResult;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.Resource;
import org.apache.nifi.registry.security.authorization.UntrustedProxyException;
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Authorizable that wraps another Authorizable and applies logic for authorizing the proxy chain, unless the resource
 * allows public access, which then skips authorizing the proxy chain.
 */
public class ProxyChainAuthorizable implements Authorizable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyChainAuthorizable.class);

    private final Authorizable wrappedAuthorizable;
    private final Authorizable proxyAuthorizable;
    private final BiFunction<Resource,RequestAction,Boolean> publicResourceCheck;

    public ProxyChainAuthorizable(final Authorizable wrappedAuthorizable,
                                  final Authorizable proxyAuthorizable,
                                  final BiFunction<Resource,RequestAction,Boolean> publicResourceCheck) {
        this.wrappedAuthorizable = Objects.requireNonNull(wrappedAuthorizable);
        this.proxyAuthorizable = Objects.requireNonNull(proxyAuthorizable);
        this.publicResourceCheck = Objects.requireNonNull(publicResourceCheck);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        if (wrappedAuthorizable.getParentAuthorizable() == null) {
            return null;
        } else {
            final Authorizable parentAuthorizable = wrappedAuthorizable.getParentAuthorizable();
            return new ProxyChainAuthorizable(parentAuthorizable, proxyAuthorizable, publicResourceCheck);
        }
    }

    @Override
    public Resource getResource() {
        return wrappedAuthorizable.getResource();
    }

    @Override
    public AuthorizationResult checkAuthorization(final Authorizer authorizer, final RequestAction action, final NiFiUser user,
                                                  final Map<String, String> resourceContext) {
        final Resource requestResource = wrappedAuthorizable.getRequestedResource();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requested resource is {}", new Object[]{requestResource.getIdentifier()});
        }

        // if public access is allowed then we want to skip proxy authorization so just return
        final Boolean isPublicAccessAllowed = publicResourceCheck.apply(requestResource, action);
        if (isPublicAccessAllowed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Proxy chain will not be checked, public access is allowed for {} on {}",
                        new Object[]{action.toString(), requestResource.getIdentifier()});
            }
            return AuthorizationResult.approved();
        }

        // otherwise public access is not allowed so check the proxy chain for the given action
        NiFiUser proxyUser = user.getChain();
        while (proxyUser  != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Checking proxy [{}] for {}", new Object[]{proxyUser, action});
            }

            // if the proxy is denied then break out of the loop and return a denied result
            final AuthorizationResult proxyAuthorizationResult = proxyAuthorizable.checkAuthorization(authorizer, action, proxyUser);
            if (proxyAuthorizationResult.getResult() == AuthorizationResult.Result.Denied) {
                final String deniedMessage = String.format("Untrusted proxy [%s] for %s operation.", proxyUser.getIdentity(), action.toString());
                return AuthorizationResult.denied(deniedMessage);
            }

            proxyUser = proxyUser.getChain();
        }

        // at this point the proxy chain was approved so continue to check the original Authorizable
        return wrappedAuthorizable.checkAuthorization(authorizer, action, user, resourceContext);
    }

    @Override
    public void authorize(final Authorizer authorizer, final RequestAction action, final NiFiUser user,
                          final Map<String, String> resourceContext) throws AccessDeniedException {
        final Resource requestResource = wrappedAuthorizable.getRequestedResource();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requested resource is {}", new Object[]{requestResource.getIdentifier()});
        }

        // if public access is allowed then we want to skip proxy authorization so just return
        final Boolean isPublicAccessAllowed = publicResourceCheck.apply(requestResource, action);
        if (isPublicAccessAllowed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Proxy chain will not be authorized, public access is allowed for {} on {}",
                        new Object[]{action.toString(), requestResource.getIdentifier()});
            }
            return;
        }

        // otherwise public access is not allowed so authorize proxy chain for the given action
        NiFiUser proxyUser = user.getChain();
        while (proxyUser  != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Authorizing proxy [{}] for {}", new Object[]{proxyUser, action});
            }

            try {
                proxyAuthorizable.authorize(authorizer, action, proxyUser);
            } catch (final AccessDeniedException e) {
                final String actionString = action.toString();
                throw new UntrustedProxyException(String.format("Untrusted proxy [%s] for %s operation.", proxyUser.getIdentity(), actionString));
            }
            proxyUser = proxyUser.getChain();
        }

        // at this point the proxy chain was authorized so continue to authorize the original Authorizable
        wrappedAuthorizable.authorize(authorizer, action, user, resourceContext);
    }

}
