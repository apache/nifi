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
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Authorizable that first checks if public access is allowed for the resource and action. If it is then it short-circuits
 * and returns approved, otherwise it continues and delegates to the wrapped Authorizable.
 */
public class PublicCheckingAuthorizable implements Authorizable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PublicCheckingAuthorizable.class);

    private final Authorizable wrappedAuthorizable;
    private final BiFunction<Resource, RequestAction,Boolean> publicResourceCheck;

    public PublicCheckingAuthorizable(final Authorizable wrappedAuthorizable,
                                      final BiFunction<Resource,RequestAction,Boolean> publicResourceCheck) {
        this.wrappedAuthorizable = Objects.requireNonNull(wrappedAuthorizable);
        this.publicResourceCheck = Objects.requireNonNull(publicResourceCheck);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return wrappedAuthorizable.getParentAuthorizable();
    }

    @Override
    public Resource getResource() {
        return wrappedAuthorizable.getResource();
    }

    @Override
    public AuthorizationResult checkAuthorization(final Authorizer authorizer, final RequestAction action, final NiFiUser user,
                                                  final Map<String, String> resourceContext) {
        final Resource resource = getResource();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requested resource is {}", new Object[]{resource.getIdentifier()});
        }

        // if public access is allowed then return approved
        final Boolean isPublicAccessAllowed = publicResourceCheck.apply(resource, action);
        if(isPublicAccessAllowed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Public access is allowed for {}", new Object[]{resource.getIdentifier()});
            }
            return AuthorizationResult.approved();
        }

        // otherwise delegate to the original inheriting authorizable
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Delegating to inheriting authorizable for {}", new Object[]{resource.getIdentifier()});
        }
        return wrappedAuthorizable.checkAuthorization(authorizer, action, user, resourceContext);
    }

    @Override
    public void authorize(final Authorizer authorizer, final RequestAction action, final NiFiUser user,
                          final Map<String, String> resourceContext) throws AccessDeniedException {
        final Resource resource = getResource();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requested resource is {}", new Object[]{resource.getIdentifier()});
        }

        // if public access is allowed then skip authorization and return
        final Boolean isPublicAccessAllowed = publicResourceCheck.apply(resource, action);
        if(isPublicAccessAllowed) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Public access is allowed for {}", new Object[]{resource.getIdentifier()});
            }
            return;
        }

        // otherwise delegate to the original authorizable
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Delegating to inheriting authorizable for {}", new Object[]{resource.getIdentifier()});
        }

        wrappedAuthorizable.authorize(authorizer, action, user, resourceContext);
    }
}
