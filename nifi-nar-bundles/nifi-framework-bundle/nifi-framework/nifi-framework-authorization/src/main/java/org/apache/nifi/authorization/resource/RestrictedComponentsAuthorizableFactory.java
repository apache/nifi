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
package org.apache.nifi.authorization.resource;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.RequiredPermission;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RestrictedComponentsAuthorizableFactory {

    private static final Authorizable RESTRICTED_COMPONENTS_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getRestrictedComponentsResource();
        }
    };

    public static Authorizable getRestrictedComponentsAuthorizable() {
        return RESTRICTED_COMPONENTS_AUTHORIZABLE;
    }

    public static Authorizable getRestrictedComponentsAuthorizable(final RequiredPermission requiredPermission) {
        return new Authorizable() {
            @Override
            public Authorizable getParentAuthorizable() {
                return RESTRICTED_COMPONENTS_AUTHORIZABLE;
            }

            @Override
            public Resource getResource() {
                return ResourceFactory.getRestrictedComponentsResource(requiredPermission);
            }

            @Override
            public AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) {
                if (user == null) {
                    return AuthorizationResult.denied("Unknown user.");
                }

                final AuthorizationResult resourceResult = Authorizable.super.checkAuthorization(authorizer, action, user, resourceContext);

                // if we're denied from the resource try inheriting
                if (Result.Denied.equals(resourceResult.getResult())) {
                    return getParentAuthorizable().checkAuthorization(authorizer, action, user, resourceContext);
                } else {
                    return resourceResult;
                }
            }

            @Override
            public void authorize(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) throws AccessDeniedException {
                if (user == null) {
                    throw new AccessDeniedException("Unknown user.");
                }

                try {
                    Authorizable.super.authorize(authorizer, action, user, resourceContext);
                } catch (final AccessDeniedException resourceDenied) {
                    // if we're denied from the resource try inheriting
                    try {
                        getParentAuthorizable().authorize(authorizer, action, user, resourceContext);
                    } catch (final AccessDeniedException policiesDenied) {
                        throw resourceDenied;
                    }
                }
            }
        };
    }

    public static Set<Authorizable> getRestrictedComponentsAuthorizable(final Class<?> configurableComponentClass) {
        final Set<Authorizable> authorizables = new HashSet<>();

        final Restricted restricted = configurableComponentClass.getAnnotation(Restricted.class);

        if (restricted != null) {
            final Restriction[] restrictions = restricted.restrictions();

            if (restrictions != null && restrictions.length > 0) {
                Arrays.stream(restrictions).forEach(restriction -> authorizables.add(getRestrictedComponentsAuthorizable(restriction.requiredPermission())));
            } else {
                authorizables.add(getRestrictedComponentsAuthorizable());
            }
        }

        return authorizables;
    }

}
