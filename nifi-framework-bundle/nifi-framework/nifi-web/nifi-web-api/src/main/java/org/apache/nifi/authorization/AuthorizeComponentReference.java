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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.api.dto.BundleDTO;

import java.util.Map;

public final class AuthorizeComponentReference {
    /**
     * Authorize configuration of specified Component Type including restrictions and referenced Controller Services
     *
     * @param authorizer Authorizer responsible for handling decisions
     * @param authorizableLookup Authorizable Lookup for resolving referenced Controller Services
     * @param componentType Component Type to be evaluated
     * @param componentBundle Component Bundle to be evaluated
     * @param properties Component configuration properties or null when not available for evaluation
     * @param parameterContext Parameter Context or null when not available for evaluation
     */
    public static void authorizeComponentConfiguration(
            final Authorizer authorizer,
            final AuthorizableLookup authorizableLookup,
            final String componentType,
            final BundleDTO componentBundle,
            final Map<String, String> properties,
            final Authorizable parameterContext
    ) {
        ComponentAuthorizable authorizable = null;
        try {
            authorizable = authorizableLookup.getConfigurableComponent(componentType, componentBundle);
            authorizeComponentConfiguration(authorizer, authorizableLookup, authorizable, properties, parameterContext);
        } finally {
            if (authorizable != null) {
                authorizable.cleanUpResources();
            }
        }
    }

    /**
     * Authorize configuration of specified Component including restrictions and referenced Controller Services
     *
     * @param authorizer Authorizer responsible for handling decisions
     * @param authorizableLookup Authorizable Lookup for resolving referenced Controller Services
     * @param componentAuthorizable Component Authorizable to be evaluated
     * @param properties Component configuration properties required
     * @param parameterContext Parameter Context or null when not available for evaluation
     */
    public static void authorizeComponentConfiguration(
            final Authorizer authorizer,
            final AuthorizableLookup authorizableLookup,
            final ComponentAuthorizable componentAuthorizable,
            final Map<String, String> properties,
            final Authorizable parameterContext
    ) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (componentAuthorizable.isRestricted()) {
            componentAuthorizable.getRestrictedAuthorizables().forEach(restrictionAuthorizable ->
                    restrictionAuthorizable.authorize(authorizer, RequestAction.WRITE, user)
            );
        }

        AuthorizeControllerServiceReference.authorizeControllerServiceReferences(properties, componentAuthorizable, authorizer, authorizableLookup);

        if (parameterContext != null) {
            AuthorizeParameterReference.authorizeParameterReferences(properties, authorizer, parameterContext, user);
        }
    }
}
