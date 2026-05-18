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
import org.apache.nifi.parameter.ParameterContext;

import java.util.Map;

/**
 * Authorizes configuration-verification requests for any component type that supports verification.
 * Requires WRITE on the target component, plus READ on any Controller Services referenced by
 * the proposed properties.
 */
public final class AuthorizeConfigVerification {

    private AuthorizeConfigVerification() {
    }

    /**
     * Authorize a configuration-verification request against a single ComponentAuthorizable
     *
     * @param authorizer Authorizer used for determining results
     * @param lookup Authorizable Lookup used to resolve referenced Controller Services
     * @param component Component whose configuration is being verified
     * @param proposedProperties Properties submitted for verification
     */
    public static void authorize(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final ComponentAuthorizable component,
            final Map<String, String> proposedProperties
    ) {
        authorize(authorizer, lookup, component, proposedProperties, null);
    }

    /**
     * Authorize a configuration-verification request against a single ComponentAuthorizable with an optional ancestor
     *
     * @param authorizer Authorizer used for determining results
     * @param lookup Authorizable Lookup used to resolve referenced Controller Services
     * @param component Component whose configuration is being verified
     * @param proposedProperties Properties submitted for verification
     * @param ancestor optional parent Authorizable that must also be authorized
     */
    public static void authorize(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final ComponentAuthorizable component,
            final Map<String, String> proposedProperties,
            final Authorizable ancestor
    ) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (ancestor != null) {
            ancestor.authorize(authorizer, RequestAction.WRITE, user);
        }

        component.getAuthorizable().authorize(authorizer, RequestAction.WRITE, user);

        AuthorizeControllerServiceReference.authorizeControllerServiceReferences(proposedProperties, component, authorizer, lookup);

        final ParameterContext parameterContext = component.getParameterContext();
        if (parameterContext != null) {
            AuthorizeParameterReference.authorizeParameterReferences(proposedProperties, authorizer, parameterContext, user);
        }
    }
}
