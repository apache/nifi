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

import java.util.Map;

/**
 * Authorizes configuration analysis requests.
 * Requires READ on the target component and READ on any Controller Services and Parameter Context
 * consulted for the proposed property values.
 */
public final class AuthorizeComponentAnalysis {

    private AuthorizeComponentAnalysis() {
    }

    /**
     * Authorize a configuration analysis request: READ on the component, then READ on referenced
     * Controller Services and on the Parameter Context when proposed properties reference parameters.
     *
     * @param authorizer Authorizer used for determining results
     * @param lookup Authorizable Lookup used to resolve referenced Controller Services
     * @param component Component whose configuration is being analyzed
     * @param proposedProperties Properties submitted for analysis
     * @param parameterContext Parameter Context used to resolve parameter references, or {@code null} when none applies
     */
    public static void authorize(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final ComponentAuthorizable component,
            final Map<String, String> proposedProperties,
            final Authorizable parameterContext
    ) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        component.getAuthorizable().authorize(authorizer, RequestAction.READ, user);
        AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, lookup, component, proposedProperties, parameterContext);
    }

    /**
     * Authorize only proposed Controller Service and Parameter Context references for an analysis request.
     * Use when another resource (e.g. the controller root) has already been authorized for READ and the
     * target component does not require a separate READ policy for this operation.
     *
     * @param authorizer Authorizer used for determining results
     * @param lookup Authorizable Lookup used to resolve referenced Controller Services
     * @param component Component whose proposed configuration references are evaluated
     * @param proposedProperties Properties submitted for analysis
     * @param parameterContext Parameter Context used to resolve parameter references, or {@code null} when none applies
     */
    public static void authorizeProposedReferences(
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final ComponentAuthorizable component,
            final Map<String, String> proposedProperties,
            final Authorizable parameterContext) {
        AuthorizeComponentReference.authorizeComponentConfiguration(authorizer, lookup, component, proposedProperties, parameterContext);
    }
}
