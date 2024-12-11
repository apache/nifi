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

import org.apache.nifi.authorization.user.NiFiUser;

import java.util.HashSet;
import java.util.Set;

public class AuthorizeParameterProviders {

    /**
     * Unresolved Parameter Providers may be unresolved because
     * - The identifier matches an existing Parameter Provider id (unresolved because the proposed id was unchanged from resolved id)
     * - An applicable Parameter Provider does not exist
     * - The user lacks permission to an applicable Parameter Provider
     *
     * If any of those unresolved Parameter Providers do match a local Parameter Provider we need to authorize access to it. If
     * there are unresolved Parameter Providers that do not match a local Parameter Provider we need to ensure the user has
     * permissions to create one.
     *
     * @param unresolvedParameterProviders the unresolved Parameter Providers
     * @param authorizer the Authorizer
     * @param lookup the AuthorizableLookup
     * @param user the current user
     */
    public static void authorizeUnresolvedParameterProviders(final Set<String> unresolvedParameterProviders, final Authorizer authorizer,
                                                             final AuthorizableLookup lookup, final NiFiUser user) {

        // if there are no unresolved Parameter Providers we can return
        if (unresolvedParameterProviders.isEmpty()) {
            return;
        }

        // unresolved parameter providers may be unresolved because
        // - the identifier matches an existing parameter provider id (unresolved because proposed id was unchanged from resolved id)
        // - an applicable parameter provider does not exist
        // - the user lacks permission to an applicable parameter provider
        // for everything unresolved, if it matches an existing parameter provider we need to authorize it.
        final Set<String> unknownParameterProviders = new HashSet<>(unresolvedParameterProviders);
        lookup.getParameterProviders(pp -> {
            // remove this identifier from unknown/unresolved since it actually exists locally
            unknownParameterProviders.remove(pp.getIdentifier());

            // if this unresolved parameter provider matches an existing parameter provider it
            // means the request payload already referenced a local parameter provider so we need
            // to ensure the user has permissions to read it
            return unresolvedParameterProviders.contains(pp.getIdentifier());
        }).forEach(ca -> ca.getAuthorizable().authorize(authorizer, RequestAction.READ, user));

        // if there are remaining unknown parameter providers a new parameter provider will be created,
        // and we need to ensure the user has those permissions too
        if (!unknownParameterProviders.isEmpty()) {
            lookup.getController().authorize(authorizer, RequestAction.WRITE, user);
        }
    }
}
