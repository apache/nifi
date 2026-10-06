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
import org.apache.nifi.web.api.entity.ParameterContextEntity;

import java.util.Collection;
import java.util.List;

/**
 * Authorization helpers for applying Parameter Provider fetched parameters to Parameter Contexts.
 */
public final class AuthorizeParameterProviderApply {

    private AuthorizeParameterProviderApply() {
    }

    /**
     * Authorizes a Parameter Provider apply-parameters request. Requires READ on the Parameter Provider. When the request
     * will create one or more Parameter Contexts, requires WRITE on the Parameter Contexts resource. Requires READ and
     * WRITE on each existing Parameter Context that will be updated.
     *
     * @param parameterProviderId the Parameter Provider identifier
     * @param requiresParameterContextCreation whether the request will create one or more Parameter Contexts
     * @param parameterContextUpdates existing Parameter Contexts that will be updated
     * @param authorizer the Authorizer
     * @param lookup the AuthorizableLookup
     * @param user the current user
     */
    public static void authorizeApplyParameters(
            final String parameterProviderId,
            final boolean requiresParameterContextCreation,
            final Collection<ParameterContextEntity> parameterContextUpdates,
            final Authorizer authorizer,
            final AuthorizableLookup lookup,
            final NiFiUser user
    ) {
        final ComponentAuthorizable parameterProvider = lookup.getParameterProvider(parameterProviderId);
        parameterProvider.getAuthorizable().authorize(authorizer, RequestAction.READ, user);

        if (requiresParameterContextCreation) {
            // Creating a Parameter Context requires the same global WRITE permission as POST /parameter-contexts.
            lookup.getParameterContexts().authorize(authorizer, RequestAction.WRITE, user);
        }

        final Collection<ParameterContextEntity> updates = parameterContextUpdates == null ? List.of() : parameterContextUpdates;
        for (final ParameterContextEntity context : updates) {
            final Authorizable parameterContext = lookup.getParameterContext(context.getComponent().getId());
            parameterContext.authorize(authorizer, RequestAction.READ, user);
            parameterContext.authorize(authorizer, RequestAction.WRITE, user);
        }
    }
}
