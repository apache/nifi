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

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.user.NiFiUser;

import java.util.Map;

/**
 * Authorizable for policies of an Authorizable.
 */
public class AccessPolicyAuthorizable implements Authorizable, EnforcePolicyPermissionsThroughBaseResource {

    private static final Authorizable POLICIES_AUTHORIZABLE = new Authorizable() {
        @Override
        public Authorizable getParentAuthorizable() {
            return null;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getPoliciesResource();
        }
    };

    final Authorizable authorizable;

    public AccessPolicyAuthorizable(Authorizable authorizable) {
        this.authorizable = authorizable;
    }

    @Override
    public Authorizable getBaseAuthorizable() {
        return authorizable;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        final Authorizable effectiveAuthorizable = getEffectiveAuthorizable();
        if (effectiveAuthorizable.getParentAuthorizable() == null) {
            return POLICIES_AUTHORIZABLE;
        } else {
            return new AccessPolicyAuthorizable(effectiveAuthorizable.getParentAuthorizable());
        }
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getPolicyResource(getEffectiveAuthorizable().getResource());
    }

    private Authorizable getEffectiveAuthorizable() {
        // possibly consider the base resource if the authorizable uses it to enforce policy permissions
        if (authorizable instanceof EnforcePolicyPermissionsThroughBaseResource) {
            final Authorizable baseAuthorizable = ((EnforcePolicyPermissionsThroughBaseResource) authorizable).getBaseAuthorizable();

            // if the base authorizable is for a policy, we don't want to use the base otherwise it would keep unwinding and would eventually
            // evaluate to the policy of the component and not the policy of the policies for the component
            if (baseAuthorizable instanceof AccessPolicyAuthorizable) {
                return authorizable;
            } else {
                return baseAuthorizable;
            }
        } else {
            return authorizable;
        }
    }

    @Override
    public AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) {
        if (user == null) {
            throw new AccessDeniedException("Unknown user.");
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
}
