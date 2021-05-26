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
import org.apache.nifi.registry.security.authorization.AuthorizationResult.Result;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;

import java.util.Map;

public interface InheritingAuthorizable extends Authorizable {

    /**
     * Returns the result of an authorization request for the specified user for the specified action on the specified
     * resource. This method does not imply the user is directly attempting to access the specified resource. If the user is
     * attempting a direct access use Authorizable.authorize().
     *
     * @param authorizer authorizer
     * @param action action
     * @param user user
     * @return is authorized
     */
    default AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) {
        if (user == null) {
            throw new AccessDeniedException("Unknown user.");
        }

        final AuthorizationResult resourceResult = Authorizable.super.checkAuthorization(authorizer, action, user, resourceContext);

        // if we're denied from the resource try inheriting
        if (Result.Denied.equals(resourceResult.getResult()) && getParentAuthorizable() != null) {
            return getParentAuthorizable().checkAuthorization(authorizer, action, user, resourceContext);
        } else {
            return resourceResult;
        }
    }

    /**
     * Authorizes the current user for the specified action on the specified resource. If the current user is
     * not in the access policy for the specified resource, the parent authorizable resource will be checked, recursively
     *
     * @param authorizer authorizer
     * @param action action
     * @param user user
     * @param resourceContext resource context
     */
    default void authorize(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) throws AccessDeniedException {
        if (user == null) {
            throw new AccessDeniedException("Unknown user.");
        }

        try {
            Authorizable.super.authorize(authorizer, action, user, resourceContext);
        } catch (final AccessDeniedException resourceDenied) {
            // if we're denied from the resource try inheriting
            try {
                if (getParentAuthorizable() != null) {
                    getParentAuthorizable().authorize(authorizer, action, user, resourceContext);
                } else {
                    throw resourceDenied;
                }
            } catch (final AccessDeniedException policiesDenied) {
                throw resourceDenied;
            }
        }
    }

}
