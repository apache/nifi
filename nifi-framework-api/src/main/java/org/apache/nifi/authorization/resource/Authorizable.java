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
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.user.NiFiUser;

import java.util.Map;
import java.util.HashMap;

public interface Authorizable {

    /**
     * The parent for this Authorizable. May be null.
     *
     * @return the parent authorizable or null
     */
    Authorizable getParentAuthorizable();

    /**
     * The Resource for this Authorizable.
     *
     * @return the parent resource
     */
    Resource getResource();

    /**
     * Returns whether the current user is authorized for the specified action on the specified resource. This
     * method does not imply the user is directly attempting to access the specified resource. If the user is
     * attempting a direct access use Authorizable.authorize().
     *
     * @param authorizer authorizer
     * @param action action
     * @return is authorized
     */
    default boolean isAuthorized(Authorizer authorizer, RequestAction action, NiFiUser user) {
        return Result.Approved.equals(checkAuthorization(authorizer, action, user).getResult());
    }

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
            return AuthorizationResult.denied("Unknown user");
        }

        final Map<String,String> userContext;
        if (user.getClientAddress() != null && !user.getClientAddress().trim().isEmpty()) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        // build the request
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(user.getIdentity())
                .anonymous(user.isAnonymous())
                .accessAttempt(false)
                .action(action)
                .resource(getResource())
                .resourceContext(resourceContext)
                .userContext(userContext)
                .build();

        // perform the authorization
        final AuthorizationResult result = authorizer.authorize(request);

        // verify the results
        if (Result.ResourceNotFound.equals(result.getResult())) {
            final Authorizable parent = getParentAuthorizable();
            if (parent == null) {
                return AuthorizationResult.denied();
            } else {
                return parent.checkAuthorization(authorizer, action, user, resourceContext);
            }
        } else {
            return result;
        }
    }

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
    default AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user) {
        return checkAuthorization(authorizer, action, user, null);
    }

    /**
     * Authorizes the current user for the specified action on the specified resource. This method does imply the user is
     * directly accessing the specified resource.
     *
     * @param authorizer authorizer
     * @param action action
     * @param user user
     * @param resourceContext resource context
     */
    default void authorize(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) throws AccessDeniedException {
        if (user == null) {
            throw new AccessDeniedException("Unknown user");
        }

        final Map<String,String> userContext;
        if (user.getClientAddress() != null && !user.getClientAddress().trim().isEmpty()) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(user.getIdentity())
                .anonymous(user.isAnonymous())
                .accessAttempt(true)
                .action(action)
                .resource(getResource())
                .resourceContext(resourceContext)
                .userContext(userContext)
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        if (Result.ResourceNotFound.equals(result.getResult())) {
            final Authorizable parent = getParentAuthorizable();
            if (parent == null) {
                throw new AccessDeniedException("Access is denied");
            } else {
                parent.authorize(authorizer, action, user, resourceContext);
            }
        } else if (Result.Denied.equals(result.getResult())) {
            throw new AccessDeniedException(result.getExplanation());
        }
    }

    /**
     * Authorizes the current user for the specified action on the specified resource. This method does imply the user is
     * directly accessing the specified resource.
     *
     * @param authorizer authorizer
     * @param action action
     * @param user user
     */
    default void authorize(Authorizer authorizer, RequestAction action, NiFiUser user) throws AccessDeniedException {
        authorize(authorizer, action, user, null);
    }
}
