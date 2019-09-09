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
import org.apache.nifi.authorization.AuthorizationAuditor;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.user.NiFiUser;

import java.util.HashMap;
import java.util.Map;

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
     * @return the resource
     */
    Resource getResource();

    /**
     * The originally requested resource for this Authorizable. Because policies are inherited, if a resource
     * does not have a policy, this Authorizable may represent a parent resource and this method will return
     * the originally requested resource.
     *
     * @return the originally requested resource
     */
    default Resource getRequestedResource() {
        return getResource();
    }

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
            return AuthorizationResult.denied("Unknown user.");
        }

        final Map<String,String> userContext;
        if (user.getClientAddress() != null && !user.getClientAddress().trim().isEmpty()) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final Resource resource = getResource();
        final Resource requestedResource = getRequestedResource();
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(user.getIdentity())
                .groups(user.getGroups())
                .anonymous(user.isAnonymous())
                .accessAttempt(false)
                .action(action)
                .resource(resource)
                .requestedResource(requestedResource)
                .resourceContext(resourceContext)
                .userContext(userContext)
                .explanationSupplier(() -> {
                    // build the safe explanation
                    final StringBuilder safeDescription = new StringBuilder("Unable to ");

                    if (RequestAction.READ.equals(action)) {
                        safeDescription.append("view ");
                    } else {
                        safeDescription.append("modify ");
                    }
                    safeDescription.append(resource.getSafeDescription()).append(".");

                    return safeDescription.toString();
                })
                .build();

        // perform the authorization
        final AuthorizationResult result = authorizer.authorize(request);

        // verify the results
        if (Result.ResourceNotFound.equals(result.getResult())) {
            final Authorizable parent = getParentAuthorizable();
            if (parent == null) {
                return AuthorizationResult.denied("No applicable policies could be found.");
            } else {
                // create a custom authorizable to override the safe description but still defer to the parent authorizable
                final Authorizable parentProxy = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return parent.getParentAuthorizable();
                    }

                    @Override
                    public Resource getRequestedResource() {
                        return requestedResource;
                    }

                    @Override
                    public Resource getResource() {
                        final Resource parentResource = parent.getResource();
                        return new Resource() {
                            @Override
                            public String getIdentifier() {
                                return parentResource.getIdentifier();
                            }

                            @Override
                            public String getName() {
                                return parentResource.getName();
                            }

                            @Override
                            public String getSafeDescription() {
                                return resource.getSafeDescription();
                            }
                        };
                    }
                };
                return parentProxy.checkAuthorization(authorizer, action, user, resourceContext);
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
            throw new AccessDeniedException("Unknown user.");
        }

        final Map<String,String> userContext;
        if (user.getClientAddress() != null && !user.getClientAddress().trim().isEmpty()) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final Resource resource = getResource();
        final Resource requestedResource = getRequestedResource();
        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .identity(user.getIdentity())
                .groups(user.getGroups())
                .anonymous(user.isAnonymous())
                .accessAttempt(true)
                .action(action)
                .resource(resource)
                .requestedResource(requestedResource)
                .resourceContext(resourceContext)
                .userContext(userContext)
                .explanationSupplier(() -> {
                    // build the safe explanation
                    final StringBuilder safeDescription = new StringBuilder("Unable to ");

                    if (RequestAction.READ.equals(action)) {
                        safeDescription.append("view ");
                    } else {
                        safeDescription.append("modify ");
                    }
                    safeDescription.append(resource.getSafeDescription()).append(".");

                    return safeDescription.toString();
                })
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        if (Result.ResourceNotFound.equals(result.getResult())) {
            final Authorizable parent = getParentAuthorizable();
            if (parent == null) {
                final AuthorizationResult failure = AuthorizationResult.denied("No applicable policies could be found.");

                // audit authorization request
                if (authorizer instanceof AuthorizationAuditor) {
                    ((AuthorizationAuditor) authorizer).auditAccessAttempt(request, failure);
                }

                // denied
                throw new AccessDeniedException(failure.getExplanation());
            } else {
                // create a custom authorizable to override the safe description but still defer to the parent authorizable
                final Authorizable parentProxy = new Authorizable() {
                    @Override
                    public Authorizable getParentAuthorizable() {
                        return parent.getParentAuthorizable();
                    }

                    @Override
                    public Resource getRequestedResource() {
                        return requestedResource;
                    }

                    @Override
                    public Resource getResource() {
                        final Resource parentResource = parent.getResource();
                        return new Resource() {
                            @Override
                            public String getIdentifier() {
                                return parentResource.getIdentifier();
                            }

                            @Override
                            public String getName() {
                                return parentResource.getName();
                            }

                            @Override
                            public String getSafeDescription() {
                                return resource.getSafeDescription();
                            }
                        };
                    }
                };
                parentProxy.authorize(authorizer, action, user, resourceContext);
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
