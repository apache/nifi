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
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.web.ResourceNotFoundException;

import java.util.List;
import java.util.Map;

/**
 * Authorizable for authorizing access to data. Data based authorizable requires authorization for the entire DN chain.
 */
public class DataAuthorizable implements Authorizable, EnforcePolicyPermissionsThroughBaseResource {
    final Authorizable authorizable;

    public DataAuthorizable(final Authorizable authorizable) {
        this.authorizable = authorizable;
    }

    @Override
    public Authorizable getBaseAuthorizable() {
        return authorizable;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        if (authorizable.getParentAuthorizable() == null) {
            return null;
        } else {
            return new DataAuthorizable(authorizable.getParentAuthorizable());
        }
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getDataResource(authorizable.getResource());
    }

    @Override
    public AuthorizationResult checkAuthorization(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) {
        if (user == null) {
            return AuthorizationResult.denied("Unknown user");
        }

        AuthorizationResult result = null;

        // calculate the dn chain
        final List<String> dnChain = NiFiUserUtils.buildProxiedEntitiesChain(user);
        for (final String identity : dnChain) {
            try {
                final String clientAddress = user.getIdentity().equals(identity) ? user.getClientAddress() : null;
                final NiFiUser chainUser = new StandardNiFiUser(identity, clientAddress) {
                    @Override
                    public boolean isAnonymous() {
                        // allow current user to drive anonymous flag as anonymous users are never chained... supports single user case
                        return user.isAnonymous();
                    }
                };

                result = Authorizable.super.checkAuthorization(authorizer, action, chainUser, resourceContext);
            } catch (final ResourceNotFoundException e) {
                result = AuthorizationResult.denied("Unknown source component.");
            }

            if (!Result.Approved.equals(result.getResult())) {
                break;
            }
        }

        if (result == null) {
            result = AuthorizationResult.denied();
        }

        return result;
    }

    @Override
    public void authorize(Authorizer authorizer, RequestAction action, NiFiUser user, Map<String, String> resourceContext) throws AccessDeniedException {
        if (user == null) {
            throw new AccessDeniedException("Unknown user");
        }

        // calculate the dn chain
        final List<String> dnChain = NiFiUserUtils.buildProxiedEntitiesChain(user);
        for (final String identity : dnChain) {
            try {
                final String clientAddress = user.getIdentity().equals(identity) ? user.getClientAddress() : null;
                final NiFiUser chainUser = new StandardNiFiUser(identity, clientAddress) {
                    @Override
                    public boolean isAnonymous() {
                        // allow current user to drive anonymous flag as anonymous users are never chained... supports single user case
                        return user.isAnonymous();
                    }
                };

                Authorizable.super.authorize(authorizer, action, chainUser, resourceContext);
            } catch (final ResourceNotFoundException e) {
                throw new AccessDeniedException("Unknown source component.");
            }
        }
    }
}
