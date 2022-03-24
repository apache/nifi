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

package org.apache.nifi.provenance.authorization;

import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.provenance.ProvenanceAuthorizableFactory;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.web.ResourceNotFoundException;

public class UserEventAuthorizer implements EventAuthorizer {
    private final Authorizer authorizer;
    private final ProvenanceAuthorizableFactory resourceFactory;
    private final NiFiUser user;

    public UserEventAuthorizer(final Authorizer authorizer, final ProvenanceAuthorizableFactory authorizableFactory, final NiFiUser user) {
        this.authorizer = authorizer;
        this.resourceFactory = authorizableFactory;
        this.user = user;
    }

    @Override
    public boolean isAuthorized(final ProvenanceEventRecord event) {
        if (authorizer == null || user == null) {
            return true;
        }

        final Authorizable eventAuthorizable;
        try {
            eventAuthorizable = resourceFactory.createProvenanceDataAuthorizable(event.getComponentId());
        } catch (final ResourceNotFoundException rnfe) {
            return false;
        }

        final AuthorizationResult result = eventAuthorizable.checkAuthorization(authorizer, RequestAction.READ, user);
        return Result.Approved.equals(result.getResult());
    }

    @Override
    public void authorize(final ProvenanceEventRecord event) {
        if (authorizer == null) {
            return;
        }

        final Authorizable eventAuthorizable = resourceFactory.createProvenanceDataAuthorizable(event.getComponentId());
        eventAuthorizable.authorize(authorizer, RequestAction.READ, user);
    }
}
