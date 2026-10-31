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

import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;

public class SystemTestAuthorizer implements Authorizer {
    private static final String DENY_SOURCE_DATA_PROPERTY = "Deny Source Data Identity";
    private static final String DATA_RESOURCE_PREFIX = "/data/";

    private volatile String identityDeniedSourceData;

    @Override
    public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {
        if (request.getAction() == RequestAction.READ && identityDeniedSourceData != null && identityDeniedSourceData.equals(request.getIdentity())
                && request.getRequestedResource().getIdentifier().startsWith(DATA_RESOURCE_PREFIX)) {
            return AuthorizationResult.denied("Read Source Data is not granted for this system-test identity");
        }

        return AuthorizationResult.approved();
    }

    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {

    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        identityDeniedSourceData = configurationContext.getProperties().get(DENY_SOURCE_DATA_PROPERTY);
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {

    }
}
