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
package org.apache.nifi.authorization.single.user;

import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single User Authorizer
 */
public class SingleUserAuthorizer implements Authorizer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUserAuthorizer.class);

    @Override
    public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {
        return AuthorizationResult.approved();
    }

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
        LOGGER.info("Initializing Authorizer");
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        LOGGER.info("Configuring Authorizer");
    }

    @Override
    public void preDestruction() throws AuthorizerDestructionException {
        LOGGER.info("Destroying Authorizer");
    }
}
