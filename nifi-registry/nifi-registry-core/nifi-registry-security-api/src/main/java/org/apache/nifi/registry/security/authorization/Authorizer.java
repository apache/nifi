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
package org.apache.nifi.registry.security.authorization;

import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;

/**
 * Authorizes user requests.
 */
public interface Authorizer {

    /**
     * Determines if the specified user/entity is authorized to access the specified resource within the given context.
     * These details are all contained in the AuthorizationRequest.
     *
     * NOTE: This method will be called often and frequently. Because of this, if the underlying implementation needs to
     * make remote calls or expensive calculations those should probably be done asynchronously and/or cache the results.
     *
     * @param   request The authorization request
     * @return  the authorization result
     * @throws  AuthorizationAccessException if unable to access the policies
     */
    AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException;

    /**
     * Called immediately after instance creation for implementers to perform additional setup
     *
     * @param initializationContext in which to initialize
     */
    void initialize(AuthorizerInitializationContext initializationContext) throws SecurityProviderCreationException;

    /**
     * Called to configure the Authorizer.
     *
     * @param configurationContext at the time of configuration
     * @throws SecurityProviderCreationException for any issues configuring the provider
     */
    void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException;

    /**
     * Called immediately before instance destruction for implementers to release resources.
     *
     * @throws SecurityProviderDestructionException If pre-destruction fails.
     */
    void preDestruction() throws SecurityProviderDestructionException;

}
