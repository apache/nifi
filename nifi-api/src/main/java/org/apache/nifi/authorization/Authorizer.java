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

import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;

/**
 * Authorizes user requests.
 */
public interface Authorizer {

    /**
     * Determines if the specified user/entity is authorized to access the specified resource within the given context.
     *
     * @param   request The authorization request
     * @return  the authorization result
     * @throws  AuthorityAccessException if unable to access the authorities
     */
    AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException;

    /**
     * Called immediately after instance creation for implementers to perform additional setup
     *
     * @param initializationContext in which to initialize
     */
    void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException;

    /**
     * Called to configure the Authorizer.
     *
     * @param configurationContext at the time of configuration
     * @throws AuthorizerCreationException for any issues configuring the provider
     */
    void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException;

    /**
     * Called immediately before instance destruction for implementers to release resources.
     *
     * @throws AuthorizerDestructionException If pre-destruction fails.
     */
    void preDestruction() throws AuthorizerDestructionException;

}
