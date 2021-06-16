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
package org.apache.nifi.registry.client;

import java.io.IOException;

/**
 * Client for interacting with the AccessResource.
 */
public interface AccessClient {

    /**
     * Get an access token by authenticating with a username and password aginst the configured identity provider.
     *
     * @param username the username
     * @param password the password
     * @return the access token
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    String getToken(String username, String password) throws NiFiRegistryException, IOException;

    /**
     * Gets an access token via spnego. It is expected that the caller of this method has wrapped the call
     * in a {@code doAs()} using a {@link javax.security.auth.Subject}.
     *
     * @return the token
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    String getTokenFromKerberosTicket() throws NiFiRegistryException, IOException;

    /**
     * Performs a logout for the user represented by the given token.
     *
     * @param token the toke to authenticate with
     *
     * @throws IOException if an I/O error occurs
     * @throws NiFiRegistryException if an non I/O error occurs
     */
    void logout(String token) throws NiFiRegistryException, IOException;

}
