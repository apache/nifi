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

package org.apache.nifi.minifi.c2.api.security.authorization;

import org.springframework.security.core.Authentication;

import javax.ws.rs.core.UriInfo;

/**
 * Interface responsible for authorizing a given authentication to access a given uri
 */
public interface Authorizer {
    /**
     * Throws an AuthorizationException if the authentication should not access the given uri
     *
     * @param authentication the authentication
     * @param uriInfo the uri
     * @throws AuthorizationException if the authentication should not access the uri
     */
    void authorize(Authentication authentication, UriInfo uriInfo) throws AuthorizationException;
}
