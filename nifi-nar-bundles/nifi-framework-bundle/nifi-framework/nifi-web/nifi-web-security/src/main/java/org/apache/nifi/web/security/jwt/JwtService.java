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
package org.apache.nifi.web.security.jwt;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.security.core.Authentication;

/**
 *
 */
public class JwtService {

    /**
     * Gets the Authentication by extracting a JWT token from the specified request.
     *
     * @param request Request to extract the token from
     * @return The user identifier from the token
     */
    public String getAuthentication(final HttpServletRequest request) {
        // extract/verify token from incoming request
        // load user details with identifier from token
        // create authentication using user details
        return null;
    }

    /**
     * Adds a token for the specified authentication in the specified response.
     *
     * @param response The response to add the token to
     * @param authentication The authentication to generate a token for
     */
    public void addToken(final HttpServletResponse response, final Authentication authentication) {
        // create a token the specified authentication
        // add the token as a response header
    }
}
