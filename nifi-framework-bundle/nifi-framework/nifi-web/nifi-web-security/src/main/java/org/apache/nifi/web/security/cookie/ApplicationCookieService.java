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
package org.apache.nifi.web.security.cookie;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.net.URI;
import java.util.Optional;

/**
 * Application Cookie Service capable of generating and retrieving HTTP Cookies using standard properties
 */
public interface ApplicationCookieService {
    /**
     * Generate cookie with specified value
     *
     * @param resourceUri Resource URI containing path and domain
     * @param response HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name to be added
     * @param value Cookie value to be added
     */
    void addCookie(URI resourceUri, HttpServletResponse response, ApplicationCookieName applicationCookieName, String value);

    /**
     * Generate cookie with session-based expiration and specified value
     *
     * @param resourceUri Resource URI containing path and domain
     * @param response HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name
     * @param value Cookie value to be added
     */
    void addSessionCookie(URI resourceUri, HttpServletResponse response, ApplicationCookieName applicationCookieName, String value);

    /**
     * Get cookie value using specified name
     *
     * @param request HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name to be retrieved
     * @return Optional Cookie Value
     */
    Optional<String> getCookieValue(HttpServletRequest request, ApplicationCookieName applicationCookieName);

    /**
     * Generate cookie with an empty value instructing the client to remove the cookie
     *
     * @param resourceUri Resource URI containing path and domain
     * @param response HTTP Servlet Response
     * @param applicationCookieName Application Cookie Name to be removed
     */
    void removeCookie(URI resourceUri, HttpServletResponse response, ApplicationCookieName applicationCookieName);
}
