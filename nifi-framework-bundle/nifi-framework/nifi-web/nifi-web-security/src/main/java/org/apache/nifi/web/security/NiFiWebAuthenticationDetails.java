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
package org.apache.nifi.web.security;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import java.util.Objects;

/**
 * Authentication details for NiFi web. Stores the user agent in addition to the remote address and session id.
 */
public class NiFiWebAuthenticationDetails extends WebAuthenticationDetails {
    private final String userAgent;

    public NiFiWebAuthenticationDetails(final HttpServletRequest request) {
        super(request);
        this.userAgent = request.getHeader(HttpHeaders.USER_AGENT);
    }

    public NiFiWebAuthenticationDetails(final String remoteAddress, final String sessionId, String userAgent) {
        super(remoteAddress, sessionId);
        this.userAgent = userAgent;
    }

    public String getUserAgent() {
        return userAgent;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        final NiFiWebAuthenticationDetails that = (NiFiWebAuthenticationDetails) o;
        return Objects.equals(userAgent, that.userAgent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userAgent);
    }

    @Override
    public String toString() {
        return "NiFiWebAuthenticationDetails{" +
            "userAgent='" + userAgent + '\'' +
            ", remoteIpAddress='" + getRemoteAddress() + '\'' +
            ", sessionId='" + getSessionId() + '\'' +
            "} ";
    }
}
