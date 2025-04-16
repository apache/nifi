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
 * Standard Web Authentication Details with additional headers including User-Agent and X-Forwarded-For
 */
public class NiFiWebAuthenticationDetails extends WebAuthenticationDetails {
    private static final String FORWARD_FOR_HEADER = "X-Forwarded-For";

    private final String userAgent;

    private final String forwardedFor;

    public NiFiWebAuthenticationDetails(final HttpServletRequest request) {
        super(request);
        this.userAgent = request.getHeader(HttpHeaders.USER_AGENT);
        this.forwardedFor = request.getHeader(FORWARD_FOR_HEADER);
    }

    /**
     * Get User Agent can be null when the User-Agent header is not provided
     *
     * @return User Agent
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * Get Forwarded For addresses can be null when the X-Forwarded-For header is not provided
     *
     * @return Forwarded For addresses
     */
    public String getForwardedFor() {
        return forwardedFor;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final NiFiWebAuthenticationDetails details = (NiFiWebAuthenticationDetails) o;
        return Objects.equals(userAgent, details.userAgent) && Objects.equals(forwardedFor, details.forwardedFor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userAgent, forwardedFor);
    }

    @Override
    public String toString() {
        return "remoteAddress=[%s] userAgent=[%s] forwardedFor=[%s]".formatted(getRemoteAddress(), userAgent, forwardedFor);
    }
}
