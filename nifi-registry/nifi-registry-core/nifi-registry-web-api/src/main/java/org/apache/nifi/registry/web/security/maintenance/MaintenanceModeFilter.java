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
package org.apache.nifi.registry.web.security.maintenance;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.GenericFilterBean;

import java.io.IOException;
import java.util.Set;

/**
 * Servlet filter that enforces maintenance mode by rejecting write operations with HTTP 503.
 * When maintenance mode is enabled via {@link MaintenanceModeManager}, any request using a
 * mutating HTTP method (POST, PUT, PATCH, DELETE) is rejected immediately with:
 * <ul>
 *   <li>Status: 503 Service Unavailable</li>
 *   <li>Retry-After: 60 seconds</li>
 *   <li>Body: plain-text message describing the maintenance mode state</li>
 * </ul>
 * Read-only methods (GET, HEAD, OPTIONS) pass through unchanged.
 */
public class MaintenanceModeFilter extends GenericFilterBean {

    private static final Set<String> WRITE_METHODS = Set.of("POST", "PUT", "PATCH", "DELETE");
    private static final String ACTUATOR_PATH_PREFIX = "/actuator/";
    private static final int RETRY_AFTER_SECONDS = 60;
    private static final String MAINTENANCE_RESPONSE_BODY = "NiFi Registry is in maintenance mode. Write operations are temporarily disabled. Please try again later.";

    private final MaintenanceModeManager maintenanceModeManager;

    public MaintenanceModeFilter(final MaintenanceModeManager maintenanceModeManager) {
        this.maintenanceModeManager = maintenanceModeManager;
    }

    @Override
    public void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse,
            final FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        final HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        if (httpRequest.getServletPath().startsWith(ACTUATOR_PATH_PREFIX)) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        if (maintenanceModeManager.isEnabled() && isWriteMethod(httpRequest.getMethod())) {
            sendMaintenanceModeResponse(httpResponse);
            return;
        }

        filterChain.doFilter(servletRequest, servletResponse);
    }

    private boolean isWriteMethod(final String method) {
        return WRITE_METHODS.contains(method.toUpperCase());
    }

    private void sendMaintenanceModeResponse(final HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        response.setContentType("text/plain");
        response.setHeader("Retry-After", String.valueOf(RETRY_AFTER_SECONDS));
        response.getWriter().println(MAINTENANCE_RESPONSE_BODY);
    }
}
