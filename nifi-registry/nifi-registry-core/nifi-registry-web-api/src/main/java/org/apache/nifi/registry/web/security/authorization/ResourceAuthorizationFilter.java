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
package org.apache.nifi.registry.web.security.authorization;

import org.apache.nifi.registry.security.authorization.AuthorizableLookup;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException;
import org.apache.nifi.registry.security.authorization.resource.Authorizable;
import org.apache.nifi.registry.security.authorization.resource.ResourceType;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.service.AuthorizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This filter is designed to perform a resource authorization check in the Spring Security filter chain.
 *
 * It authorizes the current authenticated user for the {@link RequestAction} (based on the HttpMethod) requested
 * on the {@link ResourceType} (based on the URI path).
 *
 * This filter is designed to be place after any authentication and before any application endpoints.
 *
 * This filter can be used in place of or in addition to authorization checks that occur in the application
 * downstream of this filter.
 *
 * To configure this filter, provide an {@link AuthorizationService} that will be used to perform the authorization
 * check, as well as a set of rules that control which resource and HTTP methods are handled by this filter.
 *
 * Any (ResourceType, HttpMethod) pair that is not configured to require authorization by this filter will be
 * allowed to proceed in the filter chain without an authorization check.
 *
 * Any (ResourceType, HttpMethod) pair that is configured to require authorization by this filter will map
 * the HttpMethod to a NiFi Registry RequestAction (configurable when creating this filter), and the
 * (Resource Authorizable, RequestAction) pair will be sent to the AuthorizationService, which will use the
 * configured Authorizer to authorize the current user for the action on the requested resource.
 */
public class ResourceAuthorizationFilter extends GenericFilterBean {

    private static final Logger logger = LoggerFactory.getLogger(ResourceAuthorizationFilter.class);

    private Map<ResourceType, HttpMethodAuthorizationRules> resourceTypeAuthorizationRules;
    private AuthorizationService authorizationService;
    private AuthorizableLookup authorizableLookup;

    ResourceAuthorizationFilter(Builder builder) {
        if (builder.getAuthorizationService() == null || builder.getResourceTypeAuthorizationRules() == null) {
            throw new IllegalArgumentException("Builder is missing one or more required fields [authorizationService, resourceTypeAuthorizationRules].");
        }
        this.resourceTypeAuthorizationRules = builder.getResourceTypeAuthorizationRules();
        this.authorizationService = builder.getAuthorizationService();
        this.authorizableLookup = this.authorizationService.getAuthorizableLookup();
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {

        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;

        boolean authorizationCheckIsRequired = false;
        String resourcePath = null;
        RequestAction action = null;

        // Only require authorization if the NiFi Registry is running securely.
        if (servletRequest.isSecure()) {

            // Only require authorization for resources for which this filter has been configured
            resourcePath = httpServletRequest.getServletPath();
            if (resourcePath != null) {
                final ResourceType resourceType = ResourceType.mapFullResourcePathToResourceType(resourcePath);
                final HttpMethodAuthorizationRules authorizationRules = resourceTypeAuthorizationRules.get(resourceType);
                if (authorizationRules != null) {
                    final String httpMethodStr = httpServletRequest.getMethod().toUpperCase();
                    HttpMethod httpMethod = HttpMethod.resolve(httpMethodStr);

                    // Only require authorization for HTTP methods included in this resource type's rule set
                    if (httpMethod != null && authorizationRules.requiresAuthorization(httpMethod)) {
                        authorizationCheckIsRequired = true;
                        action = authorizationRules.mapHttpMethodToAction(httpMethod);
                    }
                }
            }
        }

        if (!authorizationCheckIsRequired) {
            forwardRequestWithoutAuthorizationCheck(httpServletRequest, httpServletResponse, filterChain);
            return;
        }

        // Perform authorization check
        try {
            authorizeAccess(resourcePath, action);
            successfulAuthorization(httpServletRequest, httpServletResponse, filterChain);
        } catch (Exception e) {
            logger.debug("Exception occurred while performing authorization check.", e);
            failedAuthorization(httpServletRequest, httpServletResponse, filterChain, e);
        }
    }

    private boolean userIsAuthenticated() {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        return (user != null && !user.isAnonymous());
    }

    private void authorizeAccess(String path, RequestAction action) throws AccessDeniedException {

        if (path == null || action == null) {
            throw new IllegalArgumentException("Authorization is required, but a required input [resource, action] is absent.");
        }

        Authorizable authorizable = authorizableLookup.getAuthorizableByResource(path);

        if (authorizable == null) {
            throw new IllegalStateException("Resource Authorization Filter configured for non-authorizable resource: " + path);
        }

        // throws AccessDeniedException if current user is not authorized to perform requested action on resource
        authorizationService.authorize(authorizable, action);
    }

    private void forwardRequestWithoutAuthorizationCheck(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws IOException, ServletException {
        logger.debug("Request filter authorization check is not required for this HTTP Method on this resource. " +
                "Allowing request to proceed. An additional authorization check might be performed downstream of this filter.");
        chain.doFilter(req, res);
    }

    private void successfulAuthorization(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws IOException, ServletException {
        logger.debug("Request filter authorization check passed. Allowing request to proceed.");
        chain.doFilter(req, res);
    }

    private void failedAuthorization(HttpServletRequest request, HttpServletResponse response, FilterChain chain, Exception failure) throws IOException, ServletException {
        logger.debug("Request filter authorization check failed. Blocking access.");

        NiFiUser user = NiFiUserUtils.getNiFiUser();
        final String identity = (user != null) ? user.toString() : "<no user found>";
        final int status = !userIsAuthenticated() ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_FORBIDDEN;

        logger.info("{} does not have permission to perform this action on the requested resource. {} Returning {} response.", identity, failure.getMessage(), status);
        logger.debug("", failure);

        if (!response.isCommitted()) {
            response.setStatus(status);
            response.setContentType("text/plain");
            response.getWriter().println(String.format("Access is denied due to: %s Contact the system administrator.", failure.getLocalizedMessage()));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private AuthorizationService authorizationService;
        final private Map<ResourceType, HttpMethodAuthorizationRules> resourceTypeAuthorizationRules;

        // create via ResourceAuthorizationFilter.builder()
        private Builder() {
            this.resourceTypeAuthorizationRules = new HashMap<>();
        }

        public AuthorizationService getAuthorizationService() {
            return authorizationService;
        }

        public Builder setAuthorizationService(AuthorizationService authorizationService) {
            this.authorizationService = authorizationService;
            return this;
        }

        public Map<ResourceType, HttpMethodAuthorizationRules> getResourceTypeAuthorizationRules() {
            return resourceTypeAuthorizationRules;
        }

        public Builder addResourceType(ResourceType resourceType) {
            this.resourceTypeAuthorizationRules.put(resourceType, new HttpMethodAuthorizationRules() {});
            return this;
        }

        public Builder addResourceType(ResourceType resourceType, HttpMethodAuthorizationRules authorizationRules) {
            this.resourceTypeAuthorizationRules.put(resourceType, authorizationRules);
            return this;
        }

        public ResourceAuthorizationFilter build() {
            return new ResourceAuthorizationFilter(this);
        }
    }

}
