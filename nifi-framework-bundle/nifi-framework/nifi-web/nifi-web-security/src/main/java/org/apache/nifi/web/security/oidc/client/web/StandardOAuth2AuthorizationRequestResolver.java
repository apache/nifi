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
package org.apache.nifi.web.security.oidc.client.web;

import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.DefaultOAuth2AuthorizationRequestResolver;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestCustomizers;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestResolver;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.web.util.UriComponentsBuilder;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Objects;

import static org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestRedirectFilter.DEFAULT_AUTHORIZATION_REQUEST_BASE_URI;

/**
 * Authorization Request Resolver supports handling of headers from reverse proxy servers
 */
public class StandardOAuth2AuthorizationRequestResolver implements OAuth2AuthorizationRequestResolver {
    private final OAuth2AuthorizationRequestResolver resolver;

    /**
     * Resolver constructor delegates to the Spring Security Default Resolver and uses the default Request Base URI
     *
     * @param clientRegistrationRepository Client Registration Repository
     */
    public StandardOAuth2AuthorizationRequestResolver(final ClientRegistrationRepository clientRegistrationRepository) {
        Objects.requireNonNull(clientRegistrationRepository, "Repository required");
        final DefaultOAuth2AuthorizationRequestResolver requestResolver = new DefaultOAuth2AuthorizationRequestResolver(clientRegistrationRepository, DEFAULT_AUTHORIZATION_REQUEST_BASE_URI);
        // Enable RFC 7636 Proof Key for Code Exchange on Requests
        requestResolver.setAuthorizationRequestCustomizer(OAuth2AuthorizationRequestCustomizers.withPkce());
        resolver = requestResolver;
    }

    /**
     * Resolve Authorization Request delegating to default resolver
     *
     * @param request HTTP Servlet Request
     * @return OAuth2 Authorization Request or null when not resolved
     */
    @Override
    public OAuth2AuthorizationRequest resolve(final HttpServletRequest request) {
        final OAuth2AuthorizationRequest authorizationRequest = resolver.resolve(request);
        return getResolvedAuthorizationRequest(authorizationRequest, request);
    }

    /**
     * Resolve Authorization Request delegating to default resolver
     *
     * @param request HTTP Servlet Request
     * @param clientRegistrationId Client Registration Identifier
     * @return OAuth2 Authorization Request or null when not resolved
     */
    @Override
    public OAuth2AuthorizationRequest resolve(final HttpServletRequest request, final String clientRegistrationId) {
        final OAuth2AuthorizationRequest authorizationRequest = resolver.resolve(request, clientRegistrationId);
        return getResolvedAuthorizationRequest(authorizationRequest, request);
    }

    private OAuth2AuthorizationRequest getResolvedAuthorizationRequest(final OAuth2AuthorizationRequest authorizationRequest, final HttpServletRequest request) {
        final OAuth2AuthorizationRequest resolvedAuthorizationRequest;

        if (authorizationRequest == null) {
            resolvedAuthorizationRequest = null;
        } else {
            final String redirectUri = authorizationRequest.getRedirectUri();
            if (redirectUri == null) {
                resolvedAuthorizationRequest = authorizationRequest;
            } else {
                final String requestBasedRedirectUri = getRequestBasedRedirectUri(redirectUri, request);
                resolvedAuthorizationRequest = OAuth2AuthorizationRequest.from(authorizationRequest).redirectUri(requestBasedRedirectUri).build();
            }
        }

        return resolvedAuthorizationRequest;
    }

    private String getRequestBasedRedirectUri(final String redirectUri, final HttpServletRequest request) {
        final String redirectUriPath = UriComponentsBuilder.fromUriString(redirectUri).build().getPath();
        final URI baseUri =  RequestUriBuilder.fromHttpServletRequest(request).path(redirectUriPath).build();
        return UriComponentsBuilder.fromUriString(redirectUri)
                .scheme(baseUri.getScheme())
                .host(baseUri.getHost())
                .port(baseUri.getPort())
                .replacePath(baseUri.getPath())
                .build()
                .toUriString();
    }
}
