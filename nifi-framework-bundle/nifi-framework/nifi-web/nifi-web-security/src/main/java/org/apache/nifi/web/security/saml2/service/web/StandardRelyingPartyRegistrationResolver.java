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
package org.apache.nifi.web.security.saml2.service.web;

import org.apache.nifi.web.security.saml2.registration.Saml2RegistrationProperty;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.web.RelyingPartyRegistrationResolver;
import org.springframework.web.util.UriComponentsBuilder;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Standard Relying Party Registration Resolver with support for proxy headers
 */
public class StandardRelyingPartyRegistrationResolver implements Converter<HttpServletRequest, RelyingPartyRegistration>, RelyingPartyRegistrationResolver {
    private static final String BASE_URL_KEY = "baseUrl";

    private static final String REGISTRATION_ID_KEY = "registrationId";

    private static final Logger logger = LoggerFactory.getLogger(StandardRelyingPartyRegistrationResolver.class);

    private final RelyingPartyRegistrationRepository repository;

    /**
     * Standard Resolver with Registration Repository and Allowed Context Paths from application properties
     *
     * @param repository Relying Party Registration Repository required
     */
    public StandardRelyingPartyRegistrationResolver(final RelyingPartyRegistrationRepository repository) {
        this.repository = Objects.requireNonNull(repository, "Repository required");
    }

    /**
     * Convert Request to Relying Party Registration using internal default Registration Identifier
     *
     * @param request HTTP Servlet Request
     * @return Relying Party Registration
     */
    @Override
    public RelyingPartyRegistration convert(final HttpServletRequest request) {
        return resolve(request, Saml2RegistrationProperty.REGISTRATION_ID.getProperty());
    }

    /**
     * Resolve Registration from Repository and resolve template locations based on allowed proxy headers
     *
     * @param request HTTP Servlet Request
     * @param relyingPartyRegistrationId Registration Identifier requested
     * @return Relying Party Registration or null when identifier not found
     */
    @Override
    public RelyingPartyRegistration resolve(final HttpServletRequest request, final String relyingPartyRegistrationId) {
        Objects.requireNonNull(request, "Request required");
        final RelyingPartyRegistration registration = repository.findByRegistrationId(relyingPartyRegistrationId);

        final RelyingPartyRegistration resolved;
        if (registration == null) {
            resolved = null;
            logger.warn("Relying Party Registration [{}] not found", relyingPartyRegistrationId);
        } else {
            final String baseUrl = getBaseUrl(request);

            final String assertionConsumerServiceLocation = resolveUrl(registration.getAssertionConsumerServiceLocation(), baseUrl, registration);
            final String singleLogoutServiceLocation = resolveUrl(registration.getSingleLogoutServiceLocation(), baseUrl, registration);
            final String singleLogoutServiceResponseLocation = resolveUrl(registration.getSingleLogoutServiceResponseLocation(), baseUrl, registration);

            resolved = registration.mutate()
                    .assertionConsumerServiceLocation(assertionConsumerServiceLocation)
                    .singleLogoutServiceLocation(singleLogoutServiceLocation)
                    .singleLogoutServiceResponseLocation(singleLogoutServiceResponseLocation)
                    .build();
        }

        return resolved;
    }

    private String resolveUrl(final String templateUrl, final String baseUrl, final RelyingPartyRegistration registration) {
        final String resolved;
        if (templateUrl == null) {
            resolved = null;
        } else {
            final Map<String, String> uriVariables = new HashMap<>();
            uriVariables.put(BASE_URL_KEY, baseUrl);
            uriVariables.put(REGISTRATION_ID_KEY, registration.getRegistrationId());
            resolved = UriComponentsBuilder.fromUriString(templateUrl).buildAndExpand(uriVariables).toUriString();
        }
        return resolved;
    }

    private String getBaseUrl(final HttpServletRequest request) {
        final URI requestUri = RequestUriBuilder.fromHttpServletRequest(request).build();
        final String httpUrl = requestUri.toString();
        final String contextPath = request.getContextPath();
        return UriComponentsBuilder.fromUriString(httpUrl).path(contextPath).replaceQuery(null).fragment(null).build().toString();
    }
}
