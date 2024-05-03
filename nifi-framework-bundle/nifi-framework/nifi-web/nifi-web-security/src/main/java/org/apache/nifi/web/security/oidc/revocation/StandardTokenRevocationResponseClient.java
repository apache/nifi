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
package org.apache.nifi.web.security.oidc.revocation;

import org.apache.nifi.web.security.oidc.client.web.OidcRegistrationProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.endpoint.OAuth2ParameterNames;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestOperations;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation for handling Token Revocation Requests using Spring REST Operations
 */
public class StandardTokenRevocationResponseClient implements TokenRevocationResponseClient {
    static final String REVOCATION_ENDPOINT = "revocation_endpoint";

    private static final Logger logger = LoggerFactory.getLogger(StandardTokenRevocationResponseClient.class);

    private final RestOperations restOperations;

    private final ClientRegistrationRepository clientRegistrationRepository;

    public StandardTokenRevocationResponseClient(
            final RestOperations restOperations,
            final ClientRegistrationRepository clientRegistrationRepository
    ) {
        this.restOperations = Objects.requireNonNull(restOperations, "REST Operations required");
        this.clientRegistrationRepository = Objects.requireNonNull(clientRegistrationRepository, "Client Registry Repository required");
    }

    /**
     * Get Revocation Response as described in RFC 7009 Section 2.2 or return success when the Revocation Endpoint is not configured
     *
     * @param revocationRequest Revocation Request is required
     * @return Token Revocation Response
     */
    @Override
    public TokenRevocationResponse getRevocationResponse(final TokenRevocationRequest revocationRequest) {
        Objects.requireNonNull(revocationRequest, "Revocation Request required");

        final ClientRegistration clientRegistration = clientRegistrationRepository.findByRegistrationId(OidcRegistrationProperty.REGISTRATION_ID.getProperty());
        final ResponseEntity<?> responseEntity = getResponseEntity(revocationRequest, clientRegistration);
        final HttpStatusCode statusCode = responseEntity.getStatusCode();
        return new TokenRevocationResponse(statusCode.is2xxSuccessful(), statusCode.value());
    }

    private ResponseEntity<?> getResponseEntity(final TokenRevocationRequest revocationRequest, final ClientRegistration clientRegistration) {
        final RequestEntity<?> requestEntity = getRequestEntity(revocationRequest, clientRegistration);
        if (requestEntity == null) {
            return ResponseEntity.ok(null);
        } else {
            try {
                final ResponseEntity<?> responseEntity = restOperations.exchange(requestEntity, String.class);
                logger.debug("Token Revocation Request processing completed [HTTP {}]", responseEntity.getStatusCode());
                return responseEntity;
            } catch (final Exception e) {
                logger.warn("Token Revocation Request processing failed", e);
                return ResponseEntity.internalServerError().build();
            }
        }
    }

    private RequestEntity<?> getRequestEntity(final TokenRevocationRequest revocationRequest, final ClientRegistration clientRegistration) {
        final RequestEntity<?> requestEntity;
        final URI revocationEndpoint = getRevocationEndpoint(clientRegistration);
        if (revocationEndpoint == null) {
            requestEntity = null;
            logger.info("OIDC Revocation Endpoint not found");
        } else {
            final LinkedMultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
            parameters.add(OAuth2ParameterNames.TOKEN, revocationRequest.getToken());

            final String tokenTypeHint = revocationRequest.getTokenTypeHint();
            if (StringUtils.hasLength(tokenTypeHint)) {
                parameters.add(OAuth2ParameterNames.TOKEN_TYPE_HINT, tokenTypeHint);
            }

            final HttpHeaders headers = new HttpHeaders();
            final String clientId = clientRegistration.getClientId();
            final String clientSecret = clientRegistration.getClientSecret();
            headers.setBasicAuth(clientId, clientSecret, StandardCharsets.UTF_8);

            requestEntity = RequestEntity.post(revocationEndpoint)
                    .headers(headers)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(parameters);
        }
        return requestEntity;
    }

    private URI getRevocationEndpoint(final ClientRegistration clientRegistration) {
        final ClientRegistration.ProviderDetails providerDetails = clientRegistration.getProviderDetails();
        final Map<String, Object> configurationMetadata = providerDetails.getConfigurationMetadata();
        final Object revocationEndpoint = configurationMetadata.get(REVOCATION_ENDPOINT);
        return revocationEndpoint == null ? null : URI.create(revocationEndpoint.toString());
    }
}
