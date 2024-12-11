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
package org.apache.nifi.web.security.oidc.authentication;

import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.springframework.security.oauth2.client.oidc.authentication.OidcIdTokenDecoderFactory;
import org.springframework.security.oauth2.client.oidc.authentication.OidcIdTokenValidator;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.converter.ClaimTypeConverter;
import org.springframework.security.oauth2.jose.jws.JwsAlgorithm;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.web.client.RestOperations;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenID Connect ID Token Decoder Factory with configurable REST Operations for retrieval of JSON Web Keys
 */
public class StandardOidcIdTokenDecoderFactory implements JwtDecoderFactory<ClientRegistration> {
    private static final String MISSING_SIGNATURE_VERIFIER_ERROR_CODE = "missing_signature_verifier";

    private static final String UNSPECIFIED_ERROR_URI = null;

    private static final JwsAlgorithm DEFAULT_JWS_ALGORITHM = SignatureAlgorithm.RS256;

    private static final Map<JwsAlgorithm, String> SECRET_KEY_ALGORITHMS = Map.of(
            MacAlgorithm.HS256, "HmacSHA256",
            MacAlgorithm.HS384, "HmacSHA384",
            MacAlgorithm.HS512, "HmacSHA512"
    );

    private static final ClaimTypeConverter DEFAULT_CLAIM_TYPE_CONVERTER = new ClaimTypeConverter(
            OidcIdTokenDecoderFactory.createDefaultClaimTypeConverters()
    );

    private final Map<String, JwtDecoder> jwtDecoders = new ConcurrentHashMap<>();

    private final JwsAlgorithm configuredJwsAlgorithm;

    private final RestOperations restOperations;

    /**
     * Standard constructor with optional JWS Algorithm and required REST Operations for retrieving JSON Web Keys
     *
     * @param preferredJwsAlgorithm Preferred JSON Web Signature Algorithm default to RS256 when not provided
     * @param restOperations REST Operations required for retrieving JSON Web Key Set with Signature Algorithms
     */
    public StandardOidcIdTokenDecoderFactory(final String preferredJwsAlgorithm, final RestOperations restOperations) {
        this.configuredJwsAlgorithm = getJwsAlgorithm(preferredJwsAlgorithm);
        this.restOperations = Objects.requireNonNull(restOperations, "REST Operations required");
    }

    /**
     * Create JSON Web Token Decoder based on Client Registration
     *
     * @param clientRegistration Client Registration required
     * @return JSON Web Token Decoder for OpenID Connect ID Tokens
     */
    @Override
    public JwtDecoder createDecoder(final ClientRegistration clientRegistration) {
        Objects.requireNonNull(clientRegistration, "Client Registration required");
        final String registrationId = clientRegistration.getRegistrationId();
        return jwtDecoders.computeIfAbsent(registrationId, (key) -> {
            final NimbusJwtDecoder decoder = buildDecoder(clientRegistration);
            decoder.setClaimSetConverter(DEFAULT_CLAIM_TYPE_CONVERTER);
            final OAuth2TokenValidator<Jwt> tokenValidator = getTokenValidator(clientRegistration);
            decoder.setJwtValidator(tokenValidator);
            return decoder;
        });
    }

    /**
     * Get OAuth2 Token Validator based on Spring Security DefaultOidcIdTokenValidatorFactory
     *
     * @param clientRegistration Client Registration
     * @return OAuth2 Token Validator with Timestamp and OpenID Connect ID Token Validators
     */
    protected OAuth2TokenValidator<Jwt> getTokenValidator(final ClientRegistration clientRegistration) {
        return new DelegatingOAuth2TokenValidator<>(
                new JwtTimestampValidator(),
                new OidcIdTokenValidator(clientRegistration)
        );
    }

    private NimbusJwtDecoder buildDecoder(final ClientRegistration clientRegistration) {
        final NimbusJwtDecoder decoder;

        final Class<? extends JwsAlgorithm> jwsAlgorithmClass = configuredJwsAlgorithm.getClass();
        if (SignatureAlgorithm.class.isAssignableFrom(jwsAlgorithmClass)) {
            final String jwkSetUri = clientRegistration.getProviderDetails().getJwkSetUri();
            if (jwkSetUri == null || jwkSetUri.isEmpty()) {
                final String message = String.format("JSON Web Key Set URI required for Signature Verifier JWS Algorithm [%s]", configuredJwsAlgorithm);
                final OAuth2Error error = getVerifierError(message);
                throw new OAuth2AuthenticationException(error, message);
            }

            final SignatureAlgorithm signatureAlgorithm = (SignatureAlgorithm) configuredJwsAlgorithm;
            decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri)
                    .jwsAlgorithm(signatureAlgorithm)
                    .restOperations(restOperations)
                    .build();
        } else if (MacAlgorithm.class.isAssignableFrom(jwsAlgorithmClass)) {
            final String clientSecret = clientRegistration.getClientSecret();
            if (clientSecret == null || clientSecret.isEmpty()) {
                final String message = String.format("Client Secret required for MAC Verifier JWS Algorithm [%s]", configuredJwsAlgorithm);
                final OAuth2Error error = getVerifierError(message);
                throw new OAuth2AuthenticationException(error, message);
            }

            final byte[] clientSecretKey = clientSecret.getBytes(StandardCharsets.UTF_8);
            final String secretKeyAlgorithm = SECRET_KEY_ALGORITHMS.get(configuredJwsAlgorithm);
            final SecretKey secretKey = new SecretKeySpec(clientSecretKey, secretKeyAlgorithm);
            final MacAlgorithm macAlgorithm = (MacAlgorithm) configuredJwsAlgorithm;
            decoder = NimbusJwtDecoder.withSecretKey(secretKey).macAlgorithm(macAlgorithm).build();
        } else {
            final String message = String.format("Signature Verifier JWS Algorithm [%s] not supported", configuredJwsAlgorithm);
            final OAuth2Error error = getVerifierError(message);
            throw new OAuth2AuthenticationException(error, message);
        }

        return decoder;
    }

    private JwsAlgorithm getJwsAlgorithm(final String preferredJwsAlgorithm) {
        final JwsAlgorithm jwsAlgorithm;

        if (preferredJwsAlgorithm == null || preferredJwsAlgorithm.isEmpty()) {
            jwsAlgorithm = DEFAULT_JWS_ALGORITHM;
        } else {
            final MacAlgorithm macAlgorithm = MacAlgorithm.from(preferredJwsAlgorithm);
            if (macAlgorithm == null) {
                final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.from(preferredJwsAlgorithm);
                if (signatureAlgorithm == null) {
                    final String message = String.format("Preferred JWS Algorithm [%s] not supported", preferredJwsAlgorithm);
                    throw new OidcConfigurationException(message);
                }
                jwsAlgorithm = signatureAlgorithm;
            } else {
                jwsAlgorithm = macAlgorithm;
            }
        }

        return jwsAlgorithm;
    }

    private OAuth2Error getVerifierError(final String message) {
        return new OAuth2Error(MISSING_SIGNATURE_VERIFIER_ERROR_CODE, message, UNSPECIFIED_ERROR_URI);
    }
}
