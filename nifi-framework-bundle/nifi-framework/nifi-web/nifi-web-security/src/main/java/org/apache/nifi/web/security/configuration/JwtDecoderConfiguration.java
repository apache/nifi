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
package org.apache.nifi.web.security.configuration;

import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTClaimsSetVerifier;
import com.nimbusds.jwt.proc.JWTProcessor;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.converter.StandardIssuerJwtDecoder;
import org.apache.nifi.web.security.jwt.jws.StandardJWSKeySelector;
import org.apache.nifi.web.security.jwt.key.StandardJWSVerifierFactory;
import org.apache.nifi.web.security.jwt.key.StandardVerificationKeySelector;
import org.apache.nifi.web.security.jwt.key.service.StandardVerificationKeyService;
import org.apache.nifi.web.security.jwt.key.service.VerificationKeyService;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.jwt.revocation.JwtRevocationService;
import org.apache.nifi.web.security.jwt.revocation.JwtRevocationValidator;
import org.apache.nifi.web.security.jwt.revocation.StandardJwtRevocationService;
import org.apache.nifi.web.security.oidc.authentication.AccessTokenDecoderFactory;
import org.apache.nifi.web.security.oidc.authentication.StandardOidcIdTokenDecoderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.web.client.RestOperations;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * JSON Web Token Decoder Configuration with component supporting Bearer Token parsing and verification
 */
@Configuration
public class JwtDecoderConfiguration {
    private static final Set<String> REQUIRED_CLAIMS = new HashSet<>(Arrays.asList(
            SupportedClaim.ISSUER.getClaim(),
            SupportedClaim.SUBJECT.getClaim(),
            SupportedClaim.AUDIENCE.getClaim(),
            SupportedClaim.EXPIRATION.getClaim(),
            SupportedClaim.NOT_BEFORE.getClaim(),
            SupportedClaim.ISSUED_AT.getClaim(),
            SupportedClaim.JWT_ID.getClaim(),
            SupportedClaim.GROUPS.getClaim()
    ));

    private final NiFiProperties properties;

    private final ClientRegistrationRepository clientRegistrationRepository;

    private final RestOperations oidcRestOperations;

    private final StateManagerProvider stateManagerProvider;

    private final Duration keyRotationPeriod;

    @Autowired
    public JwtDecoderConfiguration(
            final NiFiProperties properties,
            final ClientRegistrationRepository clientRegistrationRepository,
            @Qualifier("oidcRestOperations")
            final RestOperations oidcRestOperations,
            final StateManagerProvider stateManagerProvider
    ) {
        this.properties = Objects.requireNonNull(properties, "Application properties required");
        this.clientRegistrationRepository = Objects.requireNonNull(clientRegistrationRepository, "Client Registration Repository required");
        this.oidcRestOperations = Objects.requireNonNull(oidcRestOperations, "OIDC REST Operations required");
        this.stateManagerProvider = Objects.requireNonNull(stateManagerProvider, "State Manager Provider required");
        this.keyRotationPeriod = properties.getSecurityUserJwsKeyRotationPeriod();
    }

    /**
     * JWT Decoder responsible for parsing and verifying Bearer Tokens from application or OIDC Identity Provider
     *
     * @return JWT Decoder delegating to OpenID Connect JWT Decoder on matching Issuer claims
     */
    @Bean
    public JwtDecoder jwtDecoder() {
        final NimbusJwtDecoder applicationJwtDecoder = new NimbusJwtDecoder(jwtProcessor());
        applicationJwtDecoder.setJwtValidator(jwtTokenValidator());
        final AccessTokenDecoderFactory accessTokenDecoderFactory = new AccessTokenDecoderFactory(properties.getOidcPreferredJwsAlgorithm(), oidcRestOperations);
        return new StandardIssuerJwtDecoder(applicationJwtDecoder, accessTokenDecoderFactory, clientRegistrationRepository);
    }

    /**
     * JSON Web Token Processor supporting application Bearer Token Decoder with configured Signing Key Selector
     *
     * @return Application JSON Web Token Processor for verification
     */
    @Bean
    public JWTProcessor<SecurityContext> jwtProcessor() {
        final DefaultJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
        final JWSKeySelector<SecurityContext> jwsKeySelector = new StandardJWSKeySelector<>(verificationKeySelector());
        jwtProcessor.setJWSKeySelector(jwsKeySelector);

        final JWTClaimsSetVerifier<SecurityContext> claimsSetVerifier = new DefaultJWTClaimsVerifier<>(null, REQUIRED_CLAIMS);
        jwtProcessor.setJWTClaimsSetVerifier(claimsSetVerifier);

        jwtProcessor.setJWSVerifierFactory(new StandardJWSVerifierFactory());
        return jwtProcessor;
    }

    /**
     * Token Validator responsible for validating JWT claims after parsing and verification based on matching Issuer
     *
     * @return Token Validator supporting application Bearer Tokens
     */
    @Bean
    public OAuth2TokenValidator<Jwt> jwtTokenValidator() {
        final OAuth2TokenValidator<Jwt> jwtRevocationValidator = new JwtRevocationValidator(jwtRevocationService());
        return new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefault(),
                jwtRevocationValidator
        );
    }

    /**
     * OpenID Connect Identifier Token Decoder with configured JWS Algorithm for verification
     *
     * @return OpenID Connect Identifier Token Decoder
     */
    @Bean
    public JwtDecoderFactory<ClientRegistration> idTokenDecoderFactory() {
        final String preferredJwdAlgorithm = properties.getOidcPreferredJwsAlgorithm();
        return new StandardOidcIdTokenDecoderFactory(preferredJwdAlgorithm, oidcRestOperations);
    }

    /**
     * JWT Revocation Service with backing local State Manager for tracking revoked application Bearer Tokens
     *
     * @return JWT Revocation Service using local State Manager
     */
    @Bean
    public JwtRevocationService jwtRevocationService() {
        final StateManager stateManager = stateManagerProvider.getStateManager(StandardJwtRevocationService.class.getName());
        return new StandardJwtRevocationService(stateManager);
    }

    /**
     * Verification Key Selector with configured key rotation period
     *
     * @return Verification Key Selector supporting JSON Web Token signature verification
     */
    @Bean
    public StandardVerificationKeySelector verificationKeySelector() {
        return new StandardVerificationKeySelector(verificationKeyService(), keyRotationPeriod);
    }

    /**
     * Verification Key Service using local State Manager for storing public keys
     *
     * @return Standard Verification Key Service with local State Manager
     */
    @Bean
    public VerificationKeyService verificationKeyService() {
        final StateManager stateManager = stateManagerProvider.getStateManager(StandardVerificationKeyService.class.getName());
        return new StandardVerificationKeyService(stateManager);
    }
}
