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
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.converter.StandardJwtAuthenticationConverter;
import org.apache.nifi.web.security.jwt.jws.StandardJWSKeySelector;
import org.apache.nifi.web.security.jwt.jws.StandardJwsSignerProvider;
import org.apache.nifi.web.security.jwt.key.command.KeyExpirationCommand;
import org.apache.nifi.web.security.jwt.key.command.KeyGenerationCommand;
import org.apache.nifi.web.security.jwt.key.StandardVerificationKeySelector;
import org.apache.nifi.web.security.jwt.key.service.StandardVerificationKeyService;
import org.apache.nifi.web.security.jwt.key.service.VerificationKeyService;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.provider.StandardBearerTokenProvider;
import org.apache.nifi.web.security.jwt.provider.SupportedClaim;
import org.apache.nifi.web.security.jwt.revocation.JwtLogoutListener;
import org.apache.nifi.web.security.jwt.revocation.JwtRevocationService;
import org.apache.nifi.web.security.jwt.revocation.JwtRevocationValidator;
import org.apache.nifi.web.security.jwt.revocation.StandardJwtLogoutListener;
import org.apache.nifi.web.security.jwt.revocation.StandardJwtRevocationService;
import org.apache.nifi.web.security.jwt.revocation.command.RevocationExpirationCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * JSON Web Token Configuration for Authentication Security
 */
@Configuration
public class JwtAuthenticationSecurityConfiguration {
    private static final Set<String> REQUIRED_CLAIMS = new HashSet<>(Arrays.asList(
            SupportedClaim.ISSUER.getClaim(),
            SupportedClaim.SUBJECT.getClaim(),
            SupportedClaim.AUDIENCE.getClaim(),
            SupportedClaim.EXPIRATION.getClaim(),
            SupportedClaim.NOT_BEFORE.getClaim(),
            SupportedClaim.ISSUED_AT.getClaim(),
            SupportedClaim.JWT_ID.getClaim()
    ));

    private final NiFiProperties niFiProperties;

    private final Authorizer authorizer;

    private final IdpUserGroupService idpUserGroupService;

    private final StateManagerProvider stateManagerProvider;

    private final Duration keyRotationPeriod;

    @Autowired
    public JwtAuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties,
            final Authorizer authorizer,
            final IdpUserGroupService idpUserGroupService,
            final StateManagerProvider stateManagerProvider
    ) {
        this.niFiProperties = niFiProperties;
        this.authorizer = authorizer;
        this.idpUserGroupService = idpUserGroupService;
        this.stateManagerProvider = stateManagerProvider;
        this.keyRotationPeriod = niFiProperties.getSecurityUserJwsKeyRotationPeriod();
    }

    @Bean
    public JwtAuthenticationProvider jwtAuthenticationProvider() {
        final JwtAuthenticationProvider jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtDecoder());
        jwtAuthenticationProvider.setJwtAuthenticationConverter(jwtAuthenticationConverter());
        return jwtAuthenticationProvider;
    }

    @Bean
    public JwtDecoder jwtDecoder() {
        final NimbusJwtDecoder jwtDecoder = new NimbusJwtDecoder(jwtProcessor());
        final OAuth2TokenValidator<Jwt> jwtValidator = new DelegatingOAuth2TokenValidator<>(
                JwtValidators.createDefault(),
                jwtRevocationValidator()
        );
        jwtDecoder.setJwtValidator(jwtValidator);
        return jwtDecoder;
    }

    @Bean
    public OAuth2TokenValidator<Jwt> jwtRevocationValidator() {
        return new JwtRevocationValidator(jwtRevocationService());
    }

    @Bean
    public JwtRevocationService jwtRevocationService() {
        final StateManager stateManager = stateManagerProvider.getStateManager(StandardJwtRevocationService.class.getName());
        return new StandardJwtRevocationService(stateManager);
    }

    @Bean
    public JwtLogoutListener jwtLogoutListener() {
        return new StandardJwtLogoutListener(jwtDecoder(), jwtRevocationService());
    }

    @Bean
    public JWTProcessor<SecurityContext> jwtProcessor() {
        final DefaultJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();
        jwtProcessor.setJWSKeySelector(jwsKeySelector());
        jwtProcessor.setJWTClaimsSetVerifier(claimsSetVerifier());
        return jwtProcessor;
    }

    @Bean
    public JWSKeySelector<SecurityContext> jwsKeySelector() {
        return new StandardJWSKeySelector<>(verificationKeySelector());
    }

    @Bean
    public JWTClaimsSetVerifier<SecurityContext> claimsSetVerifier() {
        return new DefaultJWTClaimsVerifier<>(null, REQUIRED_CLAIMS);
    }

    @Bean
    public StandardJwtAuthenticationConverter jwtAuthenticationConverter() {
        return new StandardJwtAuthenticationConverter(authorizer, idpUserGroupService, niFiProperties);
    }

    @Bean
    public BearerTokenProvider bearerTokenProvider() {
        return new StandardBearerTokenProvider(jwsSignerProvider());
    }

    @Bean
    public StandardJwsSignerProvider jwsSignerProvider() {
        return new StandardJwsSignerProvider(verificationKeySelector());
    }

    @Bean
    public StandardVerificationKeySelector verificationKeySelector() {
        return new StandardVerificationKeySelector(verificationKeyService(), keyRotationPeriod);
    }

    @Bean
    public VerificationKeyService verificationKeyService() {
        final StateManager stateManager = stateManagerProvider.getStateManager(StandardVerificationKeyService.class.getName());
        return new StandardVerificationKeyService(stateManager);
    }

    @Bean
    public KeyGenerationCommand keyGenerationCommand() {
        final KeyGenerationCommand command = new KeyGenerationCommand(jwsSignerProvider(), verificationKeySelector());
        commandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    @Bean
    public KeyExpirationCommand keyExpirationCommand() {
        final KeyExpirationCommand command = new KeyExpirationCommand(verificationKeyService());
        commandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    @Bean
    public RevocationExpirationCommand revocationExpirationCommand() {
        final RevocationExpirationCommand command = new RevocationExpirationCommand(jwtRevocationService());
        commandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    @Bean
    public ThreadPoolTaskScheduler commandScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix(getClass().getSimpleName());
        return scheduler;
    }
}
