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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.converter.StandardJwtAuthenticationConverter;
import org.apache.nifi.web.security.StandardAuthenticationEntryPoint;
import org.apache.nifi.web.security.jwt.jws.StandardJwsSignerProvider;
import org.apache.nifi.web.security.jwt.key.command.KeyExpirationCommand;
import org.apache.nifi.web.security.jwt.key.command.KeyGenerationCommand;
import org.apache.nifi.web.security.jwt.key.StandardVerificationKeySelector;
import org.apache.nifi.web.security.jwt.key.service.VerificationKeyService;
import org.apache.nifi.web.security.jwt.provider.IssuerProvider;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.provider.StandardBearerTokenProvider;
import org.apache.nifi.web.security.jwt.provider.StandardIssuerProvider;
import org.apache.nifi.web.security.jwt.resolver.StandardBearerTokenResolver;
import org.apache.nifi.web.security.jwt.revocation.JwtLogoutListener;
import org.apache.nifi.web.security.jwt.revocation.JwtRevocationService;
import org.apache.nifi.web.security.jwt.revocation.StandardJwtLogoutListener;
import org.apache.nifi.web.security.jwt.revocation.command.RevocationExpirationCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationEntryPoint;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;

import java.security.KeyPairGenerator;
import java.time.Duration;

/**
 * JSON Web Token Configuration for Authentication Security
 */
@Configuration
public class JwtAuthenticationSecurityConfiguration {

    private final NiFiProperties niFiProperties;

    private final Authorizer authorizer;

    private final JwtDecoder jwtDecoder;

    private final JwtRevocationService jwtRevocationService;

    private final StandardVerificationKeySelector verificationKeySelector;

    private final VerificationKeyService verificationKeyService;

    private final Duration keyRotationPeriod;

    @Autowired
    public JwtAuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties,
            final Authorizer authorizer,
            final JwtDecoder jwtDecoder,
            final JwtRevocationService jwtRevocationService,
            final StandardVerificationKeySelector standardVerificationKeySelector,
            final VerificationKeyService verificationKeyService
    ) {
        this.niFiProperties = niFiProperties;
        this.authorizer = authorizer;
        this.jwtDecoder = jwtDecoder;
        this.jwtRevocationService = jwtRevocationService;
        this.verificationKeySelector = standardVerificationKeySelector;
        this.verificationKeyService = verificationKeyService;
        this.keyRotationPeriod = niFiProperties.getSecurityUserJwsKeyRotationPeriod();
    }

    /**
     * Bearer Token Authentication Filter responsible for reading and authenticating Bearer JSON Web Tokens from HTTP Requests
     *
     * @param authenticationManager Authentication Manager configured with JWT Authentication Provider
     * @return Bearer Token Authentication Filter
     */
    @Bean
    public BearerTokenAuthenticationFilter bearerTokenAuthenticationFilter(final AuthenticationManager authenticationManager) {
        final BearerTokenAuthenticationFilter bearerTokenAuthenticationFilter = new BearerTokenAuthenticationFilter(authenticationManager);
        bearerTokenAuthenticationFilter.setBearerTokenResolver(bearerTokenResolver());
        bearerTokenAuthenticationFilter.setAuthenticationEntryPoint(authenticationEntryPoint());
        return bearerTokenAuthenticationFilter;
    }

    /**
     * Bearer Token Resolver responsible for reading Bearer JSON Web Tokens from HTTP headers or cookies
     *
     * @return Standard implementation of Bearer Token Resolver
     */
    @Bean
    public BearerTokenResolver bearerTokenResolver() {
        return new StandardBearerTokenResolver();
    }

    /**
     * Authentication Entry Point delegating to Bearer Token Entry Point for returning headers on authentication failures
     *
     * @return Authentication Entry Point
     */
    @Bean
    public StandardAuthenticationEntryPoint authenticationEntryPoint() {
        final BearerTokenAuthenticationEntryPoint bearerTokenAuthenticationEntryPoint = new BearerTokenAuthenticationEntryPoint();
        return new StandardAuthenticationEntryPoint(bearerTokenAuthenticationEntryPoint);
    }

    /**
     * JSON Web Token Authentication Provider responsible for decoding and verifying Bearer Tokens from HTTP Requests
     *
     * @return JSON Web Token Authentication Provider
     */
    @Bean
    public JwtAuthenticationProvider jwtAuthenticationProvider() {
        final JwtAuthenticationProvider jwtAuthenticationProvider = new JwtAuthenticationProvider(jwtDecoder);
        jwtAuthenticationProvider.setJwtAuthenticationConverter(jwtAuthenticationConverter());
        return jwtAuthenticationProvider;
    }

    /**
     * JSON Web Token Logout Listener responsible for revoking application Bearer Tokens after logout completion
     *
     * @return JSON Web Token Logout Listener using Revocation Service for tracking
     */
    @Bean
    public JwtLogoutListener jwtLogoutListener() {
        return new StandardJwtLogoutListener(jwtDecoder, jwtRevocationService);
    }

    /**
     * JSON Web Token Authentication Converter provides application User objects
     *
     * @return Authentication Converter from JSON Web Tokens to User objects
     */
    @Bean
    public StandardJwtAuthenticationConverter jwtAuthenticationConverter() {
        return new StandardJwtAuthenticationConverter(authorizer, niFiProperties);
    }

    /**
     * Application Bearer Token Provider responsible for signing and encoding new JSON Web Tokens
     *
     * @return Application Bearer Token Provider
     */
    @Bean
    public BearerTokenProvider bearerTokenProvider() {
        return new StandardBearerTokenProvider(jwsSignerProvider(), issuerProvider());
    }

    @Bean
    public IssuerProvider issuerProvider() {
        return new StandardIssuerProvider(niFiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST), niFiProperties.getConfiguredHttpOrHttpsPort());
    }

    /**
     * JSON Web Signature Signer Provider responsible for managing Bearer Token signing key pairs
     *
     * @return JSON Web Signature Signer Provider
     */
    @Bean
    public StandardJwsSignerProvider jwsSignerProvider() {
        return new StandardJwsSignerProvider(verificationKeySelector);
    }

    /**
     * Key Generation Command responsible for rotating JSON Web Signature key pairs based on configuration
     *
     * @param keyPairGenerator Key Pair Generator for JSON Web Signatures
     * @return Key Generation Command scheduled according to application properties
     */
    @Bean
    public KeyGenerationCommand keyGenerationCommand(final KeyPairGenerator keyPairGenerator) {
        final KeyGenerationCommand command = new KeyGenerationCommand(jwsSignerProvider(), verificationKeySelector, keyPairGenerator);
        commandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    /**
     * Key Expiration Command responsible for removing expired signing key pairs
     *
     * @return Key Expiration Command scheduled according to application properties
     */
    @Bean
    public KeyExpirationCommand keyExpirationCommand() {
        final KeyExpirationCommand command = new KeyExpirationCommand(verificationKeyService);
        commandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    /**
     * Revocation Expiration Command responsible for removing expired application Bearer Token revocation records
     *
     * @return Revocation Expiration Command scheduled according to application properties
     */
    @Bean
    public RevocationExpirationCommand revocationExpirationCommand() {
        final RevocationExpirationCommand command = new RevocationExpirationCommand(jwtRevocationService);
        commandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    /**
     * Command Scheduler responsible for running commands in background thread
     *
     * @return Thread Pool Task Scheduler with named threads
     */
    @Bean
    public ThreadPoolTaskScheduler commandScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix(JwtAuthenticationSecurityConfiguration.class.getSimpleName());
        return scheduler;
    }
}
