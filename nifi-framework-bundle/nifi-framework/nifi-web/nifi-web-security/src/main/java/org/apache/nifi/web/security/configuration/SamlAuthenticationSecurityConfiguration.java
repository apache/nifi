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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.apache.nifi.web.security.saml2.service.authentication.ResponseAuthenticationConverter;
import org.apache.nifi.web.security.saml2.registration.Saml2RegistrationProperty;
import org.apache.nifi.web.security.saml2.service.web.StandardRelyingPartyRegistrationResolver;
import org.apache.nifi.web.security.saml2.service.web.StandardSaml2AuthenticationRequestRepository;
import org.apache.nifi.web.security.saml2.web.authentication.Saml2AuthenticationSuccessHandler;
import org.apache.nifi.web.security.saml2.registration.StandardRelyingPartyRegistrationRepository;
import org.apache.nifi.web.security.saml2.web.authentication.identity.AttributeNameIdentityConverter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2LocalLogoutFilter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2SingleLogoutFilter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2SingleLogoutHandler;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2LogoutSuccessHandler;
import org.apache.nifi.web.security.saml2.web.authentication.logout.StandardSaml2LogoutRequestRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.saml2.provider.service.authentication.AbstractSaml2AuthenticationRequest;
import org.springframework.security.saml2.provider.service.authentication.OpenSaml5AuthenticationProvider;
import org.springframework.security.saml2.provider.service.authentication.logout.OpenSaml5LogoutRequestValidator;
import org.springframework.security.saml2.provider.service.authentication.logout.OpenSaml5LogoutResponseValidator;
import org.springframework.security.saml2.provider.service.authentication.logout.Saml2LogoutRequestValidator;
import org.springframework.security.saml2.provider.service.authentication.logout.Saml2LogoutResponseValidator;
import org.springframework.security.saml2.provider.service.metadata.OpenSaml5MetadataResolver;
import org.springframework.security.saml2.provider.service.metadata.Saml2MetadataResolver;
import org.springframework.security.saml2.provider.service.registration.InMemoryRelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.web.authentication.OpenSaml5AuthenticationRequestResolver;
import org.springframework.security.saml2.provider.service.web.authentication.Saml2WebSsoAuthenticationFilter;
import org.springframework.security.saml2.provider.service.web.Saml2WebSsoAuthenticationRequestFilter;
import org.springframework.security.saml2.provider.service.web.RelyingPartyRegistrationResolver;
import org.springframework.security.saml2.provider.service.web.Saml2AuthenticationRequestRepository;
import org.springframework.security.saml2.provider.service.web.Saml2AuthenticationTokenConverter;
import org.springframework.security.saml2.provider.service.web.Saml2MetadataFilter;
import org.springframework.security.saml2.provider.service.web.authentication.Saml2AuthenticationRequestResolver;
import org.springframework.security.saml2.provider.service.web.authentication.logout.OpenSaml5LogoutRequestResolver;
import org.springframework.security.saml2.provider.service.web.authentication.logout.OpenSaml5LogoutResponseResolver;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestRepository;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestResolver;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutResponseFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutResponseResolver;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2RelyingPartyInitiatedLogoutSuccessHandler;
import org.springframework.security.web.authentication.session.NullAuthenticatedSessionStrategy;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * SAML Configuration for Authentication Security
 */
@Configuration
public class SamlAuthenticationSecurityConfiguration {
    private static final Duration REQUEST_EXPIRATION = Duration.ofSeconds(60);

    private static final long REQUEST_MAXIMUM_CACHE_SIZE = 1000;

    private final NiFiProperties properties;

    private final BearerTokenProvider bearerTokenProvider;

    private final LogoutRequestManager logoutRequestManager;

    private final X509ExtendedKeyManager keyManager;

    private final X509ExtendedTrustManager trustManager;

    public SamlAuthenticationSecurityConfiguration(
            @Autowired final NiFiProperties properties,
            @Autowired final BearerTokenProvider bearerTokenProvider,
            @Autowired final LogoutRequestManager logoutRequestManager,
            @Autowired(required = false) final X509ExtendedKeyManager keyManager,
            @Autowired(required = false) final X509ExtendedTrustManager trustManager
    ) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
        this.bearerTokenProvider = Objects.requireNonNull(bearerTokenProvider, "Bearer Token Provider required");
        this.logoutRequestManager = Objects.requireNonNull(logoutRequestManager, "Logout Request Manager required");
        this.keyManager = keyManager;
        this.trustManager = trustManager;
    }

    /**
     * Spring Security SAML 2 Metadata Filter returns SAML 2 Metadata XML
     *
     * @return SAML 2 Metadata Filter
     */
    @Bean
    public Saml2MetadataFilter saml2MetadataFilter() {
        final Saml2MetadataFilter filter = new Saml2MetadataFilter(relyingPartyRegistrationResolver(), saml2MetadataResolver());
        filter.setRequestMatcher(new AntPathRequestMatcher(SamlUrlPath.METADATA.getPath()));
        return filter;
    }

    /**
     * Spring Security SAML 2 Web SSO Authentication Request Filter for SAML 2 initial login sending to an IDP
     *
     * @return SAML 2 Authentication Request Filter
     */
    @Bean
    public Saml2WebSsoAuthenticationRequestFilter saml2WebSsoAuthenticationRequestFilter() {
        final Saml2WebSsoAuthenticationRequestFilter filter = new Saml2WebSsoAuthenticationRequestFilter(saml2AuthenticationRequestResolver());
        filter.setAuthenticationRequestRepository(saml2AuthenticationRequestRepository());
        return filter;
    }

    /**
     * Spring Security SAML 2 Web SSO Authentication Filter for SAML 2 login response processing from an IDP
     *
     * @param authenticationManager Spring Security Authentication Manager
     * @return SAML 2 Authentication Filter
     */
    @Bean
    public Saml2WebSsoAuthenticationFilter saml2WebSsoAuthenticationFilter(final AuthenticationManager authenticationManager) {
        final Saml2AuthenticationTokenConverter authenticationTokenConverter = new Saml2AuthenticationTokenConverter(relyingPartyRegistrationResolver());
        final Saml2WebSsoAuthenticationFilter filter = new Saml2WebSsoAuthenticationFilter(authenticationTokenConverter, SamlUrlPath.LOGIN_RESPONSE_REGISTRATION_ID.getPath());
        filter.setAuthenticationManager(authenticationManager);
        filter.setAuthenticationSuccessHandler(getAuthenticationSuccessHandler());
        filter.setAuthenticationRequestRepository(saml2AuthenticationRequestRepository());
        // Disable HTTP Sessions
        filter.setAllowSessionCreation(false);
        filter.setSessionAuthenticationStrategy(new NullAuthenticatedSessionStrategy());
        return filter;
    }

    /**
     * Spring Security Single Logout Filter for initiating Single Logout Requests sending to an IDP
     *
     * @return SAML 2 Single Logout Filter
     */
    @Bean
    public Saml2SingleLogoutFilter saml2SingleLogoutFilter() {
        return new Saml2SingleLogoutFilter(logoutRequestManager, saml2SingleLogoutSuccessHandler());
    }

    /**
     * Spring Security SAML 2 Single Logout Request Filter processing from an IDP
     *
     * @return SAML 2 Logout Request Filter
     */
    @Bean
    public Saml2LogoutRequestFilter saml2LogoutRequestFilter() {
        final Saml2LogoutRequestFilter filter = new Saml2LogoutRequestFilter(
                relyingPartyRegistrationResolver(),
                saml2LogoutRequestValidator(),
                saml2LogoutResponseResolver(),
                saml2SingleLogoutHandler()
        );
        filter.setLogoutRequestMatcher(new AntPathRequestMatcher(SamlUrlPath.SINGLE_LOGOUT_RESPONSE.getPath()));
        return filter;
    }

    /**
     * Spring Security SAML 2 Single Logout Response Filter processing from an IDP
     *
     * @return SAML 2 Logout Response Filter
     */
    @Bean
    public Saml2LogoutResponseFilter saml2LogoutResponseFilter() {
        final Saml2LogoutResponseFilter saml2LogoutResponseFilter = new Saml2LogoutResponseFilter(
                relyingPartyRegistrationResolver(),
                saml2LogoutResponseValidator(),
                saml2LogoutSuccessHandler()
        );
        saml2LogoutResponseFilter.setLogoutRequestRepository(saml2LogoutRequestRepository());
        saml2LogoutResponseFilter.setLogoutRequestMatcher(new AntPathRequestMatcher(SamlUrlPath.SINGLE_LOGOUT_RESPONSE.getPath()));
        return saml2LogoutResponseFilter;
    }

    /**
     * Standard SAML 2 Single Logout Handler
     *
     * @return SAML 2 Single Logout Handler
     */
    @Bean
    public Saml2SingleLogoutHandler saml2SingleLogoutHandler() {
        return new Saml2SingleLogoutHandler();
    }

    /**
     * SAML 2 Local Logout Filter for clearing application caches on Logout requests
     *
     * @return SAML 2 Local Logout Filter
     */
    @Bean
    public Saml2LocalLogoutFilter saml2LocalLogoutFilter() {
        return new Saml2LocalLogoutFilter(saml2LogoutSuccessHandler());
    }

    /**
     * Spring Security OpenSAML Authentication Provider for processing SAML 2 login responses
     *
     * @return OpenSAML Authentication Provider
     */
    @Bean
    public OpenSaml5AuthenticationProvider openSamlAuthenticationProvider() {
        final OpenSaml5AuthenticationProvider provider = new OpenSaml5AuthenticationProvider();
        final ResponseAuthenticationConverter responseAuthenticationConverter = new ResponseAuthenticationConverter(properties.getSamlGroupAttributeName());
        provider.setResponseAuthenticationConverter(responseAuthenticationConverter);
        return provider;
    }

    /**
     * Spring Security SAML 2 Authentication Request Resolver uses OpenSAML 4
     *
     * @return OpenSAML SAML 2 Authentication Request Resolver
     */
    @Bean
    public Saml2AuthenticationRequestResolver saml2AuthenticationRequestResolver() {
        return new OpenSaml5AuthenticationRequestResolver(relyingPartyRegistrationResolver());
    }

    /**
     * Spring Security SAML 2 Logout Request Validator
     *
     * @return OpenSAML Logout Request Validator
     */
    @Bean
    public Saml2LogoutRequestValidator saml2LogoutRequestValidator() {
        return new OpenSaml5LogoutRequestValidator();
    }

    /**
     * Spring Security SAML 2 Logout Response Validator
     *
     * @return OpenSAML Logout Response Validator
     */
    @Bean
    public Saml2LogoutResponseValidator saml2LogoutResponseValidator() {
        return new OpenSaml5LogoutResponseValidator();
    }

    /**
     * Spring Security SAML 2 Logout Request Resolver uses OpenSAML 4
     *
     * @return OpenSAML SAML 2 Logout Request Resolver
     */
    @Bean
    public Saml2LogoutRequestResolver saml2LogoutRequestResolver() {
        return new OpenSaml5LogoutRequestResolver(relyingPartyRegistrationResolver());
    }

    /**
     * Spring Security SAML 2 Logout Response Resolver uses OpenSAML 4
     *
     * @return OpenSAML SAML 2 Logout Response Resolver
     */
    @Bean
    public Saml2LogoutResponseResolver saml2LogoutResponseResolver() {
        return new OpenSaml5LogoutResponseResolver(relyingPartyRegistrationResolver());
    }

    /**
     * Spring Security Saml 2 Authentication Request Repository for tracking SAML 2 across multiple HTTP requests
     *
     * @return SAML 2 Authentication Request Repository
     */
    @Bean
    public Saml2AuthenticationRequestRepository<AbstractSaml2AuthenticationRequest> saml2AuthenticationRequestRepository() {
        final Cache<Object, Object> caffeineCache = Caffeine.newBuilder()
                .maximumSize(REQUEST_MAXIMUM_CACHE_SIZE)
                .expireAfterWrite(REQUEST_EXPIRATION)
                .build();
        final CaffeineCache cache = new CaffeineCache(Saml2AuthenticationRequestRepository.class.getSimpleName(), caffeineCache);
        return new StandardSaml2AuthenticationRequestRepository(cache);
    }

    /**
     * Spring Security SAML 2 Relying Party Registration Resolver for SAML 2 initial login processing
     *
     * @return Default Relying Party Registration Resolver
     */
    @Bean
    public RelyingPartyRegistrationResolver relyingPartyRegistrationResolver() {
        return new StandardRelyingPartyRegistrationResolver(relyingPartyRegistrationRepository());
    }

    /**
     * Spring Security SAML 2 Relying Party Registration Repository generated using NiFi Properties
     *
     * @return Standard Relying Party Registration Repository or placeholder repository when SAML is disabled
     */
    @Bean
    public RelyingPartyRegistrationRepository relyingPartyRegistrationRepository() {
        return properties.isSamlEnabled()
                ? new StandardRelyingPartyRegistrationRepository(properties, keyManager, trustManager)
                : getDisabledRelyingPartyRegistrationRepository();
    }

    /**
     * Spring Security SAML 2 Metadata Resolver
     *
     * @return OpenSAML SAML 2 Metadata Resolver
     */
    @Bean
    public Saml2MetadataResolver saml2MetadataResolver() {
        final OpenSaml5MetadataResolver resolver = new OpenSaml5MetadataResolver();
        resolver.setSignMetadata(properties.isSamlRequestSigningEnabled());
        return resolver;
    }

    /**
     * Standard SAML 2 Logout Success Handler for Logout processing after Single or Local Logout success
     *
     * @return SAML 2 Logout Success Handler
     */
    @Bean
    public Saml2LogoutSuccessHandler saml2LogoutSuccessHandler() {
        return new Saml2LogoutSuccessHandler(logoutRequestManager);
    }

    /**
     * SAML 2 Logout Success Handler for Single Logout processing
     *
     * @return Spring Security SAML 2 Logout Success Handler
     */
    @Bean
    public Saml2RelyingPartyInitiatedLogoutSuccessHandler saml2SingleLogoutSuccessHandler() {
        final Saml2RelyingPartyInitiatedLogoutSuccessHandler handler = new Saml2RelyingPartyInitiatedLogoutSuccessHandler(saml2LogoutRequestResolver());
        handler.setLogoutRequestRepository(saml2LogoutRequestRepository());
        return handler;
    }

    /**
     * SAML 2 Logout Request Repository for tracking Single Logout requests
     *
     * @return SAML 2 Logout Request Repository
     */
    @Bean
    public Saml2LogoutRequestRepository saml2LogoutRequestRepository() {
        final Cache<Object, Object> caffeineCache = Caffeine.newBuilder()
                .maximumSize(REQUEST_MAXIMUM_CACHE_SIZE)
                .expireAfterWrite(REQUEST_EXPIRATION)
                .build();
        final CaffeineCache cache = new CaffeineCache(Saml2LogoutRequestRepository.class.getSimpleName(), caffeineCache);
        return new StandardSaml2LogoutRequestRepository(cache);
    }

    private Saml2AuthenticationSuccessHandler getAuthenticationSuccessHandler() {
        final long authenticationExpiration = (long) FormatUtils.getPreciseTimeDuration(properties.getSamlAuthenticationExpiration(), TimeUnit.MILLISECONDS);
        final Duration expiration = Duration.ofMillis(authenticationExpiration);
        final Saml2AuthenticationSuccessHandler handler = new Saml2AuthenticationSuccessHandler(
                bearerTokenProvider,
                IdentityMappingUtil.getIdentityMappings(properties),
                IdentityMappingUtil.getGroupMappings(properties),
                expiration
        );

        final String identityAttributeName = properties.getSamlIdentityAttributeName();
        if (StringUtils.isNotBlank(identityAttributeName)) {
            final AttributeNameIdentityConverter identityConverter = new AttributeNameIdentityConverter(identityAttributeName);
            handler.setIdentityConverter(identityConverter);
        }

        return handler;
    }

    private RelyingPartyRegistrationRepository getDisabledRelyingPartyRegistrationRepository() {
        final RelyingPartyRegistration registration = RelyingPartyRegistration
                .withRegistrationId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty())
                .entityId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty())
                .assertingPartyMetadata(assertingPartyMetadata -> {
                    assertingPartyMetadata.entityId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty());
                    assertingPartyMetadata.singleSignOnServiceLocation(SamlUrlPath.LOGIN_RESPONSE_REGISTRATION_ID.getPath());
                })
                .build();
        return new InMemoryRelyingPartyRegistrationRepository(registration);
    }
}
