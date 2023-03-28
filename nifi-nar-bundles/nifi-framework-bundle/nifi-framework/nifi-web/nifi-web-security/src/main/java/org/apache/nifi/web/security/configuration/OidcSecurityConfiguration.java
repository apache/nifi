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
import okhttp3.OkHttpClient;
import org.apache.nifi.admin.service.IdpUserGroupService;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.StandardAuthenticationEntryPoint;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.oidc.OidcConfigurationException;
import org.apache.nifi.web.security.oidc.OidcUrlPath;
import org.apache.nifi.web.security.oidc.client.web.AuthorizedClientExpirationCommand;
import org.apache.nifi.web.security.oidc.client.web.OidcBearerTokenRefreshFilter;
import org.apache.nifi.web.security.oidc.client.web.converter.AuthenticationResultConverter;
import org.apache.nifi.web.security.oidc.client.web.converter.AuthorizedClientConverter;
import org.apache.nifi.web.security.oidc.client.web.StandardAuthorizationRequestRepository;
import org.apache.nifi.web.security.oidc.client.web.converter.StandardAuthorizedClientConverter;
import org.apache.nifi.web.security.oidc.client.web.StandardOidcAuthorizedClientRepository;
import org.apache.nifi.web.security.oidc.logout.OidcLogoutFilter;
import org.apache.nifi.web.security.oidc.logout.OidcLogoutSuccessHandler;
import org.apache.nifi.web.security.oidc.registration.ClientRegistrationProvider;
import org.apache.nifi.web.security.oidc.registration.DisabledClientRegistrationRepository;
import org.apache.nifi.web.security.oidc.registration.StandardClientRegistrationProvider;
import org.apache.nifi.web.security.oidc.revocation.StandardTokenRevocationResponseClient;
import org.apache.nifi.web.security.oidc.revocation.TokenRevocationResponseClient;
import org.apache.nifi.web.security.oidc.web.authentication.OidcAuthenticationSuccessHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.OkHttp3ClientHttpRequestFactory;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.oauth2.client.endpoint.DefaultAuthorizationCodeTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.DefaultRefreshTokenTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2AccessTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2AuthorizationCodeGrantRequest;
import org.springframework.security.oauth2.client.http.OAuth2ErrorResponseErrorHandler;
import org.springframework.security.oauth2.client.oidc.authentication.OidcAuthorizationCodeAuthenticationProvider;
import org.springframework.security.oauth2.client.oidc.authentication.OidcIdTokenDecoderFactory;
import org.springframework.security.oauth2.client.oidc.userinfo.OidcUserService;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.client.userinfo.DefaultOAuth2UserService;
import org.springframework.security.oauth2.client.web.AuthorizationRequestRepository;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationCodeGrantFilter;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestRedirectFilter;
import org.springframework.security.oauth2.client.web.OAuth2LoginAuthenticationFilter;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.oauth2.core.http.converter.OAuth2AccessTokenResponseHttpMessageConverter;
import org.springframework.security.oauth2.jose.jws.JwsAlgorithm;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoderFactory;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.web.authentication.AuthenticationEntryPointFailureHandler;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.authentication.session.NullAuthenticatedSessionStrategy;
import org.springframework.security.web.savedrequest.NullRequestCache;
import org.springframework.security.web.savedrequest.RequestCache;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * OpenID Connect Configuration for Spring Security
 */
@Configuration
public class OidcSecurityConfiguration {
    private static final Duration REQUEST_EXPIRATION = Duration.ofSeconds(60);

    private static final long AUTHORIZATION_REQUEST_CACHE_SIZE = 1000;

    private static final Duration DEFAULT_SOCKET_TIMEOUT = Duration.ofSeconds(5);

    private static final String NIFI_TRUSTSTORE_STRATEGY = "NIFI";

    private static final RequestCache nullRequestCache = new NullRequestCache();

    private final Duration keyRotationPeriod;

    private final NiFiProperties properties;

    private final StateManagerProvider stateManagerProvider;

    private final PropertyEncryptor propertyEncryptor;

    private final BearerTokenProvider bearerTokenProvider;

    private final BearerTokenResolver bearerTokenResolver;

    private final IdpUserGroupService idpUserGroupService;

    private final JwtDecoder jwtDecoder;

    private final LogoutRequestManager logoutRequestManager;

    @Autowired
    public OidcSecurityConfiguration(
            final NiFiProperties properties,
            final StateManagerProvider stateManagerProvider,
            final PropertyEncryptor propertyEncryptor,
            final BearerTokenProvider bearerTokenProvider,
            final BearerTokenResolver bearerTokenResolver,
            final IdpUserGroupService idpUserGroupService,
            final JwtDecoder jwtDecoder,
            final LogoutRequestManager logoutRequestManager
    ) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
        this.stateManagerProvider = Objects.requireNonNull(stateManagerProvider, "State Manager Provider required");
        this.propertyEncryptor = Objects.requireNonNull(propertyEncryptor, "Property Encryptor required");
        this.bearerTokenProvider = Objects.requireNonNull(bearerTokenProvider, "Bearer Token Provider required");
        this.bearerTokenResolver = Objects.requireNonNull(bearerTokenResolver, "Bearer Token Resolver required");
        this.idpUserGroupService = Objects.requireNonNull(idpUserGroupService, "User Group Service required");
        this.jwtDecoder = Objects.requireNonNull(jwtDecoder, "JWT Decoder required");
        this.logoutRequestManager = Objects.requireNonNull(logoutRequestManager, "Logout Request Manager required");
        this.keyRotationPeriod = properties.getSecurityUserJwsKeyRotationPeriod();
    }

    /**
     * Authorization Code Grant Filter handles Authorization Server responses and updates the Authorized Client
     * Repository with ID Token and optional Refresh Token information
     *
     * @param authenticationManager Spring Security Authentication Manager
     * @return OAuth2 Authorization Code Grant Filter
     */
    @Bean
    public OAuth2AuthorizationCodeGrantFilter oAuth2AuthorizationCodeGrantFilter(final AuthenticationManager authenticationManager) {
        final OAuth2AuthorizationCodeGrantFilter filter = new OAuth2AuthorizationCodeGrantFilter(
                clientRegistrationRepository(),
                authorizedClientRepository(),
                authenticationManager
        );
        filter.setAuthorizationRequestRepository(authorizationRequestRepository());
        filter.setRequestCache(nullRequestCache);
        return filter;
    }

    /**
     * Authorization Request Redirect Filter handles initial OpenID Connect authentication and redirects to the
     * Authorization Server using default filter path from Spring Security
     *
     * @return OAuth2 Authorization Request Redirect Filter
     */
    @Bean
    public OAuth2AuthorizationRequestRedirectFilter oAuth2AuthorizationRequestRedirectFilter() {
        final OAuth2AuthorizationRequestRedirectFilter filter = new OAuth2AuthorizationRequestRedirectFilter(clientRegistrationRepository());
        filter.setAuthorizationRequestRepository(authorizationRequestRepository());
        filter.setRequestCache(nullRequestCache);
        return filter;
    }

    /**
     * Login Authentication Filter handles Authentication Responses from the Authorization Server
     *
     * @param authenticationManager Spring Security Authentication Manager
     * @param authenticationEntryPoint Authentication Entry Point for handling failures
     * @return OAuth2 Login Authentication Filter
     */
    @Bean
    public OAuth2LoginAuthenticationFilter oAuth2LoginAuthenticationFilter(final AuthenticationManager authenticationManager, final StandardAuthenticationEntryPoint authenticationEntryPoint) {
        final OAuth2LoginAuthenticationFilter filter = new OAuth2LoginAuthenticationFilter(
                clientRegistrationRepository(),
                authorizedClientRepository(),
                OidcUrlPath.CALLBACK.getPath()
        );
        filter.setAuthenticationManager(authenticationManager);
        filter.setAuthorizationRequestRepository(authorizationRequestRepository());
        filter.setAuthenticationSuccessHandler(getAuthenticationSuccessHandler());
        filter.setAllowSessionCreation(false);
        filter.setSessionAuthenticationStrategy(new NullAuthenticatedSessionStrategy());
        filter.setAuthenticationResultConverter(new AuthenticationResultConverter());

        final AuthenticationEntryPointFailureHandler authenticationFailureHandler = new AuthenticationEntryPointFailureHandler(authenticationEntryPoint);
        filter.setAuthenticationFailureHandler(authenticationFailureHandler);
        return filter;
    }

    /**
     * OpenID Connect Bearer Token Refresh Filter exchanges OAuth2 Refresh Tokens with the Authorization Server and
     * generates new application Bearer Tokens on successful responses
     *
     * @return Bearer Token Refresh Filter
     */
    @Bean
    public OidcBearerTokenRefreshFilter oidcBearerTokenRefreshFilter() {
        final DefaultRefreshTokenTokenResponseClient refreshTokenResponseClient = new DefaultRefreshTokenTokenResponseClient();
        refreshTokenResponseClient.setRestOperations(oidcRestOperations());

        final String refreshWindowProperty = properties.getOidcTokenRefreshWindow();
        final double refreshWindowSeconds = FormatUtils.getPreciseTimeDuration(refreshWindowProperty, TimeUnit.SECONDS);
        final Duration refreshWindow = Duration.ofSeconds(Math.round(refreshWindowSeconds));

        return new OidcBearerTokenRefreshFilter(
                refreshWindow,
                bearerTokenProvider,
                bearerTokenResolver,
                jwtDecoder,
                authorizedClientRepository(),
                refreshTokenResponseClient
        );
    }

    /**
     * Logout Filter for completing logout processing using RP-Initiated Logout 1.0 when supported
     *
     * @return OpenID Connect Logout Filter
     */
    @Bean
    public OidcLogoutFilter oidcLogoutFilter() {
        return new OidcLogoutFilter(oidcLogoutSuccessHandler());
    }

    /**
     * Logout Success Handler redirects to the Authorization Server when supported
     *
     * @return Logout Success Handler
     */
    @Bean
    public LogoutSuccessHandler oidcLogoutSuccessHandler() {
        return new OidcLogoutSuccessHandler(
                logoutRequestManager,
                idpUserGroupService,
                clientRegistrationRepository(),
                authorizedClientRepository(),
                tokenRevocationResponseClient()
        );
    }

    /**
     * Authorization Code Grant Authentication Provider wired to Spring Security Authentication Manager
     *
     * @return OpenID Connect Authorization Code Authentication Provider
     */
    @Bean
    public OidcAuthorizationCodeAuthenticationProvider oidcAuthorizationCodeAuthenticationProvider() {
        final OidcAuthorizationCodeAuthenticationProvider provider = new OidcAuthorizationCodeAuthenticationProvider(
                accessTokenResponseClient(),
                oidcUserService()
        );
        provider.setJwtDecoderFactory(idTokenDecoderFactory());
        return provider;
    }

    /**
     * Access Token Response Client for retrieving Access Tokens using Authorization Codes
     *
     * @return OAuth2 Access Token Response Client
     */
    @Bean
    public OAuth2AccessTokenResponseClient<OAuth2AuthorizationCodeGrantRequest> accessTokenResponseClient() {
        final DefaultAuthorizationCodeTokenResponseClient accessTokenResponseClient = new DefaultAuthorizationCodeTokenResponseClient();
        accessTokenResponseClient.setRestOperations(oidcRestOperations());
        return accessTokenResponseClient;
    }

    /**
     * OpenID Connect User Service wired to Authentication Provider for retrieving User Information
     *
     * @return OpenID Connect User Service
     */
    @Bean
    public OidcUserService oidcUserService() {
        final OidcUserService oidcUserService = new OidcUserService();
        final DefaultOAuth2UserService userService = new DefaultOAuth2UserService();
        userService.setRestOperations(oidcRestOperations());
        oidcUserService.setOauth2UserService(userService);
        return oidcUserService;
    }

    /**
     * Authorized Client Repository for storing OpenID Connect Tokens in application State Manager
     *
     * @return Authorized Client Repository
     */
    @Bean
    public StandardOidcAuthorizedClientRepository authorizedClientRepository() {
        final StateManager stateManager = stateManagerProvider.getStateManager(StandardOidcAuthorizedClientRepository.class.getName());
        return new StandardOidcAuthorizedClientRepository(stateManager, authorizedClientConverter());
    }

    @Bean
    public AuthorizedClientExpirationCommand authorizedClientExpirationCommand() {
        final AuthorizedClientExpirationCommand command = new AuthorizedClientExpirationCommand(authorizedClientRepository(), tokenRevocationResponseClient());
        oidcCommandScheduler().scheduleAtFixedRate(command, keyRotationPeriod);
        return command;
    }

    /**
     * Command Scheduled for OpenID Connect operations
     *
     * @return Thread Pool Task Executor
     */
    @Bean
    public ThreadPoolTaskScheduler oidcCommandScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix(OidcSecurityConfiguration.class.getSimpleName());
        return scheduler;
    }

    /**
     * Authorized Client Converter for OpenID Connect Tokens supporting serialization of OpenID Connect Tokens
     *
     * @return Authorized Client Converter
     */
    @Bean
    public AuthorizedClientConverter authorizedClientConverter() {
        return new StandardAuthorizedClientConverter(propertyEncryptor, clientRegistrationRepository());
    }

    /**
     * OpenID Connect Authorization Request Repository with Cache abstraction based on Caffeine implementation
     *
     * @return Authorization Request Repository
     */
    @Bean
    public AuthorizationRequestRepository<OAuth2AuthorizationRequest> authorizationRequestRepository() {
        final Cache<Object, Object> caffeineCache = Caffeine.newBuilder()
                .maximumSize(AUTHORIZATION_REQUEST_CACHE_SIZE)
                .expireAfterWrite(REQUEST_EXPIRATION)
                .build();
        final CaffeineCache cache = new CaffeineCache(StandardAuthorizationRequestRepository.class.getSimpleName(), caffeineCache);
        return new StandardAuthorizationRequestRepository(cache);
    }

    /**
     * OpenID Connect Identifier Token Decoder with configured JWS Algorithm for verification
     *
     * @return OpenID Connect Identifier Token Decoder
     */
    @Bean
    public JwtDecoderFactory<ClientRegistration> idTokenDecoderFactory() {
        OidcIdTokenDecoderFactory idTokenDecoderFactory = new OidcIdTokenDecoderFactory();

        final String preferredJwdAlgorithm = properties.getOidcPreferredJwsAlgorithm();
        if (StringUtils.isNotEmpty(preferredJwdAlgorithm)) {
            idTokenDecoderFactory.setJwsAlgorithmResolver(clientRegistration -> getJwsAlgorithm(preferredJwdAlgorithm));
        }

        return idTokenDecoderFactory;
    }

    /**
     * Token Revocation Response Client responsible for transmitting Refresh Token revocation requests to the Provider
     *
     * @return Token Revocation Response Client
     */
    @Bean
    public TokenRevocationResponseClient tokenRevocationResponseClient() {
        return new StandardTokenRevocationResponseClient(oidcRestOperations(), clientRegistrationRepository());
    }

    /**
     * Client Registration Repository for OpenID Connect Discovery
     *
     * @return Client Registration Repository
     */
    @Bean
    public ClientRegistrationRepository clientRegistrationRepository() {
        final ClientRegistrationRepository clientRegistrationRepository;
        if (properties.isOidcEnabled()) {
            final ClientRegistrationProvider clientRegistrationProvider = new StandardClientRegistrationProvider(properties, oidcRestOperations());
            final ClientRegistration clientRegistration = clientRegistrationProvider.getClientRegistration();
            clientRegistrationRepository = new InMemoryClientRegistrationRepository(clientRegistration);
        } else {
            clientRegistrationRepository = new DisabledClientRegistrationRepository();
        }
        return clientRegistrationRepository;
    }

    /**
     * OpenID Connect REST Operations for communication with Authorization Servers
     *
     * @return REST Operations
     */
    @Bean
    public RestOperations oidcRestOperations() {
        final RestTemplate restTemplate = new RestTemplate(oidcClientHttpRequestFactory());
        restTemplate.setErrorHandler(new OAuth2ErrorResponseErrorHandler());
        restTemplate.setMessageConverters(
                Arrays.asList(
                        new FormHttpMessageConverter(),
                        new OAuth2AccessTokenResponseHttpMessageConverter(),
                        new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter()
                )
        );
        return restTemplate;
    }

    /**
     * OpenID Connect Client HTTP Request Factory for communication with Authorization Servers
     *
     * @return Client HTTP Request Factory
     */
    @Bean
    public ClientHttpRequestFactory oidcClientHttpRequestFactory() {
        final OkHttpClient httpClient = getHttpClient();
        return new OkHttp3ClientHttpRequestFactory(httpClient);
    }

    private OkHttpClient getHttpClient() {
        final Duration connectTimeout = getTimeout(properties.getOidcConnectTimeout());
        final Duration readTimeout = getTimeout(properties.getOidcReadTimeout());

        final OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(connectTimeout)
                .readTimeout(readTimeout);

        if (NIFI_TRUSTSTORE_STRATEGY.equals(properties.getOidcClientTruststoreStrategy())) {
            setSslSocketFactory(builder);
        }

        return builder.build();
    }

    private Duration getTimeout(final String timeoutExpression) {
        try {
            final double duration = FormatUtils.getPreciseTimeDuration(timeoutExpression, TimeUnit.MILLISECONDS);
            final long rounded = Math.round(duration);
            return Duration.ofMillis(rounded);
        } catch (final RuntimeException e) {
            return DEFAULT_SOCKET_TIMEOUT;
        }
    }

    private void setSslSocketFactory(final OkHttpClient.Builder builder) {
        final TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);

        try {
            final X509TrustManager trustManager = Objects.requireNonNull(SslContextFactory.getX509TrustManager(tlsConfiguration), "TrustManager required");
            final TrustManager[] trustManagers = new TrustManager[] { trustManager };
            final SSLContext sslContext = Objects.requireNonNull(SslContextFactory.createSslContext(tlsConfiguration, trustManagers), "SSLContext required");
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
            builder.sslSocketFactory(sslSocketFactory, trustManager);
        } catch (final TlsException e) {
            throw new OidcConfigurationException("OpenID Connect HTTP TLS configuration failed", e);
        }
    }

    private OidcAuthenticationSuccessHandler getAuthenticationSuccessHandler() {
        final List<String> userClaimNames = new ArrayList<>();
        userClaimNames.add(properties.getOidcClaimIdentifyingUser());
        userClaimNames.addAll(properties.getOidcFallbackClaimsIdentifyingUser());

        return new OidcAuthenticationSuccessHandler(
                bearerTokenProvider,
                idpUserGroupService,
                IdentityMappingUtil.getIdentityMappings(properties),
                IdentityMappingUtil.getGroupMappings(properties),
                userClaimNames,
                properties.getOidcClaimGroups()
        );
    }

    private JwsAlgorithm getJwsAlgorithm(final String preferredJwsAlgorithm) {
        final JwsAlgorithm jwsAlgorithm;

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

        return jwsAlgorithm;
    }
}
