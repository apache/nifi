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
package org.apache.nifi.registry.web.security;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.resource.ResourceType;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.service.AuthorizationService;
import org.apache.nifi.registry.web.security.authentication.AnonymousIdentityFilter;
import org.apache.nifi.registry.web.security.authentication.IdentityAuthenticationProvider;
import org.apache.nifi.registry.web.security.authentication.IdentityFilter;
import org.apache.nifi.registry.web.security.authentication.csrf.CsrfCookieFilter;
import org.apache.nifi.registry.web.security.authentication.csrf.CsrfCookieName;
import org.apache.nifi.registry.web.security.authentication.csrf.CsrfRequestMatcher;
import org.apache.nifi.registry.web.security.authentication.csrf.StandardCookieCsrfTokenRepository;
import org.apache.nifi.registry.web.security.authentication.csrf.StandardCsrfTokenRequestAttributeHandler;
import org.apache.nifi.registry.web.security.authentication.jwt.JwtIdentityProvider;
import org.apache.nifi.registry.web.security.authentication.x509.X509IdentityAuthenticationProvider;
import org.apache.nifi.registry.web.security.authentication.x509.X509IdentityProvider;
import org.apache.nifi.registry.web.security.authorization.ResourceAuthorizationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.access.intercept.AuthorizationFilter;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.csrf.CsrfException;

import java.io.IOException;

import static org.springframework.security.web.util.matcher.AntPathRequestMatcher.antMatcher;

/**
 * Spring Security Filter Configuration
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class NiFiRegistrySecurityConfig {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistrySecurityConfig.class);

    @Autowired
    private IdentityMapper identityMapper;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private Authorizer authorizer;

    @Autowired
    private X509IdentityProvider x509IdentityProvider;

    @Autowired
    private JwtIdentityProvider jwtIdentityProvider;

    @Bean
    public SecurityFilterChain securityFilterChain(final HttpSecurity http) throws Exception {
        return http
                .addFilterBefore(x509AuthenticationFilter(), AnonymousAuthenticationFilter.class)
                .addFilterBefore(jwtAuthenticationFilter(), AnonymousAuthenticationFilter.class)
                // Add Resource Authorization after Spring Security but before Jersey Resources
                .addFilterAfter(resourceAuthorizationFilter(), AuthorizationFilter.class)
                .anonymous(anonymous -> anonymous.authenticationFilter(new AnonymousIdentityFilter()))
                .csrf(csrf -> {
                    csrf.requireCsrfProtectionMatcher(new CsrfRequestMatcher());
                    csrf.csrfTokenRepository(new StandardCookieCsrfTokenRepository());
                    csrf.csrfTokenRequestHandler(new StandardCsrfTokenRequestAttributeHandler());
                })
                .addFilterAfter(new CsrfCookieFilter(), BasicAuthenticationFilter.class)
                .logout(AbstractHttpConfigurer::disable)
                .rememberMe(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .servletApi(AbstractHttpConfigurer::disable)
                .securityContext(AbstractHttpConfigurer::disable)
                .sessionManagement(AbstractHttpConfigurer::disable)
                .headers(headers -> headers
                        .contentSecurityPolicy(contentSecurityPolicy -> contentSecurityPolicy.policyDirectives("frame-ancestors 'self'"))
                        .httpStrictTransportSecurity(hstsConfig -> hstsConfig.maxAgeInSeconds(31540000))
                        .frameOptions(HeadersConfigurer.FrameOptionsConfig::sameOrigin)
                )
                .authorizeRequests((authorize) -> authorize
                        .requestMatchers(
                                antMatcher("/access/token"),
                                antMatcher("/access/token/identity-provider"),
                                antMatcher("/access/token/kerberos"),
                                antMatcher("/access/oidc/callback"),
                                antMatcher("/access/oidc/exchange"),
                                antMatcher("/access/oidc/request")
                        )
                        .permitAll()
                        .anyRequest().fullyAuthenticated()
                )
                .exceptionHandling(exceptionHandling -> exceptionHandling
                        .authenticationEntryPoint(http401AuthenticationEntryPoint())
                        .accessDeniedHandler(new StandardAccessDeniedHandler())
                )
                .build();
    }

    @Bean
    public AuthenticationManager authenticationManager() {
        return new ProviderManager(x509AuthenticationProvider(), jwtAuthenticationProvider());
    }

    private IdentityFilter x509AuthenticationFilter() {
        return new IdentityFilter(x509IdentityProvider);
    }

    private IdentityAuthenticationProvider x509AuthenticationProvider() {
        return new X509IdentityAuthenticationProvider(authorizer, x509IdentityProvider, identityMapper);
    }

    private IdentityFilter jwtAuthenticationFilter() {
        return new IdentityFilter(jwtIdentityProvider);
    }

    private IdentityAuthenticationProvider jwtAuthenticationProvider() {
        return new IdentityAuthenticationProvider(authorizer, jwtIdentityProvider, identityMapper);
    }

    private ResourceAuthorizationFilter resourceAuthorizationFilter() {
        return ResourceAuthorizationFilter.builder()
                    .setAuthorizationService(authorizationService)
                    .addResourceType(ResourceType.Actuator)
                    .addResourceType(ResourceType.Swagger)
                    .build();
    }

    private AuthenticationEntryPoint http401AuthenticationEntryPoint() {
        // This gets used for both secured and unsecured configurations. It will be called by Spring Security if a request makes it through the filter chain without being authenticated.
        // For unsecured, this should never be reached because the custom AnonymousAuthenticationFilter should always populate a fully-authenticated anonymous user
        // For secured, this will cause attempt to access any API endpoint (except those explicitly ignored) without providing credentials to return a 401 Unauthorized challenge
        return (request, response, authenticationException) -> {

            // return a 401 response
            final int status = HttpServletResponse.SC_UNAUTHORIZED;
            logger.info("Client could not be authenticated due to: {} Returning 401 response.", authenticationException.toString());
            logger.debug("HTTP 401 Unauthorized", authenticationException);

            if (!response.isCommitted()) {
                response.setStatus(status);
                response.setContentType("text/plain");
                response.getWriter().println(String.format("%s Contact the system administrator.", authenticationException.getLocalizedMessage()));
            }
        };
    }

    private static class StandardAccessDeniedHandler implements AccessDeniedHandler {

        @Override
        public void handle(final HttpServletRequest request, final HttpServletResponse response, final AccessDeniedException accessDeniedException) throws IOException {
            final String message;
            final int status;
            if (accessDeniedException instanceof CsrfException) {
                status = HttpServletResponse.SC_FORBIDDEN;
                message = "Access Denied: CSRF Header and Cookie not matched";
                logger.info("Access Denied: CSRF Header [{}] not matched: {}", CsrfCookieName.REQUEST_TOKEN.getCookieName(), accessDeniedException.toString());
            } else {
                status = HttpServletResponse.SC_UNAUTHORIZED;
                message = "Access Denied";
                logger.debug(message, accessDeniedException);
            }

            response.setStatus(status);
            response.setContentType("text/plain");
            response.getWriter().println(message);
        }
    }
}
