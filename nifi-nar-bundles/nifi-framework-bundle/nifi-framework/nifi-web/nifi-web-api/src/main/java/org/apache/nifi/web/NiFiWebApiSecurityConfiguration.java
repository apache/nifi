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
package org.apache.nifi.web;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.anonymous.NiFiAnonymousAuthenticationFilter;
import org.apache.nifi.web.security.csrf.CsrfCookieRequestMatcher;
import org.apache.nifi.web.security.csrf.StandardCookieCsrfTokenRepository;
import org.apache.nifi.web.security.knox.KnoxAuthenticationFilter;
import org.apache.nifi.web.security.log.AuthenticationUserFilter;
import org.apache.nifi.web.security.oidc.OIDCEndpoints;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2LocalLogoutFilter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2SingleLogoutFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationFilter;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationFilter;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationRequestFilter;
import org.springframework.security.saml2.provider.service.web.Saml2MetadataFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutResponseFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.ExceptionTranslationFilter;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.util.matcher.AndRequestMatcher;

import java.util.List;

/**
 * Application Security Configuration using Spring Security
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class NiFiWebApiSecurityConfiguration {
    /**
     * Spring Security Authentication Manager configured using Authentication Providers from specific configuration classes
     *
     * @param authenticationProviders Autowired Authentication Providers
     * @return Authentication Manager
     */
    @Bean
    public AuthenticationManager authenticationManager(final List<AuthenticationProvider> authenticationProviders) {
        return new ProviderManager(authenticationProviders);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            final HttpSecurity http,
            final NiFiProperties properties,
            final X509AuthenticationFilter x509AuthenticationFilter,
            final BearerTokenAuthenticationFilter bearerTokenAuthenticationFilter,
            final KnoxAuthenticationFilter knoxAuthenticationFilter,
            final NiFiAnonymousAuthenticationFilter anonymousAuthenticationFilter,
            final Saml2WebSsoAuthenticationFilter saml2WebSsoAuthenticationFilter,
            final Saml2WebSsoAuthenticationRequestFilter saml2WebSsoAuthenticationRequestFilter,
            final Saml2MetadataFilter saml2MetadataFilter,
            final Saml2LogoutRequestFilter saml2LogoutRequestFilter,
            final Saml2LogoutResponseFilter saml2LogoutResponseFilter,
            final Saml2SingleLogoutFilter saml2SingleLogoutFilter,
            final Saml2LocalLogoutFilter saml2LocalLogoutFilter
    ) throws Exception {
        http
                .logout().disable()
                .anonymous().disable()
                .requestCache().disable()
                .rememberMe().disable()
                .sessionManagement().disable()
                .headers().disable()
                .servletApi().disable()
                .securityContext().disable()
                .authorizeHttpRequests(authorize -> authorize
                        .antMatchers(
                                "/access",
                                "/access/config",
                                "/access/token",
                                "/access/kerberos",
                                "/access/knox/callback",
                                "/access/knox/request",
                                "/access/logout/complete",
                                OIDCEndpoints.TOKEN_EXCHANGE,
                                OIDCEndpoints.LOGIN_REQUEST,
                                OIDCEndpoints.LOGIN_CALLBACK,
                                OIDCEndpoints.LOGOUT_CALLBACK
                        ).permitAll()
                        .anyRequest().authenticated()
                )
                .csrf(csrf -> csrf
                        .csrfTokenRepository(
                                new StandardCookieCsrfTokenRepository()
                        )
                        .requireCsrfProtectionMatcher(
                                new AndRequestMatcher(CsrfFilter.DEFAULT_CSRF_MATCHER, new CsrfCookieRequestMatcher())
                        )
                )
                .exceptionHandling(exceptionHandling -> exceptionHandling
                        .authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED))
                )
                .addFilterBefore(x509AuthenticationFilter, AnonymousAuthenticationFilter.class)
                .addFilterBefore(bearerTokenAuthenticationFilter, AnonymousAuthenticationFilter.class)
                .addFilterBefore(new AuthenticationUserFilter(), ExceptionTranslationFilter.class);

        if (properties.isKnoxSsoEnabled()) {
            http.addFilterBefore(knoxAuthenticationFilter, AnonymousAuthenticationFilter.class);
        }

        if (properties.isAnonymousAuthenticationAllowed() || properties.isHttpEnabled()) {
            http.addFilterAfter(anonymousAuthenticationFilter, AnonymousAuthenticationFilter.class);
        }

        if (properties.isSamlEnabled()) {
            http.addFilterBefore(saml2WebSsoAuthenticationFilter, AnonymousAuthenticationFilter.class);
            http.addFilterBefore(saml2WebSsoAuthenticationRequestFilter, AnonymousAuthenticationFilter.class);

            // Metadata and Logout Filters must be invoked prior to CSRF or other security filtering
            http.addFilterBefore(saml2MetadataFilter, CsrfFilter.class);
            http.addFilterBefore(saml2LocalLogoutFilter, CsrfFilter.class);

            if (properties.isSamlSingleLogoutEnabled()) {
                http.addFilterBefore(saml2SingleLogoutFilter, CsrfFilter.class);
                http.addFilterBefore(saml2LogoutRequestFilter, CsrfFilter.class);
                http.addFilterBefore(saml2LogoutResponseFilter, CsrfFilter.class);
            }
        }

        return http.build();
    }
}
