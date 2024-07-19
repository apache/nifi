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

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.StandardAuthenticationEntryPoint;
import org.apache.nifi.web.security.anonymous.NiFiAnonymousAuthenticationFilter;
import org.apache.nifi.web.security.csrf.CsrfCookieFilter;
import org.apache.nifi.web.security.csrf.CsrfCookieRequestMatcher;
import org.apache.nifi.web.security.csrf.SkipReplicatedCsrfFilter;
import org.apache.nifi.web.security.csrf.StandardCookieCsrfTokenRepository;
import org.apache.nifi.web.security.csrf.StandardCsrfTokenRequestAttributeHandler;
import org.apache.nifi.web.security.log.AuthenticationUserFilter;
import org.apache.nifi.web.security.oidc.client.web.OidcBearerTokenRefreshFilter;
import org.apache.nifi.web.security.oidc.logout.OidcLogoutFilter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2LocalLogoutFilter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2SingleLogoutFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationCodeGrantFilter;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestRedirectFilter;
import org.springframework.security.oauth2.client.web.OAuth2LoginAuthenticationFilter;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.saml2.provider.service.web.authentication.Saml2WebSsoAuthenticationFilter;
import org.springframework.security.saml2.provider.service.web.Saml2WebSsoAuthenticationRequestFilter;
import org.springframework.security.saml2.provider.service.web.Saml2MetadataFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutResponseFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.ExceptionTranslationFilter;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.util.matcher.AndRequestMatcher;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatchers;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Application Security Configuration using Spring Security
 */
@Import({
        AuthenticationSecurityConfiguration.class
})
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class WebSecurityConfiguration {
    private static final List<String> UNFILTERED_PATHS = List.of(
            "/access/token",
            "/access/logout/complete",
            "/authentication/configuration"
    );

    private static final RequestMatcher UNFILTERED_PATHS_REQUEST_MATCHER = new OrRequestMatcher(
            UNFILTERED_PATHS.stream().map(AntPathRequestMatcher::new).collect(Collectors.toList())
    );

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
            final StandardAuthenticationEntryPoint authenticationEntryPoint,
            final X509AuthenticationFilter x509AuthenticationFilter,
            final BearerTokenAuthenticationFilter bearerTokenAuthenticationFilter,
            final NiFiAnonymousAuthenticationFilter anonymousAuthenticationFilter,
            final OAuth2LoginAuthenticationFilter oAuth2LoginAuthenticationFilter,
            final OAuth2AuthorizationCodeGrantFilter oAuth2AuthorizationCodeGrantFilter,
            final OAuth2AuthorizationRequestRedirectFilter oAuth2AuthorizationRequestRedirectFilter,
            final OidcBearerTokenRefreshFilter oidcBearerTokenRefreshFilter,
            final OidcLogoutFilter oidcLogoutFilter,
            final Saml2WebSsoAuthenticationFilter saml2WebSsoAuthenticationFilter,
            final Saml2WebSsoAuthenticationRequestFilter saml2WebSsoAuthenticationRequestFilter,
            final Saml2MetadataFilter saml2MetadataFilter,
            final Saml2LogoutRequestFilter saml2LogoutRequestFilter,
            final Saml2LogoutResponseFilter saml2LogoutResponseFilter,
            final Saml2SingleLogoutFilter saml2SingleLogoutFilter,
            final Saml2LocalLogoutFilter saml2LocalLogoutFilter
    ) throws Exception {
        http
                .logout(AbstractHttpConfigurer::disable)
                .rememberMe(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .servletApi(AbstractHttpConfigurer::disable)
                .securityContext(AbstractHttpConfigurer::disable)
                .sessionManagement(AbstractHttpConfigurer::disable)
                .headers(AbstractHttpConfigurer::disable)
                .securityMatchers(securityMatchers -> securityMatchers
                        .requestMatchers(
                                RequestMatchers.not(UNFILTERED_PATHS_REQUEST_MATCHER)
                        )
                )
                .authorizeHttpRequests(authorize -> authorize
                        .anyRequest().authenticated()
                )
                .addFilterBefore(new SkipReplicatedCsrfFilter(), CsrfFilter.class)
                .addFilterAfter(new CsrfCookieFilter(), BasicAuthenticationFilter.class)
                .csrf(csrf -> csrf
                        .csrfTokenRepository(
                                new StandardCookieCsrfTokenRepository()
                        )
                        .requireCsrfProtectionMatcher(
                                new AndRequestMatcher(CsrfFilter.DEFAULT_CSRF_MATCHER, new CsrfCookieRequestMatcher())
                        )
                        .csrfTokenRequestHandler(new StandardCsrfTokenRequestAttributeHandler())
                )
                .exceptionHandling(exceptionHandling -> exceptionHandling
                        .authenticationEntryPoint(authenticationEntryPoint)
                )
                .addFilterBefore(x509AuthenticationFilter, AnonymousAuthenticationFilter.class)
                .addFilterBefore(bearerTokenAuthenticationFilter, AnonymousAuthenticationFilter.class)
                .addFilterBefore(new AuthenticationUserFilter(), ExceptionTranslationFilter.class);

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

        if (properties.isOidcEnabled()) {
            http.addFilterBefore(oAuth2LoginAuthenticationFilter, AnonymousAuthenticationFilter.class);
            http.addFilterBefore(oAuth2AuthorizationCodeGrantFilter, AnonymousAuthenticationFilter.class);
            http.addFilterBefore(oAuth2AuthorizationRequestRedirectFilter, AnonymousAuthenticationFilter.class);
            http.addFilterBefore(oidcBearerTokenRefreshFilter, AnonymousAuthenticationFilter.class);
            http.addFilterBefore(oidcLogoutFilter, CsrfFilter.class);
        }

        return http.build();
    }
}
