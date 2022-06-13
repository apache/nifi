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
import org.apache.nifi.web.security.anonymous.NiFiAnonymousAuthenticationProvider;
import org.apache.nifi.web.security.csrf.CsrfCookieRequestMatcher;
import org.apache.nifi.web.security.csrf.StandardCookieCsrfTokenRepository;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.resolver.StandardBearerTokenResolver;
import org.apache.nifi.web.security.knox.KnoxAuthenticationFilter;
import org.apache.nifi.web.security.knox.KnoxAuthenticationProvider;
import org.apache.nifi.web.security.log.AuthenticationUserFilter;
import org.apache.nifi.web.security.oidc.OIDCEndpoints;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2LocalLogoutFilter;
import org.apache.nifi.web.security.saml2.web.authentication.logout.Saml2SingleLogoutFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationProvider;
import org.springframework.security.oauth2.server.resource.web.BearerTokenAuthenticationFilter;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationFilter;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationRequestFilter;
import org.springframework.security.saml2.provider.service.web.Saml2MetadataFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutRequestFilter;
import org.springframework.security.saml2.provider.service.web.authentication.logout.Saml2LogoutResponseFilter;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;
import org.springframework.security.web.csrf.CsrfFilter;
import org.springframework.security.web.util.matcher.AndRequestMatcher;

/**
 * NiFi Web Api Spring security. Applies the various NiFiAuthenticationFilter servlet filters which will extract authentication
 * credentials from API requests.
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class NiFiWebApiSecurityConfiguration extends WebSecurityConfigurerAdapter {
    private NiFiProperties properties;

    private X509AuthenticationFilter x509AuthenticationFilter;
    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;
    private X509AuthenticationProvider x509AuthenticationProvider;
    private JwtAuthenticationProvider jwtAuthenticationProvider;

    private KnoxAuthenticationFilter knoxAuthenticationFilter;
    private KnoxAuthenticationProvider knoxAuthenticationProvider;

    private NiFiAnonymousAuthenticationFilter anonymousAuthenticationFilter;
    private NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider;

    private BearerTokenProvider bearerTokenProvider;

    private Saml2WebSsoAuthenticationFilter saml2WebSsoAuthenticationFilter;
    private Saml2WebSsoAuthenticationRequestFilter saml2WebSsoAuthenticationRequestFilter;
    private Saml2MetadataFilter saml2MetadataFilter;
    private Saml2LogoutRequestFilter saml2LogoutRequestFilter;
    private Saml2LogoutResponseFilter saml2LogoutResponseFilter;
    private Saml2SingleLogoutFilter saml2SingleLogoutFilter;
    private Saml2LocalLogoutFilter saml2LocalLogoutFilter;
    private AuthenticationProvider openSamlAuthenticationProvider;

    public NiFiWebApiSecurityConfiguration() {
        super(true); // disable defaults
    }

    /**
     * Configure Web Security with ignoring matchers for authentication requests
     *
     * @param webSecurity Spring Web Security Configuration
     */
    @Override
    public void configure(final WebSecurity webSecurity) {
        webSecurity
                .ignoring()
                    .antMatchers(
                            "/access",
                            "/access/config",
                            "/access/token",
                            "/access/kerberos",
                            OIDCEndpoints.TOKEN_EXCHANGE,
                            OIDCEndpoints.LOGIN_REQUEST,
                            OIDCEndpoints.LOGIN_CALLBACK,
                            OIDCEndpoints.LOGOUT_CALLBACK,
                            "/access/knox/callback",
                            "/access/knox/request",
                            "/access/logout/complete");
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .rememberMe().disable()
                .authorizeRequests().anyRequest().fullyAuthenticated().and()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and()
                .csrf().requireCsrfProtectionMatcher(
                        new AndRequestMatcher(CsrfFilter.DEFAULT_CSRF_MATCHER, new CsrfCookieRequestMatcher()))
                        .csrfTokenRepository(new StandardCookieCsrfTokenRepository(properties.getAllowedContextPathsAsList()));

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

        http.addFilterBefore(x509FilterBean(), AnonymousAuthenticationFilter.class);
        http.addFilterBefore(bearerTokenAuthenticationFilter(), AnonymousAuthenticationFilter.class);
        http.addFilterBefore(knoxFilterBean(), AnonymousAuthenticationFilter.class);
        http.addFilterAfter(anonymousFilterBean(), AnonymousAuthenticationFilter.class);
        http.addFilterAfter(new AuthenticationUserFilter(), AnonymousAuthenticationFilter.class);

        // disable default anonymous handling because it doesn't handle conditional authentication well
        http.anonymous().disable();
    }

    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        // override xxxBean method so the authentication manager is available in app context (necessary for the method level security)
        return super.authenticationManagerBean();
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth
                .authenticationProvider(x509AuthenticationProvider)
                .authenticationProvider(jwtAuthenticationProvider)
                .authenticationProvider(knoxAuthenticationProvider)
                .authenticationProvider(anonymousAuthenticationProvider);

        if (properties.isSamlEnabled()) {
            auth.authenticationProvider(openSamlAuthenticationProvider);
        }
    }

    @Bean
    public KnoxAuthenticationFilter knoxFilterBean() throws Exception {
        if (knoxAuthenticationFilter == null) {
            knoxAuthenticationFilter = new KnoxAuthenticationFilter();
            knoxAuthenticationFilter.setProperties(properties);
            knoxAuthenticationFilter.setAuthenticationManager(authenticationManager());
        }
        return knoxAuthenticationFilter;
    }

    @Bean
    public X509AuthenticationFilter x509FilterBean() throws Exception {
        if (x509AuthenticationFilter == null) {
            x509AuthenticationFilter = new X509AuthenticationFilter();
            x509AuthenticationFilter.setProperties(properties);
            x509AuthenticationFilter.setCertificateExtractor(certificateExtractor);
            x509AuthenticationFilter.setPrincipalExtractor(principalExtractor);
            x509AuthenticationFilter.setAuthenticationManager(authenticationManager());
        }
        return x509AuthenticationFilter;
    }

    @Bean
    public BearerTokenAuthenticationFilter bearerTokenAuthenticationFilter() throws Exception {
        final BearerTokenAuthenticationFilter filter = new BearerTokenAuthenticationFilter(authenticationManager());
        filter.setBearerTokenResolver(bearerTokenResolver());
        return filter;
    }

    @Bean
    public BearerTokenResolver bearerTokenResolver() {
        return new StandardBearerTokenResolver();
    }

    @Bean
    public NiFiAnonymousAuthenticationFilter anonymousFilterBean() throws Exception {
        if (anonymousAuthenticationFilter == null) {
            anonymousAuthenticationFilter = new NiFiAnonymousAuthenticationFilter();
            anonymousAuthenticationFilter.setProperties(properties);
            anonymousAuthenticationFilter.setAuthenticationManager(authenticationManager());
        }
        return anonymousAuthenticationFilter;
    }

    @Autowired
    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setJwtAuthenticationProvider(JwtAuthenticationProvider jwtAuthenticationProvider) {
        this.jwtAuthenticationProvider = jwtAuthenticationProvider;
    }

    @Autowired
    public void setKnoxAuthenticationProvider(KnoxAuthenticationProvider knoxAuthenticationProvider) {
        this.knoxAuthenticationProvider = knoxAuthenticationProvider;
    }

    @Autowired
    public void setAnonymousAuthenticationProvider(NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider) {
        this.anonymousAuthenticationProvider = anonymousAuthenticationProvider;
    }

    @Autowired
    public void setX509AuthenticationProvider(X509AuthenticationProvider x509AuthenticationProvider) {
        this.x509AuthenticationProvider = x509AuthenticationProvider;
    }

    @Autowired
    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    @Autowired
    public void setPrincipalExtractor(X509PrincipalExtractor principalExtractor) {
        this.principalExtractor = principalExtractor;
    }

    @Autowired
    public void setBearerTokenProvider(final BearerTokenProvider bearerTokenProvider) {
        this.bearerTokenProvider = bearerTokenProvider;
    }

    @Autowired
    public void setSaml2WebSsoAuthenticationFilter(final Saml2WebSsoAuthenticationFilter saml2WebSsoAuthenticationFilter) {
        this.saml2WebSsoAuthenticationFilter = saml2WebSsoAuthenticationFilter;
    }

    @Autowired
    public void setSaml2WebSsoAuthenticationRequestFilter(final Saml2WebSsoAuthenticationRequestFilter saml2WebSsoAuthenticationRequestFilter) {
        this.saml2WebSsoAuthenticationRequestFilter = saml2WebSsoAuthenticationRequestFilter;
    }

    @Autowired
    public void setSaml2MetadataFilter(final Saml2MetadataFilter saml2MetadataFilter) {
        this.saml2MetadataFilter = saml2MetadataFilter;
    }

    @Autowired
    public void setSaml2LogoutRequestFilter(final Saml2LogoutRequestFilter saml2LogoutRequestFilter) {
        this.saml2LogoutRequestFilter = saml2LogoutRequestFilter;
    }

    @Autowired
    public void setSaml2LogoutResponseFilter(final Saml2LogoutResponseFilter saml2LogoutResponseFilter) {
        this.saml2LogoutResponseFilter = saml2LogoutResponseFilter;
    }

    @Autowired
    public void setSaml2SingleLogoutFilter(final Saml2SingleLogoutFilter saml2SingleLogoutFilter) {
        this.saml2SingleLogoutFilter = saml2SingleLogoutFilter;
    }

    @Autowired
    public void setSaml2LocalLogoutFilter(final Saml2LocalLogoutFilter saml2LocalLogoutFilter) {
        this.saml2LocalLogoutFilter = saml2LocalLogoutFilter;
    }

    @Qualifier("openSamlAuthenticationProvider")
    @Autowired
    public void setOpenSamlAuthenticationProvider(final AuthenticationProvider openSamlAuthenticationProvider) {
        this.openSamlAuthenticationProvider = openSamlAuthenticationProvider;
    }
}
