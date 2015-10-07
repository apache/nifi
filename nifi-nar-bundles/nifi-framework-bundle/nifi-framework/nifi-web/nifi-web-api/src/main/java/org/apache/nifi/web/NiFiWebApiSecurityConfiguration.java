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

import javax.servlet.Filter;
import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.NiFiAuthenticationProvider;
import org.apache.nifi.web.security.anonymous.NiFiAnonymousUserFilter;
import org.apache.nifi.web.security.NiFiAuthenticationEntryPoint;
import org.apache.nifi.web.security.form.LoginAuthenticationFilter;
import org.apache.nifi.web.security.form.LoginAuthenticationProvider;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationProvider;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.node.NodeAuthorizedUserFilter;
import org.apache.nifi.web.security.x509.SubjectDnX509PrincipalExtractor;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.web.security.x509.ocsp.OcspCertificateValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * NiFi Web Api Spring security
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class NiFiWebApiSecurityConfiguration extends WebSecurityConfigurerAdapter {

    private NiFiProperties properties;
    private UserService userService;
    private AuthenticationUserDetailsService userDetailsService;
    private JwtService jwtService;
    private LoginIdentityProvider loginIdentityProvider;

    public NiFiWebApiSecurityConfiguration() {
        super(true); // disable defaults
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .rememberMe().disable()
                .exceptionHandling()
                .authenticationEntryPoint(new NiFiAuthenticationEntryPoint())
                .and()
                .authorizeRequests()
                .anyRequest().fullyAuthenticated()
                .and()
                .sessionManagement()
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS);

        // verify that login authentication is enabled
        if (loginIdentityProvider != null) {
            // login authentication for /token - exchanges for JWT for subsequent API usage
            http.addFilterBefore(buildLoginFilter("/token"), UsernamePasswordAuthenticationFilter.class);

            // login registration
            if (loginIdentityProvider.supportsRegistration()) {
                http.addFilterBefore(buildRegistrationFilter("/registration"), UsernamePasswordAuthenticationFilter.class);
            }
        }

        // cluster authorized user
        http.addFilterBefore(buildNodeAuthorizedUserFilter(), AnonymousAuthenticationFilter.class);

        // x509
        http.addFilterBefore(buildX509Filter(), AnonymousAuthenticationFilter.class);

        // jwt
        http.addFilterBefore(buildJwtFilter(), AnonymousAuthenticationFilter.class);

        // anonymous
        http.anonymous().authenticationFilter(buildAnonymousFilter());
    }

    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        // override xxxBean method so the authentication manager is available in app context (necessary for the method level security)
        return super.authenticationManagerBean();
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        final LoginAuthenticationProvider baseLoginAuthenticationProvider = new LoginAuthenticationProvider();
        baseLoginAuthenticationProvider.setLoginIdentityProvider(loginIdentityProvider);

        final AuthenticationProvider loginAuthenticationProvider = new NiFiAuthenticationProvider(baseLoginAuthenticationProvider, userDetailsService);
        final AuthenticationProvider x509AuthenticationProvider = new NiFiAuthenticationProvider(new X509AuthenticationProvider(), userDetailsService);
        final AuthenticationProvider jwtAuthenticationProvider = new NiFiAuthenticationProvider(new JwtAuthenticationProvider(), userDetailsService);

        auth
                .authenticationProvider(loginAuthenticationProvider)
                .authenticationProvider(x509AuthenticationProvider)
                .authenticationProvider(jwtAuthenticationProvider);
    }

    private LoginAuthenticationFilter buildLoginFilter(final String url) {
        final LoginAuthenticationFilter loginFilter = new LoginAuthenticationFilter(url);
        loginFilter.setJwtService(jwtService);
        loginFilter.setLoginIdentityProvider(loginIdentityProvider);
        loginFilter.setPrincipalExtractor(new SubjectDnX509PrincipalExtractor());
        loginFilter.setCertificateExtractor(new X509CertificateExtractor());
        return loginFilter;
    }

    private Filter buildRegistrationFilter(final String url) {
        return null;
    }

    private NodeAuthorizedUserFilter buildNodeAuthorizedUserFilter() {
        return new NodeAuthorizedUserFilter(properties);
    }

    private JwtAuthenticationFilter buildJwtFilter() throws Exception {
        final JwtAuthenticationFilter jwtFilter = new JwtAuthenticationFilter();
        jwtFilter.setJwtService(jwtService);
        jwtFilter.setAuthenticationManager(authenticationManager());
        return jwtFilter;
    }

    private X509AuthenticationFilter buildX509Filter() throws Exception {
        final X509AuthenticationFilter x509Filter = new X509AuthenticationFilter();
        x509Filter.setPrincipalExtractor(new SubjectDnX509PrincipalExtractor());
        x509Filter.setCertificateExtractor(new X509CertificateExtractor());
        x509Filter.setCertificateValidator(new OcspCertificateValidator(properties));
        x509Filter.setAuthenticationManager(authenticationManager());
        return x509Filter;
    }

    private AnonymousAuthenticationFilter buildAnonymousFilter() {
        final NiFiAnonymousUserFilter anonymousFilter = new NiFiAnonymousUserFilter();
        anonymousFilter.setUserService(userService);
        return anonymousFilter;
    }

    @Autowired
    public void setUserDetailsService(AuthenticationUserDetailsService userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

    @Autowired
    public void setUserService(UserService userService) {
        this.userService = userService;
    }

    @Autowired
    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setJwtService(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @Autowired
    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }
}
