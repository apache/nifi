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

import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.NiFiAuthenticationProvider;
import org.apache.nifi.web.security.anonymous.NiFiAnonymousUserFilter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.security.jwt.JwtService;
import org.apache.nifi.web.security.node.NodeAuthorizedUserFilter;
import org.apache.nifi.web.security.token.NiFiAuthortizationRequestToken;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.web.security.x509.X509IdentityProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.AuthenticationUserDetailsService;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

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
    private X509CertificateExtractor certificateExtractor;
    private X509IdentityProvider certificateIdentityProvider;
    private LoginIdentityProvider loginIdentityProvider;

    private NodeAuthorizedUserFilter nodeAuthorizedUserFilter;
    private JwtAuthenticationFilter jwtAuthenticationFilter;
    private X509AuthenticationFilter x509AuthenticationFilter;
    private NiFiAnonymousUserFilter anonymousAuthenticationFilter;

    public NiFiWebApiSecurityConfiguration() {
        super(true); // disable defaults
    }

    @Override
    public void configure(WebSecurity webSecurity) throws Exception {
        webSecurity
                .ignoring()
                    .antMatchers("/access/**");
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .rememberMe().disable()
                .authorizeRequests()
                    .anyRequest().fullyAuthenticated()
                    .and()
                .sessionManagement()
                    .sessionCreationPolicy(SessionCreationPolicy.STATELESS);

        // cluster authorized user
        http.addFilterBefore(nodeAuthorizedUserFilterBean(), AnonymousAuthenticationFilter.class);

        // anonymous
        http.anonymous().authenticationFilter(anonymousFilterBean());

        // x509
        http.addFilterAfter(x509FilterBean(), AnonymousAuthenticationFilter.class);

        // jwt
        http.addFilterAfter(jwtFilterBean(), AnonymousAuthenticationFilter.class);
    }

    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        // override xxxBean method so the authentication manager is available in app context (necessary for the method level security)
        return super.authenticationManagerBean();
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider(new NiFiAuthenticationProvider(userDetailsService));
    }

    @Bean
    public NodeAuthorizedUserFilter nodeAuthorizedUserFilterBean() throws Exception {
        if (nodeAuthorizedUserFilter == null) {
            nodeAuthorizedUserFilter = new NodeAuthorizedUserFilter();
            nodeAuthorizedUserFilter.setProperties(properties);
            nodeAuthorizedUserFilter.setCertificateExtractor(certificateExtractor);
            nodeAuthorizedUserFilter.setCertificateIdentityProvider(certificateIdentityProvider);
        }
        return nodeAuthorizedUserFilter;
    }

    @Bean
    public JwtAuthenticationFilter jwtFilterBean() throws Exception {
        if (jwtAuthenticationFilter == null) {
            jwtAuthenticationFilter = new JwtAuthenticationFilter();
            jwtAuthenticationFilter.setProperties(properties);
            jwtAuthenticationFilter.setAuthenticationManager(authenticationManager());

            // only consider the tokens when configured for login
            if (loginIdentityProvider != null) {
                jwtAuthenticationFilter.setJwtService(jwtService);
            }
        }
        return jwtAuthenticationFilter;
    }

    @Bean
    public X509AuthenticationFilter x509FilterBean() throws Exception {
        if (x509AuthenticationFilter == null) {
            x509AuthenticationFilter = new X509AuthenticationFilter();
            x509AuthenticationFilter.setProperties(properties);
            x509AuthenticationFilter.setCertificateExtractor(certificateExtractor);
            x509AuthenticationFilter.setCertificateIdentityProvider(certificateIdentityProvider);
            x509AuthenticationFilter.setAuthenticationManager(authenticationManager());
        }
        return x509AuthenticationFilter;
    }

    @Bean
    public NiFiAnonymousUserFilter anonymousFilterBean() throws Exception {
        if (anonymousAuthenticationFilter == null) {
            anonymousAuthenticationFilter = new NiFiAnonymousUserFilter();
            anonymousAuthenticationFilter.setUserService(userService);
        }
        return anonymousAuthenticationFilter;
    }

    @Autowired
    public void setUserDetailsService(AuthenticationUserDetailsService<NiFiAuthortizationRequestToken> userDetailsService) {
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

    @Autowired
    public void setCertificateExtractor(X509CertificateExtractor certificateExtractor) {
        this.certificateExtractor = certificateExtractor;
    }

    @Autowired
    public void setCertificateIdentityProvider(X509IdentityProvider certificateIdentityProvider) {
        this.certificateIdentityProvider = certificateIdentityProvider;
    }

}
