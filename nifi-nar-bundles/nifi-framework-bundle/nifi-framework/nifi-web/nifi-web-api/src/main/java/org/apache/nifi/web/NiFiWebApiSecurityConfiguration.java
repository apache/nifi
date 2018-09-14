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
import org.apache.nifi.web.security.anonymous.NiFiAnonymousUserFilter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationFilter;
import org.apache.nifi.web.security.jwt.JwtAuthenticationProvider;
import org.apache.nifi.web.security.knox.KnoxAuthenticationFilter;
import org.apache.nifi.web.security.knox.KnoxAuthenticationProvider;
import org.apache.nifi.web.security.otp.OtpAuthenticationFilter;
import org.apache.nifi.web.security.otp.OtpAuthenticationProvider;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.web.security.x509.X509IdentityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;

/**
 * NiFi Web Api Spring security
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class NiFiWebApiSecurityConfiguration extends WebSecurityConfigurerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(NiFiWebApiSecurityConfiguration.class);

    private NiFiProperties properties;

    private X509AuthenticationFilter x509AuthenticationFilter;
    private X509CertificateExtractor certificateExtractor;
    private X509PrincipalExtractor principalExtractor;
    private X509IdentityProvider certificateIdentityProvider;
    private X509AuthenticationProvider x509AuthenticationProvider;

    private JwtAuthenticationFilter jwtAuthenticationFilter;
    private JwtAuthenticationProvider jwtAuthenticationProvider;

    private OtpAuthenticationFilter otpAuthenticationFilter;
    private OtpAuthenticationProvider otpAuthenticationProvider;

    private KnoxAuthenticationFilter knoxAuthenticationFilter;
    private KnoxAuthenticationProvider knoxAuthenticationProvider;

    private NiFiAnonymousUserFilter anonymousAuthenticationFilter;

    public NiFiWebApiSecurityConfiguration() {
        super(true); // disable defaults
    }

    @Override
    public void configure(WebSecurity webSecurity) throws Exception {
        // ignore the access endpoints for obtaining the access config, the access token
        // granting, and access status for a given user (note: we are not ignoring the
        // the /access/download-token and /access/ui-extension-token endpoints
        webSecurity
                .ignoring()
                    .antMatchers("/access", "/access/config", "/access/token", "/access/kerberos", "/access/oidc/**", "/access/knox/**");
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .cors().and()
                .rememberMe().disable()
                .authorizeRequests()
                    .anyRequest().fullyAuthenticated()
                    .and()
                .sessionManagement()
                    .sessionCreationPolicy(SessionCreationPolicy.STATELESS);

        // x509
        http.addFilterBefore(x509FilterBean(), AnonymousAuthenticationFilter.class);

        // jwt
        http.addFilterBefore(jwtFilterBean(), AnonymousAuthenticationFilter.class);

        // otp
        http.addFilterBefore(otpFilterBean(), AnonymousAuthenticationFilter.class);

        // knox
        http.addFilterBefore(knoxFilterBean(), AnonymousAuthenticationFilter.class);

        // anonymous
        http.anonymous().authenticationFilter(anonymousFilterBean());
    }


    @Bean
    CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedMethods(Arrays.asList("HEAD", "GET"));
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/process-groups/*/templates/upload", configuration);
        return source;
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
                .authenticationProvider(otpAuthenticationProvider)
                .authenticationProvider(knoxAuthenticationProvider);
    }

    @Bean
    public JwtAuthenticationFilter jwtFilterBean() throws Exception {
        if (jwtAuthenticationFilter == null) {
            jwtAuthenticationFilter = new JwtAuthenticationFilter();
            jwtAuthenticationFilter.setProperties(properties);
            jwtAuthenticationFilter.setAuthenticationManager(authenticationManager());
        }
        return jwtAuthenticationFilter;
    }

    @Bean
    public OtpAuthenticationFilter otpFilterBean() throws Exception {
        if (otpAuthenticationFilter == null) {
            otpAuthenticationFilter = new OtpAuthenticationFilter();
            otpAuthenticationFilter.setProperties(properties);
            otpAuthenticationFilter.setAuthenticationManager(authenticationManager());
        }
        return otpAuthenticationFilter;
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
    public NiFiAnonymousUserFilter anonymousFilterBean() throws Exception {
        if (anonymousAuthenticationFilter == null) {
            anonymousAuthenticationFilter = new NiFiAnonymousUserFilter();
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
    public void setOtpAuthenticationProvider(OtpAuthenticationProvider otpAuthenticationProvider) {
        this.otpAuthenticationProvider = otpAuthenticationProvider;
    }

    @Autowired
    public void setKnoxAuthenticationProvider(KnoxAuthenticationProvider knoxAuthenticationProvider) {
        this.knoxAuthenticationProvider = knoxAuthenticationProvider;
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
    public void setCertificateIdentityProvider(X509IdentityProvider certificateIdentityProvider) {
        this.certificateIdentityProvider = certificateIdentityProvider;
    }
}
