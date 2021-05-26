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

import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.authorization.resource.ResourceType;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.service.AuthorizationService;
import org.apache.nifi.registry.web.security.authentication.AnonymousIdentityFilter;
import org.apache.nifi.registry.web.security.authentication.IdentityAuthenticationProvider;
import org.apache.nifi.registry.web.security.authentication.IdentityFilter;
import org.apache.nifi.registry.web.security.authentication.jwt.JwtIdentityProvider;
import org.apache.nifi.registry.web.security.authentication.x509.X509IdentityAuthenticationProvider;
import org.apache.nifi.registry.web.security.authentication.x509.X509IdentityProvider;
import org.apache.nifi.registry.web.security.authorization.ResourceAuthorizationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.access.intercept.FilterSecurityInterceptor;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * NiFi Registry Web Api Spring security
 */
@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class NiFiRegistrySecurityConfig extends WebSecurityConfigurerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistrySecurityConfig.class);

    @Autowired
    private IdentityMapper identityMapper;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private Authorizer authorizer;

    private AnonymousIdentityFilter anonymousAuthenticationFilter = new AnonymousIdentityFilter();

    @Autowired
    private X509IdentityProvider x509IdentityProvider;
    private IdentityFilter x509AuthenticationFilter;
    private IdentityAuthenticationProvider x509AuthenticationProvider;

    @Autowired
    private JwtIdentityProvider jwtIdentityProvider;
    private IdentityFilter jwtAuthenticationFilter;
    private IdentityAuthenticationProvider jwtAuthenticationProvider;

    private ResourceAuthorizationFilter resourceAuthorizationFilter;

    public NiFiRegistrySecurityConfig() {
        super(true); // disable defaults
    }

    @Override
    public void configure(WebSecurity webSecurity) throws Exception {
        // allow any client to access the endpoint for logging in to generate an access token
        webSecurity.ignoring().antMatchers( "/access/token", "/access/token/kerberos",
                "/access/oidc/exchange", "/access/oidc/callback", "/access/oidc/request", "/access/token/identity-provider" );
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .rememberMe().disable()
                .authorizeRequests()
                    .anyRequest().fullyAuthenticated()
                    .and()
                .exceptionHandling()
                    .authenticationEntryPoint(http401AuthenticationEntryPoint())
                    .and()
                .sessionManagement()
                    .sessionCreationPolicy(SessionCreationPolicy.STATELESS);

        // Apply security headers for registry API. Security headers for docs and UI are applied with Jetty filters in registry-core.
        http.headers().xssProtection();
        http.headers().contentSecurityPolicy("frame-ancestors 'self'");
        http.headers().httpStrictTransportSecurity().maxAgeInSeconds(31540000);
        http.headers().frameOptions().sameOrigin();

        // x509
        http.addFilterBefore(x509AuthenticationFilter(), AnonymousAuthenticationFilter.class);

        // jwt
        http.addFilterBefore(jwtAuthenticationFilter(), AnonymousAuthenticationFilter.class);

        // otp
        // todo, if needed one-time password auth filter goes here

        // add an anonymous authentication filter that will populate the authenticated,
        // anonymous user if no other user identity is detected earlier in the Spring filter chain
        http.anonymous().authenticationFilter(anonymousAuthenticationFilter);

        // After Spring Security filter chain is complete (so authentication is done),
        // but before the Jersey application endpoints get the request,
        // insert the ResourceAuthorizationFilter to do its authorization checks
        http.addFilterAfter(resourceAuthorizationFilter(), FilterSecurityInterceptor.class);
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth
                .authenticationProvider(x509AuthenticationProvider())
                .authenticationProvider(jwtAuthenticationProvider());
    }

    private IdentityFilter x509AuthenticationFilter() throws Exception {
        if (x509AuthenticationFilter == null) {
            x509AuthenticationFilter = new IdentityFilter(x509IdentityProvider);
        }
        return x509AuthenticationFilter;
    }

    private IdentityAuthenticationProvider x509AuthenticationProvider() {
        if (x509AuthenticationProvider == null) {
            x509AuthenticationProvider = new X509IdentityAuthenticationProvider(authorizer, x509IdentityProvider, identityMapper);
        }
        return x509AuthenticationProvider;
    }

    private IdentityFilter jwtAuthenticationFilter() throws Exception {
        if (jwtAuthenticationFilter == null) {
            jwtAuthenticationFilter = new IdentityFilter(jwtIdentityProvider);
        }
        return jwtAuthenticationFilter;
    }

    private IdentityAuthenticationProvider jwtAuthenticationProvider() {
        if (jwtAuthenticationProvider == null) {
            jwtAuthenticationProvider = new IdentityAuthenticationProvider(authorizer, jwtIdentityProvider, identityMapper);
        }
        return jwtAuthenticationProvider;
    }

    private ResourceAuthorizationFilter resourceAuthorizationFilter() {
        if (resourceAuthorizationFilter == null) {
            resourceAuthorizationFilter = ResourceAuthorizationFilter.builder()
                    .setAuthorizationService(authorizationService)
                    .addResourceType(ResourceType.Actuator)
                    .addResourceType(ResourceType.Swagger)
                    .build();
        }
        return resourceAuthorizationFilter;
    }

    private AuthenticationEntryPoint http401AuthenticationEntryPoint() {
        // This gets used for both secured and unsecured configurations. It will be called by Spring Security if a request makes it through the filter chain without being authenticated.
        // For unsecured, this should never be reached because the custom AnonymousAuthenticationFilter should always populate a fully-authenticated anonymous user
        // For secured, this will cause attempt to access any API endpoint (except those explicitly ignored) without providing credentials to return a 401 Unauthorized challenge
        return new AuthenticationEntryPoint() {
            @Override
            public void commence(HttpServletRequest request,
                                 HttpServletResponse response,
                                 AuthenticationException authenticationException)
                    throws IOException, ServletException {

                // return a 401 response
                final int status = HttpServletResponse.SC_UNAUTHORIZED;
                logger.info("Client could not be authenticated due to: {} Returning 401 response.", authenticationException.toString());
                logger.debug("", authenticationException);

                if (!response.isCommitted()) {
                    response.setStatus(status);
                    response.setContentType("text/plain");
                    response.getWriter().println(String.format("%s Contact the system administrator.", authenticationException.getLocalizedMessage()));
                }
            }
        };
    }

}
