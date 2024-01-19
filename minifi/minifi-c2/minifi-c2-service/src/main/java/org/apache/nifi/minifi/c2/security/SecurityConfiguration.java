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

package org.apache.nifi.minifi.c2.security;

import org.apache.nifi.minifi.c2.security.authentication.C2AnonymousAuthenticationFilter;
import org.apache.nifi.minifi.c2.security.authentication.X509AuthenticationFilter;
import org.apache.nifi.minifi.c2.security.authentication.X509AuthenticationProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

import static org.springframework.security.web.util.matcher.AntPathRequestMatcher.antMatcher;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
@ImportResource({"classpath:minifi-c2-web-security-context.xml"})
public class SecurityConfiguration {
    private AuthenticationProvider authenticationProvider;
    private X509AuthenticationFilter x509AuthenticationFilter;
    private C2AnonymousAuthenticationFilter c2AnonymousAuthenticationFilter;

    @Bean
    public AuthenticationManager authenticationManager() {
        return new ProviderManager(authenticationProvider);
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
            final HttpSecurity http
    ) throws Exception {
        return http
                .rememberMe(AbstractHttpConfigurer::disable)
                .sessionManagement(sessionManagement -> sessionManagement.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeRequests(authorize -> authorize
                        .requestMatchers(
                                antMatcher("/access"),
                                antMatcher("/access/config"),
                                antMatcher("/access/token"),
                                antMatcher("/access/kerberos")
                        )
                        .permitAll()
                        .anyRequest().fullyAuthenticated()
                )
                .addFilterBefore(x509AuthenticationFilter, AnonymousAuthenticationFilter.class)
                .anonymous(anonymous -> anonymous.authenticationFilter(c2AnonymousAuthenticationFilter))
                .build();
    }

    @Autowired
    public void setX509AuthenticationProvider(X509AuthenticationProvider x509AuthenticationProvider) {
        this.authenticationProvider = x509AuthenticationProvider;
    }

    @Autowired
    public void setX509AuthenticationFilter(X509AuthenticationFilter x509AuthenticationFilter) {
        this.x509AuthenticationFilter = x509AuthenticationFilter;
    }

    @Autowired
    public void setC2AnonymousAuthenticationFilter(C2AnonymousAuthenticationFilter c2AnonymousAuthenticationFilter) {
        this.c2AnonymousAuthenticationFilter = c2AnonymousAuthenticationFilter;
    }
}
