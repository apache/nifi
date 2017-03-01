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
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@ImportResource({"classpath:minifi-c2-web-security-context.xml"})
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {
    private AuthenticationProvider authenticationProvider;
    private X509AuthenticationFilter x509AuthenticationFilter;
    private C2AnonymousAuthenticationFilter c2AnonymousAuthenticationFilter;

    public SecurityConfiguration() {
        super(true);
    }

    @Bean
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        // override xxxBean method so the authentication manager is available in app context (necessary for the method level security)
        return super.authenticationManagerBean();
    }

    @Override
    public void configure(WebSecurity web) throws Exception {
        web.ignoring().antMatchers("/access", "/access/config", "/access/token", "/access/kerberos");
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .rememberMe().disable().authorizeRequests().anyRequest().fullyAuthenticated().and()
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);
        http.addFilterBefore(x509AuthenticationFilter, AnonymousAuthenticationFilter.class);
        http.anonymous().authenticationFilter(c2AnonymousAuthenticationFilter);
    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider(authenticationProvider);
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
