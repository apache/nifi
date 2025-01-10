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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.anonymous.NiFiAnonymousAuthenticationFilter;
import org.apache.nifi.web.security.anonymous.NiFiAnonymousAuthenticationProvider;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.spring.LoginIdentityProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.AuthenticationManager;

/**
 * Spring Configuration for Authentication Security
 */
@Configuration
@Import({
        ClientRegistrationConfiguration.class,
        KeyPairGeneratorConfiguration.class,
        JwtAuthenticationSecurityConfiguration.class,
        JwtDecoderConfiguration.class,
        OidcSecurityConfiguration.class,
        SamlAuthenticationSecurityConfiguration.class,
        X509AuthenticationSecurityConfiguration.class
})
public class AuthenticationSecurityConfiguration {
    private final NiFiProperties niFiProperties;

    private final ExtensionManager extensionManager;

    private final Authorizer authorizer;

    @Autowired
    public AuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties,
            final ExtensionManager extensionManager,
            final Authorizer authorizer
    ) {
        this.niFiProperties = niFiProperties;
        this.extensionManager = extensionManager;
        this.authorizer = authorizer;
    }

    @Bean
    public NiFiAnonymousAuthenticationFilter anonymousAuthenticationFilter(final AuthenticationManager authenticationManager) {
        final NiFiAnonymousAuthenticationFilter anonymousAuthenticationFilter = new NiFiAnonymousAuthenticationFilter();
        anonymousAuthenticationFilter.setProperties(niFiProperties);
        anonymousAuthenticationFilter.setAuthenticationManager(authenticationManager);
        return anonymousAuthenticationFilter;
    }

    @Bean
    public LoginIdentityProviderFactoryBean loginIdentityProviderFactoryBean() {
        final LoginIdentityProviderFactoryBean loginIdentityProviderFactoryBean = new LoginIdentityProviderFactoryBean();
        loginIdentityProviderFactoryBean.setProperties(niFiProperties);
        loginIdentityProviderFactoryBean.setExtensionManager(extensionManager);
        return loginIdentityProviderFactoryBean;
    }

    @Bean
    public Object loginIdentityProvider() throws Exception {
        return loginIdentityProviderFactoryBean().getObject();
    }

    @Bean
    public LogoutRequestManager logoutRequestManager() {
        return new LogoutRequestManager();
    }

    @Bean
    public NiFiAnonymousAuthenticationProvider anonymousAuthenticationProvider() {
        return new NiFiAnonymousAuthenticationProvider(niFiProperties, authorizer);
    }
}
