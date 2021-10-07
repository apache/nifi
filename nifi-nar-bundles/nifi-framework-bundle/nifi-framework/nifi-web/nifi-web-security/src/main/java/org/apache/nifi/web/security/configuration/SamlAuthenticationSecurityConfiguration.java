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

import org.apache.nifi.admin.service.IdpCredentialService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.saml.SAMLService;
import org.apache.nifi.web.security.saml.impl.StandardSAMLConfigurationFactory;
import org.apache.nifi.web.security.saml.impl.StandardSAMLCredentialStore;
import org.apache.nifi.web.security.saml.impl.StandardSAMLService;
import org.apache.nifi.web.security.saml.impl.StandardSAMLStateManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * SAML Configuration for Authentication Security
 */
@Configuration
public class SamlAuthenticationSecurityConfiguration {
    private final NiFiProperties niFiProperties;

    private final BearerTokenProvider bearerTokenProvider;

    private final IdpCredentialService idpCredentialService;

    @Autowired
    public SamlAuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties,
            final BearerTokenProvider bearerTokenProvider,
            final IdpCredentialService idpCredentialService
    ) {
        this.niFiProperties = niFiProperties;
        this.bearerTokenProvider = bearerTokenProvider;
        this.idpCredentialService = idpCredentialService;
    }

    @Bean(initMethod = "initialize", destroyMethod = "shutdown")
    public SAMLService samlService() {
        return new StandardSAMLService(samlConfigurationFactory(), niFiProperties);
    }

    @Bean
    public StandardSAMLStateManager samlStateManager() {
        return new StandardSAMLStateManager(bearerTokenProvider);
    }

    @Bean
    public StandardSAMLCredentialStore samlCredentialStore() {
        return new StandardSAMLCredentialStore(idpCredentialService);
    }

    @Bean
    public StandardSAMLConfigurationFactory samlConfigurationFactory() {
        return new StandardSAMLConfigurationFactory();
    }
}
