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
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.x509.SubjectDnX509PrincipalExtractor;
import org.apache.nifi.web.security.x509.X509AuthenticationFilter;
import org.apache.nifi.web.security.x509.X509AuthenticationProvider;
import org.apache.nifi.web.security.x509.X509CertificateExtractor;
import org.apache.nifi.web.security.x509.X509CertificateValidator;
import org.apache.nifi.web.security.x509.X509IdentityProvider;
import org.apache.nifi.web.security.x509.ocsp.OcspCertificateValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.web.authentication.preauth.x509.X509PrincipalExtractor;

/**
 * X.509 Configuration for Authentication Security
 */
@Configuration
public class X509AuthenticationSecurityConfiguration {
    private final NiFiProperties niFiProperties;

    private final Authorizer authorizer;

    @Autowired
    public X509AuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties,
            final Authorizer authorizer
    ) {
        this.niFiProperties = niFiProperties;
        this.authorizer = authorizer;
    }

    @Bean
    public X509AuthenticationFilter x509AuthenticationFilter(final AuthenticationManager authenticationManager) {
        final X509AuthenticationFilter x509AuthenticationFilter = new X509AuthenticationFilter();
        x509AuthenticationFilter.setProperties(niFiProperties);
        x509AuthenticationFilter.setCertificateExtractor(certificateExtractor());
        x509AuthenticationFilter.setPrincipalExtractor(principalExtractor());
        x509AuthenticationFilter.setAuthenticationManager(authenticationManager);
        return x509AuthenticationFilter;
    }

    @Bean
    public X509AuthenticationProvider x509AuthenticationProvider() {
        return new X509AuthenticationProvider(certificateIdentityProvider(), authorizer, niFiProperties);
    }

    @Bean
    public X509CertificateExtractor certificateExtractor() {
        return new X509CertificateExtractor();
    }

    @Bean
    public X509PrincipalExtractor principalExtractor() {
        return new SubjectDnX509PrincipalExtractor();
    }

    @Bean
    public OcspCertificateValidator ocspValidator() {
        return new OcspCertificateValidator(niFiProperties);
    }

    @Bean
    public X509CertificateValidator certificateValidator() {
        final X509CertificateValidator certificateValidator = new X509CertificateValidator();
        certificateValidator.setOcspValidator(ocspValidator());
        return certificateValidator;
    }

    @Bean
    public X509IdentityProvider certificateIdentityProvider() {
        final X509IdentityProvider identityProvider = new X509IdentityProvider();
        identityProvider.setCertificateValidator(certificateValidator());
        identityProvider.setPrincipalExtractor(principalExtractor());
        return identityProvider;
    }
}
