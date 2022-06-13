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
package org.apache.nifi.web.security.saml2.registration;

import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml2.Saml2Exception;
import org.springframework.security.saml2.core.Saml2X509Credential;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;

import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Standard implementation of Relying Party Registration Repository based on NiFi Properties
 */
public class StandardRelyingPartyRegistrationRepository implements RelyingPartyRegistrationRepository {
    static final String BASE_URL_FORMAT = "{baseUrl}%s";

    static final String LOGIN_RESPONSE_LOCATION = String.format(BASE_URL_FORMAT, SamlUrlPath.LOGIN_RESPONSE.getPath());

    static final String SINGLE_LOGOUT_RESPONSE_SERVICE_LOCATION = String.format(BASE_URL_FORMAT, SamlUrlPath.SINGLE_LOGOUT_RESPONSE.getPath());

    private static final char[] BLANK_PASSWORD = new char[0];

    private static final Logger logger = LoggerFactory.getLogger(StandardRelyingPartyRegistrationRepository.class);

    private final Saml2CredentialProvider saml2CredentialProvider = new StandardSaml2CredentialProvider();

    private final NiFiProperties properties;

    private final RelyingPartyRegistration relyingPartyRegistration;

    /**
     * Standard implementation builds a Registration based on NiFi Properties and returns the same instance for all queries
     *
     * @param properties NiFi Application Properties
     */
    public StandardRelyingPartyRegistrationRepository(final NiFiProperties properties) {
        this.properties = properties;
        this.relyingPartyRegistration = getRelyingPartyRegistration();
    }

    @Override
    public RelyingPartyRegistration findByRegistrationId(final String registrationId) {
        return relyingPartyRegistration;
    }

    private RelyingPartyRegistration getRelyingPartyRegistration() {
        final RegistrationBuilderProvider registrationBuilderProvider = new StandardRegistrationBuilderProvider(properties);
        final RelyingPartyRegistration.Builder builder = registrationBuilderProvider.getRegistrationBuilder();

        builder.registrationId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty());

        final String entityId = properties.getSamlServiceProviderEntityId();
        builder.entityId(entityId);

        builder.assertionConsumerServiceLocation(LOGIN_RESPONSE_LOCATION);

        if (properties.isSamlSingleLogoutEnabled()) {
            builder.singleLogoutServiceLocation(SINGLE_LOGOUT_RESPONSE_SERVICE_LOCATION);
            builder.singleLogoutServiceResponseLocation(SINGLE_LOGOUT_RESPONSE_SERVICE_LOCATION);
        }

        final Collection<Saml2X509Credential> configuredCredentials = getCredentials();
        final List<Saml2X509Credential> signingCredentials = configuredCredentials.stream()
                .filter(Saml2X509Credential::isSigningCredential)
                .collect(Collectors.toList());
        logger.debug("Loaded SAML2 Signing Credentials [{}]", signingCredentials.size());

        builder.signingX509Credentials(credentials -> credentials.addAll(signingCredentials));
        builder.decryptionX509Credentials(credentials -> credentials.addAll(signingCredentials));

        final List<Saml2X509Credential> verificationCredentials = configuredCredentials.stream()
                .filter(Saml2X509Credential::isVerificationCredential)
                .collect(Collectors.toList());
        logger.debug("Loaded SAML2 Verification Credentials [{}]", verificationCredentials.size());

        builder.assertingPartyDetails(assertingPartyDetails -> assertingPartyDetails
                .signingAlgorithms(signingAlgorithms -> signingAlgorithms.add(properties.getSamlSignatureAlgorithm()))
                .verificationX509Credentials(credentials -> credentials.addAll(verificationCredentials))
                .encryptionX509Credentials(credentials -> credentials.addAll(verificationCredentials))
        );

        return builder.build();
    }

    private Collection<Saml2X509Credential> getCredentials() {
        final TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(properties);

        final List<Saml2X509Credential> credentials = new ArrayList<>();

        if (tlsConfiguration.isKeystorePopulated()) {
            final KeyStore keyStore = getKeyStore(tlsConfiguration);
            final char[] keyPassword = tlsConfiguration.getKeyPassword() == null
                    ? tlsConfiguration.getKeystorePassword().toCharArray()
                    : tlsConfiguration.getKeyPassword().toCharArray();
            final Collection<Saml2X509Credential> keyStoreCredentials = saml2CredentialProvider.getCredentials(keyStore, keyPassword);

            credentials.addAll(keyStoreCredentials);
        }

        if (tlsConfiguration.isTruststorePopulated()) {
            final KeyStore trustStore = getTrustStore(tlsConfiguration);
            final Collection<Saml2X509Credential> trustStoreCredentials = saml2CredentialProvider.getCredentials(trustStore, BLANK_PASSWORD);
            credentials.addAll(trustStoreCredentials);
        }

        return credentials;
    }

    private KeyStore getTrustStore(final TlsConfiguration tlsConfiguration) {
        try {
            return KeyStoreUtils.loadKeyStore(
                    tlsConfiguration.getTruststorePath(),
                    tlsConfiguration.getTruststorePassword().toCharArray(),
                    tlsConfiguration.getTruststoreType().getType()
            );
        } catch (final TlsException e) {
            throw new Saml2Exception("Trust Store loading failed", e);
        }
    }

    private KeyStore getKeyStore(final TlsConfiguration tlsConfiguration) {
        try {
            return KeyStoreUtils.loadKeyStore(
                    tlsConfiguration.getKeystorePath(),
                    tlsConfiguration.getKeystorePassword().toCharArray(),
                    tlsConfiguration.getKeystoreType().getType()
            );
        } catch (final TlsException e) {
            throw new Saml2Exception("Key Store loading failed", e);
        }
    }
}
