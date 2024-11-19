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

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.saml2.SamlUrlPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml2.core.Saml2X509Credential;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Standard implementation of Relying Party Registration Repository based on NiFi Properties
 */
public class StandardRelyingPartyRegistrationRepository implements RelyingPartyRegistrationRepository {
    static final String BASE_URL_FORMAT = "{baseUrl}%s";

    static final String LOGIN_RESPONSE_LOCATION = String.format(BASE_URL_FORMAT, SamlUrlPath.LOGIN_RESPONSE.getPath());

    static final String SINGLE_LOGOUT_RESPONSE_SERVICE_LOCATION = String.format(BASE_URL_FORMAT, SamlUrlPath.SINGLE_LOGOUT_RESPONSE.getPath());

    private static final String RSA_PUBLIC_KEY_ALGORITHM = "RSA";

    private static final Principal[] UNFILTERED_ISSUERS = {};

    private static final Logger logger = LoggerFactory.getLogger(StandardRelyingPartyRegistrationRepository.class);

    private final NiFiProperties properties;

    private final X509ExtendedTrustManager trustManager;

    private final X509ExtendedKeyManager keyManager;

    private final RelyingPartyRegistration relyingPartyRegistration;

    /**
     * Standard implementation builds a Registration based on NiFi Properties and returns the same instance for all queries
     *
     * @param keyManager Key Manager loaded from properties
     * @param trustManager Trust manager loaded from properties
     * @param properties NiFi Application Properties
     */
    public StandardRelyingPartyRegistrationRepository(
            final NiFiProperties properties,
            final X509ExtendedKeyManager keyManager,
            final X509ExtendedTrustManager trustManager
    ) {
        this.properties = properties;
        this.keyManager = keyManager;
        this.trustManager = trustManager;
        this.relyingPartyRegistration = getRelyingPartyRegistration();
    }

    @Override
    public RelyingPartyRegistration findByRegistrationId(final String registrationId) {
        return relyingPartyRegistration;
    }

    private RelyingPartyRegistration getRelyingPartyRegistration() {
        final RegistrationBuilderProvider registrationBuilderProvider = new StandardRegistrationBuilderProvider(properties, keyManager, trustManager);
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
                .toList();
        logger.debug("Loaded SAML2 Signing Credentials [{}]", signingCredentials.size());

        builder.signingX509Credentials(credentials -> credentials.addAll(signingCredentials));
        builder.decryptionX509Credentials(credentials -> credentials.addAll(signingCredentials));

        final List<Saml2X509Credential> verificationCredentials = configuredCredentials.stream()
                .filter(Saml2X509Credential::isVerificationCredential)
                .toList();
        logger.debug("Loaded SAML2 Verification Credentials [{}]", verificationCredentials.size());

        builder.assertingPartyMetadata(assertingPartyMetadata -> assertingPartyMetadata
                .signingAlgorithms(signingAlgorithms -> signingAlgorithms.add(properties.getSamlSignatureAlgorithm()))
                .verificationX509Credentials(credentials -> credentials.addAll(verificationCredentials))
                .encryptionX509Credentials(credentials -> credentials.addAll(verificationCredentials))
        );

        return builder.build();
    }

    private Collection<Saml2X509Credential> getCredentials() {
        final List<Saml2X509Credential> credentials = new ArrayList<>();

        if (keyManager != null) {
            final List<String> keyAliases = getKeyAliases();
            for (final String alias : keyAliases) {
                final PrivateKey privateKey = keyManager.getPrivateKey(alias);
                final X509Certificate[] certificateChain = keyManager.getCertificateChain(alias);
                final X509Certificate certificate = certificateChain[0];
                final Saml2X509Credential credential = new Saml2X509Credential(
                        privateKey,
                        certificate,
                        Saml2X509Credential.Saml2X509CredentialType.SIGNING,
                        Saml2X509Credential.Saml2X509CredentialType.DECRYPTION
                );
                credentials.add(credential);
            }
        }

        if (trustManager != null) {
            for (final X509Certificate certificate : trustManager.getAcceptedIssuers()) {
                final Saml2X509Credential credential = new Saml2X509Credential(
                        certificate,
                        Saml2X509Credential.Saml2X509CredentialType.ENCRYPTION,
                        Saml2X509Credential.Saml2X509CredentialType.VERIFICATION
                );
                credentials.add(credential);
            }
        }

        return credentials;
    }

    private List<String> getKeyAliases() {
        final List<String> keyAliases = new ArrayList<>();

        final String[] serverAliases = keyManager.getServerAliases(RSA_PUBLIC_KEY_ALGORITHM, UNFILTERED_ISSUERS);
        if (serverAliases != null) {
            keyAliases.addAll(Arrays.asList(serverAliases));
        }

        return keyAliases;
    }
}
