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

import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;
import org.opensaml.xmlsec.signature.support.SignatureConstants;
import org.springframework.security.saml2.core.Saml2X509Credential;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;

import javax.security.auth.x500.X500Principal;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardRelyingPartyRegistrationRepositoryTest {
    private static final String METADATA_PATH = "/saml/sso-circle-meta.xml";

    private static final String ENTITY_ID = "nifi";

    private static final X500Principal CERTIFICATE_PRINCIPAL = new X500Principal("CN=localhost");

    @Test
    void testFindByRegistrationId() {
        final NiFiProperties properties = getProperties();
        final StandardRelyingPartyRegistrationRepository repository = new StandardRelyingPartyRegistrationRepository(properties);

        final RelyingPartyRegistration registration = repository.findByRegistrationId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty());

        assertRegistrationPropertiesFound(registration);

        assertNull(registration.getSingleLogoutServiceLocation());
        assertNull(registration.getSingleLogoutServiceResponseLocation());

        final RelyingPartyRegistration.AssertingPartyDetails assertingPartyDetails = registration.getAssertingPartyDetails();
        assertFalse(assertingPartyDetails.getWantAuthnRequestsSigned());
        assertTrue(assertingPartyDetails.getSigningAlgorithms().contains(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256));

        final Collection<Saml2X509Credential> signingCredentials = registration.getSigningX509Credentials();
        assertTrue(signingCredentials.isEmpty());
    }

    @Test
    void testFindByRegistrationIdSingleLogoutEnabled() {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();

        final NiFiProperties properties = getSingleLogoutProperties(tlsConfiguration);
        final StandardRelyingPartyRegistrationRepository repository = new StandardRelyingPartyRegistrationRepository(properties);

        final RelyingPartyRegistration registration = repository.findByRegistrationId(Saml2RegistrationProperty.REGISTRATION_ID.getProperty());

        assertRegistrationPropertiesFound(registration);

        assertEquals(StandardRelyingPartyRegistrationRepository.SINGLE_LOGOUT_RESPONSE_SERVICE_LOCATION, registration.getSingleLogoutServiceLocation());
        assertEquals(StandardRelyingPartyRegistrationRepository.SINGLE_LOGOUT_RESPONSE_SERVICE_LOCATION, registration.getSingleLogoutServiceResponseLocation());

        final RelyingPartyRegistration.AssertingPartyDetails assertingPartyDetails = registration.getAssertingPartyDetails();
        assertFalse(assertingPartyDetails.getWantAuthnRequestsSigned());
        assertTrue(assertingPartyDetails.getSigningAlgorithms().contains(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA512));

        assertSigningCredentialsFound(registration);
        assertEncryptionCredentialsFound(assertingPartyDetails);
    }

    private void assertSigningCredentialsFound(final RelyingPartyRegistration registration) {
        final Collection<Saml2X509Credential> signingCredentials = registration.getSigningX509Credentials();
        assertFalse(signingCredentials.isEmpty());
        final Saml2X509Credential credential = signingCredentials.iterator().next();
        final X509Certificate certificate = credential.getCertificate();
        assertEquals(CERTIFICATE_PRINCIPAL, certificate.getSubjectX500Principal());
        assertEquals(CERTIFICATE_PRINCIPAL, certificate.getIssuerX500Principal());
    }

    private void assertEncryptionCredentialsFound(final RelyingPartyRegistration.AssertingPartyDetails assertingPartyDetails) {
        final Collection<Saml2X509Credential> encryptionCredentials = assertingPartyDetails.getEncryptionX509Credentials();
        assertFalse(encryptionCredentials.isEmpty());
        final Optional<Saml2X509Credential> certificateCredential = encryptionCredentials.stream().filter(
                credential -> CERTIFICATE_PRINCIPAL.equals(credential.getCertificate().getSubjectX500Principal())
        ).findFirst();
        assertTrue(certificateCredential.isPresent(), "Trust Store certificate credential not found");
    }

    private void assertRegistrationPropertiesFound(final RelyingPartyRegistration registration) {
        assertNotNull(registration);
        assertEquals(Saml2RegistrationProperty.REGISTRATION_ID.getProperty(), registration.getRegistrationId());
        assertEquals(ENTITY_ID, registration.getEntityId());
        assertEquals(StandardRelyingPartyRegistrationRepository.LOGIN_RESPONSE_LOCATION, registration.getAssertionConsumerServiceLocation());
    }

    private NiFiProperties getProperties() {
        final Properties properties = getStandardProperties();
        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }

    private NiFiProperties getSingleLogoutProperties(final TlsConfiguration tlsConfiguration) {
        final Properties properties = getStandardProperties();
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_SINGLE_LOGOUT_ENABLED, Boolean.TRUE.toString());
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_SIGNATURE_ALGORITHM, SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA512);

        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        properties.setProperty(NiFiProperties.SECURITY_KEY_PASSWD, tlsConfiguration.getKeyPassword());
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());

        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }

    private Properties getStandardProperties() {
        final Properties properties = new Properties();
        final String metadataUrl = getFileMetadataUrl();
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_IDP_METADATA_URL, metadataUrl);
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_SP_ENTITY_ID, ENTITY_ID);
        return properties;
    }

    private String getFileMetadataUrl() {
        final URL resource = Objects.requireNonNull(getClass().getResource(METADATA_PATH));
        return resource.toString();
    }
}
