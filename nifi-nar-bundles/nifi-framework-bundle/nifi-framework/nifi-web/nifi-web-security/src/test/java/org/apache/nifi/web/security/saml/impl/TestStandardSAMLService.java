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
package org.apache.nifi.web.security.saml.impl;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.saml.SAMLConfigurationFactory;
import org.apache.nifi.web.security.saml.SAMLService;
import org.apache.nifi.web.security.saml.impl.tls.TruststoreStrategy;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardSAMLService {

    private NiFiProperties properties;
    private SAMLConfigurationFactory samlConfigurationFactory;
    private SAMLService samlService;

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void setup() {
        properties = mock(NiFiProperties.class);
        samlConfigurationFactory = new StandardSAMLConfigurationFactory();
        samlService = new StandardSAMLService(samlConfigurationFactory, properties);
    }

    @After
    public void teardown() {
        samlService.shutdown();
    }

    @Test
    public void testSamlEnabledWithFileBasedIdpMetadata() throws GeneralSecurityException, IOException {
        final String spEntityId = "org:apache:nifi";
        final File idpMetadataFile = new File("src/test/resources/saml/sso-circle-meta.xml");
        final String baseUrl = "https://localhost:8443/nifi-api";

        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();

        when(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE)).thenReturn(tlsConfiguration.getKeystorePath());
        when(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD)).thenReturn(tlsConfiguration.getKeystorePassword());
        when(properties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD)).thenReturn(tlsConfiguration.getKeyPassword());
        when(properties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE)).thenReturn(tlsConfiguration.getKeystoreType().getType());
        when(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE)).thenReturn(tlsConfiguration.getTruststorePath());
        when(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD)).thenReturn(tlsConfiguration.getTruststorePassword());
        when(properties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE)).thenReturn(tlsConfiguration.getTruststoreType().getType());
        when(properties.getPropertyKeys()).thenReturn(new HashSet<>(Arrays.asList(
                NiFiProperties.SECURITY_KEYSTORE,
                NiFiProperties.SECURITY_KEYSTORE_PASSWD,
                NiFiProperties.SECURITY_KEY_PASSWD,
                NiFiProperties.SECURITY_KEYSTORE_TYPE,
                NiFiProperties.SECURITY_TRUSTSTORE,
                NiFiProperties.SECURITY_TRUSTSTORE_PASSWD,
                NiFiProperties.SECURITY_TRUSTSTORE_TYPE
        )));

        when(properties.isSamlEnabled()).thenReturn(true);
        when(properties.getSamlServiceProviderEntityId()).thenReturn(spEntityId);
        when(properties.getSamlIdentityProviderMetadataUrl()).thenReturn("file://" + idpMetadataFile.getAbsolutePath());
        when(properties.getSamlAuthenticationExpiration()).thenReturn("12 hours");
        when(properties.getSamlHttpClientTruststoreStrategy()).thenReturn(TruststoreStrategy.JDK.name());

        // initialize the saml service
        samlService.initialize();
        assertTrue(samlService.isSamlEnabled());

        // initialize the service provider
        assertFalse(samlService.isServiceProviderInitialized());
        samlService.initializeServiceProvider(baseUrl);
        assertTrue(samlService.isServiceProviderInitialized());

        // obtain the service provider metadata xml
        final String spMetadataXml = samlService.getServiceProviderMetadata();
        assertTrue(spMetadataXml.contains("entityID=\"org:apache:nifi\""));
        assertTrue(spMetadataXml.contains("<md:AssertionConsumerService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" Location=\"https://localhost:8443/nifi-api/access/saml/login/consumer\""));
        assertTrue(spMetadataXml.contains("<md:SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" Location=\"https://localhost:8443/nifi-api/access/saml/single-logout/consumer\"/>"));
        assertTrue(spMetadataXml.contains("<md:SingleLogoutService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect\" Location=\"https://localhost:8443/nifi-api/access/saml/single-logout/consumer\"/>"));
    }

    @Test
    public void testInitializeWhenSamlNotEnabled() {
        when(properties.isSamlEnabled()).thenReturn(false);
        samlService.initialize();
        assertFalse(samlService.isSamlEnabled());
        assertThrows(IllegalStateException.class, () -> samlService.initializeServiceProvider("https://localhost:8443/nifi-api"));
        assertThrows(IllegalStateException.class, () -> samlService.getServiceProviderMetadata());
    }
}
