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
package org.apache.nifi.bootstrap.property;

import org.apache.nifi.bootstrap.property.SecurityApplicationPropertyHandler.SecurityProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityApplicationPropertyHandlerTest {

    private static final String EMPTY = "";

    private static final String PROPERTIES_FILE_NAME = "nifi.properties";

    private static final String PORT = "8443";

    private static final String STORE_TYPE = "PKCS12";

    private static final String KEYSTORE_FILE = "keystore.p12";

    private static final String TRUSTSTORE_FILE = "truststore.p12";

    private static final int DNS_NAME_TYPE = 2;

    private static final String FIRST_PROXY_HOST = "maximum.domain.subject.alternative.name.length.greater.than.sixty.four.characters.nifi.apache.org";

    private static final int FIRST_PROXY_HOST_PORT = 443;

    private static final String SECOND_PROXY_HOST = "nifi.local";

    private static final String WEB_PROXY_HOST_PROPERTY = "%s:%d,%s".formatted(FIRST_PROXY_HOST, FIRST_PROXY_HOST_PORT, SECOND_PROXY_HOST);

    private static final Logger logger = LoggerFactory.getLogger(SecurityApplicationPropertyHandlerTest.class);

    @TempDir
    private Path tempDir;

    private SecurityApplicationPropertyHandler handler;

    @BeforeEach
    void setHandler() {
        handler = new SecurityApplicationPropertyHandler(logger);
    }

    @Test
    void testHandlePropertiesSuccess() throws IOException, GeneralSecurityException {
        final Properties sourceProperties = getSourceProperties();
        sourceProperties.setProperty(SecurityProperty.WEB_PROXY_HOST.getName(), WEB_PROXY_HOST_PROPERTY);

        final Path propertiesPath = writeProperties(sourceProperties);

        final Path propertiesLocation = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, propertiesLocation);

        try {
            handler.handleProperties(propertiesLocation);

            final Properties properties = loadProperties(propertiesLocation);
            assertStoreCreated(properties, SecurityProperty.KEYSTORE);
            assertStoreCreated(properties, SecurityProperty.TRUSTSTORE);

            assertNotEquals(EMPTY, properties.getProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName()));
            assertNotEquals(EMPTY, properties.getProperty(SecurityProperty.KEYSTORE_PASSWD.getName()));
            assertNotEquals(EMPTY, properties.getProperty(SecurityProperty.KEY_PASSWD.getName()));

            // Run again to evaluate handling of existing store files
            handler.handleProperties(propertiesLocation);

            final Properties reloadedProperties = loadProperties(propertiesLocation);

            assertEquals(properties.getProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName()), reloadedProperties.getProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName()));
            assertEquals(properties.getProperty(SecurityProperty.KEYSTORE_PASSWD.getName()), reloadedProperties.getProperty(SecurityProperty.KEYSTORE_PASSWD.getName()));
            assertEquals(properties.getProperty(SecurityProperty.KEY_PASSWD.getName()), reloadedProperties.getProperty(SecurityProperty.KEY_PASSWD.getName()));

            assertKeyStoreReadable(reloadedProperties);
        } finally {
            deleteStores(propertiesLocation);
        }
    }

    private Properties getSourceProperties() {
        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(SecurityProperty.HTTPS_PORT.getName(), PORT);

        final Path keystorePath = tempDir.resolve(KEYSTORE_FILE);
        sourceProperties.setProperty(SecurityProperty.KEYSTORE.getName(), keystorePath.toAbsolutePath().toString());
        sourceProperties.setProperty(SecurityProperty.KEYSTORE_TYPE.getName(), STORE_TYPE);
        sourceProperties.setProperty(SecurityProperty.KEYSTORE_PASSWD.getName(), EMPTY);
        sourceProperties.setProperty(SecurityProperty.KEY_PASSWD.getName(), EMPTY);

        final Path truststorePath = tempDir.resolve(TRUSTSTORE_FILE);
        sourceProperties.setProperty(SecurityProperty.TRUSTSTORE.getName(), truststorePath.toAbsolutePath().toString());
        sourceProperties.setProperty(SecurityProperty.TRUSTSTORE_TYPE.getName(), STORE_TYPE);
        sourceProperties.setProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName(), EMPTY);
        return sourceProperties;
    }

    @Test
    void testHandlePropertiesHttpsNotConfigured() throws IOException {
        final Properties sourceProperties = new Properties();
        sourceProperties.setProperty(SecurityProperty.KEYSTORE.getName(), EMPTY);
        sourceProperties.setProperty(SecurityProperty.KEYSTORE_PASSWD.getName(), EMPTY);
        sourceProperties.setProperty(SecurityProperty.TRUSTSTORE.getName(), EMPTY);
        sourceProperties.setProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName(), EMPTY);

        final Path propertiesPath = writeProperties(sourceProperties);

        final Path propertiesLocation = tempDir.resolve(propertiesPath.getFileName());
        Files.copy(propertiesPath, propertiesLocation);

        handler.handleProperties(propertiesLocation);

        final Properties properties = loadProperties(propertiesLocation);

        final String keystorePasswd = properties.getProperty(SecurityProperty.KEYSTORE_PASSWD.getName());
        assertTrue(keystorePasswd.isBlank());

        final String truststorePasswd = properties.getProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName());
        assertTrue(truststorePasswd.isBlank());
    }

    private void assertKeyStoreReadable(final Properties properties) throws GeneralSecurityException, IOException {
        final String storeLocation = properties.getProperty(SecurityProperty.KEYSTORE.getName());
        final String storePasswd = properties.getProperty(SecurityProperty.KEYSTORE_PASSWD.getName());

        final Path storePath = Paths.get(storeLocation);
        final KeyStore keyStore = KeyStore.getInstance(STORE_TYPE);
        try (InputStream inputStream = Files.newInputStream(storePath)) {
            keyStore.load(inputStream, storePasswd.toCharArray());
        }

        final X509Certificate certificate = (X509Certificate) keyStore.getCertificate(SecurityApplicationPropertyHandler.ENTRY_ALIAS);
        assertNotNull(certificate);
        assertEquals(SecurityApplicationPropertyHandler.CERTIFICATE_ISSUER, certificate.getIssuerX500Principal());
        assertEquals(SecurityApplicationPropertyHandler.CERTIFICATE_ISSUER, certificate.getSubjectX500Principal());

        assertSubjectAlternativeNamesFound(certificate);
    }

    private void assertSubjectAlternativeNamesFound(final X509Certificate certificate) throws CertificateParsingException {
        final Collection<List<?>> subjectAlternativeNames = certificate.getSubjectAlternativeNames();

        assertFalse(subjectAlternativeNames.isEmpty());

        final List<String> alternativeNames = new ArrayList<>();

        for (final List<?> subjectAlternativeName : subjectAlternativeNames) {
            final Iterator<?> generalNameValue = subjectAlternativeName.iterator();
            assertTrue(generalNameValue.hasNext());

            final Object generalName = generalNameValue.next();
            assertEquals(DNS_NAME_TYPE, generalName);

            assertTrue(generalNameValue.hasNext());
            final Object alternativeName = generalNameValue.next();
            alternativeNames.add(alternativeName.toString());
        }

        assertTrue(alternativeNames.contains(FIRST_PROXY_HOST), "Web Proxy Host [%s] not found".formatted(FIRST_PROXY_HOST));
        assertTrue(alternativeNames.contains(SECOND_PROXY_HOST), "Web Proxy Host [%s] not found".formatted(SECOND_PROXY_HOST));
    }

    private void assertStoreCreated(final Properties properties, final SecurityProperty securityProperty) {
        final String location = properties.getProperty(securityProperty.getName());
        final Path path = Paths.get(location);
        assertTrue(Files.exists(path));
    }

    private void deleteStores(final Path propertiesLocation) throws IOException {
        final Properties properties = loadProperties(propertiesLocation);
        deleteStore(properties, SecurityProperty.KEYSTORE);
        deleteStore(properties, SecurityProperty.TRUSTSTORE);
    }

    private void deleteStore(final Properties properties, final SecurityProperty securityProperty) throws IOException {
        final String location = properties.getProperty(securityProperty.getName());
        if (location != null && !location.isBlank()) {
            final Path path = Paths.get(location);
            if (Files.exists(path)) {
                Files.delete(path);
            }
        }
    }

    private Path writeProperties(final Properties properties) throws IOException {
        final Path propertiesPath = tempDir.resolve(PROPERTIES_FILE_NAME);
        try (OutputStream outputStream = Files.newOutputStream(propertiesPath)) {
            properties.store(outputStream, null);
        }
        return propertiesPath;
    }

    private Properties loadProperties(final Path propertiesLocation) throws IOException {
        try (final InputStream inputStream = Files.newInputStream(propertiesLocation)) {
            final Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        }
    }
}
