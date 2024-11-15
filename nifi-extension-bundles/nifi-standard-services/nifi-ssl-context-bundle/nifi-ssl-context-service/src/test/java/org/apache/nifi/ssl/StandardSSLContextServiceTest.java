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
package org.apache.nifi.ssl;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class StandardSSLContextServiceTest {
    private static final String SERVICE_PROPERTY = "SSL Context Svc ID";

    private static final String SERVICE_ID = StandardSSLContextService.class.getSimpleName();

    private static final String ALIAS = "entry-0";

    private static final String KEY_STORE_EXTENSION = ".p12";

    private static final String KEY_STORE_PASS = UUID.randomUUID().toString();

    private static final String TRUST_STORE_PASS = UUID.randomUUID().toString();

    @TempDir
    private static Path keyStoreDirectory;

    private static String keyStoreType;

    private static Path keyStorePath;

    private static Path trustStorePath;

    private TestRunner runner;

    private StandardSSLContextService service;

    @BeforeAll
    public static void setConfiguration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder().build();
        keyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), KEY_STORE_PASS.toCharArray(), new Certificate[]{certificate});

        keyStorePath = Files.createTempFile(keyStoreDirectory, "keyStore", KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEY_STORE_PASS.toCharArray());
        }

        keyStoreType = keyStore.getType().toUpperCase();

        final KeyStore trustStore = new EphemeralKeyStoreBuilder().addCertificate(certificate).build();
        trustStorePath = Files.createTempFile(keyStoreDirectory, "trustStore", KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(trustStorePath)) {
            trustStore.store(outputStream, TRUST_STORE_PASS.toCharArray());
        }
    }

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardSSLContextService();
    }

    @Test
    public void testNotValidMissingProperties() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service, Map.of());
        runner.assertNotValid(service);
    }

    @Test
    public void testNotValidMissingKeyStoreType() throws InitializationException {
        final Map<String, String> properties = new HashMap<>();

        properties.put(StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.addControllerService(SERVICE_ID, service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testNotValidMissingTrustStoreType() throws InitializationException {
        final Map<String, String> properties = new HashMap<>();

        properties.put(StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        runner.addControllerService(SERVICE_ID, service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testNotValidIncorrectPassword() throws InitializationException {
        final Map<String, String> properties = new HashMap<>();

        runner.addControllerService(SERVICE_ID, service, properties);

        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), String.class.getSimpleName());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), String.class.getSimpleName());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);

        runner.assertNotValid(service);
    }

    @Test
    public void testShouldFailToAddControllerServiceWithNonExistentFiles() throws InitializationException {
        final Map<String, String> properties = new HashMap<>();

        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/DOES-NOT-EXIST.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), keyStorePath.toString());
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), keyStorePath.toString());
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.addControllerService(SERVICE_ID, service, properties);

        runner.assertNotValid(service);
    }

    @Test
    public void testCreateContext() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final SSLContext sslContext = service.createContext();
        assertNotNull(sslContext);
    }

    @Test
    public void testCreateTrustManager() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final X509TrustManager trustManager = service.createTrustManager();
        assertNotNull(trustManager);
    }

    @Test
    public void testCreateTrustManagerKeyStoreConfigured() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final X509TrustManager trustManager = service.createTrustManager();
        assertNotNull(trustManager);
    }

    @Test
    public void testCreateKeyManager() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final Optional<X509ExtendedKeyManager> keyManagerFound = service.createKeyManager();
        assertTrue(keyManagerFound.isPresent());
    }

    @Test
    public void testCreateKeyManagerKeyStoreNotConfigured() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final Optional<X509ExtendedKeyManager> keyManagerFound = service.createKeyManager();
        assertTrue(keyManagerFound.isEmpty());
    }

    @Test
    public void testCreateContextExpressionLanguageProperties() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setEnvironmentVariableValue("keystore", keyStorePath.toString());
        runner.setEnvironmentVariableValue("truststore", trustStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), "${keystore}");
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), "${truststore}");
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final SSLContext sslContext = service.createContext();
        assertNotNull(sslContext);
    }

    @Test
    public void testValidPropertiesChanged() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.KEY_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        runner.disableControllerService(service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/DOES-NOT-EXIST.jks");
        runner.assertNotValid(service);

        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), keyStorePath.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), String.class.getSimpleName());

        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testValidPropertiesChangedValidationExpired(@TempDir final Path tempDir) throws InitializationException, IOException {
        final Path tempKeyStore = tempDir.resolve("keyStore.p12");
        final Path tempTrustStore = tempDir.resolve("trustStore.p12");

        Files.copy(keyStorePath, tempKeyStore, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(trustStorePath, tempTrustStore, StandardCopyOption.REPLACE_EXISTING);

        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), tempKeyStore.toString());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEY_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), keyStoreType);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), tempTrustStore.toString());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.enableControllerService(service);

        runner.setProperty(SERVICE_PROPERTY, SERVICE_ID);
        runner.assertValid(service);

        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext();
        // This service does not use the state manager
        final ValidationContext validationContext = new MockValidationContext(processContext, null);

        // Even though the keystore file is no longer present, because no property changed, the cached result is still valid
        Collection<ValidationResult> validationResults = service.customValidate(validationContext);
        assertTrue(validationResults.isEmpty(), "validation results is not empty");

        // Have to exhaust the cached result by checking n-1 more times
        for (int i = 2; i < service.getValidationCacheExpiration(); i++) {
            validationResults = service.customValidate(validationContext);
            assertTrue(validationResults.isEmpty(), "validation results is not empty");
        }

        validationResults = service.customValidate(validationContext);
        assertFalse(validationResults.isEmpty(), "validation results is empty");
    }

    @Test
    public void testCreateContextTrustStoreWithoutKeyStore() throws InitializationException {
        final Map<String, String> properties = new HashMap<>();

        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), trustStorePath.toString());
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), TRUST_STORE_PASS);
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), keyStoreType);
        runner.addControllerService(SERVICE_ID, service, properties);
        runner.enableControllerService(service);

        final SSLContext sslContext = service.createContext();
        assertNotNull(sslContext);
    }
}
