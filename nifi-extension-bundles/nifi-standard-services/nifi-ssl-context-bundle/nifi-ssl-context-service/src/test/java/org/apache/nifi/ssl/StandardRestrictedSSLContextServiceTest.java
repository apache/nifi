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

import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class StandardRestrictedSSLContextServiceTest {

    private static final String ALIAS = "entry-0";

    private static final String SERVICE_ID = StandardRestrictedSSLContextService.class.getSimpleName();

    private static final String KEY_STORE_EXTENSION = ".p12";

    private static final String KEY_STORE_PASS = StandardRestrictedSSLContextServiceTest.class.getName();

    @TempDir
    private static Path keyStoreDirectory;

    private static Path keyStorePath;

    private static String keyStoreType;

    @Mock
    private Processor processor;

    private StandardRestrictedSSLContextService service;

    private TestRunner runner;

    @BeforeAll
    public static void setConfiguration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder().build();
        keyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), KEY_STORE_PASS.toCharArray(), new Certificate[]{certificate});

        keyStorePath = Files.createTempFile(keyStoreDirectory, StandardRestrictedSSLContextServiceTest.class.getSimpleName(), KEY_STORE_EXTENSION);
        try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEY_STORE_PASS.toCharArray());
        }

        keyStoreType = keyStore.getType().toUpperCase();
    }

    @BeforeEach
    public void setRunner() {
        runner = TestRunners.newTestRunner(processor);
        service = new StandardRestrictedSSLContextService();
    }

    @Test
    public void testMinimumPropertiesValid() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        setMinimumProperties();
        runner.assertValid(service);
    }

    @Test
    public void testPreferredProtocolsValid() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        setMinimumProperties();

        for (final String protocol : TlsPlatform.getPreferredProtocols()) {
            runner.setProperty(service, StandardRestrictedSSLContextService.SSL_ALGORITHM, protocol);
            runner.assertValid(service);
        }
    }

    @Test
    public void testPreferredProtocolsCreateContext() throws InitializationException {
        runner.addControllerService(SERVICE_ID, service);
        setMinimumProperties();

        for (final String protocol : TlsPlatform.getPreferredProtocols()) {
            runner.setProperty(service, StandardRestrictedSSLContextService.SSL_ALGORITHM, protocol);
            runner.assertValid(service);
            runner.enableControllerService(service);

            final SSLContext sslContext = service.createContext();
            assertEquals(protocol, sslContext.getProtocol());

            runner.disableControllerService(service);
        }
    }

    private void setMinimumProperties() {
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE, keyStorePath.toString());
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE_PASSWORD, KEY_STORE_PASS);
        runner.setProperty(service, StandardRestrictedSSLContextService.KEYSTORE_TYPE, keyStoreType);
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE, keyStorePath.toString());
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE_PASSWORD, KEY_STORE_PASS);
        runner.setProperty(service, StandardRestrictedSSLContextService.TRUSTSTORE_TYPE, keyStoreType);
    }
}
