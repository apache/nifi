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
package org.apache.nifi.key.service;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.OutputEncryptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.util.UUID;

import static org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder.AES_256_CBC;
import static org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder.DES3_CBC;
import static org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder.PBE_SHA1_3DES;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardPrivateKeyServiceTest {
    private static final String SERVICE_ID = StandardPrivateKeyServiceTest.class.getSimpleName();

    private static final BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

    private static final String PATH_NOT_FOUND = "/path/not/found";

    private static final String KEY_NOT_VALID = "-----BEGIN KEY NOT VALID-----";

    private static final String RSA_ALGORITHM = "RSA";

    private static final OutputEncryptor DISABLED_ENCRYPTOR = null;

    private static PrivateKey generatedPrivateKey;

    StandardPrivateKeyService service;

    TestRunner runner;

    @BeforeAll
    static void setPrivateKey() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        generatedPrivateKey = keyPair.getPrivate();
    }

    @BeforeEach
    void setService() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardPrivateKeyService();
        runner.addControllerService(SERVICE_ID, service);
    }

    @Test
    void testMissingRequiredProperties() {
        runner.assertNotValid(service);
    }

    @Test
    void testKeyFileNotFound() {
        runner.setProperty(StandardPrivateKeyService.KEY_FILE, PATH_NOT_FOUND);
        runner.assertNotValid();
    }

    @Test
    void testKeyNotValid() {
        runner.setProperty(StandardPrivateKeyService.KEY, KEY_NOT_VALID);
        runner.assertNotValid();
    }

    @Test
    void testGetPrivateKeyEncodedKey() throws Exception {
        final String encodedPrivateKey = getEncodedPrivateKey(generatedPrivateKey, DISABLED_ENCRYPTOR);

        runner.setProperty(service, StandardPrivateKeyService.KEY, encodedPrivateKey);
        runner.enableControllerService(service);

        final PrivateKey privateKey = service.getPrivateKey();
        assertEquals(generatedPrivateKey, privateKey);
    }

    @ParameterizedTest
    @MethodSource("encryptionAlgorithms")
    void testGetPrivateKeyEncryptedKey(final String encryptionAlgorithm) throws Exception {
        final String password = UUID.randomUUID().toString();
        final OutputEncryptor outputEncryptor = getOutputEncryptor(encryptionAlgorithm, password);
        final String encryptedPrivateKey = getEncodedPrivateKey(generatedPrivateKey, outputEncryptor);
        final Path keyPath = writeKey(encryptedPrivateKey);

        runner.setProperty(service, StandardPrivateKeyService.KEY_FILE, keyPath.toString());
        runner.setProperty(service, StandardPrivateKeyService.KEY_PASSWORD, password);
        runner.enableControllerService(service);

        final PrivateKey privateKey = service.getPrivateKey();
        assertEquals(generatedPrivateKey, privateKey);
    }

    private static String[] encryptionAlgorithms() {
        return new String[] {
                AES_256_CBC,
                DES3_CBC,
                PBE_SHA1_3DES
        };
    }

    private Path writeKey(final String encodedPrivateKey) throws IOException {
        final Path keyPath = Files.createTempFile(StandardPrivateKeyServiceTest.class.getSimpleName(), RSA_ALGORITHM);
        keyPath.toFile().deleteOnExit();

        final byte[] keyBytes = encodedPrivateKey.getBytes(StandardCharsets.UTF_8);

        Files.write(keyPath, keyBytes);
        return keyPath;
    }

    private String getEncodedPrivateKey(final PrivateKey privateKey, final OutputEncryptor outputEncryptor) throws Exception {
        final StringWriter stringWriter = new StringWriter();
        try (final JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) {
            final JcaPKCS8Generator generator = new JcaPKCS8Generator(privateKey, outputEncryptor);
            pemWriter.writeObject(generator);
        }
        return stringWriter.toString();
    }

    private OutputEncryptor getOutputEncryptor(final String encryptionAlgorithm, final String password) throws OperatorCreationException {
        return new JceOpenSSLPKCS8EncryptorBuilder(new ASN1ObjectIdentifier(encryptionAlgorithm))
                .setProvider(BOUNCY_CASTLE_PROVIDER)
                .setPassword(password.toCharArray())
                .build();
    }
}
