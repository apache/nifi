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
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardPrivateKeyServiceTest {
    private static final String SERVICE_ID = StandardPrivateKeyServiceTest.class.getSimpleName();

    private static final String PATH_NOT_FOUND = "/path/not/found";

    private static final String KEY_NOT_VALID = "-----BEGIN KEY NOT VALID-----";

    private static final String RSA_ALGORITHM = "RSA";

    private static final String PBES2_HMAC_SHA256_AES_256 = "PBEWithHmacSHA256AndAES_256";

    private static final String PBES2_HMAC_SHA512_AES_128 = "PBEWithHmacSHA512AndAES_128";

    private static final String PBE_SHA1_DESEDE = "PBEWithSHA1AndDESede";

    private static final String PBES2_ALGORITHM = "PBES2";

    private static final String PBES2_CIPHER_PREFIX = "PBEWithHmac";

    private static final String PRIVATE_KEY_TYPE = "PRIVATE KEY";

    private static final String ENCRYPTED_PRIVATE_KEY_TYPE = "ENCRYPTED PRIVATE KEY";

    private static final String PEM_FORMAT = "-----BEGIN %s-----\n%s\n-----END %s-----\n";

    private static final Base64.Encoder ENCODER = Base64.getMimeEncoder();

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
    void testGetPrivateKeyEncodedKey() {
        final String encodedPrivateKey = getPem(PRIVATE_KEY_TYPE, generatedPrivateKey.getEncoded());

        runner.setProperty(service, StandardPrivateKeyService.KEY, encodedPrivateKey);
        runner.enableControllerService(service);

        final PrivateKey privateKey = service.getPrivateKey();
        assertEquals(generatedPrivateKey, privateKey);
    }

    @ParameterizedTest
    @MethodSource("encryptionAlgorithms")
    void testGetPrivateKeyEncryptedKey(final String encryptionAlgorithm) throws Exception {
        final String password = UUID.randomUUID().toString();
        final String encryptedPrivateKey = getEncryptedPrivateKey(generatedPrivateKey, encryptionAlgorithm, password);
        final Path keyPath = writeKey(encryptedPrivateKey);

        runner.setProperty(service, StandardPrivateKeyService.KEY_FILE, keyPath.toString());
        runner.setProperty(service, StandardPrivateKeyService.KEY_PASSWORD, password);
        runner.enableControllerService(service);

        final PrivateKey privateKey = service.getPrivateKey();
        assertEquals(generatedPrivateKey, privateKey);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("key-file", StandardPrivateKeyService.KEY_FILE.getName()),
                Map.entry("key", StandardPrivateKeyService.KEY.getName()),
                Map.entry("key-password", StandardPrivateKeyService.KEY_PASSWORD.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }

    private static String[] encryptionAlgorithms() {
        return new String[] {
                PBES2_HMAC_SHA256_AES_256,
                PBES2_HMAC_SHA512_AES_128,
                PBE_SHA1_DESEDE
        };
    }

    private Path writeKey(final String encodedPrivateKey) throws IOException {
        final Path keyPath = Files.createTempFile(StandardPrivateKeyServiceTest.class.getSimpleName(), RSA_ALGORITHM);
        keyPath.toFile().deleteOnExit();

        Files.writeString(keyPath, encodedPrivateKey);
        return keyPath;
    }

    private String getEncryptedPrivateKey(final PrivateKey privateKey, final String encryptionAlgorithm, final String password) throws GeneralSecurityException, IOException {
        final SecretKey secretKey = SecretKeyFactory.getInstance(encryptionAlgorithm).generateSecret(new PBEKeySpec(password.toCharArray()));
        final Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        final byte[] encrypted = cipher.doFinal(privateKey.getEncoded());

        final EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = new EncryptedPrivateKeyInfo(getEncryptionParameters(cipher), encrypted);
        return getPem(ENCRYPTED_PRIVATE_KEY_TYPE, encryptedPrivateKeyInfo.getEncoded());
    }

    /**
     * Get encryption parameters with an algorithm name that EncryptedPrivateKeyInfo can resolve to an Object Identifier,
     * which requires PBES2 instead of cipher names such as PBEWithHmacSHA256AndAES_256
     */
    private AlgorithmParameters getEncryptionParameters(final Cipher cipher) throws GeneralSecurityException, IOException {
        final AlgorithmParameters cipherParameters = cipher.getParameters();

        final AlgorithmParameters encryptionParameters;
        if (cipherParameters.getAlgorithm().startsWith(PBES2_CIPHER_PREFIX)) {
            encryptionParameters = AlgorithmParameters.getInstance(PBES2_ALGORITHM);
            encryptionParameters.init(cipherParameters.getEncoded());
        } else {
            encryptionParameters = cipherParameters;
        }

        return encryptionParameters;
    }

    private String getPem(final String type, final byte[] encoded) {
        return PEM_FORMAT.formatted(type, ENCODER.encodeToString(encoded), type);
    }
}
