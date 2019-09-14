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
package org.apache.nifi.properties.sensitive.keystore;

import org.apache.nifi.properties.sensitive.AbstractSensitivePropertyProviderTest;
import org.apache.nifi.properties.sensitive.ByteArrayKeyStoreProvider;
import org.apache.nifi.properties.sensitive.SensitivePropertyConfigurationException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.apache.nifi.properties.sensitive.CipherUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Tests the Key Store (Wrapped) Sensitive Property Provider.
 *
 * These tests only need file system access to run, and no configuration is required.  These tests also construct their own
 * keys and key stores, no user keys are ever referenced.
 *
 */
public class KeyStoreWrappedSensitivePropertyProviderIT extends AbstractSensitivePropertyProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreWrappedSensitivePropertyProviderIT.class);

    private static SecureRandom random = new SecureRandom();
    private static Map<String, KeyStoreTestCase> testCases = new HashMap<>();

    private static class KeyStoreTestCase {
        String storeType;      // each test case has a KeyStore of this named type
        String storePassword;  // and has a random password
        byte[] storeContents;  // and has content serialized as bytes when built

        String keyAlias;       // each test case also contains a key with this alias
        String keyPassword;    // and that key has a random password, too
    }

    private static final String[] keyAlgos = {"AES"};
    private static final int[] keySizes = {16, 24, 32};

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @BeforeClass
    public static void setUpKeyPair() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Before
    public void setUpTest() throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        final byte[] keyBytes = new byte[16];

        // This builds one test case per key store type, each with unique passwords and shared keys:
        for (String keyStoreType : KeyStoreWrappedSensitivePropertyProvider.KEYSTORE_TYPES) {
            KeyStoreTestCase testCase = new KeyStoreTestCase();
            testCases.put(keyStoreType, testCase);

            testCase.storeType = keyStoreType;
            testCase.storePassword = CipherUtils.getRandomHex(12);

            KeyStore testKeyStore = KeyStore.getInstance(keyStoreType);
            testKeyStore.load(null, null);

            HashMap<String, String> randomKeys = new HashMap<>();
            int randomKeyCount = CipherUtils.getRandomInt(4, 12);

            // Here we loop and create all kinds of keys.  Later we'll pick one at random as the test alias.  We're not
            // after showing that we can create keys, so we just create a few to show we're using one of many:
            for (int i = 0; i < randomKeyCount; i++) {
                random.nextBytes(keyBytes);
                SecretKeySpec randomKey = new SecretKeySpec(
                        Arrays.copyOfRange(keyBytes, 0, keySizes[CipherUtils.getRandomInt(0, keySizes.length)]),
                        keyAlgos[random.nextInt(keyAlgos.length)]);

                String randomAlias = CipherUtils.getRandomHex(8);
                String randomPassword = CipherUtils.getRandomHex(8);
                KeyStore.Entry keyEntry = new KeyStore.SecretKeyEntry(randomKey);

                testKeyStore.setEntry(randomAlias, keyEntry, new KeyStore.PasswordProtection(randomPassword.toCharArray()));
                randomKeys.put(randomAlias, randomPassword);
            }

            // Select one key and password for the test:
            String randomAlias = (String) randomKeys.keySet().toArray()[CipherUtils.getRandomInt(0, randomKeyCount)];
            testCase.keyAlias = randomAlias;
            testCase.keyPassword = randomKeys.get(randomAlias);

            // Save the store to a stream and reference the output bytes for later:
            ByteArrayOutputStream storeOutput = new ByteArrayOutputStream();
            testKeyStore.store(storeOutput, testCase.storePassword.toCharArray());
            testCase.storeContents = storeOutput.toByteArray();

            logger.info("Created key store type {} with {} random keys, total size {} bytes, {} bytes/key",
                    keyStoreType.toUpperCase(),
                    randomKeyCount,
                    testCase.storeContents.length,
                    testCase.storeContents.length / randomKeyCount);
        }
    }

    // These tests show that the KeyStoreWrappedSensitivePropertyProvider loads Key Stores and Keys that have been protected
    // with a password.
    @Test
    public void testStoreLoad() {
        SensitivePropertyProvider spp;
        ByteArrayKeyStoreProvider byteKeyStore;

        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            final KeyStoreTestCase config = entry.getValue();
            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);
            setTestProps(config.storePassword, config.keyPassword);

            // This shows we can load a store as expected, when we supply the correct store + key passwords:
            try {
                spp = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
                Assert.assertNotNull(spp);
            } catch (final SensitivePropertyConfigurationException ignored) {
                Assert.assertNull(ignored);
            }

            // This shows that we fail to load a store when we supply an incorrect store password:
            byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, "unlikely store password");
            boolean failed = false;
            try {
                spp = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
                failed = true;
            } catch (final SensitivePropertyConfigurationException ignored) {
            }
            Assert.assertFalse(failed);

            // This shows that we can load a store successfully and still fail to load a key with an incorrect key password:
            setTestProps(config.storePassword, "unlikely key password");
            failed = false;
            try {
                spp = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
                failed = true;
            } catch (final SensitivePropertyConfigurationException ignored) {
            }
            Assert.assertFalse(failed);

            clearTestProps();
        }
    }

    // These tests show we can create KeyStoreWrappedSensitivePropertyProvider instances (for all known store types) and use
    // them to protect and unprotect values.
    @Test
    public void testProtectUnprotect() {
        long start = System.nanoTime();
        long total = 0;

        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            int tests = CipherUtils.getRandomInt(16, 256);
            total += tests;

            int bytesPlain = 0;
            int bytesCipher = 0;

            final KeyStoreTestCase config = entry.getValue();
            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            final ByteArrayKeyStoreProvider byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);

            setTestProps(config.storePassword, config.keyPassword);
            final SensitivePropertyProvider spp = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);

            for (int i=0; i<tests; i++) {
                int plainSize = CipherUtils.getRandomInt(2, 1024);
                bytesPlain += plainSize;

                String plainText = CipherUtils.getRandomHex(plainSize);
                String cipherText = spp.protect(plainText);
                bytesCipher += cipherText.length();

                Assert.assertNotEquals(plainText, cipherText);
                Assert.assertEquals(plainText, spp.unprotect(cipherText));
            }

            clearTestProps();
            logger.info("Ran {} tests using {} key store, total plaintext size {} bytes, total ciphertext size {} bytes",
                    tests,
                    config.storeType.toUpperCase(),
                    bytesPlain,
                    bytesCipher
                    );
        }

        long finish = System.nanoTime();
        double ms = (finish - start) / 1_000_000.0;
        NumberFormat dbl = new DecimalFormat("#,##0.00");
        logger.info("Total op count {}, total time {}ms, overall {} ops/sec", total, dbl.format(ms), dbl.format((total/ms) * 1000));
    }

    private void clearTestProps() {
        System.clearProperty("keystore.file");
        System.clearProperty("keystore.password");
        System.clearProperty("keystore.key-password");
    }

    private static void setTestProps(String storePass, String keyPassword) {
        System.setProperty("keystore.file", "");
        System.setProperty("keystore.password", storePass);
        System.setProperty("keystore.key-password", keyPassword);

    }

    // These tests show the provider will not accept an invalid key.
    @Test
    public void testShouldThrowExceptionsWithBadKeys() throws Exception {
        try {
            new KeyStoreWrappedSensitivePropertyProvider("");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("The key cannot be empty").matcher(e.getMessage()).matches());
        }

        try {
            new KeyStoreWrappedSensitivePropertyProvider("this is an invalid key and will not work");
        } catch (final SensitivePropertyConfigurationException e) {
            Assert.assertTrue(Pattern.compile("Invalid Key Store key").matcher(e.getMessage()).matches());
        }
    }

    // These tests show the provider can encrypt and decrypt values as expected.
    @Test
    public void testProtectAndUnprotect() {
        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            final KeyStoreTestCase config = entry.getValue();
            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            final ByteArrayKeyStoreProvider byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);
            setTestProps(config.storePassword, config.keyPassword);

            SensitivePropertyProvider sensitivePropertyProvider = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
            int plainSize = CipherUtils.getRandomInt(32, 256);
            checkProviderCanProtectAndUnprotectValue(sensitivePropertyProvider, plainSize);
            logger.info("GCP SPP protected and unprotected string of " + plainSize + " bytes using material: " + clientMaterial);
        }
    }


     // These tests show that the provider cannot encrypt empty values.
    @Test
    public void testShouldHandleProtectEmptyValue() throws Exception {
        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            final KeyStoreTestCase config = entry.getValue();
            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            final ByteArrayKeyStoreProvider byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);
            setTestProps(config.storePassword, config.keyPassword);

            SensitivePropertyProvider sensitivePropertyProvider = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
            checkProviderProtectDoesNotAllowBlankValues(sensitivePropertyProvider);
        }
    }

     // These tests show that the provider cannot decrypt invalid ciphertext.
    @Test
    public void testProviderUnprotectWithBadValues() throws Exception {
        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            final KeyStoreTestCase config = entry.getValue();
            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            final ByteArrayKeyStoreProvider byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);
            setTestProps(config.storePassword, config.keyPassword);

            SensitivePropertyProvider sensitivePropertyProvider = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
            checkProviderUnprotectDoesNotAllowInvalidBase64Values(sensitivePropertyProvider);
        }
    }

     // These tests show that the provider cannot decrypt text encoded but not encrypted.
    @Test
    public void testShouldThrowExceptionWithValidBase64EncodedTextInvalidCipherText() throws Exception {
        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            final KeyStoreTestCase config = entry.getValue();
            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            final ByteArrayKeyStoreProvider byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);
            setTestProps(config.storePassword, config.keyPassword);

            SensitivePropertyProvider sensitivePropertyProvider = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
            checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(sensitivePropertyProvider);
        }
    }

    // These tests show we can use the provider to encrypt/decrypt property values.
    @Test
    public void testShouldProtectAndUnprotectProperties() throws Exception {
        for (final Map.Entry<String, KeyStoreTestCase> entry : testCases.entrySet()) {
            final KeyStoreTestCase config = entry.getValue();

            File tmp = tmpDir.newFile();
            tmp.deleteOnExit();
            OutputStream fos = new FileOutputStream(tmp);
            fos.write(config.storeContents);
            fos.close();

            final String clientMaterial = KeyStoreWrappedSensitivePropertyProvider.formatForType(config.storeType, config.keyAlias);
            final ByteArrayKeyStoreProvider byteKeyStore = new ByteArrayKeyStoreProvider(config.storeContents, config.storeType, config.storePassword);

            setTestProps(config.storePassword, config.keyPassword);
            System.setProperty("keystore.file", tmp.getAbsolutePath());

            final SensitivePropertyProvider sensitivePropertyProvider = new KeyStoreWrappedSensitivePropertyProvider(clientMaterial, byteKeyStore, null);
            checkProviderCanProtectAndUnprotectProperties(sensitivePropertyProvider);
        }
    }
}