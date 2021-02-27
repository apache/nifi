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
package org.apche.nifi.pgp.service.standard;

import org.apache.nifi.pgp.service.standard.StandardPGPPublicKeyService;
import org.apache.nifi.pgp.util.PGPFileUtils;
import org.apache.nifi.pgp.util.PGPSecretKeyGenerator;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StandardPGPPublicKeyServiceTest {
    private static final String SERVICE_ID = StandardPGPPublicKeyService.class.getSimpleName();

    private static final String KEY_ENCRYPTION_PASSWORD = UUID.randomUUID().toString();

    private static PGPSecretKey rsaSecretKey;

    private static File keyringFileAscii;

    private static File keyringFileBinary;

    private static String keyAscii;

    private StandardPGPPublicKeyService service;

    private TestRunner runner;

    @BeforeClass
    public static void setKey() throws Exception {
        rsaSecretKey = PGPSecretKeyGenerator.generateRsaSecretKey(KEY_ENCRYPTION_PASSWORD.toCharArray());
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        final byte[] publicKeyEncoded = publicKey.getEncoded();

        keyAscii = PGPFileUtils.getArmored(publicKeyEncoded);
        keyringFileAscii = PGPFileUtils.getKeyFile(keyAscii.getBytes(StandardCharsets.US_ASCII));
        keyringFileBinary = PGPFileUtils.getKeyFile(publicKeyEncoded);
    }

    @Before
    public void setService() {
        service = new StandardPGPPublicKeyService();
        final Processor processor = mock(Processor.class);
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testMissingRequiredProperties() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.assertNotValid(service);
    }

    @Test
    public void testFindPublicKeyBinaryKeyringFile() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPublicKeyService.KEYRING_FILE, keyringFileBinary.getAbsolutePath());
        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPublicKeyFound(rsaSecretKey);
    }

    @Test
    public void testFindPublicKeyAsciiKeyringFile() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPublicKeyService.KEYRING_FILE, keyringFileAscii.getAbsolutePath());
        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPublicKeyFound(rsaSecretKey);
    }

    @Test
    public void testFindPublicKeyAsciiKeyring() throws Exception {
        runner.addControllerService(SERVICE_ID, service);

        runner.setProperty(service, StandardPGPPublicKeyService.KEYRING, keyAscii);
        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPublicKeyFound(rsaSecretKey);
    }

    @Test
    public void testFindPublicKeyBinaryKeyringFileAndAsciiKeyring() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPublicKeyService.KEYRING_FILE, keyringFileBinary.getAbsolutePath());

        final PGPSecretKey secretKey = PGPSecretKeyGenerator.generateRsaSecretKey(KEY_ENCRYPTION_PASSWORD.toCharArray());
        final PGPPublicKey publicKey = secretKey.getPublicKey();
        final String publicKeyArmored = PGPFileUtils.getArmored(publicKey.getEncoded());
        runner.setProperty(service, StandardPGPPublicKeyService.KEYRING, publicKeyArmored);

        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPublicKeyFound(rsaSecretKey);
        assertPublicKeyFound(secretKey);
    }

    private void assertPublicKeyFound(final PGPSecretKey secretKey) {
        final long keyIdentifier = secretKey.getKeyID();
        final String publicKeySearch = Long.toHexString(keyIdentifier).toUpperCase();
        final Optional<PGPPublicKey> optionalPublicKey = service.findPublicKey(publicKeySearch);
        assertTrue(optionalPublicKey.isPresent());
        final PGPPublicKey publicKey = optionalPublicKey.get();
        assertEquals(keyIdentifier, publicKey.getKeyID());
    }
}
