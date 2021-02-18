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

import org.apache.nifi.pgp.service.standard.StandardPGPPrivateKeyService;
import org.apache.nifi.pgp.util.PGPFileUtils;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.pgp.util.PGPSecretKeyGenerator;

import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPSecretKey;

import org.bouncycastle.openpgp.PGPSecretKeyRing;
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

public class StandardPGPPrivateKeyServiceTest {
    private static final String SERVICE_ID = StandardPGPPrivateKeyService.class.getSimpleName();

    private static final String KEY_ENCRYPTION_PASSWORD = UUID.randomUUID().toString();

    private static PGPSecretKey rsaSecretKey;

    private static File rsaKeyringFileAscii;

    private static File rsaKeyringFileBinary;

    private static String rsaKeyAscii;

    private static PGPSecretKeyRing elGamalKeyring;

    private static String elGamalKeyringAscii;

    private StandardPGPPrivateKeyService service;

    private TestRunner runner;

    @BeforeClass
    public static void setKey() throws Exception {
        rsaSecretKey = PGPSecretKeyGenerator.generateRsaSecretKey(KEY_ENCRYPTION_PASSWORD.toCharArray());

        final byte[] secretKeyEncoded = rsaSecretKey.getEncoded();
        rsaKeyAscii = PGPFileUtils.getArmored(secretKeyEncoded);

        rsaKeyringFileAscii = PGPFileUtils.getKeyFile(rsaKeyAscii.getBytes(StandardCharsets.US_ASCII));
        rsaKeyringFileBinary = PGPFileUtils.getKeyFile(secretKeyEncoded);

        elGamalKeyring = PGPSecretKeyGenerator.generateDsaElGamalSecretKeyRing(KEY_ENCRYPTION_PASSWORD.toCharArray());
        elGamalKeyringAscii = PGPFileUtils.getArmored(elGamalKeyring.getEncoded());
    }

    @Before
    public void setService() {
        service = new StandardPGPPrivateKeyService();
        final Processor processor = mock(Processor.class);
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testMissingRequiredProperties() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.assertNotValid(service);
    }

    @Test
    public void testEnableInvalidPassword() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING_FILE, rsaKeyringFileBinary.getAbsolutePath());
        runner.setProperty(service, StandardPGPPrivateKeyService.KEY_PASSWORD, String.class.getSimpleName());

        runner.assertNotValid(service);
    }

    @Test
    public void testFindPrivateKeyRsaBinaryKeyring() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING_FILE, rsaKeyringFileBinary.getAbsolutePath());
        runner.setProperty(service, StandardPGPPrivateKeyService.KEY_PASSWORD, KEY_ENCRYPTION_PASSWORD);

        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPrivateKeyFound(rsaSecretKey);
    }

    @Test
    public void testFindPrivateKeyRsaAsciiKeyring() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING_FILE, rsaKeyringFileAscii.getAbsolutePath());
        runner.setProperty(service, StandardPGPPrivateKeyService.KEY_PASSWORD, KEY_ENCRYPTION_PASSWORD);

        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPrivateKeyFound(rsaSecretKey);
    }

    @Test
    public void testFindPrivateKeyRsaAsciiKey() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING, rsaKeyAscii);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEY_PASSWORD, KEY_ENCRYPTION_PASSWORD);

        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPrivateKeyFound(rsaSecretKey);
    }

    @Test
    public void testFindPrivateKeyElGamalAsciiKeyring() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING, elGamalKeyringAscii);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEY_PASSWORD, KEY_ENCRYPTION_PASSWORD);

        runner.assertValid(service);
        runner.enableControllerService(service);

        for (final PGPSecretKey secretKey : elGamalKeyring) {
            assertPrivateKeyFound(secretKey);
        }
    }

    @Test
    public void testFindPrivateKeyRsaBinaryKeyringFileAndElGamalAsciiKeyring() throws Exception {
        runner.addControllerService(SERVICE_ID, service);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING_FILE, rsaKeyringFileBinary.getAbsolutePath());
        runner.setProperty(service, StandardPGPPrivateKeyService.KEYRING, elGamalKeyringAscii);
        runner.setProperty(service, StandardPGPPrivateKeyService.KEY_PASSWORD, KEY_ENCRYPTION_PASSWORD);

        runner.assertValid(service);
        runner.enableControllerService(service);

        assertPrivateKeyFound(rsaSecretKey);

        for (final PGPSecretKey secretKey : elGamalKeyring) {
            assertPrivateKeyFound(secretKey);
        }
    }

    private void assertPrivateKeyFound(final PGPSecretKey pgpSecretKey) {
        final long keyIdentifier = pgpSecretKey.getKeyID();
        final Optional<PGPPrivateKey> optionalPrivateKey = service.findPrivateKey(keyIdentifier);
        assertTrue(optionalPrivateKey.isPresent());
        final PGPPrivateKey privateKey = optionalPrivateKey.get();
        assertEquals(keyIdentifier, privateKey.getKeyID());
    }
}
