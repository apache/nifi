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
package org.apache.nifi.pgp.controllerservices;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.pgp.controllerservices.PGPKeyMaterialControllerService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.pgp.PGPPublicKeys;
import org.apache.nifi.security.pgp.PGPSecretKeys;
import org.apache.nifi.security.pgp.StandardPGPOperator;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


@Tags({"integration"})
public class PGPKeyMaterialControllerServiceTest {
    private static final String SERVICE_ID = "pgp-key-service";
    private static final String CORRECT_PASSWORD = "password";
    private static final String INCORRECT_PASSWORD = "incorrect";

    static final List<String> userIDs = new ArrayList<>(Arrays.asList(
            "rsa-encrypter",  // NB:  the first two elements in this list are
            "rsa-passphrase", //      the two keys exported explicitly by name
            "dsa-signer",
            "dsa-passphrase",
            "dsa-signer-elg-encrypter",
            "dsa-signer-elg-encrypter-passphrase",
            "ed25519-signer-cv25519-encrypter",
            "ed25519-signer-cv25519-encrypter-passphrase"
    ));
    static final String validUserId = userIDs.get(1);
    static final String invalidUserId = validUserId + "anything else like this makes it invalid";

    public static TestKey publicKey;
    public static TestKey publicKeyRing;
    public static TestKey secretKey;
    public static TestKey secretKeyRing;

    private static StandardPGPOperator operator;
    private PGPKeyMaterialControllerService service;

    @Before
    public void setupService() {
        service = new PGPKeyMaterialControllerService();
    }

    /**
     * Refer to the gen-keys.sh script and the accompanying README for background on the static test key generation.
     *
     * Note that for the assertions within this setup, we're not comparing key list sizes to the user id list size because
     * exported keys aren't 1:1.
     */
    @BeforeClass
    public static void checkPGPKeyBaseline() throws IOException {
        operator = new StandardPGPOperator();

        // These are the known-good public keyring files created by the script:
        PGPPublicKeys publicKeyRingAsc = operator.readPublicKeys(resourceStream("many-public-keys.asc"));
        PGPPublicKeys publicKeyRingBin = operator.readPublicKeys(resourceStream("many-public-keys.bin"));

        // This shows that each of the exported ascii keys is also in the exported binary keys:
        Assert.assertFalse(publicKeyRingAsc.isEmpty());
        Assert.assertEquals(publicKeyRingAsc.size(), publicKeyRingBin.size());
        for (PGPPublicKey publicKeyAsc : publicKeyRingAsc.values()) {
            PGPPublicKey publicKeyBin = publicKeyRingAsc.getPublicKey(publicKeyAsc.getKeyID());
            Assert.assertNotNull(publicKeyBin);
        }

        // These are the known-good secret keyring files, created by the script:
        PGPSecretKeys secretKeyRingAsc = operator.readSecretKeys(resourceStream("many-secret-keys.asc"));
        PGPSecretKeys secretKeyRingBin = operator.readSecretKeys(resourceStream("many-secret-keys.bin"));

        // This shows that each of the exported ascii keys is also in the exported binary keys:
        Assert.assertFalse(secretKeyRingAsc.isEmpty());
        Assert.assertEquals(secretKeyRingAsc.size(), secretKeyRingBin.size());
        for (PGPSecretKey secretKeyAsc : secretKeyRingAsc.values()) {
            PGPSecretKey secretKeyBin = secretKeyRingBin.getSecretKey(secretKeyAsc.getKeyID());
            Assert.assertNotNull(secretKeyBin);
        }

        // This shows that the key material provider can select each of the known-good user ids from the public and private key rings.
        for (String userID : userIDs) {
            Assert.assertNotNull(publicKeyRingAsc.getPublicKey(userID));
            Assert.assertNotNull(publicKeyRingBin.getPublicKey(userID));

            Assert.assertNotNull(secretKeyRingAsc.getSecretKey(userID));
            Assert.assertNotNull(secretKeyRingBin.getSecretKey(userID));
        }

        // This shows that the operator has loaded our binary and ascii public key files, and that those files contain
        // the same public keys.
        PGPPublicKeys publicKeyAsc = operator.readPublicKeys(resourceStream("one-public-key.asc"));
        PGPPublicKeys publicKeyBin = operator.readPublicKeys(resourceStream("one-public-key.bin"));
        Assert.assertFalse(publicKeyAsc.isEmpty());
        Assert.assertEquals(publicKeyAsc.size(), publicKeyBin.size());
        Assert.assertNotNull(publicKeyAsc.getPublicKey(userIDs.get(1)));
        Assert.assertNotNull(publicKeyBin.getPublicKey(userIDs.get(1)));
        PGPPublicKey publicKeyInnerAsc = publicKeyAsc.values().iterator().next();
        PGPPublicKey publicKeyInnerBin = publicKeyBin.values().iterator().next();
        Assert.assertNotNull(publicKeyInnerAsc);
        Assert.assertNotNull(publicKeyInnerBin);

        // This shows that the operator has loaded our binary and ascii secret key files, and that those files contain
        // the same secret keys.
        PGPSecretKeys secretKeyAsc = operator.readSecretKeys(resourceStream("one-secret-key.asc"));
        PGPSecretKeys secretKeyBin = operator.readSecretKeys(resourceStream("one-secret-key.bin"));
        Assert.assertFalse(secretKeyAsc.isEmpty());
        Assert.assertEquals(secretKeyAsc.size(), secretKeyBin.size());
        Assert.assertNotNull(secretKeyAsc.getSecretKey(userIDs.get(1)));
        Assert.assertNotNull(secretKeyBin.getSecretKey(userIDs.get(1)));
        PGPSecretKey secretKeyInnerAsc = secretKeyAsc.values().iterator().next();
        PGPSecretKey secretKeyInnerBin = secretKeyBin.values().iterator().next();
        Assert.assertNotNull(secretKeyInnerAsc);
        Assert.assertNotNull(secretKeyInnerBin);

        // This shows that the inner, exported ascii keys are the same as the inner, exported binary keys:
        Assert.assertEquals(publicKeyInnerAsc.getKeyID(), publicKeyInnerBin.getKeyID());
        Assert.assertEquals(secretKeyInnerAsc.getKeyID(), secretKeyInnerBin.getKeyID());
    }

    // Distinct from the key resource verification, this loads the keys and key rings test resources.
    @BeforeClass
    public static void setupKeyAndKeyRings() throws IOException {
        publicKey = new TestKey("one-public-key.asc","one-public-key.bin");
        publicKeyRing = new TestKey("many-public-keys.asc", "many-public-keys.bin");

        secretKey = new TestKey("one-secret-key.asc", "one-secret-key.bin", CORRECT_PASSWORD);
        secretKeyRing = new TestKey("many-secret-keys.asc","many-secret-keys.bin", CORRECT_PASSWORD);
    }


    class MockProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        }
    }

    @Test
    public void testControllerConfiguredWithoutProperties() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(new MockProcessor());

        // This shows the service is invalid when not configured.
        runner.addControllerService(SERVICE_ID, service);
        runner.assertNotValid(service);
    }


    @Test
    public void testControllerConfiguredWithPublicKeyText() throws InitializationException {
        final PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();
        final TestRunner runner = TestRunners.newTestRunner(new MockProcessor());

        // This shows the service is valid when given a public key.
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKey.getKeyText());
        }});
        runner.assertValid(service);

        // This shows the service is valid valid for encryption with public key text and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKey.getKeyText());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), validUserId);
        }});
        runner.assertValid(service);

        // This shows the service is invalid for encryption with public key text and incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKey.getKeyText());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), validUserId + "invalid");
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for encryption with public keyring text and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKeyRing.getKeyText());
        }});
        runner.assertNotValid(service);

        // This shows the service is valid for encryption with public keyring text and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKeyRing.getKeyText());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), validUserId);
        }});
        runner.assertValid(service);

        // This shows the service is invalid for encryption with public key text and incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKeyRing.getKeyText());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), invalidUserId);
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void testControllerConfiguredForPBE() throws InitializationException {
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();
        TestRunner runner = TestRunners.newTestRunner(new MockProcessor());

        // This shows the service is valid for encryption with PBE password:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PBE_PASSPHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // What passwords are invalid and how are they tested?
    }


    @Test
    public void testControllerConfiguredWithPublicKeyFile() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new MockProcessor());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // This shows the service is valid for encryption with public key file and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_FILE.getName(), publicKey.getKeyFilename());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), validUserId);
        }});
        runner.assertValid(service);

        // This shows the service is valid for encryption with a public key file and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_FILE.getName(), publicKey.getKeyFilename());
        }});
        runner.assertValid(service);

        // This shows the service is invalid for encryption with a public keyring file and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_FILE.getName(), publicKeyRing.getKeyFilename());
        }});
        runner.assertNotValid(service);

        // This shows the service is valid for encryption with a public keyring file and correct user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_FILE.getName(), publicKeyRing.getKeyFilename());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), validUserId);
        }});
        runner.assertValid(service);

        // This shows the service is invalid for encryption with a public key file and an incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_FILE.getName(), publicKeyRing.getKeyFilename());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), invalidUserId);

        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for encryption with a public key file and an incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_FILE.getName(), publicKey.getKeyFilename());
            put(StandardPGPOperator.PUBLIC_KEY_USER_ID.getName(), invalidUserId);

        }});
        runner.assertNotValid(service);
    }


    @Test
    public void testControllerConfiguredWithSecretKeyText() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new MockProcessor());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // This shows the service is invalid for decryption with a secret key and no passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKey.getKeyText());
        }});
        runner.assertNotValid(service);

        // This shows the service is valid for decryption with a secret key and valid passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKey.getKeyText());
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // This shows the service is invalid for decryption with a secret key and incorrect passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKey.getKeyText());
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for decryption when a keyring is used without specifying the user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKeyRing.getKeyText());
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for decryption with a secret keyring and no specific key:
        service = new PGPKeyMaterialControllerService();
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKeyRing.getKeyText());
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for decryption with a secret key and an incorrect passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKeyRing.getKeyText());
            put(StandardPGPOperator.SECRET_KEY_USER_ID.getName(), validUserId);
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void testControllerConfiguredWithSecretKeyFile() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new MockProcessor());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // This shows the service is invalid for decryption with a secret key file and no passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKey.getKeyFilename());
        }});
        runner.assertNotValid(service);

        // This shows the service is valid for decryption with a secret key file and a valid passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKey.getKeyFilename());
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // This shows the service is invalid for decryption with a secret key file and an invalid passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKey.getKeyFilename());
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for decryption with a secret keyring, valid passphrase, but no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKeyRing.getKeyFilename());
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for decryption with a secret keyring, no passphrase, and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKeyRing.getKeyFilename());
        }});
        runner.assertNotValid(service);

        // This shows the service is invalid for decryption with a secret key, valid user, and incorrect passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKeyRing.getKeyFilename());
            put(StandardPGPOperator.SECRET_KEY_USER_ID.getName(), validUserId);
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // This shows the service is valid for decryption with a secret key, valid user, and correct passphrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_FILE.getName(), secretKeyRing.getKeyFilename());
            put(StandardPGPOperator.SECRET_KEY_USER_ID.getName(), validUserId);
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
    }


    private static URL resourceURL(String name) {
        return PGPKeyMaterialControllerServiceTest.class.getResource("/" + PGPKeyMaterialControllerServiceTest.class.getSimpleName() +  "/" + name);
    }


    private static InputStream resourceStream(String name) {
        return PGPKeyMaterialControllerServiceTest.class.getResourceAsStream("/" + PGPKeyMaterialControllerServiceTest.class.getSimpleName() +  "/" + name);
    }


    private static byte[] resourceBytes(String name) throws IOException {
        return IOUtils.toByteArray(resourceStream(name));
    }


    public static class TestKey {
        private final String filename;
        private final String text;
        private final String password;

        public TestKey(String ascPath, String binPath) throws IOException {
            this(ascPath, binPath, null);
        }

        public TestKey(String ascPath, String binPath, String privateKeyPassword) throws IOException {
            this.text = new String(resourceBytes(ascPath));
            this.filename = resourceURL(binPath).getPath();
            this.password = privateKeyPassword;
        }

        public String getKeyFilename() {
            return filename;
        }

        public String getKeyText() {
            return text;
        }

        public String getPrivateKeyPassword() {
            return password;
        }
    }
}
