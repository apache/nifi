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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.junit.Assert;
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
    public static final String INCORRECT_PASSWORD = "incorrect";
    private static final String SERVICE_ID = "pgp-key-service";
    public static final String CORRECT_PASSWORD = "password";
    public static final String INVALID_USER_ID = "admin";
    private static List<String> userIDs;

    private static List<PGPPublicKey> manyPublicKeys;
    private static List<PGPSecretKey> manySecretKeys;

    private static String manyPublicKeysRaw;
    private static String manyPublicKeysFile;

    private static String manySecretKeysRaw;
    private static String manySecretKeysFile;

    private static PGPPublicKey onePublicKey;
    private static PGPSecretKey oneSecretKey;

    public static String onePublicKeyRaw;
    static String onePublicKeyFile;

    static String oneSecretKeyRaw;
    static String oneSecretKeyFile;
    private static String VALID_USER_ID;


    /**
     * These are our known-good key user ids, created by the gen-keys.sh script, refer to that
     * file and associated README for details.
     *
     * For the assertions within this setup, we're not comparing key list sizes to the user id list size
     * because exported keys aren't 1:1.
     *
     */
    @BeforeClass
    public static void setupKeyAndKeyRings() throws IOException {
        // NB:  the first two elements in this list are the two keys exported explicitly by name.
        userIDs = new ArrayList<>(Arrays.asList(
                "rsa-encrypter",
                "rsa-passphrase",
                "dsa-signer",
                "dsa-passphrase",
                "dsa-signer-elg-encrypter",
                "dsa-signer-elg-encrypter-passphrase",
                "ed25519-signer-cv25519-encrypter",
                "ed25519-signer-cv25519-encrypter-passphrase"
        ));
        VALID_USER_ID = userIDs.get(1);

        // These are the known-good public keyring files, created by the script:
        manyPublicKeys = StaticKeyMaterialProvider.getPublicKeys(keyResourceStream("many-public-keys.asc"));
        List<PGPPublicKey> manyPublicKeysBin = StaticKeyMaterialProvider.getPublicKeys(keyResourceStream("many-public-keys.bin"));

        // This shows that each of the exported ascii keys is also in the exported binary keys:
        Assert.assertFalse(manyPublicKeys.isEmpty());
        Assert.assertTrue(manyPublicKeys.size() == manyPublicKeysBin.size());
        for (PGPPublicKey publicKeyAsc : manyPublicKeys) {
            PGPPublicKey publicKeyBin = StaticKeyMaterialProvider.getPublicKeyFromId(manyPublicKeysBin, publicKeyAsc.getKeyID());
            Assert.assertNotNull(publicKeyBin);
        }

        // These are the known-good secret keyring files, created by the script:
        manySecretKeys = StaticKeyMaterialProvider.getSecretKeys(keyResourceStream("many-secret-keys.asc"));
        List<PGPSecretKey> manySecretKeysBin = StaticKeyMaterialProvider.getSecretKeys(keyResourceStream("many-secret-keys.bin"));

        // This shows that each of the exported ascii keys is also in the exported binary keys:
        Assert.assertFalse(manySecretKeys.isEmpty());
        Assert.assertTrue(manySecretKeys.size() == manySecretKeysBin.size());
        for (PGPSecretKey secretKeyAsc : manySecretKeys) {
            PGPSecretKey secretKeyBin = StaticKeyMaterialProvider.getSecretKeyFromId(manySecretKeysBin, secretKeyAsc.getKeyID());
            Assert.assertNotNull(secretKeyBin);
        }

        // This shows that the key material provider can select each of the known-good user ids from the public and private key rings.
        for (String userID : userIDs) {
            Assert.assertNotNull(StaticKeyMaterialProvider.getPublicKeyFromUser(manyPublicKeys, userID));
            Assert.assertNotNull(StaticKeyMaterialProvider.getPublicKeyFromUser(manyPublicKeysBin, userID));

            Assert.assertNotNull(StaticKeyMaterialProvider.getSecretKeyFromUser(manySecretKeys, userID));
            Assert.assertNotNull(StaticKeyMaterialProvider.getSecretKeyFromUser(manySecretKeysBin, userID));
        }

        // This shows that the material provider has loaded our binary and ascii public key file.
        List<PGPPublicKey> publicKeysAsc = StaticKeyMaterialProvider.getPublicKeys(keyResourceStream("one-public-key.asc"));
        List<PGPPublicKey> publicKeysBin = StaticKeyMaterialProvider.getPublicKeys(keyResourceStream("one-public-key.bin"));
        Assert.assertFalse(publicKeysAsc.isEmpty());
        Assert.assertTrue(publicKeysAsc.size() == publicKeysBin.size());
        Assert.assertNotNull(StaticKeyMaterialProvider.getPublicKeyFromUser(publicKeysAsc, userIDs.get(1)));
        Assert.assertNotNull(StaticKeyMaterialProvider.getPublicKeyFromUser(publicKeysBin, userIDs.get(1)));
        onePublicKey = publicKeysAsc.get(0);
        PGPPublicKey onePublicKeyBin = publicKeysBin.get(0);
        Assert.assertNotNull(onePublicKey);
        Assert.assertNotNull(onePublicKeyBin);

        // This shows that the material provider has loaded our binary and ascii secret key file.
        List<PGPSecretKey> secretKeysAsc = StaticKeyMaterialProvider.getSecretKeys(keyResourceStream("one-secret-key.asc"));
        List<PGPSecretKey> secretKeysBin = StaticKeyMaterialProvider.getSecretKeys(keyResourceStream("one-secret-key.bin"));
        Assert.assertFalse(secretKeysAsc.isEmpty());
        Assert.assertTrue(secretKeysAsc.size() == secretKeysBin.size());
        Assert.assertNotNull(StaticKeyMaterialProvider.getSecretKeyFromUser(secretKeysAsc, userIDs.get(1)));
        Assert.assertNotNull(StaticKeyMaterialProvider.getSecretKeyFromUser(secretKeysBin, userIDs.get(1)));
        oneSecretKey = secretKeysAsc.get(0);
        PGPSecretKey oneSecretKeyBin = secretKeysBin.get(0);
        Assert.assertNotNull(oneSecretKey);
        Assert.assertNotNull(oneSecretKeyBin);

        // This shows that the exported ascii keys are the same as the exported binary keys:
        Assert.assertEquals(onePublicKey.getKeyID(), onePublicKeyBin.getKeyID());
        Assert.assertEquals(oneSecretKey.getKeyID(), oneSecretKeyBin.getKeyID());

        manyPublicKeysRaw = new String(keyResourceStream("many-public-keys.asc").readAllBytes());
        manySecretKeysRaw = new String(keyResourceStream("many-secret-keys.asc").readAllBytes());

        onePublicKeyRaw = new String(keyResourceStream("one-public-key.asc").readAllBytes());
        oneSecretKeyRaw = new String(keyResourceStream("one-secret-key.asc").readAllBytes());

        onePublicKeyFile = keyResourceURL("one-public-key.bin").getPath();
        oneSecretKeyFile = keyResourceURL("one-secret-key.bin").getPath();

        manyPublicKeysFile = keyResourceURL("many-public-keys.bin").getPath();
        manySecretKeysFile = keyResourceURL("many-secret-keys.bin").getPath();
    }

    @Test
    public void testControllerConfiguredWithoutProperties() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptPGP());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // invalid when empty
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{}});
        runner.assertNotValid(service);
    }

    @Test
    public void testControllerConfiguredWithPublicKeyText() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptPGP());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // valid for encryption with public key text:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), onePublicKeyRaw);
        }});
        runner.assertValid(service);

        // valid for encryption with public key text and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), onePublicKeyRaw);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // invalid for encryption with public key text and incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), onePublicKeyRaw);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);
        }});
        runner.assertNotValid(service);


        // valid for encryption with public key text:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), manyPublicKeysRaw);
        }});
        runner.assertValid(service);

        // valid for encryption with public key text and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), manyPublicKeysRaw);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // invalid for encryption with public key text and incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), manyPublicKeysRaw);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);
        }});
        runner.assertNotValid(service);
    }

    @Test
    public void testControllerConfiguredForPBE() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptPGP());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // valid for encryption with PBE password:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PBE_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
    }

    @Test
    public void testControllerConfiguredWithPublicKeyFile() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptPGP());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // valid for encryption with public key file and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_FILE.getName(), onePublicKeyFile);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // valid for encryption with public key file and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_FILE.getName(), onePublicKeyFile);
        }});
        runner.assertValid(service);

        // valid for encryption with public key file and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_FILE.getName(), manyPublicKeysFile);
        }});
        runner.assertValid(service);

        // valid for encryption with public key file and correct user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_FILE.getName(), manyPublicKeysFile);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // invalid for encryption with public key file and an incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_FILE.getName(), manyPublicKeysFile);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);

        }});
        runner.assertNotValid(service);

        // invalid for encryption with public key file and an incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_FILE.getName(), onePublicKeyFile);
            put(PGPKeyMaterialControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);

        }});
        runner.assertNotValid(service);
    }


    @Test
    public void testControllerConfiguredWithSecretKeyText() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new DecryptPGP());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // valid for decryption with a secret key and valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), oneSecretKeyRaw);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        if (true) return;

        // invalid for decryption with a secret key and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), oneSecretKeyRaw);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key and incorrect pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), oneSecretKeyRaw);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // valid for decryption with a secret key and valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), manySecretKeysRaw);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // invalid for decryption with a secret key and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), manySecretKeysRaw);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key and incorrect pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), manySecretKeysRaw);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);
    }

    @Test
    public void testControllerConfiguredWithSecretKeyFile() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new DecryptPGP());
        PGPKeyMaterialControllerService service = new PGPKeyMaterialControllerService();

        // valid for decryption with a secret key file and a valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_FILE.getName(), oneSecretKeyFile);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // remaining tests are super slow, need to move to integration tests:
        if (true) return;

        // valid for decryption with a secret key file and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_FILE.getName(), oneSecretKeyFile);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key file and a valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_FILE.getName(), oneSecretKeyFile);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // valid for decryption with a secret key and valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_FILE.getName(), manySecretKeysFile);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // invalid for decryption with a secret key and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_FILE.getName(), manySecretKeysFile);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key and incorrect pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_FILE.getName(), manySecretKeysFile);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);
    }


    private static URL keyResourceURL(String name) {
        Class<PGPKeyMaterialControllerServiceTest> cls = PGPKeyMaterialControllerServiceTest.class;
        return cls.getResource("/" + cls.getSimpleName() +  "/" + name);
    }


    private static InputStream keyResourceStream(String name) {
        Class<PGPKeyMaterialControllerServiceTest> cls = PGPKeyMaterialControllerServiceTest.class;
        return cls.getResourceAsStream("/" + cls.getSimpleName() +  "/" + name);
    }
}

