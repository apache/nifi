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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@Tags({"integration"})
public class PGPControllerServiceTest {
    public static final String INCORRECT_PASSWORD = "incorrect";
    private static final String SERVICE_ID = "pgp-key-service";
    public static final String CORRECT_PASSWORD = "password";
    public static final String INVALID_USER_ID = "admin";
    private static List<String> userIDs;

    private static PGPControllerService.PGPPublicKeys manyPublicKeys;
    private static PGPControllerService.PGPSecretKeys manySecretKeys;

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
        manyPublicKeys = PGPControllerService.readPublicKeys(keyResourceStream("many-public-keys.asc"));
        PGPControllerService.PGPPublicKeys manyPublicKeysBin = PGPControllerService.readPublicKeys(keyResourceStream("many-public-keys.bin"));

        // This shows that each of the exported ascii keys is also in the exported binary keys:
        Assert.assertFalse(manyPublicKeys.isEmpty());
        Assert.assertTrue(manyPublicKeys.size() == manyPublicKeysBin.size());
        for (PGPPublicKey publicKeyAsc : manyPublicKeys) {
            PGPPublicKey publicKeyBin = manyPublicKeys.getPublicKey(publicKeyAsc.getKeyID());
            Assert.assertNotNull(publicKeyBin);
        }

        // These are the known-good secret keyring files, created by the script:
        manySecretKeys = PGPControllerService.readSecretKeys(keyResourceStream("many-secret-keys.asc"));
        PGPControllerService.PGPSecretKeys manySecretKeysBin = PGPControllerService.readSecretKeys(keyResourceStream("many-secret-keys.bin"));

        // This shows that each of the exported ascii keys is also in the exported binary keys:
        Assert.assertFalse(manySecretKeys.isEmpty());
        Assert.assertTrue(manySecretKeys.size() == manySecretKeysBin.size());
        for (PGPSecretKey secretKeyAsc : manySecretKeys) {
            PGPSecretKey secretKeyBin = manySecretKeysBin.getSecretKey(secretKeyAsc.getKeyID());
            Assert.assertNotNull(secretKeyBin);
        }

        // This shows that the key material provider can select each of the known-good user ids from the public and private key rings.
        for (String userID : userIDs) {
            Assert.assertNotNull(manyPublicKeys.getPublicKey(userID));
            Assert.assertNotNull(manyPublicKeysBin.getPublicKey(userID));

            Assert.assertNotNull(manySecretKeys.getSecretKey(userID));
            Assert.assertNotNull(manySecretKeysBin.getSecretKey(userID));
        }

        // This shows that the material provider has loaded our binary and ascii public key file.
        PGPControllerService.PGPPublicKeys publicKeysAsc = PGPControllerService.readPublicKeys(keyResourceStream("one-public-key.asc"));
        PGPControllerService.PGPPublicKeys publicKeysBin = PGPControllerService.readPublicKeys(keyResourceStream("one-public-key.bin"));
        Assert.assertFalse(publicKeysAsc.isEmpty());
        Assert.assertTrue(publicKeysAsc.size() == publicKeysBin.size());
        Assert.assertNotNull(publicKeysAsc.getPublicKey(userIDs.get(1)));
        Assert.assertNotNull(publicKeysBin.getPublicKey(userIDs.get(1)));
        onePublicKey = publicKeysAsc.get(0);
        PGPPublicKey onePublicKeyBin = publicKeysBin.get(0);
        Assert.assertNotNull(onePublicKey);
        Assert.assertNotNull(onePublicKeyBin);

        // This shows that the material provider has loaded our binary and ascii secret key file.
        PGPControllerService.PGPSecretKeys secretKeysAsc = PGPControllerService.readSecretKeys(keyResourceStream("one-secret-key.asc"));
        PGPControllerService.PGPSecretKeys secretKeysBin = PGPControllerService.readSecretKeys(keyResourceStream("one-secret-key.bin"));
        Assert.assertFalse(secretKeysAsc.isEmpty());
        Assert.assertTrue(secretKeysAsc.size() == secretKeysBin.size());
        Assert.assertNotNull(secretKeysAsc.getSecretKey(userIDs.get(1)));
        Assert.assertNotNull(secretKeysBin.getSecretKey(userIDs.get(1)));
        oneSecretKey = secretKeysAsc.get(0);
        PGPSecretKey oneSecretKeyBin = secretKeysBin.get(0);
        Assert.assertNotNull(oneSecretKey);
        Assert.assertNotNull(oneSecretKeyBin);

        // This shows that the exported ascii keys are the same as the exported binary keys:
        Assert.assertEquals(onePublicKey.getKeyID(), onePublicKeyBin.getKeyID());
        Assert.assertEquals(oneSecretKey.getKeyID(), oneSecretKeyBin.getKeyID());

        manyPublicKeysRaw = new String(keyResourceBytes("many-public-keys.asc"));
        manySecretKeysRaw = new String(keyResourceBytes("many-secret-keys.asc"));

        onePublicKeyRaw = new String(keyResourceBytes("one-public-key.asc"));
        oneSecretKeyRaw = new String(keyResourceBytes("one-secret-key.asc"));

        onePublicKeyFile = keyResourceURL("one-public-key.bin").getPath();
        oneSecretKeyFile = keyResourceURL("one-secret-key.bin").getPath();

        manyPublicKeysFile = keyResourceURL("many-public-keys.bin").getPath();
        manySecretKeysFile = keyResourceURL("many-secret-keys.bin").getPath();
    }

    @Test
    public void testControllerConfiguredWithoutProperties() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptContentPGP());
        PGPControllerService service = new PGPControllerService();

        // invalid when empty
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{}});
        runner.assertNotValid(service);
    }

    @Test
    public void testControllerConfiguredWithPublicKeyText() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptContentPGP());
        PGPControllerService service = new PGPControllerService();

        // valid for encryption with public key text:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), onePublicKeyRaw);
        }});
        runner.assertValid(service);

        // valid for encryption with public key text and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), onePublicKeyRaw);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // invalid for encryption with public key text and incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), onePublicKeyRaw);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);
        }});
        runner.assertNotValid(service);


        // valid for encryption with public key text:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), manyPublicKeysRaw);
        }});
        runner.assertValid(service);

        // valid for encryption with public key text and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), manyPublicKeysRaw);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // invalid for encryption with public key text and incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), manyPublicKeysRaw);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);
        }});
        runner.assertNotValid(service);
    }

    @Test
    public void testControllerConfiguredForPBE() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptContentPGP());
        PGPControllerService service = new PGPControllerService();

        // valid for encryption with PBE password:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PBE_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
    }

    @Test
    public void testControllerConfiguredWithPublicKeyFile() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new EncryptContentPGP());
        PGPControllerService service = new PGPControllerService();

        // valid for encryption with public key file and user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_FILE.getName(), onePublicKeyFile);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // valid for encryption with public key file and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_FILE.getName(), onePublicKeyFile);
        }});
        runner.assertValid(service);

        // valid for encryption with public key file and no user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_FILE.getName(), manyPublicKeysFile);
        }});
        runner.assertValid(service);

        // valid for encryption with public key file and correct user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_FILE.getName(), manyPublicKeysFile);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), VALID_USER_ID);
        }});
        runner.assertValid(service);

        // invalid for encryption with public key file and an incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_FILE.getName(), manyPublicKeysFile);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);

        }});
        runner.assertNotValid(service);

        // invalid for encryption with public key file and an incorrect user id:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_FILE.getName(), onePublicKeyFile);
            put(PGPControllerService.PUBLIC_KEY_USER_ID.getName(), INVALID_USER_ID);

        }});
        runner.assertNotValid(service);
    }


    @Test
    public void testControllerConfiguredWithSecretKeyText() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new DecryptContentPGP());
        PGPControllerService service = new PGPControllerService();

        // valid for decryption with a secret key and valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), oneSecretKeyRaw);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        if (true) return;

        // invalid for decryption with a secret key and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), oneSecretKeyRaw);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key and incorrect pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), oneSecretKeyRaw);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // valid for decryption with a secret key and valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), manySecretKeysRaw);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // invalid for decryption with a secret key and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), manySecretKeysRaw);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key and incorrect pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), manySecretKeysRaw);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);
    }

    @Test
    public void testControllerConfiguredWithSecretKeyFile() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new DecryptContentPGP());
        PGPControllerService service = new PGPControllerService();

        // valid for decryption with a secret key file and a valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_FILE.getName(), oneSecretKeyFile);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // valid for decryption with a secret key file and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_FILE.getName(), oneSecretKeyFile);
        }});
        runner.assertNotValid(service);

        // invalid for decryption with a secret key file and a valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_FILE.getName(), oneSecretKeyFile);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertNotValid(service);

        // valid for decryption with a secret key and valid pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_FILE.getName(), manySecretKeysFile);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), CORRECT_PASSWORD);
        }});
        runner.assertValid(service);

        // valid for decryption with a secret key and no pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_FILE.getName(), manySecretKeysFile);
        }});
        runner.assertValid(service);

        // valid for decryption with a secret key and incorrect pass-phrase:
        runner.addControllerService(SERVICE_ID, service, new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_FILE.getName(), manySecretKeysFile);
            put(PGPControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), INCORRECT_PASSWORD);
        }});
        runner.assertValid(service);
    }


    @Ignore
    @Test
    public void testSomeBasicEncryptAndDecrypt() throws InitializationException {
        String algo = "7";

        // Basic encryption via an encryption processor configured with a key material service:
        TestRunner runner = TestRunners.newTestRunner(new EncryptContentPGP());
        runner.setProperty(EncryptContentPGP.ENCRYPT_ALGORITHM, algo);
        runner.setProperty(EncryptContentPGP.PGP_KEY_SERVICE, SERVICE_ID);

        PGPControllerService service = new PGPControllerService();
        runner.addControllerService(SERVICE_ID, service);
        //runner.setProperty(service, PGPControllerService.PUBLIC_KEYRING_TEXT, tempPubKey);
        runner.enableControllerService(service);

        String plainText = "hello, pgp encrypt.";
        byte[] plainBytes = plainText.getBytes(StandardCharsets.UTF_8);
        MockFlowFile flow = runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContentPGP.REL_SUCCESS, 1);

        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptContentPGP.REL_SUCCESS);
        Assert.assertEquals(1, flows.size());
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // decrypt via a decryption processor configured with a new key material service:
        runner = TestRunners.newTestRunner(new DecryptContentPGP());
        runner.setProperty(DecryptContentPGP.PGP_KEY_SERVICE, SERVICE_ID);

        service = new PGPControllerService();
        runner.addControllerService(SERVICE_ID, service);
        //runner.setProperty(service, PGPControllerService.SECRET_KEYRING_TEXT, tempPrivKey);
        runner.enableControllerService(service);

        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptContentPGP.REL_SUCCESS, 1);

        flows = runner.getFlowFilesForRelationship(DecryptContentPGP.REL_SUCCESS);
        Assert.assertEquals(1, flows.size());
        Assert.assertArrayEquals(flows.get(0).toByteArray(), plainBytes);


        // Basic signing via a signing processor configured with a key material service:
        runner = TestRunners.newTestRunner(new SignContentAttributePGP());
        runner.setProperty(SignContentAttributePGP.SIGNATURE_HASH_ALGORITHM, "8"); // sha256
        runner.setProperty(SignContentAttributePGP.PGP_KEY_SERVICE, SERVICE_ID);

        service = new PGPControllerService();
        runner.addControllerService(SERVICE_ID, service);
        //runner.setProperty(service, PGPControllerService.SECRET_KEYRING_TEXT, tempPrivKey);
        runner.enableControllerService(service);

        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(SignContentAttributePGP.REL_SUCCESS, 1);

        flows = runner.getFlowFilesForRelationship(SignContentAttributePGP.REL_SUCCESS);
        Assert.assertEquals(1, flows.size());

        flow = flows.get(0);
        String sigValue = flow.getAttribute(AbstractProcessorPGP.DEFAULT_SIGNATURE_ATTRIBUTE);
        Assert.assertNotNull(sigValue);
        Assert.assertNotEquals(sigValue, "");

        runner = TestRunners.newTestRunner(new VerifyContentAttributePGP());
        runner.setProperty(VerifyContentAttributePGP.PGP_KEY_SERVICE, SERVICE_ID);
        service = new PGPControllerService();
        runner.addControllerService(SERVICE_ID, service);
        //runner.setProperty(service, PGPControllerService.PUBLIC_KEYRING_TEXT, tempPubKey);
        runner.enableControllerService(service);

        runner.enqueue(flow);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(VerifyContentAttributePGP.REL_SUCCESS, 1);
    }


    private static URL keyResourceURL(String name) {
        Class<PGPControllerServiceTest> cls = PGPControllerServiceTest.class;
        return cls.getResource("/" + cls.getSimpleName() +  "/" + name);
    }


    private static InputStream keyResourceStream(String name) {
        Class<PGPControllerServiceTest> cls = PGPControllerServiceTest.class;
        return cls.getResourceAsStream("/" + cls.getSimpleName() +  "/" + name);
    }


    private static byte[] keyResourceBytes(String name) throws IOException {
        return IOUtils.toByteArray(keyResourceStream(name));
    }
}

