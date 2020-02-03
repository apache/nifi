package org.apache.nifi.processors.standard.pgp;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AbstractTestPGP {
    static final String SERVICE_ID = AbstractProcessorPGP.SERVICE_ID;
    TestRunner runner;
    PGPKeyMaterialControllerService service;
    byte[] plainBytes;

    @BeforeClass
    public static void setupServiceControllerTestClass() throws IOException {
        PGPKeyMaterialControllerServiceTest.setupKeyAndKeyRings();
    }

    @Before
    public void recreatePlainBytes() {
        plainBytes = Random.randomBytes(128 + Random.randomInt(128+1024));
    }

    public void recreateRunner(Processor processor) {
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(SERVICE_ID, SERVICE_ID);
    }

    public void recreateService(Map<String, String> properties) throws InitializationException {
        service = new PGPKeyMaterialControllerService();
        runner.addControllerService(SERVICE_ID, service, properties);
    }
}

/**
 * The controller tests are separate but related; refer to those classes for more tests.
 *
 */
public class TestEncryptPGP extends AbstractTestPGP {


    @Test
    public void combinedProcessorsReferenceTest() throws InitializationException {
        // Configure an EncryptPGP processor with a PGP key service that has our public key:
        recreateRunner(new EncryptPGP());
        runner.setProperty(EncryptPGP.ENCRYPT_ALGORITHM, EncryptPGP.getCipherDefaultValue());
        recreateService(new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), PGPKeyMaterialControllerServiceTest.onePublicKeyRaw);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the EncryptPGP processor encrypts data and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptPGP.REL_SUCCESS, 1);
        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptPGP.REL_SUCCESS);
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // Configure a DecryptPGP processor with a PGP key service that has our secret key and password:
        recreateRunner(new DecryptPGP());
        recreateService(new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), PGPKeyMaterialControllerServiceTest.oneSecretKeyRaw);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), PGPKeyMaterialControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the DecryptPGP processor decrypts data and routes it correctly:
        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptPGP.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(DecryptPGP.REL_SUCCESS);
        Assert.assertArrayEquals(flows.get(0).toByteArray(), plainBytes);

        // Configure a SignPGP processor with a PGP key service that has our secret key and password:
        recreateRunner(new SignPGP());
        runner.setProperty(SignPGP.SIGNATURE_HASH_ALGORITHM, SignPGP.getSignatureHashDefaultValue());
        recreateService(new HashMap<>() {{
            put(PGPKeyMaterialControllerService.SECRET_KEYRING_TEXT.getName(), PGPKeyMaterialControllerServiceTest.oneSecretKeyRaw);
            put(PGPKeyMaterialControllerService.PRIVATE_KEY_PASS_PHRASE.getName(), PGPKeyMaterialControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the SignPGP processor signs the flow and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(SignPGP.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(SignPGP.REL_SUCCESS);
        Assert.assertEquals(1, flows.size());
        MockFlowFile flow = flows.get(0);
        String sigValue = flow.getAttribute(AbstractProcessorPGP.DEFAULT_SIGNATURE_ATTRIBUTE);
        Assert.assertNotNull(sigValue);
        Assert.assertNotEquals(sigValue, "");

        // Configure a VerifyPGP processor with a PGP key service that has our public key and password:
        recreateRunner(new VerifyPGP());
        recreateService(new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PUBLIC_KEYRING_TEXT.getName(), PGPKeyMaterialControllerServiceTest.onePublicKeyRaw);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the VerifyPGP processor verifies the signature and routes it correctly:
        runner.enqueue(flow);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(VerifyPGP.REL_SUCCESS, 1);
    }

    @Test
    public void combinedEncryptAndDecryptPbeReferenceTest() throws InitializationException {
        // Configure an EncryptPGP processor with a PGP key service that has our PBE pass-phrase:
        recreateRunner(new EncryptPGP());
        runner.setProperty(EncryptPGP.ENCRYPT_ALGORITHM, EncryptPGP.getCipherDefaultValue());
        recreateService(new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PBE_PASS_PHRASE.getName(), PGPKeyMaterialControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the EncryptPGP processor encrypts data and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptPGP.REL_SUCCESS, 1);
        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptPGP.REL_SUCCESS);
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // Configure a DecryptPGP processor with a PGP key service that has our PBE pass-phrase:
        recreateRunner(new DecryptPGP());
        recreateService(new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PBE_PASS_PHRASE.getName(), PGPKeyMaterialControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the DecryptPGP processor decrypts data and routes it correctly:
        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptPGP.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(DecryptPGP.REL_SUCCESS);
        Assert.assertArrayEquals(flows.get(0).toByteArray(), plainBytes);
    }

        // Move to EncryptStreamCallback / DecryptStreamCallback tests
    private static void runEncryptAndDecrypt(KeyProvider keys) throws IOException, PGPException {
        byte[] plain = Random.randomBytes(32 + Random.randomInt(4096));
        InputStream plainInput = new ByteArrayInputStream(plain);
        ByteArrayOutputStream cipherOutput = new ByteArrayOutputStream();
        EncryptStreamSession enc = new PublicKeyEncryptKeySession(null, keys.getPublicKey(), PGPEncryptedData.BLOWFISH, true);

        EncryptStreamCallback.encrypt(plainInput, cipherOutput, enc);
        byte[] ciphered = cipherOutput.toByteArray();
        InputStream cipherInput = new ByteArrayInputStream(cipherOutput.toByteArray());
        ByteArrayOutputStream plainOutput = new ByteArrayOutputStream();
        DecryptStreamSession dec = new PrivateKeyDecryptStreamSession(null, keys.getPrivateKey());

        DecryptStreamCallback.decrypt(cipherInput, plainOutput, dec);
        byte[] deciphered = plainOutput.toByteArray();

        Assert.assertNotEquals(plain.length, ciphered.length);
        Assert.assertNotEquals(Hex.encodeHexString(plain), Hex.encodeHexString(ciphered));
        Assert.assertEquals(plain.length, deciphered.length);
        Assert.assertEquals(Hex.encodeHexString(plain), Hex.encodeHexString(deciphered));
    }

    // Move to SignStreamCallback / VerifyStreamCallback tests
    private static void runSignAndVerify(KeyProvider keys) throws IOException, PGPException {
        byte[] plain = Random.randomBytes(32 + Random.randomInt(4096));
        InputStream plainInput = new ByteArrayInputStream(plain);
        ByteArrayOutputStream sigOutput = new ByteArrayOutputStream();
        SignStreamSession options = new SignStreamSession(keys.getPrivateKey(), PGPUtil.SHA256);
        OutputStream plainOut = new ByteArrayOutputStream();
        SignStreamCallback.sign(plainInput, plainOut, sigOutput, options);
        byte[] signature = sigOutput.toByteArray();
        VerifyStreamSession verifyOptions = new VerifyStreamSession(null, keys.getPublicKey(), new ByteArrayInputStream(signature));

        boolean verified = VerifyStreamCallback.verify(verifyOptions, new ByteArrayInputStream(plain), new ByteArrayOutputStream());
        Assert.assertNotEquals(Hex.encodeHexString(plain), Hex.encodeHexString(signature));
        Assert.assertTrue("Signature unverified: ", verified);
    }

    @Ignore
    @Test
    public void testEncryptContentBenchmarks() throws IOException, InterruptedException {
        TestRunner testEnc = TestRunners.newTestRunner(new EncryptContent());

        ProcessorBenchmark.run(
                "EncryptContent/PBE",
                testEnc,
                EncryptContent.REL_SUCCESS,
                EncryptContent.REL_FAILURE,

                () -> {
                    return new HashMap<>() {{
                        put("PGP", new HashMap<>() {{
                            put(EncryptContent.ENCRYPTION_ALGORITHM, "PGP");
                        }});

                        put("PGP+armor", new HashMap<>() {{
                            put(EncryptContent.ENCRYPTION_ALGORITHM, "PGP_ASCII_ARMOR");
                        }});
                    }};
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    testEnc.setProperty(EncryptContent.PASSWORD, Random.randomBytes(32).toString());
                    testEnc.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());
                    testEnc.setProperty(EncryptContent.PGP_SYMMETRIC_ENCRYPTION_CIPHER, "1");
                    testEnc.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);
                    for (PropertyDescriptor prop : config.keySet()) {
                        testEnc.setProperty(prop, config.get(prop));
                    }
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    testEnc.setProperty(EncryptContent.MODE, EncryptContent.DECRYPT_MODE);
                }
        );

        TestRunner testPGP = TestRunners.newTestRunner(new EncryptPGP());
        // testPGP.addControllerService("pgp-", service, new HashMap<>());

        ProcessorBenchmark.run(
                "EncryptPGP/PBE",
                testPGP,
                EncryptPGP.REL_SUCCESS,
                EncryptPGP.REL_FAILURE,

                () -> {
                    Map<String, Map<PropertyDescriptor, String>> configs = new HashMap<>();

                    for (AllowableValue allowableValue : EncryptPGP.ENCRYPT_ALGORITHM.getAllowableValues()) {
                        configs.put(allowableValue.getDisplayName(),
                                new HashMap<>() {{ put(EncryptPGP.ENCRYPT_ALGORITHM, allowableValue.getValue()); }});
                    }

                    return configs;
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    // testPGP.setProperty(service, Random.randomBytes(32).toString());
                    testPGP.setProperty(EncryptPGP.ENCRYPT_ENCODING, "0");

                    for (PropertyDescriptor key : config.keySet()) {
                        testPGP.setProperty(key, config.get(key));
                    }
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    // testPGP.setProperty(EncryptContentPGP.MODE, EncryptContentPGP.DECRYPT_MODE);
                }
        );
    }

    private static MockFlowFile runProcessor(TestRunner runner, Relationship success, Relationship failure, Map<PropertyDescriptor, String> forward, Map<PropertyDescriptor, String> reverse) throws IOException {
        byte[] body = Random.randomBytes(1024*1024);
        for (Map.Entry<PropertyDescriptor, String> property : forward.entrySet()) {
            runner.setProperty(property.getKey(), property.getValue());
        }
        runner.setThreadCount(1);
        runner.enqueue(body);
        runner.clearTransferState();
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 1);
        Assert.assertEquals(runner.getFlowFilesForRelationship(failure).size(), 0);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(success).get(0);
        // todo:  intermediate check against new parameter "differentInBetween"
        runner.assertQueueEmpty();
        for (Map.Entry<PropertyDescriptor, String> property : reverse.entrySet()) {
            runner.setProperty(property.getKey(), property.getValue());
        }
        runner.enqueue(flowFile);
        runner.clearTransferState();
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 1);
        Assert.assertEquals(runner.getFlowFilesForRelationship(failure).size(), 0);
        flowFile = runner.getFlowFilesForRelationship(success).get(0);
        flowFile.assertContentEquals(body);
        // System.out.println("Decrypted: " + Hex.encodeHexString(Arrays.copyOf(flowFile.toByteArray(), 32)));
        // System.out.println("Original : " + Hex.encodeHexString(Arrays.copyOf(body, 32)));
        return flowFile;
    }
}
