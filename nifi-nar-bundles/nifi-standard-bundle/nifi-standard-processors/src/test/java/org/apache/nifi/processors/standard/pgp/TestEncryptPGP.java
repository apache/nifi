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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
        // Configure an EncryptPGP processor with a PGP key service that has our PBE passphrase:
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

        // Configure a DecryptPGP processor with a PGP key service that has our PBE passphrase:
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

    @Ignore
    @Test
    public void testEncryptContentBenchmarks() throws IOException, InterruptedException, InitializationException {
        TestRunner testEnc = TestRunners.newTestRunner(new EncryptContent());
        ProcessorBenchmark.run(
                "EncryptContent (encrypt then decrypt mode) using PBE",
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

        service = new PGPKeyMaterialControllerService();
        final TestRunner[] testPGP = {TestRunners.newTestRunner(new EncryptPGP())};
        testPGP[0].setProperty(SERVICE_ID, SERVICE_ID);
        testPGP[0].addControllerService(SERVICE_ID, service, new HashMap<>() {{
            put(PGPKeyMaterialControllerService.PBE_PASS_PHRASE.getName(), PGPKeyMaterialControllerServiceTest.CORRECT_PASSWORD);
        }});
        testPGP[0].enableControllerService(service);

        ProcessorBenchmark.run(
                "EncryptPGP then DecryptPGP using PBE",
                testPGP[0],
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
                    testPGP[0].setProperty(EncryptPGP.ENCRYPT_ENCODING, "0");
                    for (PropertyDescriptor key : config.keySet()) {
                        testPGP[0].setProperty(key, config.get(key));
                    }
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    testPGP[0] = TestRunners.newTestRunner(new DecryptPGP());
                    testPGP[0].setProperty(SERVICE_ID, SERVICE_ID);
                    testPGP[0].addControllerService(AbstractProcessorPGP.SERVICE_ID, service, new HashMap<>() {{
                        put(PGPKeyMaterialControllerService.PBE_PASS_PHRASE.getName(), PGPKeyMaterialControllerServiceTest.CORRECT_PASSWORD);
                    }});
                    testPGP[0].enableControllerService(service);
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
