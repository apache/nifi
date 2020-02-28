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
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This test class exercises all four PGP processors in isolation and in combination.
 *
 * The combinations are encrypt+decrypt, sign+verify, and all four together.  At the moment, little more
 * than the happy path is tested and shown.
 *
 * In isolation, we can show that the processors have certain values and behave as expected to a small degree.  However,
 * we cannot accurately test a decrypt operation without first using an encrypt operation, nor can we test a verify
 * operation without first invoking a corresponding sign operation.
 *
 * This implementation re-uses the {@link PGPControllerServiceTest} class for keys and key material.
 *
 */
public class PGPProcessorsTest {
    TestRunner runner;
    PGPControllerService service;
    byte[] plainBytes;

    @BeforeClass
    public static void setupServiceControllerTestClass() throws IOException {
        PGPControllerServiceTest.setupKeyAndKeyRings();
    }

    @Before
    public void recreatePlainBytes() {
        plainBytes = Random.randomBytes(128 + Random.randomInt(128+1024));
    }

    public void buildTestRunner(Processor processor) {
        runner = TestRunners.newTestRunner(processor);

    }

    public void buildPGPService(Map<String, String> properties) throws InitializationException {
        service = new PGPControllerService();
        runner.setProperty(AbstractPGPProcessor.SERVICE_ID, AbstractPGPProcessor.SERVICE_ID);
        runner.addControllerService(AbstractPGPProcessor.SERVICE_ID, service, properties);
    }


    @Test
    public void combinedProcessorsReferenceTest() throws InitializationException {
        // Configure an EncryptContentPGPProcessor processor with a PGP key service that has our public key:
        buildTestRunner(new EncryptContentPGPProcessor());
        runner.setProperty(EncryptContentPGPProcessor.ENCRYPT_ALGORITHM, EncryptContentPGPProcessor.getCipherDefaultValue());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), PGPControllerServiceTest.onePublicKeyRaw);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the EncryptContentPGPProcessor processor encrypts data and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContentPGPProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptContentPGPProcessor.REL_SUCCESS);
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // Configure a DecryptContentPGPProcessor processor with a PGP key service that has our secret key and password:
        buildTestRunner(new DecryptContentPGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), PGPControllerServiceTest.oneSecretKeyRaw);
            put(PGPControllerService.PRIVATE_KEY_PASSPHRASE.getName(), PGPControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the DecryptContentPGPProcessor processor decrypts data and routes it correctly:
        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptContentPGPProcessor.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(DecryptContentPGPProcessor.REL_SUCCESS);
        Assert.assertArrayEquals(flows.get(0).toByteArray(), plainBytes);

        // Configure a SignContentAttributePGPProcessor processor with a PGP key service that has our secret key and password:
        buildTestRunner(new SignContentAttributePGPProcessor());
        runner.setProperty(SignContentAttributePGPProcessor.SIGNATURE_HASH_ALGORITHM, SignContentAttributePGPProcessor.getSignatureHashDefaultValue());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.SECRET_KEYRING_TEXT.getName(), PGPControllerServiceTest.oneSecretKeyRaw);
            put(PGPControllerService.PRIVATE_KEY_PASSPHRASE.getName(), PGPControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the SignContentAttributePGPProcessor processor signs the flow and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(SignContentAttributePGPProcessor.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(SignContentAttributePGPProcessor.REL_SUCCESS);
        Assert.assertEquals(1, flows.size());
        MockFlowFile flow = flows.get(0);
        String sigValue = flow.getAttribute(AbstractPGPProcessor.DEFAULT_SIGNATURE_ATTRIBUTE);
        Assert.assertNotNull(sigValue);
        Assert.assertNotEquals(sigValue, "");

        // Configure a VerifyContentAttributePGPProcessor processor with a PGP key service that has our public key and password:
        buildTestRunner(new VerifyContentAttributePGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.PUBLIC_KEYRING_TEXT.getName(), PGPControllerServiceTest.onePublicKeyRaw);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the VerifyContentAttributePGPProcessor processor verifies the signature and routes it correctly:
        runner.enqueue(flow);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(VerifyContentAttributePGPProcessor.REL_SUCCESS, 1);
    }


    @Test
    public void combinedEncryptAndDecryptPbeReferenceTest() throws InitializationException {
        // Configure an EncryptContentPGPProcessor processor with a PGP key service that has our PBE passphrase:
        buildTestRunner(new EncryptContentPGPProcessor());
        runner.setProperty(EncryptContentPGPProcessor.ENCRYPT_ALGORITHM, EncryptContentPGPProcessor.getCipherDefaultValue());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.PBE_PASSPHRASE.getName(), PGPControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the EncryptContentPGPProcessor processor encrypts data and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContentPGPProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptContentPGPProcessor.REL_SUCCESS);
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // Configure a DecryptContentPGPProcessor processor with a PGP key service that has our PBE passphrase:
        buildTestRunner(new DecryptContentPGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.PBE_PASSPHRASE.getName(), PGPControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the DecryptContentPGPProcessor processor decrypts data and routes it correctly:
        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptContentPGPProcessor.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(DecryptContentPGPProcessor.REL_SUCCESS);
        Assert.assertArrayEquals(flows.get(0).toByteArray(), plainBytes);
    }


    @Ignore
    @Test
    public void benchmarkProcessors() throws IOException, InterruptedException, InitializationException {
        buildTestRunner(new EncryptContent());

        ProcessorBenchmark.run(
                "EncryptContent/PBE",
                runner,
                EncryptContent.REL_SUCCESS,

                () -> {
                    return new HashMap<String, Map<PropertyDescriptor, String>>() {{
                        put("PGP", new HashMap<PropertyDescriptor, String>() {{
                            put(EncryptContent.ENCRYPTION_ALGORITHM, "PGP");
                        }});

                        put("PGP+armor", new HashMap<PropertyDescriptor, String>() {{
                            put(EncryptContent.ENCRYPTION_ALGORITHM, "PGP_ASCII_ARMOR");
                        }});
                    }};
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    runner.setProperty(EncryptContent.PASSWORD, Random.randomBytes(32).toString());
                    runner.setProperty(EncryptContent.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());
                    runner.setProperty(EncryptContent.PGP_SYMMETRIC_ENCRYPTION_CIPHER, "1");
                    runner.setProperty(EncryptContent.MODE, EncryptContent.ENCRYPT_MODE);
                    for (PropertyDescriptor prop : config.keySet()) {
                        runner.setProperty(prop, config.get(prop));
                    }
                }

        );

        buildTestRunner(new EncryptContentPGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
            put(PGPControllerService.PBE_PASSPHRASE.getName(), PGPControllerServiceTest.CORRECT_PASSWORD);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        ProcessorBenchmark.run(
                "EncryptContentPGPProcessor/PBE",
                runner,
                EncryptContentPGPProcessor.REL_SUCCESS,

                () -> {
                    Map<String, Map<PropertyDescriptor, String>> configs = new HashMap<>();

                    for (AllowableValue allowableValue : EncryptContentPGPProcessor.ENCRYPT_ALGORITHM.getAllowableValues()) {
                        configs.put(allowableValue.getDisplayName(),
                                new HashMap<PropertyDescriptor, String>() {{ put(EncryptContentPGPProcessor.ENCRYPT_ALGORITHM, allowableValue.getValue()); }});
                    }

                    return configs;
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    runner.setProperty(EncryptContentPGPProcessor.ENCRYPT_ENCODING, "0");

                    for (PropertyDescriptor key : config.keySet()) {
                        runner.setProperty(key, config.get(key));
                    }
                }

        );
    }


    @Test
    public void encryptProcessorTest() throws InitializationException {
        buildTestRunner(new EncryptContentPGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void decryptProcessorTest() throws InitializationException {
        buildTestRunner(new DecryptContentPGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void signProcessorTest() throws InitializationException {
        buildTestRunner(new SignContentAttributePGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void verifyProcessorTest() throws InitializationException {
        buildTestRunner(new VerifyContentAttributePGPProcessor());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }
}
