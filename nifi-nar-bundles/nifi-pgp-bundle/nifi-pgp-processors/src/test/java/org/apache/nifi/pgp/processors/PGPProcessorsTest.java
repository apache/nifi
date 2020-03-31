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
package org.apache.nifi.pgp.processors;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.pgp.controllerservices.PGPKeyMaterialControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.pgp.StandardPGPOperator;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
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
 */

public class PGPProcessorsTest {
    public static final String PBE_PASSPHRASE = "password";
    private static String publicKey;
    private static String secretKey;

    TestRunner runner;
    PGPKeyMaterialControllerService service;
    byte[] plainBytes;



    @BeforeClass
    public static void setupServiceControllerTestClass() throws IOException {
        publicKey = keyResource("/pgp/public-key.asc");
        secretKey = keyResource("/pgp/secret-key.asc");

        Assert.assertNotNull(publicKey);
        Assert.assertNotNull(secretKey);

        Assert.assertTrue(publicKey.length() > 500);
        Assert.assertTrue(secretKey.length() > 1000);
    }

    @Before
    public void recreatePlainBytes() {
        plainBytes = Random.randomBytes(128 + Random.randomInt(128+1024));
    }

    public void buildTestRunner(Processor processor) {
        runner = TestRunners.newTestRunner(processor);

    }

    public void buildPGPService(Map<String, String> properties) throws InitializationException {
        service = new PGPKeyMaterialControllerService();
        runner.setProperty(StandardPGPOperator.PGP_KEY_SERVICE.getName(), "pgp-service");
        runner.addControllerService("pgp-service", service, properties);
    }


    @Test
    public void combinedProcessorsReferenceTest() throws InitializationException {
        // Configure an EncryptContentPGP processor with a PGP key service that has our public key:
        buildTestRunner(new EncryptContentPGP());
        runner.setProperty(StandardPGPOperator.ENCRYPT_ALGORITHM, StandardPGPOperator.getCipherDefaultValue());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKey);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the EncryptContentPGP processor encrypts data and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContentPGP.REL_SUCCESS, 1);
        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptContentPGP.REL_SUCCESS);
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // Configure a DecryptContentPGP processor with a PGP key service that has our secret key and password:
        buildTestRunner(new DecryptContentPGP());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKey);
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), "password");
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the DecryptContentPGP processor decrypts data and routes it correctly:
        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptContentPGP.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(DecryptContentPGP.REL_SUCCESS);
        Assert.assertArrayEquals(plainBytes, flows.get(0).toByteArray());

        // Configure a SignContentAttributePGP processor with a PGP key service that has our secret key and password:
        buildTestRunner(new SignContentAttributePGP());
        runner.setProperty(StandardPGPOperator.SIGNATURE_HASH_ALGORITHM, StandardPGPOperator.getSignatureHashDefaultValue());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.SECRET_KEYRING_TEXT.getName(), secretKey);
            put(StandardPGPOperator.PRIVATE_KEY_PASSPHRASE.getName(), "password");
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the SignContentAttributePGP processor signs the flow and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(SignContentAttributePGP.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(SignContentAttributePGP.REL_SUCCESS);
        Assert.assertEquals(1, flows.size());
        MockFlowFile flow = flows.get(0);
        String sigValue = flow.getAttribute(StandardPGPOperator.DEFAULT_SIGNATURE_ATTRIBUTE);
        Assert.assertNotNull(sigValue);
        Assert.assertNotEquals("", sigValue);

        // Configure a VerifyContentAttributePGP processor with a PGP key service that has our public key:
        buildTestRunner(new VerifyContentAttributePGP());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.PUBLIC_KEYRING_TEXT.getName(), publicKey);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the VerifyContentAttributePGP processor verifies the signature and routes it correctly:
        runner.enqueue(flow);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(VerifyContentAttributePGP.REL_SUCCESS, 1);
    }


    @Test
    public void combinedEncryptAndDecryptPbeReferenceTest() throws InitializationException {
        // Configure an EncryptContentPGP processor with a PGP key service that has our PBE passphrase:
        buildTestRunner(new EncryptContentPGP());
        runner.setProperty(StandardPGPOperator.ENCRYPT_ALGORITHM, StandardPGPOperator.getCipherDefaultValue());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.PBE_PASSPHRASE.getName(), PBE_PASSPHRASE);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the EncryptContentPGP processor encrypts data and routes it correctly:
        runner.enqueue(plainBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContentPGP.REL_SUCCESS, 1);
        List<MockFlowFile> flows = runner.getFlowFilesForRelationship(EncryptContentPGP.REL_SUCCESS);
        byte[] cipherBytes = flows.get(0).toByteArray();
        Assert.assertNotEquals(Hex.encodeHex(cipherBytes), Hex.encodeHex(plainBytes));

        // Configure a DecryptContentPGP processor with a PGP key service that has our PBE passphrase:
        buildTestRunner(new DecryptContentPGP());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.PBE_PASSPHRASE.getName(), PBE_PASSPHRASE);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        // This shows the DecryptContentPGP processor decrypts data and routes it correctly:
        runner.enqueue(cipherBytes);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(DecryptContentPGP.REL_SUCCESS, 1);
        flows = runner.getFlowFilesForRelationship(DecryptContentPGP.REL_SUCCESS);
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

        buildTestRunner(new EncryptContentPGP());
        buildPGPService(new HashMap<String, String>() {{
            put(StandardPGPOperator.PBE_PASSPHRASE.getName(), PBE_PASSPHRASE);
        }});
        runner.assertValid(service);
        runner.enableControllerService(service);

        ProcessorBenchmark.run(
                "EncryptContentPGP/PBE",
                runner,
                EncryptContentPGP.REL_SUCCESS,

                () -> {
                    Map<String, Map<PropertyDescriptor, String>> configs = new HashMap<>();

                    for (AllowableValue allowableValue : StandardPGPOperator.ENCRYPT_ALGORITHM.getAllowableValues()) {
                        configs.put(allowableValue.getDisplayName(),
                                new HashMap<PropertyDescriptor, String>() {{ put(StandardPGPOperator.ENCRYPT_ALGORITHM, allowableValue.getValue()); }});
                    }

                    return configs;
                },

                (TestRunner runner, Map<PropertyDescriptor, String> config) -> {
                    runner.setProperty(StandardPGPOperator.ENCRYPT_ENCODING, "0");

                    for (PropertyDescriptor key : config.keySet()) {
                        runner.setProperty(key, config.get(key));
                    }
                }

        );
    }


    @Test
    public void encryptProcessorTest() throws InitializationException {
        buildTestRunner(new EncryptContentPGP());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void decryptProcessorTest() throws InitializationException {
        buildTestRunner(new DecryptContentPGP());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void signProcessorTest() throws InitializationException {
        buildTestRunner(new SignContentAttributePGP());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }


    @Test
    public void verifyProcessorTest() throws InitializationException {
        buildTestRunner(new VerifyContentAttributePGP());
        buildPGPService(new HashMap<String, String>() {{
        }});
        runner.assertNotValid(service);
    }

    private static String keyResource(String name) throws IOException {
        final InputStream resource = PGPProcessorsTest.class.getResourceAsStream(name);
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        int i;

        while ((i = resource.read()) >= 0) {
            output.write(i);
        }
        resource.close();

        return new String(output.toByteArray(), Charset.defaultCharset());
    }
}
