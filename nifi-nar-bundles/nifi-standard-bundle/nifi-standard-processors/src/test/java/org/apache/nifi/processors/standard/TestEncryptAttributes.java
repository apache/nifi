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

package org.apache.nifi.processors.standard;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;
import java.util.Map;

public class TestEncryptAttributes {

    private static final Logger logger = LoggerFactory.getLogger(TestEncryptAttributes.class);

    // Initialize some common property values which will be used for setting up processor
    private static final EncryptionMethod[] ENCRYPTION_METHODS = EncryptionMethod.values();
    final String RAW_HEX_KEY= "abababababababababababababababab";
    private static final String PRIVATE_KEYRING = "src/test/resources/TestEncryptContent/secring.gpg";
    private static final String PUBLIC_KEYRING = "src/test/resources/TestEncryptContent/pubring.gpg";
    private static final String PRIVATE_KEYRING_PASSPHRASE = "PASSWORD";
    private static final String FILENAME_ATTR_KEY = CoreAttributes.FILENAME.key();
    private static final String UUID_ATTR_KEY = CoreAttributes.UUID.key();


    @Before
    public void setUp() {
        Security.addProvider(new BouncyCastleProvider());
    }


    @Test
    public void testRoundTrip() {
        final TestRunner testRunner = TestRunners.newTestRunner(new EncryptAttributes());

        for (final EncryptionMethod encryptionMethod : ENCRYPTION_METHODS) {
            if (encryptionMethod.isUnlimitedStrength())
                continue;
            if (encryptionMethod.isKeyedCipher()){
                testRunner.setProperty(EncryptAttributes.RAW_KEY_HEX, RAW_HEX_KEY);
                testRunner.setProperty(EncryptAttributes.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.NONE.name());
            } else {
                testRunner.setProperty(EncryptAttributes.PASSWORD, "short");
                testRunner.setProperty(EncryptAttributes.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.name());
                testRunner.setProperty(EncryptAttributes.ALLOW_WEAK_CRYPTO, EncryptAttributes.WEAK_CRYPTO_ALLOWED_NAME);
            }

            logger.info("Attempting {}", encryptionMethod.name());
            testRunner.setProperty(EncryptAttributes.ENCRYPTION_ALGORITHM, encryptionMethod.name());
            testRunner.setProperty(EncryptAttributes.MODE, EncryptAttributes.ENCRYPT_MODE);

            //create FlowFile and pass it to processor
            ProcessSession session = testRunner.getProcessSessionFactory().createSession();
            FlowFile ff = session.create();
            final Map<String, String> initialAttrs = ff.getAttributes();

            //Enqueue and process it
            testRunner.enqueue(ff);
            testRunner.clearTransferState();    //TODO:cleanProvenanceState?
            testRunner.run();
            testRunner.assertAllFlowFilesTransferred(EncryptAttributes.REL_SUCCESS, 1);

            //get new attributes
            MockFlowFile encryptedAttributesFlowFile = testRunner.getFlowFilesForRelationship(EncryptAttributes.REL_SUCCESS).get(0);
            final Map<String, String> encryptedAttrs = encryptedAttributesFlowFile.getAttributes();

            //Check for each attributes
            for (String attr : initialAttrs.keySet()) {
                //Since we are not encrypting filename and uuid
                if (!attr.equals(FILENAME_ATTR_KEY)
                        && !attr.equals(UUID_ATTR_KEY)) {
                    Assert.assertNotEquals("Encryption of " + attr + " was not successful",
                            initialAttrs.get(attr), encryptedAttrs.get(attr));
                }
            }

            //perform decryption
            testRunner.assertQueueEmpty();
            testRunner.setProperty(EncryptAttributes.MODE, EncryptAttributes.DECRYPT_MODE);
            testRunner.enqueue(encryptedAttributesFlowFile);
            testRunner.clearTransferState();
            testRunner.run();
            testRunner.assertAllFlowFilesTransferred(EncryptAttributes.REL_SUCCESS, 1);

            //get Decrypted Attributes
            MockFlowFile decryptedAttributesFlowFile = testRunner.getFlowFilesForRelationship(EncryptAttributes.REL_SUCCESS).get(0);
            final Map<String, String> decryptedAttrs = decryptedAttributesFlowFile.getAttributes();

            for (String attr : decryptedAttrs.keySet()) {
                if (!attr.equals(FILENAME_ATTR_KEY)
                        && !attr.equals(UUID_ATTR_KEY)) {
                    Assert.assertNotEquals("Decryption of " + attr + " was not successful", encryptedAttrs.get(attr), decryptedAttrs.get(attr));
                }
                Assert.assertEquals("Decryption of " + attr + " was not successful",
                        initialAttrs.get(attr), decryptedAttrs.get(attr));
            }

            logger.info("Test complete for {}", encryptionMethod.name());
        }

    }

    @Test
    public void testInvalidAttributeList() {
        logger.info("Testing invalidAttributes");

        final TestRunner runner = TestRunners.newTestRunner(new EncryptAttributes());

        runner.setProperty(EncryptAttributes.ATTRIBUTES_TO_ENCRYPT,"val1,val2");
        runner.setProperty(EncryptAttributes.MODE, EncryptAttributes.ENCRYPT_MODE);
        runner.setProperty(EncryptAttributes.PASSWORD,"helloworld");
        runner.setProperty(EncryptAttributes.KEY_DERIVATION_FUNCTION, KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.name());

        //Create session and FlowFile
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        Map<String,String> initAttrs = ff.getAttributes();

        //setup runner
        runner.assertQueueEmpty();
        runner.enqueue(ff);
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptAttributes.REL_SUCCESS, 1);

        //Get FlowFile attributes and check for attributes modification
        MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(EncryptAttributes.REL_SUCCESS).get(0);
        Map<String,String> finalAttrs = mockFlowFile.getAttributes();

        //Since the given attributes were not present in FlowFile.
        Assert.assertTrue(initAttrs.equals(finalAttrs));

        logger.info("Test Complete");
    }
}