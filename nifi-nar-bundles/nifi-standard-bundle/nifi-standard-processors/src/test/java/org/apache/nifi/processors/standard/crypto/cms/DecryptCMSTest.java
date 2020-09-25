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
package org.apache.nifi.processors.standard.crypto.cms;

import org.apache.nifi.pki.PrivateKeyService;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.cms.CMSAlgorithm;
import org.bouncycastle.cms.CMSEnvelopedDataStreamGenerator;
import org.bouncycastle.cms.jcajce.JceCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipientInfoGenerator;
import org.bouncycastle.operator.OutputEncryptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DecryptCMSTest {
    private static final String CONTENT = String.class.getSimpleName();

    private static final ASN1ObjectIdentifier DEFAULT_ALGORITHM = CMSAlgorithm.AES128_GCM;

    private static final String DECRYPTED_CONTENT_NOT_MATCHED = "Decrypted Content not matched";

    private static final String KEY_SERVICE_ID = UUID.randomUUID().toString();

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SUBJECT_DN = "CN=subject";

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final int ONE_DAY = 1;

    private KeyPair keyPair;

    private X509Certificate certificate;

    private TestRunner testRunner;

    private PrivateKeyService privateKeyService;

    @Before
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(DecryptCMS.class);

        privateKeyService = Mockito.mock(PrivateKeyService.class);
        when(privateKeyService.getIdentifier()).thenReturn(KEY_SERVICE_ID);
        testRunner.addControllerService(KEY_SERVICE_ID, privateKeyService);
        testRunner.enableControllerService(privateKeyService);
        testRunner.assertValid(privateKeyService);

        testRunner.setProperty(DecryptCMS.PRIVATE_KEY_SERVICE, KEY_SERVICE_ID);

        keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        certificate = CertificateUtils.generateSelfSignedX509Certificate(keyPair, SUBJECT_DN, SIGNING_ALGORITHM, ONE_DAY);
    }

    @Test
    public void testFlowFileNotFound() {
        testRunner.run();
        testRunner.assertQueueEmpty();
    }

    @Test
    public void testSuccess() throws Exception {
        final Optional<PrivateKey> privateKey = Optional.of(keyPair.getPrivate());
        when(privateKeyService.findPrivateKey(any(), any())).thenReturn(privateKey);

        final byte[] encryptedContent = getEncryptedContent(certificate);
        final byte[] decryptedContent = assertFlowFileSuccess(encryptedContent);
        final String decryptedString = new String(decryptedContent);
        assertEquals(DECRYPTED_CONTENT_NOT_MATCHED, CONTENT, decryptedString);
    }

    @Test
    public void testFailure() throws Exception {
        final byte[] encryptedContent = getEncryptedContent(certificate);

        testRunner.enqueue(encryptedContent);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DecryptCMS.FAILURE);
    }

    private byte[] getEncryptedContent(final X509Certificate certificate) throws Exception {
        final JceCMSContentEncryptorBuilder builder = new JceCMSContentEncryptorBuilder(DEFAULT_ALGORITHM);
        builder.setSecureRandom(SecureRandom.getInstanceStrong());
        final OutputEncryptor outputEncryptor = builder.build();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final CMSEnvelopedDataStreamGenerator dataStreamGenerator = new CMSEnvelopedDataStreamGenerator();
        final JceKeyTransRecipientInfoGenerator recipientInfo = new JceKeyTransRecipientInfoGenerator(certificate);
        dataStreamGenerator.addRecipientInfoGenerator(recipientInfo);
        final OutputStream encryptedOutputStream = dataStreamGenerator.open(outputStream, outputEncryptor);

        encryptedOutputStream.write(CONTENT.getBytes());
        encryptedOutputStream.close();
        return outputStream.toByteArray();
    }

    private byte[] assertFlowFileSuccess(final byte[] bytes) {
        testRunner.enqueue(bytes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DecryptCMS.SUCCESS);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(EncryptCMS.SUCCESS);
        final MockFlowFile flowFile = flowFiles.iterator().next();
        return testRunner.getContentAsByteArray(flowFile);
    }
}
