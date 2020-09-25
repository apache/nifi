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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.pki.CertificateService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.crypto.algorithm.CryptographicAlgorithm;
import org.apache.nifi.processors.standard.crypto.algorithm.CryptographicAlgorithmResolver;
import org.apache.nifi.processors.standard.crypto.algorithm.DefaultCryptographicAlgorithmResolver;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicAttributeKey;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicMethod;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.cms.CMSEnvelopedDataParser;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.KeyTransRecipientId;
import org.bouncycastle.cms.RecipientId;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.RecipientInformationStore;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class EncryptCMSTest {
    private static final String CONTENT = String.class.getSimpleName();

    private static final String ERRORS_NOT_FOUND = "Error Log Messages not found";

    private static final String ALGORITHM_NOT_MATCHED = "Cryptographic Algorithm not matched";

    private static final String RECIPIENTS_NOT_FOUND = "CMS Recipients not found";

    private static final String RECIPIENT_ID_NOT_MATCHED = "CMS Recipient ID Class not matched";

    private static final String SERIAL_NUMBER_NOT_MATCHED = "Recipient ID Serial Number not matched";

    private static final String ISSUER_NOT_MATCHED = "Recipient Issuer not matched";

    private static final String DECRYPTED_NOT_MATCHED = "Decrypted Content not matched";

    private static final String OBJECT_IDENTIFIER_NOT_FOUND = "Cryptographic Object Identifier not found";

    private static final String CERTIFICATE_SERVICE_ID = UUID.randomUUID().toString();

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SUBJECT_DN = "CN=subject";

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final int ONE_DAY = 1;

    private KeyPair keyPair;

    private TestRunner testRunner;

    private CertificateService certificateService;

    @Before
    public void init() throws InitializationException, NoSuchAlgorithmException {
        testRunner = TestRunners.newTestRunner(EncryptCMS.class);

        certificateService = Mockito.mock(CertificateService.class);
        when(certificateService.getIdentifier()).thenReturn(CERTIFICATE_SERVICE_ID);
        testRunner.addControllerService(CERTIFICATE_SERVICE_ID, certificateService);
        testRunner.enableControllerService(certificateService);
        testRunner.assertValid(certificateService);

        testRunner.setProperty(EncryptCMS.CERTIFICATE_SERVICE, CERTIFICATE_SERVICE_ID);

        keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
    }

    @Test
    public void testFlowFileNotFound() {
        testRunner.run();
        testRunner.assertQueueEmpty();
    }

    @Test
    public void testFlowFileProcessed() throws CMSException, IOException {
        final List<X509Certificate> certificates = Collections.emptyList();
        when(certificateService.findCertificates(any())).thenReturn(certificates);
        assertFlowFileSuccess();
    }

    @Test
    public void testRecipients() throws Exception {
        final X509Certificate certificate = CertificateUtils.generateSelfSignedX509Certificate(keyPair, SUBJECT_DN, SIGNING_ALGORITHM, ONE_DAY);
        final List<X509Certificate> certificates = Collections.singletonList(certificate);
        when(certificateService.findCertificates(any())).thenReturn(certificates);

        final CMSEnvelopedDataParser parser = assertFlowFileSuccess();

        final RecipientInformationStore recipientInformationStore = parser.getRecipientInfos();
        final Collection<RecipientInformation> recipients = recipientInformationStore.getRecipients();
        assertFalse(RECIPIENTS_NOT_FOUND, recipients.isEmpty());

        final RecipientInformation recipient = recipients.iterator().next();
        final RecipientId recipientId = recipient.getRID();
        assertEquals(RECIPIENT_ID_NOT_MATCHED, KeyTransRecipientId.class, recipientId.getClass());
        final KeyTransRecipientId keyTransRecipientId = (KeyTransRecipientId) recipientId;
        assertEquals(SERIAL_NUMBER_NOT_MATCHED, certificate.getSerialNumber(), keyTransRecipientId.getSerialNumber());
        assertEquals(ISSUER_NOT_MATCHED, certificate.getIssuerX500Principal().toString(), keyTransRecipientId.getIssuer().toString());
    }

    @Test
    public void testReadEncrypted() throws Exception {
        final X509Certificate certificate = CertificateUtils.generateSelfSignedX509Certificate(keyPair, SUBJECT_DN, SIGNING_ALGORITHM, ONE_DAY);
        final List<X509Certificate> certificates = Collections.singletonList(certificate);
        when(certificateService.findCertificates(any())).thenReturn(certificates);

        final CMSEnvelopedDataParser parser = assertFlowFileSuccess();

        final RecipientInformationStore recipientInformationStore = parser.getRecipientInfos();
        final Collection<RecipientInformation> recipients = recipientInformationStore.getRecipients();
        assertFalse(RECIPIENTS_NOT_FOUND, recipients.isEmpty());

        final RecipientInformation recipient = recipients.iterator().next();
        final JceKeyTransEnvelopedRecipient envelopedRecipient = new JceKeyTransEnvelopedRecipient(keyPair.getPrivate());
        final byte[] decrypted = recipient.getContent(envelopedRecipient);
        final String decryptedContent = new String(decrypted);

        assertEquals(DECRYPTED_NOT_MATCHED, CONTENT, decryptedContent);
    }

    @Test
    public void testCertificateServiceException() {
        when(certificateService.findCertificates(any())).thenThrow(new ProcessException(CERTIFICATE_SERVICE_ID));
        assertFlowFileFailed();
    }

    private void assertFlowFileFailed() {
        testRunner.enqueue(CONTENT);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(EncryptCMS.FAILURE);
        final List<LogMessage> errorMessages = testRunner.getLogger().getErrorMessages();
        assertFalse(ERRORS_NOT_FOUND, errorMessages.isEmpty());
    }

    private CMSEnvelopedDataParser assertFlowFileSuccess() throws CMSException, IOException {
        testRunner.enqueue(CONTENT);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(EncryptCMS.SUCCESS);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(EncryptCMS.SUCCESS);
        final MockFlowFile flowFile = flowFiles.iterator().next();
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), EncryptCMS.MIME_TYPE);

        final String objectIdentifier = flowFile.getAttribute(CryptographicAttributeKey.ALGORITHM_OBJECT_IDENTIFIER.key());
        assertNotNull(OBJECT_IDENTIFIER_NOT_FOUND, objectIdentifier);
        final CryptographicAlgorithmResolver resolver = new DefaultCryptographicAlgorithmResolver();
        final Optional<CryptographicAlgorithm> optionalAlgorithm = resolver.findCryptographicAlgorithm(objectIdentifier);
        final CryptographicAlgorithm cryptographicAlgorithm = optionalAlgorithm.orElseThrow(IllegalArgumentException::new);

        flowFile.assertAttributeEquals(CryptographicAttributeKey.ALGORITHM.key(), cryptographicAlgorithm.toString());
        flowFile.assertAttributeEquals(CryptographicAttributeKey.ALGORITHM_BLOCK_CIPHER_MODE.key(), cryptographicAlgorithm.getBlockCipherMode().getLabel());
        flowFile.assertAttributeEquals(CryptographicAttributeKey.ALGORITHM_CIPHER.key(), cryptographicAlgorithm.getCipher().getLabel());
        flowFile.assertAttributeEquals(CryptographicAttributeKey.ALGORITHM_KEY_SIZE.key(), Integer.toString(cryptographicAlgorithm.getKeySize()));
        flowFile.assertAttributeEquals(CryptographicAttributeKey.METHOD.key(), CryptographicMethod.CMS.toString());
        flowFile.assertAttributeExists(CryptographicAttributeKey.PROCESSING_COMPLETED.key());

        final byte[] content = testRunner.getContentAsByteArray(flowFile);
        final CMSEnvelopedDataParser parser = new CMSEnvelopedDataParser(content);
        final String encryptionObjectIdentifier = parser.getEncryptionAlgOID();
        assertEquals(ALGORITHM_NOT_MATCHED, objectIdentifier, encryptionObjectIdentifier);
        return parser;
    }
}
