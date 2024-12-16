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
package org.apache.nifi.processors.pgp;

import org.apache.nifi.pgp.service.api.PGPPublicKeyService;
import org.apache.nifi.pgp.util.PGPFileUtils;
import org.apache.nifi.pgp.util.PGPSecretKeyGenerator;
import org.apache.nifi.pgp.util.PGPOperationUtils;
import org.apache.nifi.pgp.service.api.KeyIdentifierConverter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class VerifyContentPGPTest {
    private static final String PASSPHRASE = UUID.randomUUID().toString();

    private static final String SERVICE_ID = PGPPublicKeyService.class.getName();

    private static final int SIGNATURE_VERSION = 4;

    private static final int HASH_ALGORITHM_ID = HashAlgorithmTags.SHA512;

    private static final String DATA = VerifyContentPGPTest.class.getName();

    private static final byte[] DATA_BINARY = DATA.getBytes(StandardCharsets.UTF_8);

    private static PGPPrivateKey rsaPrivateKey;

    private static PGPPublicKey rsaPublicKey;

    private TestRunner runner;

    @Mock
    private PGPPublicKeyService publicKeyService;

    @BeforeAll
    public static void setKeys() throws Exception {
        final PGPSecretKey rsaSecretKey = PGPSecretKeyGenerator.generateRsaSecretKey(PASSPHRASE.toCharArray());
        final PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder().build(PASSPHRASE.toCharArray());
        rsaPrivateKey = rsaSecretKey.extractPrivateKey(decryptor);
        rsaPublicKey = rsaSecretKey.getPublicKey();
    }

    @BeforeEach
    public void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(new VerifyContentPGP());

        when(publicKeyService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, publicKeyService);
        runner.enableControllerService(publicKeyService);
        runner.setProperty(EncryptContentPGP.PUBLIC_KEY_SERVICE, SERVICE_ID);
    }

    @Test
    public void testFailureDataNotFound() {
        runner.enqueue(new byte[]{});
        runner.run();

        assertFailureErrorLogged();
    }

    @Test
    public void testFailureFlowFileUnchanged() {
        runner.enqueue(DATA);
        runner.run();

        assertFailureErrorLogged();
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(VerifyContentPGP.FAILURE).getFirst();
        flowFile.assertContentEquals(DATA);
    }

    @Test
    public void testFailurePublicKeyNotFoundDataUnchanged() throws PGPException, IOException {
        final byte[] signed = PGPOperationUtils.getOnePassSignedLiteralData(DATA_BINARY, rsaPrivateKey);

        final String publicKeyIdSearch = KeyIdentifierConverter.format((rsaPublicKey.getKeyID()));
        when(publicKeyService.findPublicKey(eq(publicKeyIdSearch))).thenReturn(Optional.empty());

        runner.enqueue(signed);
        runner.run();

        assertFailureErrorLogged();
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(VerifyContentPGP.FAILURE).getFirst();
        flowFile.assertContentEquals(signed);
        assertFlowFileAttributesFound(flowFile);
    }

    @Test
    public void testSuccessAsciiDataUnpacked() throws PGPException, IOException {
        final byte[] signed = PGPOperationUtils.getOnePassSignedLiteralData(DATA_BINARY, rsaPrivateKey);
        final String armored = PGPFileUtils.getArmored(signed);

        setPublicKeyId();
        runner.enqueue(armored);
        runner.run();

        final MockFlowFile flowFile = assertSuccess();
        flowFile.assertContentEquals(DATA);
    }

    @Test
    public void testSuccessBinaryDataUnpacked() throws PGPException, IOException {
        final byte[] signed = PGPOperationUtils.getOnePassSignedLiteralData(DATA_BINARY, rsaPrivateKey);

        setPublicKeyId();
        runner.enqueue(signed);
        runner.run();

        final MockFlowFile flowFile = assertSuccess();
        flowFile.assertContentEquals(DATA);
    }

    private void setPublicKeyId() {
        final String publicKeyIdSearch = KeyIdentifierConverter.format((rsaPrivateKey.getKeyID()));
        when(publicKeyService.findPublicKey(eq(publicKeyIdSearch))).thenReturn(Optional.of(rsaPublicKey));
    }

    private MockFlowFile assertSuccess() throws PGPException {
        runner.assertAllFlowFilesTransferred(VerifyContentPGP.SUCCESS);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(VerifyContentPGP.SUCCESS).getFirst();
        assertFlowFileAttributesFound(flowFile);
        return flowFile;
    }

    private void assertFailureErrorLogged() {
        runner.assertAllFlowFilesTransferred(VerifyContentPGP.FAILURE);
        final Optional<LogMessage> optionalLogMessage = runner.getLogger().getErrorMessages().stream().findFirst();
        assertTrue(optionalLogMessage.isPresent());
    }

    private void assertFlowFileAttributesFound(final MockFlowFile flowFile) throws PGPException {
        flowFile.assertAttributeExists(PGPAttributeKey.LITERAL_DATA_FILENAME);
        flowFile.assertAttributeExists(PGPAttributeKey.LITERAL_DATA_MODIFIED);

        final String signatureAlgorithm = PGPUtil.getSignatureName(rsaPublicKey.getAlgorithm(), HASH_ALGORITHM_ID);
        final String keyId = KeyIdentifierConverter.format((rsaPrivateKey.getKeyID()));

        flowFile.assertAttributeExists(PGPAttributeKey.SIGNATURE_CREATED);
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_ALGORITHM, signatureAlgorithm);
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_HASH_ALGORITHM_ID, Integer.toString(HASH_ALGORITHM_ID));
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_KEY_ALGORITHM_ID, Integer.toString(rsaPublicKey.getAlgorithm()));
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_KEY_ID, keyId);
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_TYPE_ID, Integer.toString(PGPSignature.BINARY_DOCUMENT));
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_VERSION, Integer.toString(SIGNATURE_VERSION));
    }
}
