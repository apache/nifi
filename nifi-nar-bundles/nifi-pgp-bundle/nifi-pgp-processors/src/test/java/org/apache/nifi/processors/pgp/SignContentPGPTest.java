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

import org.apache.nifi.pgp.service.api.PGPPrivateKeyService;
import org.apache.nifi.pgp.util.PGPSecretKeyGenerator;
import org.apache.nifi.processors.pgp.attributes.FileEncoding;
import org.apache.nifi.processors.pgp.attributes.HashAlgorithm;
import org.apache.nifi.processors.pgp.attributes.SigningStrategy;
import org.apache.nifi.processors.pgp.io.KeyIdentifierConverter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPOnePassSignature;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SignContentPGPTest {
    private static final String PASSPHRASE = UUID.randomUUID().toString();

    private static final String DATA = String.class.getName();

    private static final byte[] DATA_BINARY = DATA.getBytes(StandardCharsets.UTF_8);

    private static final String SERVICE_ID = PGPPrivateKeyService.class.getName();

    private static final int SIGNATURE_VERSION = 4;

    private static PGPPrivateKey rsaPrivateKey;

    private static PGPPublicKey rsaPublicKey;

    @Mock
    private PGPPrivateKeyService privateKeyService;

    private TestRunner runner;

    @BeforeAll
    public static void setKeys() throws Exception {
        final PGPSecretKey rsaSecretKey = PGPSecretKeyGenerator.generateRsaSecretKey(PASSPHRASE.toCharArray());
        rsaPublicKey = rsaSecretKey.getPublicKey();

        final PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder().build(PASSPHRASE.toCharArray());
        rsaPrivateKey = rsaSecretKey.extractPrivateKey(decryptor);
    }

    @BeforeEach
    public void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(new SignContentPGP());

        when(privateKeyService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, privateKeyService);
        runner.enableControllerService(privateKeyService);
        runner.setProperty(SignContentPGP.PRIVATE_KEY_SERVICE, SERVICE_ID);
    }

    @Test
    public void testFailurePrivateKeyIdParsingException() {
        runner.setProperty(SignContentPGP.PRIVATE_KEY_ID, String.class.getSimpleName());

        runner.enqueue(DATA);
        runner.run();

        assertFailureErrorLogged();
    }

    @Test
    public void testFailureServiceException() {
        final String privateKeyId = KeyIdentifierConverter.format((rsaPrivateKey.getKeyID()));
        runner.setProperty(SignContentPGP.PRIVATE_KEY_ID, privateKeyId);
        when(privateKeyService.findPrivateKey(eq(rsaPrivateKey.getKeyID()))).thenThrow(new RuntimeException());

        runner.enqueue(DATA);
        runner.run();

        assertFailureErrorLogged();
    }

    @Test
    public void testFailurePrivateKeyIdNotFound() {
        final String privateKeyId = KeyIdentifierConverter.format((rsaPrivateKey.getKeyID()));
        runner.setProperty(SignContentPGP.PRIVATE_KEY_ID, privateKeyId);
        when(privateKeyService.findPrivateKey(eq(rsaPrivateKey.getKeyID()))).thenReturn(Optional.empty());

        runner.enqueue(DATA);
        runner.run();

        assertFailureErrorLogged();
    }

    @Test
    public void testSuccessFileEncodingAsciiHashAlgorithmSha512() throws PGPException, IOException {
        assertSuccess(FileEncoding.ASCII, HashAlgorithm.SHA512, SigningStrategy.SIGNED);
    }

    @Test
    public void testSuccessFileEncodingBinaryHashAlgorithmSha512() throws PGPException, IOException {
        assertSuccess(FileEncoding.BINARY, HashAlgorithm.SHA512, SigningStrategy.SIGNED);
    }

    @Test
    public void testSuccessFileEncodingBinaryHashAlgorithmSha256() throws PGPException, IOException {
        assertSuccess(FileEncoding.BINARY, HashAlgorithm.SHA256, SigningStrategy.SIGNED);
    }

    @Test
    public void testSuccessDetachedFileEncodingBinaryHashAlgorithmSha256() throws PGPException, IOException {
        assertSuccess(FileEncoding.BINARY, HashAlgorithm.SHA256, SigningStrategy.DETACHED);
    }

    private void setPrivateKey() {
        final String privateKeyId = KeyIdentifierConverter.format((rsaPrivateKey.getKeyID()));
        runner.setProperty(SignContentPGP.PRIVATE_KEY_ID, privateKeyId);
        when(privateKeyService.findPrivateKey(eq(rsaPrivateKey.getKeyID()))).thenReturn(Optional.of(rsaPrivateKey));
    }

    private void assertSuccess(final FileEncoding fileEncoding, final HashAlgorithm hashAlgorithm, final SigningStrategy signingStrategy) throws PGPException, IOException {
        setPrivateKey();

        runner.setProperty(SignContentPGP.FILE_ENCODING, fileEncoding.toString());
        runner.setProperty(SignContentPGP.HASH_ALGORITHM, hashAlgorithm.toString());
        runner.setProperty(SignContentPGP.SIGNING_STRATEGY, signingStrategy.toString());

        runner.enqueue(DATA);
        runner.run();

        runner.assertTransferCount(SignContentPGP.SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(SignContentPGP.SUCCESS).iterator().next();
        assertFlowFileAttributesFound(flowFile, fileEncoding, hashAlgorithm);

        if (SigningStrategy.DETACHED == signingStrategy) {
            assertDetachedSignatureVerified(flowFile);
        } else {
            assertSignatureVerified(flowFile);
        }
    }

    private void assertFailureErrorLogged() {
        runner.assertAllFlowFilesTransferred(SignContentPGP.FAILURE);
        final Optional<LogMessage> optionalLogMessage = runner.getLogger().getErrorMessages().stream().findFirst();
        assertTrue(optionalLogMessage.isPresent());
    }

    private void assertFlowFileAttributesFound(final MockFlowFile flowFile, final FileEncoding fileEncoding, final HashAlgorithm hashAlgorithm) throws PGPException {
        flowFile.assertAttributeEquals(PGPAttributeKey.FILE_ENCODING, fileEncoding.toString());
        flowFile.assertAttributeExists(PGPAttributeKey.COMPRESS_ALGORITHM);
        flowFile.assertAttributeExists(PGPAttributeKey.COMPRESS_ALGORITHM_ID);

        final String signatureAlgorithm = PGPUtil.getSignatureName(rsaPrivateKey.getPublicKeyPacket().getAlgorithm(), hashAlgorithm.getId());
        final String keyId = KeyIdentifierConverter.format((rsaPrivateKey.getKeyID()));

        flowFile.assertAttributeExists(PGPAttributeKey.SIGNATURE_CREATED);
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_ALGORITHM, signatureAlgorithm);
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_HASH_ALGORITHM_ID, Integer.toString(hashAlgorithm.getId()));
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_KEY_ALGORITHM_ID, Integer.toString(rsaPrivateKey.getPublicKeyPacket().getAlgorithm()));
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_KEY_ID, keyId);
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_TYPE_ID, Integer.toString(PGPSignature.BINARY_DOCUMENT));
        flowFile.assertAttributeEquals(PGPAttributeKey.SIGNATURE_VERSION, Integer.toString(SIGNATURE_VERSION));
    }

    private void assertDetachedSignatureVerified(final MockFlowFile signatureFlowFile) throws IOException, PGPException {
        final InputStream signatureContentStream = PGPUtil.getDecoderStream(signatureFlowFile.getContentStream());
        final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(signatureContentStream);

        final PGPSignatureList signatureList = (PGPSignatureList) objectFactory.nextObject();
        final PGPSignature signature = signatureList.iterator().next();

        signature.init(new JcaPGPContentVerifierBuilderProvider(), rsaPublicKey);

        signature.update(DATA_BINARY);

        final boolean verified = signature.verify();
        assertTrue(verified);
    }

    private void assertSignatureVerified(final MockFlowFile flowFile) throws IOException, PGPException {
        final InputStream flowFileContentStream = PGPUtil.getDecoderStream(flowFile.getContentStream());
        final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(flowFileContentStream);

        final PGPCompressedData compressedData = (PGPCompressedData) objectFactory.nextObject();
        final InputStream dataInputStream = compressedData.getDataStream();
        final PGPObjectFactory dataObjectFactory = new JcaPGPObjectFactory(dataInputStream);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PGPOnePassSignature onePassSignature = null;
        boolean verified = false;
        for (final Object object : dataObjectFactory) {
            if (object instanceof PGPOnePassSignatureList) {
                final PGPOnePassSignatureList onePassSignatureList = (PGPOnePassSignatureList) object;
                onePassSignature = onePassSignatureList.iterator().next();
                onePassSignature.init(new JcaPGPContentVerifierBuilderProvider(), rsaPublicKey);
            } else if (object instanceof PGPLiteralData) {
                if (onePassSignature == null) {
                    throw new IllegalStateException("One-Pass Signature not found before Literal Data");
                }

                final PGPLiteralData literalData = (PGPLiteralData) object;
                final InputStream literalInputStream = literalData.getDataStream();
                int read;
                while ((read = literalInputStream.read()) >= 0) {
                    onePassSignature.update((byte) read);
                    outputStream.write(read);
                }
            } else if (object instanceof PGPSignatureList) {
                if (onePassSignature == null) {
                    throw new IllegalStateException("One-Pass Signature not found before Signature");
                }

                final PGPSignatureList signatureList = (PGPSignatureList) object;
                final PGPSignature signature = signatureList.iterator().next();
                verified = onePassSignature.verify(signature);
            } else {
                throw new IllegalStateException(String.format("Unexpected PGP Object Found [%s]", object.getClass()));
            }
        }

        assertTrue(verified);

        final String literal = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
        assertEquals(DATA, literal);
    }
}
