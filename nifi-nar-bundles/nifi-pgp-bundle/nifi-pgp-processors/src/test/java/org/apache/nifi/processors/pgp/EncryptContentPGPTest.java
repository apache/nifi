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
import org.apache.nifi.processors.pgp.attributes.CompressionAlgorithm;
import org.apache.nifi.processors.pgp.attributes.FileEncoding;
import org.apache.nifi.processors.pgp.attributes.SymmetricKeyAlgorithm;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.pgp.util.PGPSecretKeyGenerator;

import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.PBEDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.PublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcPBEDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcPGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EncryptContentPGPTest {
    private static final String PASSPHRASE = UUID.randomUUID().toString();

    private static final String DATA = String.class.getName();

    private static final SymmetricKeyAlgorithm DEFAULT_SYMMETRIC_KEY_ALGORITHM = SymmetricKeyAlgorithm.valueOf(EncryptContentPGP.SYMMETRIC_KEY_ALGORITHM.getDefaultValue());

    private static final String SERVICE_ID = PGPPublicKeyService.class.getName();

    private static PGPSecretKey rsaSecretKey;

    private static PGPPrivateKey rsaPrivateKey;

    private static PGPPublicKey elGamalPublicKey;

    private static PGPPrivateKey elGamalPrivateKey;

    private TestRunner runner;

    @Mock
    private PGPPublicKeyService publicKeyService;

    @BeforeClass
    public static void setKeys() throws Exception {
        rsaSecretKey = PGPSecretKeyGenerator.generateRsaSecretKey(PASSPHRASE.toCharArray());
        final PGPSecretKeyRing dsaElGamalSecretKeyRing = PGPSecretKeyGenerator.generateDsaElGamalSecretKeyRing(PASSPHRASE.toCharArray());

        final PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder().build(PASSPHRASE.toCharArray());
        rsaPrivateKey = rsaSecretKey.extractPrivateKey(decryptor);
        for (final PGPSecretKey secretKey : dsaElGamalSecretKeyRing) {
            final PGPPublicKey publicKey = secretKey.getPublicKey();
            if (PGPPublicKey.ELGAMAL_ENCRYPT == publicKey.getAlgorithm()) {
                elGamalPrivateKey = secretKey.extractPrivateKey(decryptor);
                elGamalPublicKey = publicKey;
            }
        }
    }

    @Before
    public void setRunner() {
        runner = TestRunners.newTestRunner(new EncryptContentPGP());
    }

    @Test
    public void testMissingProperties() {
        runner.assertNotValid();
    }

    @Test
    public void testPublicKeyServiceMissingPublicKeySearch() throws InitializationException {
        when(publicKeyService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, publicKeyService);
        runner.enableControllerService(publicKeyService);
        runner.setProperty(EncryptContentPGP.PUBLIC_KEY_SERVICE, SERVICE_ID);

        runner.assertNotValid();
    }

    @Test
    public void testPublicKeySearchMissingPublicKeyService() {
        runner.setProperty(EncryptContentPGP.PUBLIC_KEY_SEARCH, String.class.getSimpleName());

        runner.assertNotValid();
    }

    @Test
    public void testSuccessPasswordBasedEncryptionDefaultProperties() throws IOException, PGPException {
        runner.setProperty(EncryptContentPGP.PASSPHRASE, PASSPHRASE);
        runner.enqueue(DATA);
        runner.run();

        assertSuccess(DEFAULT_SYMMETRIC_KEY_ALGORITHM, PASSPHRASE.toCharArray());
    }

    @Test
    public void testSuccessPasswordBasedEncryptionSymmetricKeyAlgorithms() throws IOException, PGPException {
        for (final SymmetricKeyAlgorithm symmetricKeyAlgorithm : SymmetricKeyAlgorithm.values()) {
            runner = TestRunners.newTestRunner(new EncryptContentPGP());
            runner.setProperty(EncryptContentPGP.PASSPHRASE, PASSPHRASE);
            runner.setProperty(EncryptContentPGP.SYMMETRIC_KEY_ALGORITHM, symmetricKeyAlgorithm.toString());
            runner.enqueue(DATA);
            runner.run();
            assertSuccess(symmetricKeyAlgorithm, PASSPHRASE.toCharArray());
        }
    }

    @Test
    public void testSuccessPasswordBasedEncryptionCompressionAlgorithms() throws IOException, PGPException {
        for (final CompressionAlgorithm compressionAlgorithm : CompressionAlgorithm.values()) {
            runner = TestRunners.newTestRunner(new EncryptContentPGP());
            runner.setProperty(EncryptContentPGP.PASSPHRASE, PASSPHRASE);
            runner.setProperty(EncryptContentPGP.COMPRESSION_ALGORITHM, compressionAlgorithm.toString());
            runner.enqueue(DATA);
            runner.run();
            assertSuccess(DEFAULT_SYMMETRIC_KEY_ALGORITHM, PASSPHRASE.toCharArray());
        }
    }

    @Test
    public void testSuccessPasswordBasedEncryptionFileEncodingAscii() throws IOException, PGPException {
        runner.setProperty(EncryptContentPGP.PASSPHRASE, PASSPHRASE);
        runner.setProperty(EncryptContentPGP.FILE_ENCODING, FileEncoding.ASCII.toString());
        runner.enqueue(DATA);
        runner.run();
        assertSuccess(DEFAULT_SYMMETRIC_KEY_ALGORITHM, PASSPHRASE.toCharArray());
    }

    @Test
    public void testSuccessPublicKeyEncryptionRsaPublicKey() throws IOException, InitializationException, PGPException {
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        setPublicKeyService(publicKey);
        final String publicKeyIdSearch = Long.toHexString(publicKey.getKeyID()).toUpperCase();
        when(publicKeyService.findPublicKey(eq(publicKeyIdSearch))).thenReturn(Optional.of(publicKey));

        runner.enqueue(DATA);
        runner.run();
        assertSuccess(rsaPrivateKey);
    }

    @Test
    public void testSuccessPasswordBasedAndPublicKeyEncryptionRsaPublicKey() throws IOException, InitializationException, PGPException {
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        setPublicKeyService(publicKey);
        final String publicKeyIdSearch = Long.toHexString(publicKey.getKeyID()).toUpperCase();
        when(publicKeyService.findPublicKey(eq(publicKeyIdSearch))).thenReturn(Optional.of(publicKey));

        runner.setProperty(EncryptContentPGP.PASSPHRASE, PASSPHRASE);

        runner.enqueue(DATA);
        runner.run();

        assertSuccess(rsaPrivateKey);
        assertSuccess(DEFAULT_SYMMETRIC_KEY_ALGORITHM, PASSPHRASE.toCharArray());
    }

    @Test
    public void testSuccessPublicKeyEncryptionElGamalPublicKey() throws IOException, InitializationException, PGPException {
        setPublicKeyService(elGamalPublicKey);
        final String publicKeyIdSearch = Long.toHexString(elGamalPublicKey.getKeyID()).toUpperCase();
        when(publicKeyService.findPublicKey(eq(publicKeyIdSearch))).thenReturn(Optional.of(elGamalPublicKey));

        runner.enqueue(DATA);
        runner.run();
        assertSuccess(elGamalPrivateKey);
    }

    @Test
    public void testFailurePublicKeyEncryptionKeyNotFound() throws InitializationException {
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        setPublicKeyService(publicKey);

        final String publicKeyIdNotFound = Long.toHexString(Long.MAX_VALUE).toUpperCase();
        runner.setProperty(EncryptContentPGP.PUBLIC_KEY_SEARCH, publicKeyIdNotFound);

        runner.enqueue(DATA);
        runner.run();
        runner.assertAllFlowFilesTransferred(EncryptContentPGP.FAILURE);
    }

    private void setPublicKeyService(final PGPPublicKey publicKey) throws InitializationException {
        when(publicKeyService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, publicKeyService);
        runner.enableControllerService(publicKeyService);

        runner.setProperty(EncryptContentPGP.PUBLIC_KEY_SERVICE, SERVICE_ID);
        final long publicKeyId = publicKey.getKeyID();
        final String publicKeyIdLong = Long.toHexString(publicKeyId).toUpperCase();
        runner.setProperty(EncryptContentPGP.PUBLIC_KEY_SEARCH, publicKeyIdLong);
    }

    private void assertSuccess(final PGPPrivateKey privateKey) throws IOException, PGPException {
        runner.assertAllFlowFilesTransferred(EncryptContentPGP.SUCCESS);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(EncryptContentPGP.SUCCESS).iterator().next();
        assertAttributesFound(DEFAULT_SYMMETRIC_KEY_ALGORITHM, flowFile);

        final PGPEncryptedDataList encryptedDataList = getEncryptedDataList(flowFile);
        final Optional<PGPEncryptedData> encryptedData = StreamSupport.stream(encryptedDataList.spliterator(), false)
                .filter(pgpEncryptedData -> pgpEncryptedData instanceof PGPPublicKeyEncryptedData)
                .findFirst();
        assertTrue("Public Key Encrypted Data not found", encryptedData.isPresent());

        final PGPPublicKeyEncryptedData publicKeyEncryptedData = (PGPPublicKeyEncryptedData) encryptedData.get();
        final String decryptedData = getDecryptedData(publicKeyEncryptedData, privateKey);
        assertEquals(DATA, decryptedData);
    }

    private void assertSuccess(final SymmetricKeyAlgorithm symmetricKeyAlgorithm, final char[] passphrase) throws IOException, PGPException {
        runner.assertAllFlowFilesTransferred(EncryptContentPGP.SUCCESS);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(EncryptContentPGP.SUCCESS).iterator().next();
        assertAttributesFound(symmetricKeyAlgorithm, flowFile);

        final PGPEncryptedDataList encryptedDataList = getEncryptedDataList(flowFile);
        final Optional<PGPEncryptedData> encryptedData = StreamSupport.stream(encryptedDataList.spliterator(), false)
                .filter(pgpEncryptedData -> pgpEncryptedData instanceof PGPPBEEncryptedData)
                .findFirst();
        assertTrue("Password Based Encrypted Data not found", encryptedData.isPresent());

        final PGPPBEEncryptedData passwordBasedEncryptedData = (PGPPBEEncryptedData) encryptedData.get();
        final String decryptedData = getDecryptedData(passwordBasedEncryptedData, passphrase);
        assertEquals(DATA, decryptedData);
    }

    private void assertAttributesFound(final SymmetricKeyAlgorithm symmetricKeyAlgorithm, final MockFlowFile flowFile) {
        flowFile.assertAttributeEquals(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM, symmetricKeyAlgorithm.toString());
        flowFile.assertAttributeEquals(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_BLOCK_CIPHER, symmetricKeyAlgorithm.getBlockCipher().toString());
        flowFile.assertAttributeEquals(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_KEY_SIZE, Integer.toString(symmetricKeyAlgorithm.getKeySize()));
        flowFile.assertAttributeEquals(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_ID, Integer.toString(symmetricKeyAlgorithm.getId()));
        flowFile.assertAttributeExists(PGPAttributeKey.FILE_ENCODING);
        flowFile.assertAttributeExists(PGPAttributeKey.COMPRESS_ALGORITHM);
        flowFile.assertAttributeExists(PGPAttributeKey.COMPRESS_ALGORITHM_ID);
    }

    private PGPEncryptedDataList getEncryptedDataList(final MockFlowFile flowFile) throws IOException {
        final FileEncoding fileEncoding = FileEncoding.valueOf(flowFile.getAttribute(PGPAttributeKey.FILE_ENCODING));
        InputStream contentStream = flowFile.getContentStream();
        if (FileEncoding.ASCII.equals(fileEncoding)) {
            contentStream = new ArmoredInputStream(contentStream);
        }

        final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(contentStream);
        final Object firstObject = objectFactory.nextObject();
        assertEquals(PGPEncryptedDataList.class, firstObject.getClass());

        return (PGPEncryptedDataList) firstObject;
    }

    private String getDecryptedData(final PGPPBEEncryptedData passwordBasedEncryptedData, final char[] passphrase) throws PGPException, IOException {
        final PBEDataDecryptorFactory decryptorFactory = new BcPBEDataDecryptorFactory(passphrase, new BcPGPDigestCalculatorProvider());
        final InputStream decryptedDataStream = passwordBasedEncryptedData.getDataStream(decryptorFactory);
        return getDecryptedData(decryptedDataStream);
    }

    private String getDecryptedData(final PGPPublicKeyEncryptedData publicKeyEncryptedData, final PGPPrivateKey privateKey) throws PGPException, IOException {
        final PublicKeyDataDecryptorFactory decryptorFactory = new BcPublicKeyDataDecryptorFactory(privateKey);
        final InputStream decryptedDataStream = publicKeyEncryptedData.getDataStream(decryptorFactory);
        return getDecryptedData(decryptedDataStream);
    }

    private String getDecryptedData(final InputStream decryptedDataStream) throws PGPException, IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(decryptedDataStream);
        final PGPLiteralData literalData = getLiteralData(objectFactory);
        StreamUtils.copy(literalData.getDataStream(), outputStream);
        return outputStream.toString();
    }

    private PGPLiteralData getLiteralData(final PGPObjectFactory objectFactory) throws PGPException {
        PGPLiteralData literalData = null;
        for (final Object object : objectFactory) {
            if (object instanceof PGPLiteralData) {
                literalData = (PGPLiteralData) object;
                break;
            } else if (object instanceof PGPCompressedData) {
                final PGPCompressedData compressedData = (PGPCompressedData) object;
                final PGPObjectFactory compressedObjectFactory = new JcaPGPObjectFactory(compressedData.getDataStream());
                literalData = getLiteralData(compressedObjectFactory);
                break;
            }
        }
        return literalData;
    }
}
