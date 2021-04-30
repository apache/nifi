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
import org.apache.nifi.processors.pgp.exception.PGPDecryptionException;
import org.apache.nifi.processors.pgp.exception.PGPProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.PGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.PGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DecryptContentPGPTest {
    private static final int ENCRYPTION_ALGORITHM = SymmetricKeyAlgorithmTags.AES_256;

    private static final boolean INTEGRITY_ENABLED = true;

    private static final boolean INTEGRITY_DISABLED = false;

    private static final String PASSPHRASE = UUID.randomUUID().toString();

    private static final String FILE_NAME = String.class.getSimpleName();

    private static final char FILE_TYPE = PGPLiteralDataGenerator.TEXT;

    private static final long MODIFIED_MILLISECONDS = 86400000;

    private static final Date MODIFIED = new Date(MODIFIED_MILLISECONDS);

    private static final String DATA = String.class.getName();

    private static final Charset DATA_CHARSET = StandardCharsets.UTF_8;

    private static final int BUFFER_SIZE = 128;

    private static final boolean NESTED_SIGNATURE_DISABLED = false;

    private static final String SERVICE_ID = PGPPrivateKeyService.class.getSimpleName();

    private static PGPSecretKey rsaSecretKey;

    private static PGPPrivateKey rsaPrivateKey;

    private static PGPPublicKey elGamalPublicKey;

    private static PGPPrivateKey elGamalPrivateKey;

    private TestRunner runner;

    @Mock
    private PGPPrivateKeyService privateKeyService;

    @BeforeClass
    public static void setKeys() throws Exception {
        rsaSecretKey = PGPSecretKeyGenerator.generateRsaSecretKey(PASSPHRASE.toCharArray());

        final PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder().build(PASSPHRASE.toCharArray());
        rsaPrivateKey = rsaSecretKey.extractPrivateKey(decryptor);
        final PGPSecretKeyRing dsaElGamalSecretKeyRing = PGPSecretKeyGenerator.generateDsaElGamalSecretKeyRing(PASSPHRASE.toCharArray());
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
        runner = TestRunners.newTestRunner(new DecryptContentPGP());
    }

    @Test
    public void testMissingProperties() {
        runner.assertNotValid();
    }

    @Test
    public void testFailureEncryptedDataNotFound() {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);
        runner.enqueue(new byte[]{});
        runner.run();

        assertFailureExceptionLogged(PGPProcessException.class);
    }

    @Test
    public void testFailurePasswordBasedEncryptionPassphraseNotConfigured() throws IOException, PGPException, InitializationException {
        setPrivateKeyService();

        final byte[] encryptedData = getPasswordBasedEncryptedData(getLiteralData(), INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertFailureExceptionLogged(PGPProcessException.class);
    }

    @Test
    public void testFailurePasswordBasedEncryptionPassphraseNotMatched() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, String.class.getSimpleName());

        final byte[] encryptedData = getPasswordBasedEncryptedData(getLiteralData(), INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertFailureExceptionLogged(PGPDecryptionException.class);
    }

    @Test
    public void testFailureLiteralDataNotFound() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final byte[] encryptedData = getPasswordBasedEncryptedData(new byte[]{}, INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertFailureExceptionLogged(PGPProcessException.class);
    }

    @Test
    public void testSuccessPasswordBasedCompressedZipIntegrityEnabled() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final byte[] literalData = getLiteralData();
        final byte[] compressedData = getCompressedData(literalData);
        final byte[] encryptedData = getPasswordBasedEncryptedData(compressedData, INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPasswordBasedIntegrityEnabled() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final byte[] encryptedData = getPasswordBasedEncryptedData(getLiteralData(), INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPasswordBasedIntegrityDisabled() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final byte[] encryptedData = getPasswordBasedEncryptedData(getLiteralData(), INTEGRITY_DISABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPasswordBasedIntegrityEnabledCamellia128() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final int encryptionAlgorithm = SymmetricKeyAlgorithmTags.CAMELLIA_128;
        final byte[] encryptedData = getPasswordBasedEncryptedData(encryptionAlgorithm, getLiteralData(), INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess(encryptionAlgorithm);
    }

    @Test
    public void testSuccessPasswordBasedIntegrityEnabledTripleDes() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final int encryptionAlgorithm = SymmetricKeyAlgorithmTags.TRIPLE_DES;
        final byte[] encryptedData = getPasswordBasedEncryptedData(encryptionAlgorithm, getLiteralData(), INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess(encryptionAlgorithm);
    }

    @Test
    public void testSuccessPasswordBasedIntegrityEnabledCast5() throws IOException, PGPException {
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final int encryptionAlgorithm = SymmetricKeyAlgorithmTags.CAST5;
        final byte[] encryptedData = getPasswordBasedEncryptedData(encryptionAlgorithm, getLiteralData(), INTEGRITY_ENABLED);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess(encryptionAlgorithm);
    }

    @Test
    public void testSuccessPublicKeyEncryptionRsaPrivateKey() throws InitializationException, IOException, PGPException {
        setPrivateKeyService();
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        when(privateKeyService.findPrivateKey(eq(publicKey.getKeyID()))).thenReturn(Optional.of(rsaPrivateKey));

        final byte[] encryptedData = getPublicKeyEncryptedData(getLiteralData(), publicKey);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPublicKeyEncryptionElGamalPrivateKey() throws InitializationException, IOException, PGPException {
        setPrivateKeyService();
        when(privateKeyService.findPrivateKey(eq(elGamalPrivateKey.getKeyID()))).thenReturn(Optional.of(elGamalPrivateKey));
        final byte[] encryptedData = getPublicKeyEncryptedData(getLiteralData(), elGamalPublicKey);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPublicKeyEncryptionRsaPrivateKeySigned() throws InitializationException, IOException, PGPException {
        setPrivateKeyService();
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        when(privateKeyService.findPrivateKey(eq(publicKey.getKeyID()))).thenReturn(Optional.of(rsaPrivateKey));

        final byte[] encryptedData = getPublicKeyEncryptedData(getLiteralData(), publicKey);
        final byte[] signedData = getSignedData(encryptedData, publicKey, rsaPrivateKey);
        runner.enqueue(signedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPasswordBasedAndPublicKeyEncryptionRsaPrivateKey() throws InitializationException, IOException, PGPException {
        setPrivateKeyService();
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        when(privateKeyService.findPrivateKey(eq(publicKey.getKeyID()))).thenReturn(Optional.of(rsaPrivateKey));
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final byte[] encryptedData = getPasswordBasedAndPublicKeyEncryptedData(getLiteralData(), publicKey);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    @Test
    public void testSuccessPasswordBasedAndPublicKeyEncryptionRsaPrivateKeyNotFound() throws InitializationException, IOException, PGPException {
        setPrivateKeyService();
        final PGPPublicKey publicKey = rsaSecretKey.getPublicKey();
        when(privateKeyService.findPrivateKey(eq(publicKey.getKeyID()))).thenReturn(Optional.empty());
        runner.setProperty(DecryptContentPGP.PASSPHRASE, PASSPHRASE);

        final byte[] encryptedData = getPasswordBasedAndPublicKeyEncryptedData(getLiteralData(), publicKey);
        runner.enqueue(encryptedData);
        runner.run();

        assertSuccess();
    }

    private void setPrivateKeyService() throws InitializationException {
        when(privateKeyService.getIdentifier()).thenReturn(SERVICE_ID);
        runner.addControllerService(SERVICE_ID, privateKeyService);
        runner.enableControllerService(privateKeyService);
        runner.setProperty(DecryptContentPGP.PRIVATE_KEY_SERVICE, SERVICE_ID);
    }

    private void assertSuccess() {
        assertSuccess(ENCRYPTION_ALGORITHM);
    }

    private void assertSuccess(final int encryptionAlgorithm) {
        runner.assertAllFlowFilesTransferred(DecryptContentPGP.SUCCESS);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptContentPGP.SUCCESS);
        final MockFlowFile flowFile = flowFiles.iterator().next();
        flowFile.assertContentEquals(DATA, DATA_CHARSET);

        flowFile.assertAttributeEquals(PGPAttributeKey.LITERAL_DATA_FILENAME, FILE_NAME);
        flowFile.assertAttributeEquals(PGPAttributeKey.LITERAL_DATA_MODIFIED, Long.toString(MODIFIED_MILLISECONDS));
        flowFile.assertAttributeEquals(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_ID, Integer.toString(encryptionAlgorithm));
    }

    private void assertFailureExceptionLogged(final Class<? extends Exception> exceptionClass) {
        runner.assertAllFlowFilesTransferred(DecryptContentPGP.FAILURE);
        final Optional<LogMessage> optionalLogMessage = runner.getLogger().getErrorMessages().stream().findFirst();
        assertTrue(optionalLogMessage.isPresent());
        final LogMessage logMessage = optionalLogMessage.get();
        assertThat(Arrays.asList(logMessage.getArgs()), hasItem(isA(exceptionClass)));
    }

    private byte[] getSignedData(final byte[] contents, final PGPPublicKey publicKey, final PGPPrivateKey privateKey) throws PGPException, IOException {
        final PGPContentSignerBuilder contentSignerBuilder = new JcaPGPContentSignerBuilder(publicKey.getAlgorithm(), PGPUtil.SHA1);
        final PGPSignatureGenerator signatureGenerator = new PGPSignatureGenerator(contentSignerBuilder);
        signatureGenerator.init(PGPSignature.BINARY_DOCUMENT, privateKey);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        signatureGenerator.generateOnePassVersion(NESTED_SIGNATURE_DISABLED).encode(outputStream);
        outputStream.write(contents);
        signatureGenerator.update(contents);
        signatureGenerator.generate().encode(outputStream);
        return outputStream.toByteArray();
    }

    private byte[] getPublicKeyEncryptedData(final byte[] contents, final PGPPublicKey publicKey) throws IOException, PGPException {
        final PGPDataEncryptorBuilder builder = new BcPGPDataEncryptorBuilder(ENCRYPTION_ALGORITHM).setWithIntegrityPacket(INTEGRITY_ENABLED);
        final PGPEncryptedDataGenerator generator = new PGPEncryptedDataGenerator(builder);
        generator.addMethod(new BcPublicKeyKeyEncryptionMethodGenerator(publicKey));
        return getEncryptedData(generator, contents);
    }

    private byte[] getPasswordBasedEncryptedData(final byte[] contents, final boolean integrityEnabled) throws IOException, PGPException {
        return getPasswordBasedEncryptedData(ENCRYPTION_ALGORITHM, contents, integrityEnabled);
    }

    private byte[] getPasswordBasedEncryptedData(final int encryptionAlgorithm, final byte[] contents, final boolean integrityEnabled) throws IOException, PGPException {
        final PGPDataEncryptorBuilder builder = new BcPGPDataEncryptorBuilder(encryptionAlgorithm).setWithIntegrityPacket(integrityEnabled);
        final PGPEncryptedDataGenerator generator = new PGPEncryptedDataGenerator(builder);
        generator.addMethod(new JcePBEKeyEncryptionMethodGenerator(PASSPHRASE.toCharArray()));
        return getEncryptedData(generator, contents);
    }

    private byte[] getPasswordBasedAndPublicKeyEncryptedData(final byte[] contents, final PGPPublicKey publicKey) throws IOException, PGPException {
        final PGPDataEncryptorBuilder builder = new BcPGPDataEncryptorBuilder(ENCRYPTION_ALGORITHM).setWithIntegrityPacket(INTEGRITY_ENABLED);
        final PGPEncryptedDataGenerator generator = new PGPEncryptedDataGenerator(builder);
        generator.addMethod(new JcePBEKeyEncryptionMethodGenerator(PASSPHRASE.toCharArray()));
        generator.addMethod(new BcPublicKeyKeyEncryptionMethodGenerator(publicKey));
        return getEncryptedData(generator, contents);
    }

    private byte[] getCompressedData(final byte[] contents) throws IOException {
        final PGPCompressedDataGenerator generator = new PGPCompressedDataGenerator(PGPCompressedData.ZIP);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (final OutputStream compressedOutputStream = generator.open(outputStream)) {
            compressedOutputStream.write(contents);
        }
        return outputStream.toByteArray();
    }

    private byte[] getLiteralData() throws IOException {
        final PGPLiteralDataGenerator generator = new PGPLiteralDataGenerator();
        final byte[] buffer = new byte[BUFFER_SIZE];
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (final OutputStream literalStream = generator.open(outputStream, FILE_TYPE, FILE_NAME, MODIFIED, buffer)) {
            literalStream.write(DATA.getBytes(DATA_CHARSET));
        }
        return outputStream.toByteArray();
    }

    private byte[] getEncryptedData(final PGPEncryptedDataGenerator generator, final byte[] contents) throws IOException, PGPException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final byte[] buffer = new byte[BUFFER_SIZE];
        try (final OutputStream encryptedStream = generator.open(outputStream, buffer)) {
            encryptedStream.write(contents);
        }
        return outputStream.toByteArray();
    }
}
