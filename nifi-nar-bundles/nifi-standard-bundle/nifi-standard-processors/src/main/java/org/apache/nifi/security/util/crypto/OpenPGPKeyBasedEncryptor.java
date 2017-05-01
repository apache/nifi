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
package org.apache.nifi.security.util.crypto;

import static org.apache.nifi.processors.standard.util.PGPUtil.BLOCK_SIZE;
import static org.apache.nifi.processors.standard.util.PGPUtil.BUFFER_SIZE;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Iterator;
import java.util.zip.Deflater;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.PublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenPGPKeyBasedEncryptor implements Encryptor {
    private static final Logger logger = LoggerFactory.getLogger(OpenPGPPasswordBasedEncryptor.class);

    private String algorithm;
    private String provider;
    // TODO: This can hold either the secret or public keyring path
    private String keyring;
    private String userId;
    private char[] passphrase;
    private String filename;

    public OpenPGPKeyBasedEncryptor(final String algorithm, final String provider, final String keyring, final String userId, final char[] passphrase, final String filename) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.keyring = keyring;
        this.userId = userId;
        this.passphrase = passphrase;
        this.filename = filename;
    }

    @Override
    public StreamCallback getEncryptionCallback() throws Exception {
        return new OpenPGPEncryptCallback(algorithm, provider, keyring, userId, filename);
    }

    @Override
    public StreamCallback getDecryptionCallback() throws Exception {
        return new OpenPGPDecryptCallback(provider, keyring, passphrase);
    }

    /**
     * Returns true if the passphrase is valid.
     * <p>
     * This is used in the EncryptContent custom validation to check if the passphrase can extract a private key from the secret key ring. After BC was upgraded from 1.46 to 1.53, the API changed
     * so this is performed differently but the functionality is equivalent.
     *
     * @param provider the provider name
     * @param secretKeyringFile the file path to the keyring
     * @param passphrase        the passphrase
     * @return true if the passphrase can successfully extract any private key
     * @throws IOException             if there is a problem reading the keyring file
     * @throws PGPException            if there is a problem parsing/extracting the private key
     * @throws NoSuchProviderException if the provider is not available
     */
    public static boolean validateKeyring(String provider, String secretKeyringFile, char[] passphrase) throws IOException, PGPException, NoSuchProviderException {
        try {
            getDecryptedPrivateKey(provider, secretKeyringFile, passphrase);
            return true;
        } catch (Exception e) {
            // If this point is reached, no private key could be extracted with the given passphrase
            return false;
        }
    }

    private static PGPPrivateKey getDecryptedPrivateKey(String provider, String secretKeyringFile, char[] passphrase) throws IOException, PGPException {
        // TODO: Verify that key IDs cannot be 0
        return getDecryptedPrivateKey(provider, secretKeyringFile, 0L, passphrase);
    }

    private static PGPPrivateKey getDecryptedPrivateKey(String provider, String secretKeyringFile, long keyId, char[] passphrase) throws IOException, PGPException {
        // TODO: Reevaluate the mechanism for executing this task as performance can suffer here and only a specific key needs to be validated

        // Read in from the secret keyring file
        try (FileInputStream keyInputStream = new FileInputStream(secretKeyringFile)) {

            // Form the SecretKeyRing collection (1.53 way with fingerprint calculator)
            PGPSecretKeyRingCollection pgpSecretKeyRingCollection = new PGPSecretKeyRingCollection(keyInputStream, new BcKeyFingerprintCalculator());

            // The decryptor is identical for all keys
            final PBESecretKeyDecryptor decryptor = new JcePBESecretKeyDecryptorBuilder().setProvider(provider).build(passphrase);

            // Iterate over all secret keyrings
            Iterator<PGPSecretKeyRing> keyringIterator = pgpSecretKeyRingCollection.getKeyRings();
            PGPSecretKeyRing keyRing;
            PGPSecretKey secretKey;

            while (keyringIterator.hasNext()) {
                keyRing = keyringIterator.next();

                // If keyId exists, get a specific secret key; else, iterate over all
                if (keyId != 0) {
                    secretKey = keyRing.getSecretKey(keyId);
                    try {
                        return secretKey.extractPrivateKey(decryptor);
                    } catch (Exception e) {
                        throw new PGPException("No private key available using passphrase", e);
                    }
                } else {
                    Iterator<PGPSecretKey> keyIterator = keyRing.getSecretKeys();

                    while (keyIterator.hasNext()) {
                        secretKey = keyIterator.next();
                        try {
                            return secretKey.extractPrivateKey(decryptor);
                        } catch (Exception e) {
                            // TODO: Log (expected) failures?
                        }
                    }
                }
            }
        }

        // If this point is reached, no private key could be extracted with the given passphrase
        throw new PGPException("No private key available using passphrase");
    }

    /*
     * Get the public key for a specific user id from a keyring.
     */
    @SuppressWarnings("rawtypes")
    public static PGPPublicKey getPublicKey(String userId, String publicKeyringFile) throws IOException, PGPException {
        // TODO: Reevaluate the mechanism for executing this task as performance can suffer here and only a specific key needs to be validated

        // Read in from the public keyring file
        try (FileInputStream keyInputStream = new FileInputStream(publicKeyringFile)) {

            // Form the PublicKeyRing collection (1.53 way with fingerprint calculator)
            PGPPublicKeyRingCollection pgpPublicKeyRingCollection = new PGPPublicKeyRingCollection(keyInputStream, new BcKeyFingerprintCalculator());

            // Iterate over all public keyrings
            Iterator<PGPPublicKeyRing> iter = pgpPublicKeyRingCollection.getKeyRings();
            PGPPublicKeyRing keyRing;
            while (iter.hasNext()) {
                keyRing = iter.next();

                // Iterate over each public key in this keyring
                Iterator<PGPPublicKey> keyIter = keyRing.getPublicKeys();
                while (keyIter.hasNext()) {
                    PGPPublicKey publicKey = keyIter.next();

                    // Iterate over each userId attached to the public key
                    Iterator userIdIterator = publicKey.getUserIDs();
                    while (userIdIterator.hasNext()) {
                        String id = (String) userIdIterator.next();
                        if (userId.equalsIgnoreCase(id)) {
                            return publicKey;
                        }
                    }
                }
            }
        }

        // If this point is reached, no public key could be extracted with the given userId
        throw new PGPException("Could not find a public key with the given userId");
    }

    private static class OpenPGPDecryptCallback implements StreamCallback {

        private String provider;
        private String secretKeyringFile;
        private char[] passphrase;

        OpenPGPDecryptCallback(final String provider, final String secretKeyringFile, final char[] passphrase) {
            this.provider = provider;
            this.secretKeyringFile = secretKeyringFile;
            this.passphrase = passphrase;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try (InputStream pgpin = PGPUtil.getDecoderStream(in)) {
                PGPObjectFactory pgpFactory = new PGPObjectFactory(pgpin, new BcKeyFingerprintCalculator());

                Object obj = pgpFactory.nextObject();
                if (!(obj instanceof PGPEncryptedDataList)) {
                    obj = pgpFactory.nextObject();
                    if (!(obj instanceof PGPEncryptedDataList)) {
                        throw new ProcessException("Invalid OpenPGP data");
                    }
                }
                PGPEncryptedDataList encList = (PGPEncryptedDataList) obj;

                try {
                    PGPPrivateKey privateKey = null;
                    PGPPublicKeyEncryptedData encData = null;

                    // Find the secret key in the encrypted data
                    Iterator it = encList.getEncryptedDataObjects();
                    while (privateKey == null && it.hasNext()) {
                        obj = it.next();
                        if (!(obj instanceof PGPPublicKeyEncryptedData)) {
                            throw new ProcessException("Invalid OpenPGP data");
                        }
                        encData = (PGPPublicKeyEncryptedData) obj;

                        // Check each encrypted data object to see if it contains the key ID for the secret key -> private key
                        try {
                            privateKey = getDecryptedPrivateKey(provider, secretKeyringFile, encData.getKeyID(), passphrase);
                        } catch (PGPException e) {
                            // TODO: Log (expected) exception?
                        }
                    }
                    if (privateKey == null) {
                        throw new ProcessException("Secret keyring does not contain the key required to decrypt");
                    }

                    // Read in the encrypted data stream and decrypt it
                    final PublicKeyDataDecryptorFactory dataDecryptor = new JcePublicKeyDataDecryptorFactoryBuilder().setProvider(provider).build(privateKey);
                    try (InputStream clear = encData.getDataStream(dataDecryptor)) {
                        // Create a plain object factory
                        JcaPGPObjectFactory plainFact = new JcaPGPObjectFactory(clear);

                        Object message = plainFact.nextObject();

                        // Check the message type and act accordingly

                        // If compressed, decompress
                        if (message instanceof PGPCompressedData) {
                            PGPCompressedData cData = (PGPCompressedData) message;
                            JcaPGPObjectFactory pgpFact = new JcaPGPObjectFactory(cData.getDataStream());

                            message = pgpFact.nextObject();
                        }

                        // If the message is literal data, read it and process to the out stream
                        if (message instanceof PGPLiteralData) {
                            PGPLiteralData literalData = (PGPLiteralData) message;

                            try (InputStream lis = literalData.getInputStream()) {
                                final byte[] buffer = new byte[BLOCK_SIZE];
                                int len;
                                while ((len = lis.read(buffer)) >= 0) {
                                    out.write(buffer, 0, len);
                                }
                            }
                        } else if (message instanceof PGPOnePassSignatureList) {
                            // TODO: This is legacy code but should verify signature list here
                            throw new PGPException("encrypted message contains a signed message - not literal data.");
                        } else {
                            throw new PGPException("message is not a simple encrypted file - type unknown.");
                        }

                        if (encData.isIntegrityProtected()) {
                            if (!encData.verify()) {
                                throw new PGPException("Failed message integrity check");
                            }
                        } else {
                            logger.warn("No message integrity check");
                        }
                    }
                } catch (Exception e) {
                    throw new ProcessException(e.getMessage());
                }
            }
        }

    }

    private static class OpenPGPEncryptCallback implements StreamCallback {

        private String algorithm;
        private String provider;
        private String publicKeyring;
        private String userId;
        private String filename;

        OpenPGPEncryptCallback(final String algorithm, final String provider, final String keyring, final String userId, final String filename) {
            this.algorithm = algorithm;
            this.provider = provider;
            this.publicKeyring = keyring;
            this.userId = userId;
            this.filename = filename;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            PGPPublicKey publicKey;
            final boolean isArmored = EncryptContent.isPGPArmoredAlgorithm(algorithm);

            try {
                publicKey = getPublicKey(userId, publicKeyring);
            } catch (Exception e) {
                throw new ProcessException("Invalid public keyring - " + e.getMessage());
            }

            try {
                OutputStream output = out;
                if (isArmored) {
                    output = new ArmoredOutputStream(out);
                }

                try {
                    // TODO: Refactor internal symmetric encryption algorithm to be customizable
                    PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(
                            new JcePGPDataEncryptorBuilder(PGPEncryptedData.AES_128).setWithIntegrityPacket(true).setSecureRandom(new SecureRandom()).setProvider(provider));

                    encryptedDataGenerator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(publicKey).setProvider(provider));

                    // TODO: Refactor shared encryption code to utility
                    try (OutputStream encryptedOut = encryptedDataGenerator.open(output, new byte[BUFFER_SIZE])) {
                        PGPCompressedDataGenerator compressedDataGenerator = new PGPCompressedDataGenerator(PGPCompressedData.ZIP, Deflater.BEST_SPEED);
                        try (OutputStream compressedOut = compressedDataGenerator.open(encryptedOut, new byte[BUFFER_SIZE])) {
                            PGPLiteralDataGenerator literalDataGenerator = new PGPLiteralDataGenerator();
                            try (OutputStream literalOut = literalDataGenerator.open(compressedOut, PGPLiteralData.BINARY, filename, new Date(), new byte[BUFFER_SIZE])) {

                                final byte[] buffer = new byte[BLOCK_SIZE];
                                int len;
                                while ((len = in.read(buffer)) >= 0) {
                                    literalOut.write(buffer, 0, len);
                                }
                            }
                        }
                    }
                } finally {
                    if (isArmored) {
                        output.close();
                    }
                }
            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }
        }
    }
}
