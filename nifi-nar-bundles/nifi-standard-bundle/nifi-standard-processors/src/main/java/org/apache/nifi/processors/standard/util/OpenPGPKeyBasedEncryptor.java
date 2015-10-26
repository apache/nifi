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
package org.apache.nifi.processors.standard.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;

public class OpenPGPKeyBasedEncryptor implements Encryptor {

    private String algorithm;
    private String provider;
    private String keyring;
    private String userId;
    private char[] passphrase;
    private String filename;

    public static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";

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

    /*
     * Validate secret keyring passphrase
     */
    public static boolean validateKeyring(String provider, String secretKeyringFile, char[] passphrase) throws IOException, PGPException, NoSuchProviderException {
        try (InputStream fin = Files.newInputStream(Paths.get(secretKeyringFile)); InputStream pin = PGPUtil.getDecoderStream(fin)) {
            PGPSecretKeyRingCollection pgpsec = new PGPSecretKeyRingCollection(pin);
            Iterator ringit = pgpsec.getKeyRings();
            while (ringit.hasNext()) {
                PGPSecretKeyRing secretkeyring = (PGPSecretKeyRing) ringit.next();
                PGPSecretKey secretkey = secretkeyring.getSecretKey();
                secretkey.extractPrivateKey(passphrase, provider);
                return true;
            }
            return false;
        }

    }

    /*
     * Get the public key for a specific user id from a keyring.
     */
    @SuppressWarnings("rawtypes")
    public static PGPPublicKey getPublicKey(String userId, String publicKeyring) throws IOException, PGPException {
        PGPPublicKey pubkey = null;
        try (InputStream fin = Files.newInputStream(Paths.get(publicKeyring)); InputStream pin = PGPUtil.getDecoderStream(fin)) {
            PGPPublicKeyRingCollection pgppub = new PGPPublicKeyRingCollection(pin);

            Iterator ringit = pgppub.getKeyRings();
            while (ringit.hasNext()) {
                PGPPublicKeyRing kring = (PGPPublicKeyRing) ringit.next();

                Iterator keyit = kring.getPublicKeys();
                while (keyit.hasNext()) {
                    pubkey = (PGPPublicKey) keyit.next();
                    boolean userIdMatch = false;

                    Iterator userit = pubkey.getUserIDs();
                    while (userit.hasNext()) {
                        String id = userit.next().toString();
                        if (id.contains(userId)) {
                            userIdMatch = true;
                            break;
                        }
                    }
                    if (pubkey.isEncryptionKey() && userIdMatch) {
                        return pubkey;
                    }
                }
            }
        }
        return null;
    }

    private static class OpenPGPDecryptCallback implements StreamCallback {

        private String provider;
        private String secretKeyring;
        private char[] passphrase;

        OpenPGPDecryptCallback(final String provider, final String keyring, final char[] passphrase) {
            this.provider = provider;
            this.secretKeyring = keyring;
            this.passphrase = passphrase;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try (InputStream pgpin = PGPUtil.getDecoderStream(in)) {
                PGPObjectFactory pgpFactory = new PGPObjectFactory(pgpin);

                Object obj = pgpFactory.nextObject();
                if (!(obj instanceof PGPEncryptedDataList)) {
                    obj = pgpFactory.nextObject();
                    if (!(obj instanceof PGPEncryptedDataList)) {
                        throw new ProcessException("Invalid OpenPGP data");
                    }
                }
                PGPEncryptedDataList encList = (PGPEncryptedDataList) obj;

                PGPSecretKeyRingCollection pgpSecretKeyring;
                try (InputStream secretKeyringIS = Files.newInputStream(Paths.get(secretKeyring)); InputStream pgpIS = PGPUtil.getDecoderStream(secretKeyringIS)) {
                    // open secret keyring file
                    pgpSecretKeyring = new PGPSecretKeyRingCollection(pgpIS);
                } catch (Exception e) {
                    throw new ProcessException("Invalid secret keyring - " + e.getMessage());
                }

                try {
                    PGPPrivateKey privateKey = null;
                    PGPPublicKeyEncryptedData encData = null;

                    // find the secret key in the encrypted data
                    Iterator it = encList.getEncryptedDataObjects();
                    while (privateKey == null && it.hasNext()) {
                        obj = it.next();
                        if (!(obj instanceof PGPPublicKeyEncryptedData)) {
                            throw new ProcessException("Invalid OpenPGP data");
                        }
                        encData = (PGPPublicKeyEncryptedData) obj;
                        PGPSecretKey secretkey = pgpSecretKeyring.getSecretKey(encData.getKeyID());
                        if (secretkey != null) {
                            privateKey = secretkey.extractPrivateKey(passphrase, provider);
                        }
                    }
                    if (privateKey == null) {
                        throw new ProcessException("Secret keyring does not contain the key required to decrypt");
                    }

                    try (InputStream clearData = encData.getDataStream(privateKey, provider)) {
                        PGPObjectFactory clearFactory = new PGPObjectFactory(clearData);

                        obj = clearFactory.nextObject();
                        if (obj instanceof PGPCompressedData) {
                            PGPCompressedData compData = (PGPCompressedData) obj;
                            clearFactory = new PGPObjectFactory(compData.getDataStream());
                            obj = clearFactory.nextObject();
                        }
                        PGPLiteralData literal = (PGPLiteralData) obj;

                        try (InputStream lis = literal.getInputStream()) {
                            final byte[] buffer = new byte[4096];
                            int len;
                            while ((len = lis.read(buffer)) >= 0) {
                                out.write(buffer, 0, len);
                            }
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
            try {
                publicKey = getPublicKey(userId, publicKeyring);
            } catch (Exception e) {
                throw new ProcessException("Invalid public keyring - " + e.getMessage());
            }

            try {
                SecureRandom secureRandom = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM);

                OutputStream output = out;
                if (EncryptContent.isPGPArmoredAlgorithm(algorithm)) {
                    output = new ArmoredOutputStream(out);
                }

                try {
                    PGPEncryptedDataGenerator encGenerator = new PGPEncryptedDataGenerator(PGPEncryptedData.CAST5, false, secureRandom, provider);
                    encGenerator.addMethod(publicKey);
                    try (OutputStream encOut = encGenerator.open(output, new byte[65536])) {

                        PGPCompressedDataGenerator compData = new PGPCompressedDataGenerator(PGPCompressedData.ZIP, Deflater.BEST_SPEED);
                        try (OutputStream compOut = compData.open(encOut, new byte[65536])) {

                            PGPLiteralDataGenerator literal = new PGPLiteralDataGenerator();
                            try (OutputStream literalOut = literal.open(compOut, PGPLiteralData.BINARY, filename, new Date(), new byte[65536])) {

                                final byte[] buffer = new byte[4096];
                                int len;
                                while ((len = in.read(buffer)) >= 0) {
                                    literalOut.write(buffer, 0, len);
                                }

                            }
                        }
                    }
                } finally {
                    if (EncryptContent.isPGPArmoredAlgorithm(algorithm)) {
                        output.close();
                    }
                }
            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }
        }

    }
}
