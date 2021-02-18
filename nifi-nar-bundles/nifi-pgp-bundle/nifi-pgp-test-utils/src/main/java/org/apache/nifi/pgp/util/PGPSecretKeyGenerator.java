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
package org.apache.nifi.pgp.util;

import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPKeyPair;
import org.bouncycastle.openpgp.PGPKeyRingGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.operator.PBESecretKeyEncryptor;
import org.bouncycastle.openpgp.operator.PGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.PGPDigestCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPKeyPair;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyEncryptorBuilder;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

/**
 * Pretty Good Privacy Secret Key Generator utilities
 */
public class PGPSecretKeyGenerator {
    private static final String RSA_KEY_ALGORITHM = "RSA";

    private static final String DSA_KEY_ALGORITHM = "DSA";

    private static final int DSA_KEY_SIZE = 1024;

    private static final String ELGAMAL_KEY_ALGORITHM = "ELGAMAL";

    private static final String KEY_IDENTITY = PGPSecretKey.class.getSimpleName();

    private static final int KEY_ENCRYPTION_ALGORITHM = PGPEncryptedData.AES_256;

    private static final int HASH_ALGORITHM = HashAlgorithmTags.SHA1;

    /**
     * Generate Secret Keyring containing DSA and ElGamal Key Pairs
     *
     * @param keyEncryptionPassword Key Encryption Password
     * @return PGP Secret Keyring containing DSA and ElGamal Key Pairs
     * @throws Exception Thrown on key generation failures
     */
    public static PGPSecretKeyRing generateDsaElGamalSecretKeyRing(final char[] keyEncryptionPassword) throws Exception {
        final PGPKeyPair dsaKeyPair = getDsaKeyPair();

        final PGPDigestCalculator digestCalculator = getDigestCalculator();
        final PBESecretKeyEncryptor encryptor = getSecretKeyEncryptor(keyEncryptionPassword, digestCalculator);
        final PGPContentSignerBuilder signerBuilder = getContentSignerBuilder(dsaKeyPair.getPublicKey().getAlgorithm());
        final PGPKeyRingGenerator keyRingGenerator = new PGPKeyRingGenerator(PGPSignature.DEFAULT_CERTIFICATION, dsaKeyPair, KEY_IDENTITY, digestCalculator, null, null, signerBuilder, encryptor);

        final PGPKeyPair elGamalKeyPair = getElGamalKeyPair();
        keyRingGenerator.addSubKey(elGamalKeyPair);

        return keyRingGenerator.generateSecretKeyRing();
    }

    /**
     * Generate RSA Secret Key using encryption password
     *
     * @param keyEncryptionPassword Key Encryption Password
     * @return PGP Secret Key containing RSA Private Key
     * @throws Exception Thrown on key generation failures
     */
    public static PGPSecretKey generateRsaSecretKey(final char[] keyEncryptionPassword) throws Exception {
        final PGPKeyPair pgpKeyPair = getRsaKeyPair();

        final PGPDigestCalculator digestCalculator = getDigestCalculator();
        final PBESecretKeyEncryptor encryptor = getSecretKeyEncryptor(keyEncryptionPassword, digestCalculator);
        final PGPContentSignerBuilder signerBuilder = getContentSignerBuilder(pgpKeyPair.getPublicKey().getAlgorithm());
        return new PGPSecretKey(PGPSignature.DEFAULT_CERTIFICATION, pgpKeyPair, KEY_IDENTITY, digestCalculator, null, null, signerBuilder, encryptor);
    }

    private static PGPKeyPair getDsaKeyPair() throws NoSuchAlgorithmException, PGPException {
        final KeyPairGenerator dsaKeyPairGenerator = KeyPairGenerator.getInstance(DSA_KEY_ALGORITHM);
        dsaKeyPairGenerator.initialize(DSA_KEY_SIZE);
        final KeyPair dsaKeyPair = dsaKeyPairGenerator.generateKeyPair();
        return getPgpKeyPair(dsaKeyPair, PGPPublicKey.DSA);
    }

    private static PGPKeyPair getElGamalKeyPair() throws NoSuchAlgorithmException, PGPException {
        final KeyPairGenerator elGamalKeyPairGenerator = KeyPairGenerator.getInstance(ELGAMAL_KEY_ALGORITHM, new BouncyCastleProvider());
        final KeyPair elGamalKeyPair = elGamalKeyPairGenerator.generateKeyPair();
        return getPgpKeyPair(elGamalKeyPair, PGPPublicKey.ELGAMAL_ENCRYPT);
    }

    private static PGPKeyPair getRsaKeyPair() throws NoSuchAlgorithmException, PGPException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(RSA_KEY_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        return getPgpKeyPair(keyPair, PGPPublicKey.RSA_GENERAL);
    }

    private static PGPKeyPair getPgpKeyPair(final KeyPair keyPair, final int algorithm) throws PGPException {
        return new JcaPGPKeyPair(algorithm, keyPair, new Date());
    }

    private static PBESecretKeyEncryptor getSecretKeyEncryptor(final char[] password, final PGPDigestCalculator digestCalculator) {
        return new JcePBESecretKeyEncryptorBuilder(KEY_ENCRYPTION_ALGORITHM, digestCalculator).build(password);
    }

    private static PGPContentSignerBuilder getContentSignerBuilder(final int algorithm) {
        return new JcaPGPContentSignerBuilder(algorithm, HASH_ALGORITHM);
    }

    private static PGPDigestCalculator getDigestCalculator() throws PGPException {
        return new JcaPGPDigestCalculatorProviderBuilder().build().get(HASH_ALGORITHM);
    }
}
