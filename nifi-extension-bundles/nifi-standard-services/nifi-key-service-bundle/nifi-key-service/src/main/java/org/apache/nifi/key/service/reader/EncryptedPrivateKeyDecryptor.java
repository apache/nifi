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
package org.apache.nifi.key.service.reader;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Decryptor for PKCS8 Encrypted Private Key Information supporting PBES2 with PBKDF2 and other password-based encryption algorithms available from security providers
 */
class EncryptedPrivateKeyDecryptor {
    private static final String PBES2_OBJECT_IDENTIFIER = "1.2.840.113549.1.5.13";

    private static final String PBKDF2_OBJECT_IDENTIFIER = "1.2.840.113549.1.5.12";

    private static final String DEFAULT_KEY_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA1";

    private static final Map<String, String> KEY_DERIVATION_ALGORITHMS = Map.of(
            "1.2.840.113549.2.7", DEFAULT_KEY_DERIVATION_ALGORITHM,
            "1.2.840.113549.2.8", "PBKDF2WithHmacSHA224",
            "1.2.840.113549.2.9", "PBKDF2WithHmacSHA256",
            "1.2.840.113549.2.10", "PBKDF2WithHmacSHA384",
            "1.2.840.113549.2.11", "PBKDF2WithHmacSHA512",
            "1.2.840.113549.2.12", "PBKDF2WithHmacSHA512/224",
            "1.2.840.113549.2.13", "PBKDF2WithHmacSHA512/256"
    );

    private static final int PBKDF2_OPTIONAL_PARAMETERS_INDEX = 2;

    private static final int BYTE_BITS = 8;

    private static final String CIPHER_TRANSFORMATION_FORMAT = "%s/CBC/PKCS5Padding";

    /**
     * Decrypt PKCS8 Encrypted Private Key Information
     *
     * @param encryptedPrivateKeyInfo DER encoded Encrypted Private Key Information
     * @param keyPassword Password for decryption
     * @return DER encoded PKCS8 Private Key Information
     */
    byte[] decrypt(final byte[] encryptedPrivateKeyInfo, final char[] keyPassword) {
        final DerElement encryptedPrivateKeyInfoElement = DerElement.read(encryptedPrivateKeyInfo);
        final DerElement encryptionAlgorithm = encryptedPrivateKeyInfoElement.getElement(0);
        final String encryptionAlgorithmIdentifier = encryptionAlgorithm.getElement(0).getObjectIdentifier();

        final byte[] privateKeyInfo;
        if (PBES2_OBJECT_IDENTIFIER.equals(encryptionAlgorithmIdentifier)) {
            final byte[] encryptedData = encryptedPrivateKeyInfoElement.getElement(1).getOctetString();
            privateKeyInfo = decryptPbes2(encryptionAlgorithm.getElement(1), encryptedData, keyPassword);
        } else {
            privateKeyInfo = decryptPasswordBasedEncryption(encryptedPrivateKeyInfo, keyPassword);
        }

        return privateKeyInfo;
    }

    private byte[] decryptPbes2(final DerElement parameters, final byte[] encryptedData, final char[] keyPassword) {
        final DerElement keyDerivationFunction = parameters.getElement(0);
        final String keyDerivationFunctionIdentifier = keyDerivationFunction.getElement(0).getObjectIdentifier();
        if (!PBKDF2_OBJECT_IDENTIFIER.equals(keyDerivationFunctionIdentifier)) {
            throw new PrivateKeyException("PBES2 Key Derivation Function [%s] not supported".formatted(keyDerivationFunctionIdentifier));
        }

        final DerElement encryptionScheme = parameters.getElement(1);
        final String encryptionSchemeIdentifier = encryptionScheme.getElement(0).getObjectIdentifier();
        final PrivateKeyCipherAlgorithm cipherAlgorithm = PrivateKeyCipherAlgorithm.findByObjectIdentifier(encryptionSchemeIdentifier)
                .orElseThrow(() -> new PrivateKeyException("PBES2 Encryption Scheme [%s] not supported".formatted(encryptionSchemeIdentifier)));
        final byte[] initializationVector = encryptionScheme.getElement(1).getOctetString();

        final byte[] derivedKey = deriveKey(keyDerivationFunction.getElement(1), keyPassword, cipherAlgorithm.getKeyLength());
        try {
            return decrypt(cipherAlgorithm, derivedKey, initializationVector, encryptedData);
        } finally {
            Arrays.fill(derivedKey, (byte) 0);
        }
    }

    private byte[] deriveKey(final DerElement pbkdf2Parameters, final char[] keyPassword, final int keyLength) {
        final byte[] salt = pbkdf2Parameters.getElement(0).getOctetString();
        if (salt.length == 0) {
            throw new PrivateKeyException("PBKDF2 Salt not found");
        }

        final BigInteger iterationCount = pbkdf2Parameters.getElement(1).getInteger();
        if (iterationCount.signum() < 1 || iterationCount.bitLength() >= Integer.SIZE) {
            throw new PrivateKeyException("PBKDF2 Iteration Count [%s] not valid".formatted(iterationCount));
        }

        String keyDerivationAlgorithm = DEFAULT_KEY_DERIVATION_ALGORITHM;
        final List<DerElement> elements = pbkdf2Parameters.getElements();
        for (final DerElement optionalParameter : elements.subList(PBKDF2_OPTIONAL_PARAMETERS_INDEX, elements.size())) {
            if (optionalParameter.getTag() == DerElement.SEQUENCE_TAG) {
                final String pseudoRandomFunctionIdentifier = optionalParameter.getElement(0).getObjectIdentifier();
                keyDerivationAlgorithm = KEY_DERIVATION_ALGORITHMS.get(pseudoRandomFunctionIdentifier);
                if (keyDerivationAlgorithm == null) {
                    throw new PrivateKeyException("PBKDF2 Pseudorandom Function [%s] not supported".formatted(pseudoRandomFunctionIdentifier));
                }
            }
        }

        final PBEKeySpec keySpec = new PBEKeySpec(keyPassword, salt, iterationCount.intValue(), keyLength * BYTE_BITS);
        try {
            final SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(keyDerivationAlgorithm);
            return secretKeyFactory.generateSecret(keySpec).getEncoded();
        } catch (final GeneralSecurityException e) {
            throw new PrivateKeyException("PBKDF2 Key Derivation with [%s] failed".formatted(keyDerivationAlgorithm), e);
        } finally {
            keySpec.clearPassword();
        }
    }

    private byte[] decrypt(final PrivateKeyCipherAlgorithm cipherAlgorithm, final byte[] key, final byte[] initializationVector, final byte[] encrypted) {
        final String keyAlgorithm = cipherAlgorithm.getKeyAlgorithm();
        try {
            final Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION_FORMAT.formatted(keyAlgorithm));
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, keyAlgorithm), new IvParameterSpec(initializationVector));
            return cipher.doFinal(encrypted);
        } catch (final GeneralSecurityException e) {
            throw new PrivateKeyException("Decrypting Private Key with [%s] failed".formatted(cipherAlgorithm.getCipherName()), e);
        }
    }

    private byte[] decryptPasswordBasedEncryption(final byte[] encryptedPrivateKeyInfo, final char[] keyPassword) {
        final EncryptedPrivateKeyInfo privateKeyInfo;
        try {
            privateKeyInfo = new EncryptedPrivateKeyInfo(encryptedPrivateKeyInfo);
        } catch (final IOException e) {
            throw new PrivateKeyException("Encrypted Private Key parsing failed", e);
        }

        final String algorithm = privateKeyInfo.getAlgName();
        final PBEKeySpec keySpec = new PBEKeySpec(keyPassword);
        try {
            final SecretKey secretKey = SecretKeyFactory.getInstance(algorithm).generateSecret(keySpec);
            final Cipher cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, privateKeyInfo.getAlgParameters());
            return privateKeyInfo.getKeySpec(cipher).getEncoded();
        } catch (final NoSuchAlgorithmException e) {
            throw new PrivateKeyException("Encryption Algorithm [%s] not supported".formatted(algorithm), e);
        } catch (final GeneralSecurityException e) {
            throw new PrivateKeyException("Decrypting Private Key with [%s] failed".formatted(algorithm), e);
        } finally {
            keySpec.clearPassword();
        }
    }
}
