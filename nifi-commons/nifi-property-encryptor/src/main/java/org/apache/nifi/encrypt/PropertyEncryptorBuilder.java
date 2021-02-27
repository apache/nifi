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
package org.apache.nifi.encrypt;

import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.apache.nifi.security.util.crypto.KeyedCipherProvider;
import org.apache.nifi.security.util.crypto.PBECipherProvider;

import javax.crypto.SecretKey;
import java.util.Objects;

/**
 * Property Encryptor Builder
 */
public class PropertyEncryptorBuilder {
    private static final PropertySecretKeyProvider SECRET_KEY_PROVIDER = new StandardPropertySecretKeyProvider();

    private final String password;

    private String algorithm = PropertyEncryptionMethod.NIFI_ARGON2_AES_GCM_256.toString();

    /**
     * Property Encryptor Builder with required password
     *
     * @param password Password required
     */
    public PropertyEncryptorBuilder(final String password) {
        Objects.requireNonNull(password, "Password required");
        this.password = password;
    }

    /**
     * Set Algorithm as either Property Encryption Method or Encryption Method
     *
     * @param algorithm Algorithm
     * @return Property Encryptor Builder
     */
    public PropertyEncryptorBuilder setAlgorithm(final String algorithm) {
        Objects.requireNonNull(algorithm, "Algorithm required");
        this.algorithm = algorithm;
        return this;
    }

    /**
     * Build Property Encryptor using current configuration
     *
     * @return Property Encryptor
     */
    public PropertyEncryptor build() {
        final PropertyEncryptionMethod propertyEncryptionMethod = findPropertyEncryptionAlgorithm(algorithm);
        if (propertyEncryptionMethod == null) {
            return getPasswordBasedCipherPropertyEncryptor();
        } else {
            final KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
            final SecretKey secretKey = SECRET_KEY_PROVIDER.getSecretKey(propertyEncryptionMethod, password);
            final EncryptionMethod encryptionMethod = propertyEncryptionMethod.getEncryptionMethod();
            return new KeyedCipherPropertyEncryptor(keyedCipherProvider, encryptionMethod, secretKey);
        }
    }

    @SuppressWarnings("deprecation")
    private PasswordBasedCipherPropertyEncryptor getPasswordBasedCipherPropertyEncryptor() {
        final EncryptionMethod encryptionMethod = findEncryptionMethod(algorithm);
        if (encryptionMethod.isPBECipher()) {
            final PBECipherProvider cipherProvider = new org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider();
            return new PasswordBasedCipherPropertyEncryptor(cipherProvider, encryptionMethod, password);
        } else {
            final String message = String.format("Algorithm [%s] not supported for Sensitive Properties", encryptionMethod.getAlgorithm());
            throw new UnsupportedOperationException(message);
        }
    }

    private PropertyEncryptionMethod findPropertyEncryptionAlgorithm(final String algorithm) {
        PropertyEncryptionMethod foundPropertyEncryptionMethod = null;

        for (final PropertyEncryptionMethod propertyEncryptionMethod : PropertyEncryptionMethod.values()) {
            if (propertyEncryptionMethod.toString().equals(algorithm)) {
                foundPropertyEncryptionMethod = propertyEncryptionMethod;
                break;
            }
        }

        return foundPropertyEncryptionMethod;
    }

    private EncryptionMethod findEncryptionMethod(final String algorithm) {
        final EncryptionMethod encryptionMethod = EncryptionMethod.forAlgorithm(algorithm);
        if (encryptionMethod == null) {
            final String message = String.format("Encryption Method not found for Algorithm [%s]", algorithm);
            throw new IllegalArgumentException(message);
        }
        return encryptionMethod;
    }
}
