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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.security.util.crypto.Argon2SecureHasher;
import org.apache.nifi.security.util.crypto.KeyDerivationBcryptSecureHasher;
import org.apache.nifi.security.util.crypto.PBKDF2SecureHasher;
import org.apache.nifi.security.util.crypto.ScryptSecureHasher;
import org.apache.nifi.security.util.crypto.SecureHasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Standard implementation of Property Secret Key Provider implementing supported Key Derivation Functions
 */
class StandardPropertySecretKeyProvider implements PropertySecretKeyProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardPropertySecretKeyProvider.class);

    private static final Charset PASSWORD_CHARSET = StandardCharsets.UTF_8;

    private static final int MINIMUM_PASSWORD_LENGTH = 12;

    private static final String PASSWORD_LENGTH_MESSAGE = String.format("Key Password length less than required [%d]", MINIMUM_PASSWORD_LENGTH);

    private static final String SECRET_KEY_ALGORITHM = "AES";

    /**
     * Get Secret Key using Property Encryption Method with provided password
     *
     * @param propertyEncryptionMethod Property Encryption Method
     * @param password Password used to derive Secret Key
     * @return Derived Secret Key
     */
    @Override
    public SecretKey getSecretKey(final PropertyEncryptionMethod propertyEncryptionMethod, final String password) {
        Objects.requireNonNull(propertyEncryptionMethod, "Property Encryption Method is required");
        Objects.requireNonNull(password, "Password is required");
        if (StringUtils.length(password) < MINIMUM_PASSWORD_LENGTH) {
            throw new EncryptionException(PASSWORD_LENGTH_MESSAGE);
        }

        final KeyDerivationFunction keyDerivationFunction = propertyEncryptionMethod.getKeyDerivationFunction();
        final int keyLength = propertyEncryptionMethod.getKeyLength();
        LOGGER.debug("Generating [{}-{}] Secret Key using [{}]", SECRET_KEY_ALGORITHM, keyLength, keyDerivationFunction.getKdfName());

        final SecureHasher secureHasher = getSecureHasher(propertyEncryptionMethod);
        final byte[] passwordBinary = password.getBytes(PASSWORD_CHARSET);
        final byte[] hash = secureHasher.hashRaw(passwordBinary);
        return new SecretKeySpec(hash, SECRET_KEY_ALGORITHM);
    }

    private static SecureHasher getSecureHasher(final PropertyEncryptionMethod propertyEncryptionMethod) {
        final KeyDerivationFunction keyDerivationFunction = propertyEncryptionMethod.getKeyDerivationFunction();
        final int hashLength = propertyEncryptionMethod.getHashLength();

        if (KeyDerivationFunction.ARGON2.equals(keyDerivationFunction)) {
            return new Argon2SecureHasher(hashLength);
        } else if (KeyDerivationFunction.BCRYPT.equals(keyDerivationFunction)) {
            return new KeyDerivationBcryptSecureHasher(hashLength);
        } else if (KeyDerivationFunction.PBKDF2.equals(keyDerivationFunction)) {
            return new PBKDF2SecureHasher(hashLength);
        } else if (KeyDerivationFunction.SCRYPT.equals(keyDerivationFunction)) {
            return new ScryptSecureHasher(hashLength);
        } else {
            final String message = String.format("Key Derivation Function [%s] not supported", keyDerivationFunction.getKdfName());
            throw new EncryptionException(message);
        }
    }
}
