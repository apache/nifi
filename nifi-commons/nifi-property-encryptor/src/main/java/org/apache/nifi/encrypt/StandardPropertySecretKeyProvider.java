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

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Objects;

/**
 * Standard implementation of Property Secret Key Provider implementing supported Key Derivation Functions
 */
class StandardPropertySecretKeyProvider implements PropertySecretKeyProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardPropertySecretKeyProvider.class);

    /** Standard Application Salt supporting deterministic encrypted property comparison */
    private static final byte[] APPLICATION_SALT = new byte[]{'N', 'i', 'F', 'i', ' ', 'S', 't', 'a', 't', 'i', 'c', ' ', 'S', 'a', 'l', 't'};

    /** Argon2 Parameter Specification configured with settings introduced in 1.12.0 */
    private static final Argon2DerivedKeyParameterSpec ARGON2_PARAMETER_SPEC = new Argon2DerivedKeyParameterSpec(65536, 5, 8, APPLICATION_SALT);

    /** PBKDF2 Parameter Specification configured with settings introduced in 0.5.0 */
    private static final Pbkdf2DerivedKeyParameterSpec PBKDF2_PARAMETER_SPEC = new Pbkdf2DerivedKeyParameterSpec(160000, APPLICATION_SALT);

    private static final int MINIMUM_PASSWORD_LENGTH = 12;

    private static final String PASSWORD_LENGTH_MESSAGE = String.format("Key Password length less than required [%d]", MINIMUM_PASSWORD_LENGTH);

    private static final String SECRET_KEY_ALGORITHM = "AES";

    private static final Argon2DerivedKeyProvider argon2DerivedKeyProvider = new Argon2DerivedKeyProvider();

    private static final Pbkdf2DerivedKeyProvider pbkdf2DerivedKeyProvider = new Pbkdf2DerivedKeyProvider();

    /**
     * Get Secret Key using Property Encryption Method with provided password
     *
     * @param propertyEncryptionMethod Property Encryption Method
     * @param password                 Password used to derive Secret Key
     * @return Derived Secret Key
     */
    @Override
    public SecretKey getSecretKey(final PropertyEncryptionMethod propertyEncryptionMethod, final String password) {
        Objects.requireNonNull(propertyEncryptionMethod, "Property Encryption Method is required");
        Objects.requireNonNull(password, "Password is required");
        if (password.length() < MINIMUM_PASSWORD_LENGTH) {
            throw new EncryptionException(PASSWORD_LENGTH_MESSAGE);
        }

        final int keyLength = propertyEncryptionMethod.getKeyLength();
        LOGGER.debug("Generating [{}-{}] Secret Key using [{}]", SECRET_KEY_ALGORITHM, keyLength, propertyEncryptionMethod.name());

        final DerivedKey derivedKey = getDerivedKey(propertyEncryptionMethod, password);
        return new SecretKeySpec(derivedKey.getEncoded(), SECRET_KEY_ALGORITHM);
    }

    private DerivedKey getDerivedKey(final PropertyEncryptionMethod propertyEncryptionMethod, final String password) {
        final char[] characters = password.toCharArray();
        final int derivedKeyLength = propertyEncryptionMethod.getDerivedKeyLength();

        if (PropertyEncryptionMethod.NIFI_ARGON2_AES_GCM_256 == propertyEncryptionMethod) {
            final DerivedKeySpec<Argon2DerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(characters, derivedKeyLength, SECRET_KEY_ALGORITHM, ARGON2_PARAMETER_SPEC);
            return argon2DerivedKeyProvider.getDerivedKey(derivedKeySpec);
        } else if (PropertyEncryptionMethod.NIFI_PBKDF2_AES_GCM_256 == propertyEncryptionMethod) {
            final DerivedKeySpec<Pbkdf2DerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(characters, derivedKeyLength, SECRET_KEY_ALGORITHM, PBKDF2_PARAMETER_SPEC);
            return pbkdf2DerivedKeyProvider.getDerivedKey(derivedKeySpec);
        } else {
            final String message = String.format("Property Encryption Method [%s] not supported", propertyEncryptionMethod);
            throw new EncryptionException(message);
        }
    }
}
