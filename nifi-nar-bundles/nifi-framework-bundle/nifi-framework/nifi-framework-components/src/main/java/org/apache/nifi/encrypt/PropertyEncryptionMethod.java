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
import org.apache.nifi.security.util.KeyDerivationFunction;

/**
 * Property Encryption Method enumerates supported values in addition to {@link org.apache.nifi.security.util.EncryptionMethod}
 */
enum PropertyEncryptionMethod {
    NIFI_ARGON2_AES_GCM_128(KeyDerivationFunction.ARGON2, EncryptionMethod.AES_GCM,128),

    NIFI_ARGON2_AES_GCM_256(KeyDerivationFunction.ARGON2, EncryptionMethod.AES_GCM, 256),

    NIFI_BCRYPT_AES_GCM_128(KeyDerivationFunction.BCRYPT, EncryptionMethod.AES_GCM, 128),

    NIFI_BCRYPT_AES_GCM_256(KeyDerivationFunction.BCRYPT, EncryptionMethod.AES_GCM, 256),

    NIFI_PBKDF2_AES_GCM_128(KeyDerivationFunction.PBKDF2, EncryptionMethod.AES_GCM, 128),

    NIFI_PBKDF2_AES_GCM_256(KeyDerivationFunction.PBKDF2, EncryptionMethod.AES_GCM, 256),

    NIFI_SCRYPT_AES_GCM_128(KeyDerivationFunction.SCRYPT, EncryptionMethod.AES_GCM, 128),

    NIFI_SCRYPT_AES_GCM_256(KeyDerivationFunction.SCRYPT, EncryptionMethod.AES_GCM, 256);

    private static final int HASH_LENGTH_DIVISOR = 8;

    private final KeyDerivationFunction keyDerivationFunction;

    private final EncryptionMethod encryptionMethod;

    private final int keyLength;

    private final int hashLength;

    PropertyEncryptionMethod(final KeyDerivationFunction keyDerivationFunction,
                             final EncryptionMethod encryptionMethod,
                             final int keyLength) {
        this.keyDerivationFunction = keyDerivationFunction;
        this.encryptionMethod = encryptionMethod;
        this.keyLength = keyLength;
        this.hashLength = keyLength / HASH_LENGTH_DIVISOR;
    }

    public KeyDerivationFunction getKeyDerivationFunction() {
        return keyDerivationFunction;
    }

    public EncryptionMethod getEncryptionMethod() {
        return encryptionMethod;
    }

    public int getKeyLength() {
        return keyLength;
    }

    public int getHashLength() {
        return hashLength;
    }
}
