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

import java.util.Optional;

/**
 * Cipher Algorithms using Cipher Block Chaining mode supported for PBES2 Encryption Schemes
 */
enum PrivateKeyCipherAlgorithm {
    AES_128_CBC("2.16.840.1.101.3.4.1.2", "AES-128-CBC", "AES", 16),

    AES_256_CBC("2.16.840.1.101.3.4.1.42", "AES-256-CBC", "AES", 32),

    DES_EDE3_CBC("1.2.840.113549.3.7", "DES-EDE3-CBC", "DESede", 24);

    private final String objectIdentifier;

    private final String cipherName;

    private final String keyAlgorithm;

    private final int keyLength;

    PrivateKeyCipherAlgorithm(final String objectIdentifier, final String cipherName, final String keyAlgorithm, final int keyLength) {
        this.objectIdentifier = objectIdentifier;
        this.cipherName = cipherName;
        this.keyAlgorithm = keyAlgorithm;
        this.keyLength = keyLength;
    }

    /**
     * Get cipher name for messages
     *
     * @return Cipher name such as AES-256-CBC
     */
    String getCipherName() {
        return cipherName;
    }

    /**
     * Get key algorithm for Secret Key and Cipher transformation
     *
     * @return Key algorithm such as AES
     */
    String getKeyAlgorithm() {
        return keyAlgorithm;
    }

    /**
     * Get key length in bytes
     *
     * @return Key length in bytes
     */
    int getKeyLength() {
        return keyLength;
    }

    /**
     * Find Cipher Algorithm using Object Identifier from PBES2 Encryption Scheme parameters
     *
     * @param objectIdentifier Object Identifier
     * @return Cipher Algorithm or empty when not supported
     */
    static Optional<PrivateKeyCipherAlgorithm> findByObjectIdentifier(final String objectIdentifier) {
        for (final PrivateKeyCipherAlgorithm cipherAlgorithm : values()) {
            if (cipherAlgorithm.objectIdentifier.equals(objectIdentifier)) {
                return Optional.of(cipherAlgorithm);
            }
        }

        return Optional.empty();
    }
}
