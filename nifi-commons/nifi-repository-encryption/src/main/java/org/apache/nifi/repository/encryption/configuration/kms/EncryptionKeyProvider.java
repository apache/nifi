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
package org.apache.nifi.repository.encryption.configuration.kms;

import org.apache.nifi.security.kms.FileBasedKeyProvider;
import org.apache.nifi.security.kms.KeyStoreKeyProvider;
import org.apache.nifi.security.kms.StaticKeyProvider;

/**
 * Configuration options for Repository Encryption Key Provider
 */
public enum EncryptionKeyProvider {
    /** File Properties Key Provider */
    FILE_PROPERTIES,

    /** KeyStore Key Provider */
    KEYSTORE,

    /** NiFi Properties Key Provider */
    NIFI_PROPERTIES;

    /**
     * Resolve Encryption Key Provider based on implementation class name ending with known names
     *
     * @param implementationClass Implementation class name
     * @return Encryption Key Provider
     * @throw IllegalArgumentException Thrown when implementation class name does not match a known class
     */
    public static EncryptionKeyProvider fromImplementationClass(final String implementationClass) {
        EncryptionKeyProvider encryptionKeyProvider;

        if (implementationClass.endsWith(FileBasedKeyProvider.class.getSimpleName())) {
            encryptionKeyProvider = EncryptionKeyProvider.FILE_PROPERTIES;
        } else if (implementationClass.endsWith(KeyStoreKeyProvider.class.getSimpleName())) {
            encryptionKeyProvider = EncryptionKeyProvider.KEYSTORE;
        } else if (implementationClass.endsWith(StaticKeyProvider.class.getSimpleName())) {
            encryptionKeyProvider = EncryptionKeyProvider.NIFI_PROPERTIES;
        } else {
            final String message = String.format("Key Provider Class [%s] not supported", implementationClass);
            throw new IllegalArgumentException(message);
        }

        return encryptionKeyProvider;
    }
}
