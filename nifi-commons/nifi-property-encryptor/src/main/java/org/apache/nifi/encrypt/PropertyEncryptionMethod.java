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

/**
 * Property Encryption Method enumerates supported values
 */
public enum PropertyEncryptionMethod {
    NIFI_ARGON2_AES_GCM_256(256),

    NIFI_PBKDF2_AES_GCM_256(256);

    private static final int BYTE_LENGTH_DIVISOR = 8;

    private final int keyLength;

    private final int derivedKeyLength;

    PropertyEncryptionMethod(final int keyLength) {
        this.keyLength = keyLength;
        this.derivedKeyLength = keyLength / BYTE_LENGTH_DIVISOR;
    }

    /**
     * Get key length in bits
     *
     * @return Key length in bites
     */
    public int getKeyLength() {
        return keyLength;
    }

    /**
     * Get derived key length in bytes
     *
     * @return Derived key length in bytes
     */
    public int getDerivedKeyLength() {
        return derivedKeyLength;
    }
}
