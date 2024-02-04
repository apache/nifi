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
package org.apache.nifi.processors.cipher.compatibility;

import org.apache.nifi.components.DescribedValue;

import java.nio.charset.StandardCharsets;

/**
 * Compatibility Mode Key Derivation Strategy supporting decryption with legacy schemes
 */
public enum CompatibilityModeKeyDerivationStrategy implements DescribedValue {
    /** Strategy based on OpenSSL EVP_BytesToKey function */
    OPENSSL_EVP_BYTES_TO_KEY(
            "OpenSSL Envelope BytesToKey using a digest algorithm with one iteration and optional salt of eight bytes",
            0,
            8,
            16,
            "Salted__".getBytes(StandardCharsets.US_ASCII)
    ),

    /** Strategy based on default configuration of org.jasypt.encryption.pbe.StandardPBEByteEncryptor */
    JASYPT_STANDARD(
            "Jasypt Java Simplified Encryption using a digest algorithm with 1000 iterations and required salt of eight or sixteen bytes",
            1000,
            8,
            16,
            new byte[0]
    );

    private final String description;
    private final int iterations;
    private final byte[] saltHeader;
    private final int saltStandardLength;
    private final int saltBufferLength;

    CompatibilityModeKeyDerivationStrategy(
            final String description,
            final int iterations,
            final int saltStandardLength,
            final int saltBufferLength,
            final byte[] saltHeader
    ) {
        this.description = description;
        this.iterations = iterations;
        this.saltStandardLength = saltStandardLength;
        this.saltBufferLength = saltBufferLength;
        this.saltHeader = saltHeader;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }

    public int getIterations() {
        return iterations;
    }

    public int getSaltStandardLength() {
        return saltStandardLength;
    }

    public int getSaltBufferLength() {
        return saltBufferLength;
    }

    public byte[] getSaltHeader() {
        return saltHeader;
    }
}
