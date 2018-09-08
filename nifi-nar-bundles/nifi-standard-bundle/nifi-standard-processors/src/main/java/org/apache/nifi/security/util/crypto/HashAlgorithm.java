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
package org.apache.nifi.security.util.crypto;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.processors.standard.CryptographicHashAttribute;
import org.apache.nifi.processors.standard.HashContent;

/**
 * Enumeration capturing information about the cryptographic hash algorithms used in
 * {@link CryptographicHashAttribute}, {@link HashContent}, and
 * {@link HashContent} processors.
 */
public enum HashAlgorithm {

    MD2("MD2", 16, "Cryptographically broken due to collisions"),
    MD5("MD5", 16, "Cryptographically broken due to collisions"),
    SHA1("SHA-1", 20, "Cryptographically broken due to collisions"),
    SHA224("SHA-224", 28, "SHA-2 family"),
    SHA256("SHA-256", 32, "SHA-2 family"),
    SHA384("SHA-384", 48, "SHA-2 family"),
    SHA512("SHA-512", 64, "SHA-2 family"),
    SHA512_224("SHA-512/224", 28, "SHA-2 using SHA-512 with truncated output"),
    SHA512_256("SHA-512/256", 32, "SHA-2 using SHA-512 with truncated output"),
    SHA3_224("SHA3-224", 28, "Keccak-based SHA3 family"),
    SHA3_256("SHA3-256", 32, "Keccak-based SHA3 family"),
    SHA3_384("SHA3-384", 48, "Keccak-based SHA3 family"),
    SHA3_512("SHA3-512", 64, "Keccak-based SHA3 family"),
    BLAKE2_160("BLAKE2-160", 20, "Also known as Blake2b"),
    BLAKE2_256("BLAKE2-256", 32, "Also known as Blake2b"),
    BLAKE2_384("BLAKE2-384", 48, "Also known as Blake2b"),
    BLAKE2_512("BLAKE2-512", 64, "Also known as Blake2b");

    private final String name;
    private final int digestBytesLength;
    private final String description;

    private static final List<String> BROKEN_ALGORITHMS = Arrays.asList(MD2.name, MD5.name, SHA1.name);

    HashAlgorithm(String name, int digestBytesLength, String description) {
        this.name = name;
        this.digestBytesLength = digestBytesLength;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public int getDigestBytesLength() {
        return digestBytesLength;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Returns {@code true} if this algorithm is considered cryptographically secure. These determinations were made as of 2018-08-30.
     *
     * Current strong algorithms:
     *
     * * SHA-224 (SHA2)
     * * SHA-256 (SHA2)
     * * SHA-384 (SHA2)
     * * SHA-512 (SHA2)
     * * SHA-512/224 (SHA2)
     * * SHA-512/256 (SHA2)
     * * SHA3-224
     * * SHA3-256
     * * SHA3-384
     * * SHA3-512
     * * Blake2b-160
     * * Blake2b-256
     * * Blake2b-384
     * * Blake2b-512
     *
     * Current broken algorithms:
     *
     * * MD2
     * * MD5
     * * SHA-1
     *
     * @return true if the algorithm is considered strong
     */
    public boolean isStrongAlgorithm() {
        return (!BROKEN_ALGORITHMS.contains(name));
    }

    /**
     * Returns {@code true} if this hash algorithm is Blake2, as it requires different initialization through BouncyCastle.
     *
     * @return true if this algorithm is in the Blake2 family
     */
    public boolean isBlake2() {
        return name.contains("BLAKE2");
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("Algorithm Name", name);
        builder.append("Digest Length", digestBytesLength + " bytes");
        builder.append("Description", description);
        return builder.toString();
    }

    /**
     * Returns a more complete description of the algorithm for {@link org.apache.nifi.components.AllowableValue} construction.
     *
     * Ex:
     *
     * {@code description} -- Cryptographically broken due to collisions
     * {@code buildAllowableValueDescription} -- SHA-1 (20 byte output) [WARNING -- Cryptographically broken] Cryptographically broken due to collisions
     *
     * @return the description for dropdown help
     */
    public String buildAllowableValueDescription() {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" (").append(digestBytesLength).append(" byte output)");
        if (!isStrongAlgorithm()) {
            sb.append(" [WARNING -- Cryptographically broken]");
        }
        if (StringUtils.isNotBlank(description)) {
            sb.append(" ").append(description);
        }
        return sb.toString();
    }

    public static HashAlgorithm fromName(String algorithmName) {
        HashAlgorithm match = Arrays.stream(HashAlgorithm.values())
                .filter(algo -> algorithmName.equalsIgnoreCase(algo.name))
                .findAny()
                .orElse(null);
        if (match == null) {
            throw new IllegalArgumentException("No algorithm matches " + algorithmName);
        } else {
            return match;
        }
    }
}
