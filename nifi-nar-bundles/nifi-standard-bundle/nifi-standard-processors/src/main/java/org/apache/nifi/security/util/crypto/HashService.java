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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a generic service for cryptographic hashing. It is used in
 * {@link org.apache.nifi.processors.standard.CalculateAttributeHash} and
 * {@link org.apache.nifi.processors.standard.HashContent}.
 * <p>
 * See also:
 * * {@link HashAlgorithm}
 */
public class HashService {
    private static final Logger logger = LoggerFactory.getLogger(HashService.class);

    /**
     * Returns the hex-encoded hash of the specified value.
     *
     * @param algorithm the hash algorithm to use
     * @param value     the value to hash (cannot be {@code null} but can be an empty String)
     * @param charset   the charset to use
     * @return the hash value in hex
     */
    public static String hashValue(HashAlgorithm algorithm, String value, Charset charset) {
        byte[] rawHash = hashValueRaw(algorithm, value, charset);
        return Hex.encodeHexString(rawHash);
    }

    /**
     * Returns the hex-encoded hash of the specified value. The default charset ({@code StandardCharsets.UTF_8}) is used.
     *
     * @param algorithm the hash algorithm to use
     * @param value     the value to hash (cannot be {@code null} but can be an empty String)
     * @return the hash value in hex
     */
    public static String hashValue(HashAlgorithm algorithm, String value) {
        return hashValue(algorithm, value, StandardCharsets.UTF_8);
    }

    /**
     * Returns the raw {@code byte[]} hash of the specified value.
     *
     * @param algorithm the hash algorithm to use
     * @param value     the value to hash (cannot be {@code null} but can be an empty String)
     * @param charset   the charset to use
     * @return the hash value in bytes
     */
    public static byte[] hashValueRaw(HashAlgorithm algorithm, String value, Charset charset) {
        if (value == null) {
            throw new IllegalArgumentException("The value cannot be null");
        }
        return hashValueRaw(algorithm, value.getBytes(charset));
    }

    /**
     * Returns the raw {@code byte[]} hash of the specified value. The default charset ({@code StandardCharsets.UTF_8}) is used.
     *
     * @param algorithm the hash algorithm to use
     * @param value     the value to hash (cannot be {@code null} but can be an empty String)
     * @return the hash value in bytes
     */
    public static byte[] hashValueRaw(HashAlgorithm algorithm, String value) {
        return hashValueRaw(algorithm, value, StandardCharsets.UTF_8);
    }

    /**
     * Returns the raw {@code byte[]} hash of the specified value.
     *
     * @param algorithm the hash algorithm to use
     * @param value     the value to hash
     * @return the hash value in bytes
     */
    public static byte[] hashValueRaw(HashAlgorithm algorithm, byte[] value) {
        if (algorithm == null) {
            throw new IllegalArgumentException("The hash algorithm cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("The value cannot be null");
        }
        if (algorithm.isBlake2()) {
            return blake2Hash(algorithm, value);
        } else {
            return traditionalHash(algorithm, value);
        }
    }

    private static byte[] traditionalHash(HashAlgorithm algorithm, byte[] value) {
        return DigestUtils.getDigest(algorithm.getName()).digest(value);
    }

    private static byte[] blake2Hash(HashAlgorithm algorithm, byte[] value) {
        int digestLengthBytes = algorithm.getDigestBytesLength();
        Blake2bDigest blake2bDigest = new Blake2bDigest(digestLengthBytes * 8);
        byte[] rawHash = new byte[blake2bDigest.getDigestSize()];
        blake2bDigest.update(value, 0, value.length);
        blake2bDigest.doFinal(rawHash, 0);
        return rawHash;
    }
}
