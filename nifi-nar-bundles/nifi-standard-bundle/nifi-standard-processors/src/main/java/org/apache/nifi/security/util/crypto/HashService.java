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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.processors.standard.CryptographicHashAttribute;
import org.apache.nifi.processors.standard.CryptographicHashContent;
import org.apache.nifi.processors.standard.HashAttribute;
import org.apache.nifi.processors.standard.HashContent;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a generic service for cryptographic hashing. It is used in
 * {@link CryptographicHashAttribute}, {@link CryptographicHashContent}, {@link HashAttribute}, and
 * {@link HashContent}.
 * <p>
 * See also:
 * * {@link HashAlgorithm}
 */
public class HashService {
    private static final Logger logger = LoggerFactory.getLogger(HashService.class);
    private static final int BUFFER_SIZE = 8192;
    private static final String UTF_16_DESCRIPTION = "This character set normally decodes using an optional BOM at the beginning of the data but encodes by inserting a BE BOM. " +
        "For hashing, it will be replaced with UTF-16BE. ";

    /**
     * Returns an array of {@link AllowableValue} elements for each {@link HashAlgorithm}. The
     * complete {@code description} is built from the digest length, safety warnings, etc. See
     * {@link HashAlgorithm#buildAllowableValueDescription()}.
     *
     * @return an ordered {@code AllowableValue[]} containing the values
     */
    public static AllowableValue[] buildHashAlgorithmAllowableValues() {
        final HashAlgorithm[] hashAlgorithms = HashAlgorithm.values();
        List<AllowableValue> allowableValues = new ArrayList<>(hashAlgorithms.length);
        for (HashAlgorithm algorithm : hashAlgorithms) {
            allowableValues.add(new AllowableValue(algorithm.getName(), algorithm.getName(), algorithm.buildAllowableValueDescription()));
        }

        return allowableValues.toArray(new AllowableValue[0]);
    }

    /**
     * Returns an array of {@link AllowableValue} elements for each {@link Charset}. Only the charsets in {@link StandardCharsets} are returned to be consistent across JVM instances.
     *
     * @return an ordered {@code AllowableValue[]} containing the values
     */
    public static AllowableValue[] buildCharacterSetAllowableValues() {
        final List<Charset> charsets = getSupportedCharsets();
        return charsets.stream().map(cs ->
                 new AllowableValue(cs.name(),
                         cs.displayName(),
                         cs == StandardCharsets.UTF_16 ? UTF_16_DESCRIPTION : cs.displayName())
        ).toArray(AllowableValue[]::new);
    }

    /**
     * Returns a {@link List} of supported {@link Charset}s on this platform. This is not a complete
     * list, as only the charsets in {@link StandardCharsets} are returned to be consistent across
     * JVM instances.
     *
     * @return the list of charsets
     */
    public static List<Charset> getSupportedCharsets() {
        return Arrays.asList(StandardCharsets.US_ASCII,
                StandardCharsets.ISO_8859_1,
                StandardCharsets.UTF_8,
                StandardCharsets.UTF_16BE,
                StandardCharsets.UTF_16LE,
                StandardCharsets.UTF_16);
    }

    /**
     * Returns the hash of the specified value. This method uses an {@link java.io.InputStream} to perform the operation in a streaming manner for large inputs.
     *
     * @param algorithm the hash algorithm to use
     * @param value     the value to hash (cannot be {@code null} but can be an empty stream)
     * @return the hash value in hex
     */
    public static String hashValueStreaming(HashAlgorithm algorithm, InputStream value) throws IOException {
        if (algorithm == null) {
            throw new IllegalArgumentException("The hash algorithm cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("The value cannot be null");
        }
        // The Blake2 algorithms are instantiated differently and rely on BouncyCastle
        if (algorithm.isBlake2()) {
            return Hex.encodeHexString(blake2HashStreaming(algorithm, value));
        } else {
            return Hex.encodeHexString(traditionalHashStreaming(algorithm, value));
        }
    }

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
        /** See the note on {@link HashServiceTest#testHashValueShouldHandleUTF16BOMIssue()} */
        if (charset == StandardCharsets.UTF_16) {
            logger.warn("The charset provided was UTF-16, but Java will insert a Big Endian BOM in the decoded message before hashing, so switching to UTF-16BE");
            charset = StandardCharsets.UTF_16BE;
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

    private static byte[] traditionalHashStreaming(HashAlgorithm algorithm, InputStream value) throws IOException {
        MessageDigest digest = DigestUtils.getDigest(algorithm.getName());
        // DigestInputStream digestInputStream = new DigestInputStream(value, digest);
        return DigestUtils.digest(digest, value);
    }

    private static byte[] blake2Hash(HashAlgorithm algorithm, byte[] value) {
        int digestLengthBytes = algorithm.getDigestBytesLength();
        Blake2bDigest blake2bDigest = new Blake2bDigest(digestLengthBytes * 8);
        byte[] rawHash = new byte[blake2bDigest.getDigestSize()];
        blake2bDigest.update(value, 0, value.length);
        blake2bDigest.doFinal(rawHash, 0);
        return rawHash;
    }

    private static byte[] blake2HashStreaming(HashAlgorithm algorithm, InputStream value) throws IOException {
        int digestLengthBytes = algorithm.getDigestBytesLength();
        Blake2bDigest blake2bDigest = new Blake2bDigest(digestLengthBytes * 8);
        byte[] rawHash = new byte[blake2bDigest.getDigestSize()];

        final byte[] buffer = new byte[BUFFER_SIZE];
        int read = value.read(buffer, 0, BUFFER_SIZE);

        while (read > -1) {
            blake2bDigest.update(buffer, 0, read);
            read = value.read(buffer, 0, BUFFER_SIZE);
        }

        blake2bDigest.doFinal(rawHash, 0);
        return rawHash;
    }
}
