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

import at.favre.lib.crypto.bcrypt.BCrypt;
import at.favre.lib.crypto.bcrypt.Radix64Encoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an implementation of {@code Bcrypt} for secure password hashing.
 * <p>
 * One <strong>critical</strong> difference is that this implementation uses a
 * <strong>static universal</strong> salt unless instructed otherwise, which provides
 * strict determinism across nodes in a cluster. The purpose for this is to allow for
 * blind equality comparison of sensitive values hashed on different nodes (with
 * potentially different {@code nifi.sensitive.props.key} values) during flow inheritance
 * (see {@code FingerprintFactory}).
 * <p>
 * The resulting output is referred to as a <em>hash</em> to be consistent with {@link SecureHasher} terminology.
 */
public class BcryptSecureHasher extends AbstractSecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(BcryptSecureHasher.class);

    /**
     * These values can be calculated automatically using the code {@see BcryptCipherProviderGroovyTest#calculateMinimumParameters} or manually updated by a maintainer
     */
    private static final int DEFAULT_COST = 12;
    private static final int DEFAULT_SALT_LENGTH = 16;

    private static final int MIN_COST = 4;
    private static final int MAX_COST = 31;
    private static final int MIN_SALT_LENGTH = 16;

    private final int cost;

    /**
     * Instantiates a Bcrypt secure hasher using the default cost parameter
     * ({@code cost = }{@link #DEFAULT_COST}
     */
    public BcryptSecureHasher() {
        this(DEFAULT_COST, 0);
    }

    /**
     * Instantiates a Bcrypt secure hasher using the provided cost parameters. A static
     * {@link #DEFAULT_SALT_LENGTH} byte salt will be generated on every hash request.
     *
     * @param cost the (log) number of key expansion rounds [4..31]
     */
    public BcryptSecureHasher(int cost) {
        this(cost, 0);
    }

    /**
     * Instantiates an Bcrypt secure hasher using the provided cost parameters. A unique
     * salt of the specified length will be generated on every hash request.
     *
     * @param cost       the (log) number of key expansion rounds [4..31]
     * @param saltLength the salt length in bytes {@code >= 8})
     */
    public BcryptSecureHasher(int cost, int saltLength) {
        validateParameters(cost, saltLength);
        this.cost = cost;
        this.saltLength = saltLength;
    }

    /**
     * Enforces valid Scrypt secure hasher cost parameters are provided.
     *
     * @param cost       the (log) number of key expansion rounds [4..31]
     * @param saltLength the salt length in bytes {@code >= 16})
     */
    private void validateParameters(Integer cost, Integer saltLength) {
        if (!isCostValid(cost)) {
            logger.error("The provided cost factor {} is outside the boundary of 4 to 31.", cost);
            throw new IllegalArgumentException("Invalid cost is not within the cost factor boundary.");
        }

        initializeSalt(saltLength);
    }

    /**
     * Returns true if the provided cost factor is within boundaries. The lower bound >= 4 and the
     * upper bound <= 31.
     *
     * @param cost the (log) number of key expansion rounds [4..31]
     * @return true if cost factor is within boundaries
     */
    public static boolean isCostValid(Integer cost) {
        if (cost < DEFAULT_COST) {
            logger.warn("The provided cost factor {} is below the recommended minimum {}.", cost, DEFAULT_COST);
        }
        return cost >= MIN_COST && cost <= MAX_COST;
    }

    public static String convertBcryptRadix64ToMimeBase64(String radix64) {
        return CipherUtility.encodeBase64NoPadding(new Radix64Encoder.Default().decode(radix64.getBytes(StandardCharsets.UTF_8)));
    }

    public static String convertMimeBase64ToBcryptRadix64(String base64) {
        return new String(new Radix64Encoder.Default().encode(Base64.decodeBase64(base64)), StandardCharsets.UTF_8);
    }

    /**
     * Returns the algorithm-specific default salt length in bytes.
     *
     * @return the default salt length
     */
    @Override
    int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }

    /**
     * Returns the algorithm-specific minimum salt length in bytes.
     *
     * @return the min salt length
     */
    @Override
    int getMinSaltLength() {
        return MIN_SALT_LENGTH;
    }

    /**
     * Returns the algorithm-specific maximum salt length in bytes.
     *
     * @return the max salt length
     */
    @Override
    int getMaxSaltLength() {
        return Integer.MAX_VALUE;
    }

    /**
     * Returns the algorithm-specific name for logging and messages.
     *
     * @return the algorithm name
     */
    @Override
    String getAlgorithmName() {
        return "Bcrypt";
    }

    /**
     * Returns {@code true} if the algorithm can accept empty (non-{@code null}) inputs.
     *
     * @return the true if {@code ""} is allowable input
     */
    @Override
    boolean acceptsEmptyInput() {
        return false;
    }

    /**
     * Internal method to hash the raw bytes.
     *
     * @param input the raw bytes to hash (can be length 0)
     * @return the generated hash
     */
    byte[] hash(byte[] input) {
        // Contains only the raw salt
        byte[] rawSalt = getSalt();

        return hash(input, rawSalt);
    }

    /**
     * Internal method to hash the raw bytes.
     *
     * @param input the raw bytes to hash (can be length 0)
     * @param rawSalt the raw bytes to salt
     * @return the generated hash
     */
    byte[] hash(byte[] input, byte[] rawSalt) {
        logger.debug("Creating Bcrypt hash with salt [{}] ({} bytes)", Hex.toHexString(rawSalt), rawSalt.length);

        if (!isSaltLengthValid(rawSalt.length)) {
            throw new IllegalArgumentException("The salt length (" + rawSalt.length + " bytes) is invalid");
        }

        final long startNanos = System.nanoTime();
        byte[] hash = BCrypt.withDefaults().hash(cost, rawSalt, input);
        final long generateNanos = System.nanoTime();

        final long totalDurationMillis = TimeUnit.NANOSECONDS.toMillis(generateNanos - startNanos);

        logger.debug("Generated Bcrypt hash in {} ms", totalDurationMillis);

        return hash;
    }
}
