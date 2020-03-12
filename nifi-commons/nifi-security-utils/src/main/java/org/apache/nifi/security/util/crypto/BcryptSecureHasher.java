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
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

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
public class BcryptSecureHasher implements SecureHasher {
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
    private final int saltLength;

    // TODO: Move to AbstractSecureHasher
    private boolean usingStaticSalt;

    // TODO: Move to AbstractSecureHasher
    // A 16 byte salt (nonce) is recommended for password hashing
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

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
     * @param cost the (log) number of key expansion rounds [4..31]
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
     * @param cost the (log) number of key expansion rounds [4..31]
     * @param saltLength the salt length in bytes {@code >= 16})
     */
    private void validateParameters(Integer cost, Integer saltLength) {
        if (!isCostValid(cost)) {
            logger.error("The provided cost factor {} is outside the boundary of 4 to 31.", cost);
            throw new IllegalArgumentException("Invalid cost is not within the cost factor boundary.");
        }

        if (saltLength > 0) {
            if (!isSaltLengthValid(saltLength)) {
                logger.error("The provided saltLength {} B is below the minimum {}.", cost, DEFAULT_SALT_LENGTH);
                throw new IllegalArgumentException("Invalid saltLength is not within the salt length boundary.");
            }
            this.usingStaticSalt = false;
        } else {
            this.usingStaticSalt = true;
            logger.debug("Configured to use static salt");
        }
    }

    /**
     * Returns {@code true} if this instance is configured to use a static salt.
     *
     * @return true if all hashes will be generated using a static salt
     */
    public boolean isUsingStaticSalt() {
        return usingStaticSalt;
    }

    /**
     * Returns a salt to use. If using a static salt (see {@link #isUsingStaticSalt()}),
     * this return value will be identical across every invocation. If using a dynamic salt,
     * it will be {@link #saltLength} bytes of a securely-generated random value.
     *
     * @return the salt value
     */
    byte[] getSalt() {
        if (isUsingStaticSalt()) {
            return STATIC_SALT;
        } else {
            SecureRandom sr = new SecureRandom();
            byte[] salt = new byte[saltLength];
            sr.nextBytes(salt);
            return salt;
        }
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

    /**
     * Returns true if the provided salt length meets the minimum boundary. The lower bound >= 16.
     *
     * @param saltLength the salt length in bytes
     * @return true if salt length is at least the minimum boundary
     */
    private static boolean isSaltLengthValid(Integer saltLength) {
        if (saltLength == 0) {
            logger.debug("The provided salt length 0 indicates a static salt of {} bytes", DEFAULT_SALT_LENGTH);
            return true;
        }
        if (saltLength < MIN_SALT_LENGTH) {
            logger.warn("The provided salt length {} B is below the recommended minimum {}.", saltLength, MIN_SALT_LENGTH);
        }
        return saltLength >= MIN_SALT_LENGTH;
    }

    /**
     * Returns a String representation of {@code Bcrypt(input)} in hex-encoded format.
     *
     * @param input the non-empty input
     * @return the hex-encoded hash
     */
    @Override
    public String hashHex(String input) {
        if (input == null || input.length() == 0) {
            logger.warn("Attempting to generate a Bcrypt hash of null or empty input; returning 0 length string");
            return "";
        }

        return Hex.toHexString(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of {@code Bcrypt(input)} in Base 64-encoded format. This is
     * <strong>standard MIME {@link Base64} encoding</strong>, not the Bcrypt-specific Radix 64 encoding.
     *
     * @param input the non-empty input
     * @return the Base 64-encoded hash
     */
    @Override
    public String hashBase64(String input) {
        if (input == null || input.length() == 0) {
            logger.warn("Attempting to generate a Bcrypt hash of null or empty input; returning 0 length string");
            return "";
        }

        return Base64.toBase64String(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a byte[] representation of {@code Bcrypt(input)}.
     *
     * @param input the input
     * @return the hash
     */
    @Override
    public byte[] hashRaw(byte[] input) {
        return hash(input);
    }

    /**
     * Internal method to hash the raw bytes.
     *
     * @param input the raw bytes to hash (can be length 0)
     * @return the generated hash
     */
    private byte[] hash(byte[] input) {
        // Contains only the raw salt
        byte[] rawSalt = getSalt();

        logger.debug("Creating Bcrypt hash with salt [{}] ({} bytes)", Hex.toHexString(rawSalt), rawSalt.length);

        final long startNanos = System.nanoTime();
        byte[] hash = BCrypt.withDefaults().hash(cost, rawSalt, input);
        final long generateNanos = System.nanoTime();

        final long totalDurationMillis = TimeUnit.NANOSECONDS.toMillis(generateNanos - startNanos);

        logger.debug("Generated Bcrypt hash in {} ms", totalDurationMillis);

        return hash;
    }
}
