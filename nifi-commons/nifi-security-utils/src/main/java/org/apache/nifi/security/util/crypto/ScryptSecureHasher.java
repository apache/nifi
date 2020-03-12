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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.security.util.crypto.scrypt.Scrypt;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an implementation of {@code Scrypt} for secure password hashing.
 * <p>
 * One <strong>critical</strong> difference is that this implementation uses a
 * <strong>static universal</strong> salt unless instructed otherwise, which provides
 * strict determinism across nodes in a cluster. The purpose for this is to allow for
 * blind equality comparison of sensitive values hashed on different nodes (with
 * potentially different {@code nifi.sensitive.props.key} values) during flow inheritance
 * (see {@code FingerprintFactory}).
 * <p>
 * The resulting output is referred to as a <em>hash</em> to be consistent with {@link SecureHasher} terminology,
 * but the length parameter is clarified as the <em>derived key length</em> {@code dkLen} in Scrypt terms, not to be
 * confused with the internal concept of <em>hash length</em> for the PBKDF2 cryptographic hash function (CHF) primitive (SHA-256).
 */
public class ScryptSecureHasher implements SecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(ScryptSecureHasher.class);

    /**
     * These values can be calculated automatically using the code {@see ScryptCipherProviderGroovyTest#calculateMinimumParameters} or manually updated by a maintainer
     */
    private static final int DEFAULT_N = Double.valueOf(Math.pow(2, 14)).intValue();
    private static final int DEFAULT_R = 8;
    private static final int DEFAULT_P = 1;
    private static final int DEFAULT_DK_LENGTH = 32;
    private static final int DEFAULT_SALT_LENGTH = Scrypt.getDefaultSaltLength();

    private static final int MIN_P = 1;
    private static final int MIN_DK_LENGTH = 1;
    private static final int MIN_N = 1;
    private static final int MIN_R = 1;
    private static final int MAX_R = Double.valueOf(Math.pow(2, 31)).intValue() - 1;
    private static final int MIN_SALT_LENGTH = 8;
    private static final int MAX_SALT_LENGTH = Double.valueOf(Math.pow(2, 31)).intValue() - 1;

    private final int n;
    private final int r;
    private final int p;
    private final int dkLength;
    private final int saltLength;

    // TODO: Move to AbstractSecureHasher
    private boolean usingStaticSalt;

    // TODO: Move to AbstractSecureHasher
    // A 16 byte salt (nonce) is recommended for password hashing
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

    // Upper boundary for several cost parameters
    private static final Integer UPPER_BOUNDARY = Double.valueOf(Math.pow(2, 32)).intValue() - 1;

    /**
     * Instantiates an Scrypt secure hasher using the default cost parameters
     * ({@code N = }{@link #DEFAULT_N},
     * {@code r = }{@link #DEFAULT_R},
     * {@code p = }{@link #DEFAULT_R},
     * {@code dkLen = }{@link #DEFAULT_DK_LENGTH}). A static salt is also used.
     */
    public ScryptSecureHasher() {
        this(DEFAULT_N, DEFAULT_R, DEFAULT_P, DEFAULT_DK_LENGTH, 0);
    }

    /**
     * Instantiates an Scrypt secure hasher using the provided cost parameters. A static
     * {@link #DEFAULT_SALT_LENGTH} byte salt will be generated on every hash request.
     *
     * @param n        number of iterations (power of 2 from {@code 1 to 2^(128 * r / 8)})
     * @param r        the block size of memory ({@code > 0})
     * @param p        parallelization factor from ({@code 1 to ((2^32-1) * 32) / (128 * r)})
     * @param dkLength the output length in bytes ({@code 1 to (2^32 - 1) * 32})
     */
    public ScryptSecureHasher(int n, int r, int p, int dkLength) {
        this(n, r, p, dkLength, 0);
    }

    /**
     * Instantiates an Scrypt secure hasher using the provided cost parameters. A unique
     * salt of the specified length will be generated on every hash request.
     *
     * @param n          number of iterations (power of 2 from {@code 1 to 2^(128 * r / 8)})
     * @param r          the block size of memory ({@code > 0})
     * @param p          parallelization factor from ({@code 1 to ((2^32-1) * 32) / (128 * r)})
     * @param dkLength   the output length in bytes ({@code 1 to (2^32 - 1) * 32})
     * @param saltLength the salt length in bytes {@code >= 8})
     */
    public ScryptSecureHasher(int n, int r, int p, int dkLength, int saltLength) {
        this.n = n;
        this.r = r;
        this.p = p;
        this.dkLength = dkLength;

        this.saltLength = saltLength;
        if (saltLength > 0) {
            this.usingStaticSalt = false;
        } else {
            this.usingStaticSalt = true;
            logger.debug("Configured to use static salt");
        }
    }

    /**
     * Enforces valid Scrypt secure hasher cost parameters are provided.
     *
     * @param n          number of iterations (power of 2 from {@code 1 to 2^(128 * r / 8)})
     * @param r          the block size of memory ({@code > 0})
     * @param p          parallelization factor from ({@code 1 to ((2^32-1) * 32) / (128 * r)})
     * @param dkLength   the output length in bytes ({@code 1 to (2^32 - 1) * 32})
     * @param saltLength the salt length in bytes {@code >= 8})
     */
    private void validateParameters(Integer n, Integer r, int p, Integer dkLength, Integer saltLength) {
        // Check r first because it is not dependent on other parameters
        if (!isRValid(r)) {
            logger.error("The provided block size r {} ( * 128 bytes) is outside the boundary of 1 to 2^31 - 1.", r);
            throw new IllegalArgumentException("Invalid r is not within the memory boundary.");
        }
        if (!isNValid(n, r)) {
            logger.error("The iteration count N {} is outside the boundary of powers of 2 from 1 to 2^(128 * r / 8).", n);
            throw new IllegalArgumentException("Invalid N exceeds the iterations boundary.");
        }
        if (!isPValid(p, r)) {
            logger.error("The provided parallelization factor {} is outside the boundary of 1 to ((2^32 - 1) * 32) / (128 * r).", p);
            throw new IllegalArgumentException("Invalid p exceeds the parallelism boundary.");
        }
        if (!isDKLengthValid(dkLength)) {
            logger.error("The provided hash length {} is outside the boundary of 1 to (2^32 - 1) * 32.", dkLength);
            throw new IllegalArgumentException("Invalid hash length is not within the dkLength boundary.");
        }

        if (saltLength > 0) {
            if (!isSaltLengthValid(saltLength)) {
                logger.error("The salt length {} is outside the boundary of 8 to 2^32 - 1.", saltLength);
                throw new IllegalArgumentException("Invalid salt length exceeds the saltLength boundary.");
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
     * Returns true if the provided iteration count N is within boundaries. The lower bound >= 1 and the
     * upper bound <= 2^(128 * r / 8).
     *
     * @param n number of iterations
     * @param r the blocksize parameter
     * @return true if iterations is within boundaries
     */
    public static boolean isNValid(Integer n, int r) {
        if (n < DEFAULT_N) {
            logger.warn("The provided iteration count N {} is below the recommended minimum {}.", n, DEFAULT_N);
        }
        return n >= MIN_N && n <= Double.valueOf(Math.pow(2, (128 * r / 8.0))).intValue();
    }

    /**
     * Returns true if the provided block size in bytes is within boundaries. The lower bound >= 1 and the
     * upper bound <= 2^32 - 1.
     *
     * @param r the integer number * 128 B used
     * @return true if r is within boundaries
     */
    public static boolean isRValid(int r) {
        if (r < DEFAULT_R) {
            logger.warn("The provided r size {} * 128 B is below the recommended minimum {}.", r, DEFAULT_R);
        }
        return r >= MIN_R && r <= MAX_R;
    }

    /**
     * Returns true if the provided parallelization factor is within boundaries. The lower bound >= 1 and the
     * upper bound <= ((2^32 - 1) * 32) / (128 * r).
     *
     * @param p degree of parallelism
     * @param r the blocksize parameter
     * @return true if parallelism is within boundaries
     */
    public static boolean isPValid(int p, int r) {
        if (p < DEFAULT_P) {
            logger.warn("The provided parallelization factor {} is below the recommended minimum {}.", p, DEFAULT_P);
        }
        long dividend = Double.valueOf((Math.pow(2, 32) - 1) * 32).longValue();
        int divisor = 128 * r;
        BigInteger MAX_P = new BigInteger(String.valueOf(dividend)).divide(new BigInteger(String.valueOf(divisor)));
        logger.debug("Calculated maximum p value as (2^32 - 1) * 32 [{}] / (128 * r) [{}] = {}", dividend, divisor, MAX_P.intValue());
        return p >= MIN_P && p <= MAX_P.intValue();
    }

    /**
     * Returns whether the provided hash (derived key) length is within boundaries. The lower bound >= 1 and the
     * upper bound <= (2^32 - 1) * 32.
     *
     * @param dkLength the output length in bytes
     * @return true if dkLength is within boundaries
     */
    public static boolean isDKLengthValid(Integer dkLength) {
        if (dkLength < DEFAULT_DK_LENGTH) {
            logger.warn("The provided hash (derived key) length {} is below the recommended minimum {}.", dkLength, DEFAULT_DK_LENGTH);
        }
        return dkLength >= MIN_DK_LENGTH && dkLength <= UPPER_BOUNDARY;
    }

    /**
     * Returns whether the provided salt length (saltLength) is within boundaries. The lower bound >= 8 and the
     * upper bound <= 2^31 - 1.
     *
     * @param saltLength the salt length in bytes
     * @return true if saltLength is within boundaries
     */
    public static boolean isSaltLengthValid(Integer saltLength) {
        if (saltLength == 0) {
            logger.debug("The provided salt length 0 indicates a static salt of {} bytes", DEFAULT_SALT_LENGTH);
            return true;
        }
        if (saltLength < DEFAULT_SALT_LENGTH) {
            logger.warn("The provided dynamic salt length {} is below the recommended minimum {}", saltLength, DEFAULT_SALT_LENGTH);
        }
        return saltLength >= MIN_SALT_LENGTH && saltLength <= MAX_SALT_LENGTH;
    }

    /**
     * Returns a String representation of {@code Scrypt(input)} in hex-encoded format.
     *
     * @param input the non-empty input
     * @return the hex-encoded hash
     */
    @Override
    public String hashHex(String input) {
        if (input == null || input.length() == 0) {
            logger.warn("Attempting to generate an Scrypt hash of null or empty input; returning 0 length string");
            return "";
        }

        return Hex.toHexString(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of {@code Scrypt(input)} in Base 64-encoded format.
     *
     * @param input the non-empty input
     * @return the Base 64-encoded hash
     */
    @Override
    public String hashBase64(String input) {
        if (input == null || input.length() == 0) {
            logger.warn("Attempting to generate an Scrypt hash of null or empty input; returning 0 length string");
            return "";
        }

        return Base64.toBase64String(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a byte[] representation of {@code Scrypt(input)}.
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

        logger.debug("Creating {} byte Scrypt hash with salt [{}]", dkLength, org.bouncycastle.util.encoders.Hex.toHexString(rawSalt));

        final long startNanos = System.nanoTime();
        byte[] hash = Scrypt.scrypt(input, rawSalt, n, r, p, dkLength * 8);
        final long generateNanos = System.nanoTime();

        final long totalDurationMillis = TimeUnit.NANOSECONDS.toMillis(generateNanos - startNanos);

        logger.debug("Generated Scrypt hash in {} ms", totalDurationMillis);

        return hash;
    }
}
