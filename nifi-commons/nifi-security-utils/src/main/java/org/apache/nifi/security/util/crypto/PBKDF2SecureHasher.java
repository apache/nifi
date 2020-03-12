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

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SHA384Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

/**
 * Provides an implementation of {@code PBKDF2} for secure password hashing.
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
public class PBKDF2SecureHasher implements SecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2SecureHasher.class);

    private static final String DEFAULT_PRF = "SHA-512";
    private static final int DEFAULT_SALT_LENGTH = 16;
    /**
     * This can be calculated automatically using the code {@see PBKDF2CipherProviderGroovyTest#calculateMinimumIterationCount} or manually updated by a maintainer
     */
    private static final int DEFAULT_ITERATION_COUNT = 160_000;
    private static final int DEFAULT_DK_LENGTH = 512;


    private static final int MIN_ITERATION_COUNT = 1;
    private static final int MIN_DK_LENGTH = 1;
    private static final int MIN_SALT_LENGTH = 8;

    private final Digest prf;
    private final int saltLength;
    private final Integer iterationCount;
    private final int dkLength;

    // TODO: Move to AbstractSecureHasher
    private boolean usingStaticSalt;

    // TODO: Move to AbstractSecureHasher
    // A 16 byte salt (nonce) is recommended for password hashing
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

    /**
     * Instantiates a PBKDF2 secure hasher with the default number of iterations and the default PRF. Currently 160,000 iterations and SHA-512.
     */
    public PBKDF2SecureHasher() { this(DEFAULT_PRF, DEFAULT_ITERATION_COUNT, 0, DEFAULT_DK_LENGTH); }

    /**
     * Instantiates a PBKDF2 secure hasher using the provided cost parameters. A static
     * {@link #DEFAULT_SALT_LENGTH} byte salt will be generated on every hash request.
     *
     * @param iterationCount the (log) number of key expansion rounds
     */
    public PBKDF2SecureHasher(int iterationCount) {
        this(DEFAULT_PRF, iterationCount, 0, DEFAULT_DK_LENGTH);
    }

    /**
     * Instantiates a PBKDF2 secure hasher using the provided cost parameters. A static
     * {@link #DEFAULT_SALT_LENGTH} byte salt will be generated on every hash request.
     *
     * @param iterationCount the (log) number of key expansion rounds
     */
    public PBKDF2SecureHasher(int iterationCount, int saltLength) {
        this(DEFAULT_PRF, iterationCount, saltLength, DEFAULT_DK_LENGTH);
    }

    /**
     * Instantiates a PBKDF2 secure hasher using the provided cost parameters. A unique
     * salt of the specified length will be generated on every hash request.
     * Currently support PRFs of MD5, SHA1, SHA256, SHA384, and SHA512. Unknown PRFs will default to
     * SHA512.
     *
     * @param prf            a String representation of the PRF name, e.g. "SHA256", "SHA-384" "sha_512"
     * @param iterationCount the number of iterations
     * @param saltLength    the salt length in bytes {@code >= 16})
     * @param dkLength      the output length in bits ({@code 1 to (2^32 - 1) * hLen})
     */
    public PBKDF2SecureHasher(String prf, Integer iterationCount, int saltLength, int dkLength) {
        validateParameters(prf, iterationCount, saltLength, dkLength);
        this.prf = resolvePRF(prf);
        this.iterationCount = iterationCount;
        this.saltLength = saltLength;
        this.dkLength = dkLength;
    }

    /**
     * Enforces valid PBKDF2 secure hasher cost parameters are provided.
     *
     * @param iterationCount the (log) number of key expansion rounds
     * @param saltLength the salt length in bytes {@code >= 16})
     * @param dkLength   the output length in bits ({@code 1 to (2^32 - 1) * hLen})
     */
    private void validateParameters(String prf, Integer iterationCount, int saltLength, int dkLength) {
        if (!isIterationCountValid(iterationCount)) {
            logger.error("The provided iteration count {} is below the minimum {}.", iterationCount, DEFAULT_ITERATION_COUNT);
            throw new IllegalArgumentException("Invalid iterationCount is not within iteration count boundary.");
        }
        if (saltLength > 0) {
            if (!isSaltLengthValid(saltLength)) {
                logger.error("The provided saltLength {} B is below the minimum {}.", saltLength, DEFAULT_SALT_LENGTH);
                throw new IllegalArgumentException("Invalid saltLength is not within the salt length boundary.");
            }
            this.usingStaticSalt = false;
        } else {
            this.usingStaticSalt = true;
            logger.debug("Configured to use static salt");
        }

        // Calculate hLen based on PRF
        Digest prfType = resolvePRF(prf);
        int hLen = prfType.getDigestSize();
        logger.info("The hLen is {}, with a PRF of {}", hLen, prfType.getAlgorithmName());

        if (!isDKLengthValid(hLen, dkLength)) {
            logger.error("The provided dkLength {} bits is below the minimum {}.", dkLength, DEFAULT_DK_LENGTH);
            throw new IllegalArgumentException("Invalid dkLength is not within derived key length boundary.");
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
     * Returns true if the provided cost factor is within boundaries. The lower bound >= 1.
     *
     * @param iterationCount the (log) number of key expansion rounds
     * @return true if cost factor is within boundaries
     */
    public static boolean isIterationCountValid(Integer iterationCount) {
        if (iterationCount < DEFAULT_ITERATION_COUNT) {
            logger.warn("The provided iteration count {} is below the recommended minimum {}.", iterationCount, DEFAULT_ITERATION_COUNT);
        }
        // By definition, all ints are <= Integer.MAX_VALUE
        return iterationCount >= MIN_ITERATION_COUNT;
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
     * Returns whether the provided hash (derived key) length is within boundaries. The lower bound >= 1 and the
     * upper bound <= ((2^32 - 1) * 32) * hLen.
     *
     * @param dkLength the output length in bits
     * @return true if dkLength is within boundaries
     */
    public static boolean isDKLengthValid(int hLen, Integer dkLength) {
        if (dkLength < DEFAULT_DK_LENGTH) {
            logger.warn("The provided dklength {} bits is below the recommended minimum {}.", dkLength, DEFAULT_DK_LENGTH);
        }
        long MAX_DK_LENGTH = (Double.valueOf((Math.pow(2, 32) - 1)).longValue()) * hLen;
        // Convert dkLength bits to bytes?
        logger.info("The max dkLength is {} bits with an hLen {} B.", MAX_DK_LENGTH, hLen);

        return dkLength >= MIN_DK_LENGTH && dkLength <= MAX_DK_LENGTH;
    }

    /**
     * Returns a String representation of {@code PBKDF2(input)} in hex-encoded format.
     *
     * @param input the non-empty input
     * @return the hex-encoded hash
     */
    @Override
    public String hashHex(String input) {
        if (input == null || input.length() == 0) {
            logger.warn("Attempting to generate a PBKDF2 hash of null or empty input; returning 0 length string");
            return "";
        }

        return Hex.toHexString(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of {@code PBKDF2(input)} in Base 64-encoded format.
     *
     * @param input the non-empty input
     * @return the Base 64-encoded hash
     */
    @Override
    public String hashBase64(String input) {
        if (input == null || input.length() == 0) {
            logger.warn("Attempting to generate a PBKDF2 hash of null or empty input; returning 0 length string");
            return "";
        }

        return Base64.toBase64String(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a byte[] representation of {@code PBKDF2(input)}.
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

        logger.debug("Creating PBKDF2 hash with salt [{}] ({} bytes)", Hex.toHexString(rawSalt), rawSalt.length);

        final long startNanos = System.nanoTime();
        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(this.prf);
        gen.init(input, rawSalt, iterationCount);
        byte[] hash = ((KeyParameter) gen.generateDerivedParameters(dkLength)).getKey();
        final long generateNanos = System.nanoTime();

        final long totalDurationMillis = TimeUnit.NANOSECONDS.toMillis(generateNanos - startNanos);

        logger.debug("Generated PBKDF2 hash in {} ms", totalDurationMillis);

        return hash;
    }

    private Digest resolvePRF(final String prf) {
        if (StringUtils.isEmpty(prf)) {
            throw new IllegalArgumentException("Cannot resolve empty PRF");
        }
        String formattedPRF = prf.toLowerCase().replaceAll("[\\W]+", "");
        logger.debug("Resolved PRF {} to {}", prf, formattedPRF);
        switch (formattedPRF) {
            case "md5":
                logger.warn("MD5 is a deprecated cryptographic hash function and should not be used");
                return new MD5Digest();
            case "sha1":
                logger.warn("SHA-1 is a deprecated cryptographic hash function and should not be used");
                return new SHA1Digest();
            case "sha256":
                return new SHA256Digest();
            case "sha384":
                return new SHA384Digest();
            case "sha512":
                return new SHA512Digest();
            default:
                logger.warn("Could not resolve PRF {}. Using default PRF {} instead", prf, DEFAULT_PRF);
                return new SHA512Digest();
        }
    }
}
