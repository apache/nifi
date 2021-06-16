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
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class PBKDF2SecureHasher extends AbstractSecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2SecureHasher.class);

    private static final String DEFAULT_PRF = "SHA-512";
    private static final int DEFAULT_SALT_LENGTH = 16;
    /**
     * This can be calculated automatically using the code {@see PBKDF2CipherProviderGroovyTest#calculateMinimumIterationCount} or manually updated by a maintainer
     */
    private static final int DEFAULT_ITERATION_COUNT = 160_000;

    // Different sources list this in bits and bytes, but RFC 8018 uses bytes (octets [8-bit sequences] to be precise)
    private static final int DEFAULT_DK_LENGTH = 32;

    private static final int MIN_ITERATION_COUNT = 1;
    private static final int MIN_DK_LENGTH = 1;
    private static final int MIN_SALT_LENGTH = 8;

    private final Digest prf;
    private final Integer iterationCount;
    private final int dkLength;

    /**
     * Instantiates a PBKDF2 secure hasher with the default number of iterations and the default PRF. Currently 160,000 iterations and SHA-512.
     */
    public PBKDF2SecureHasher() {
        this(DEFAULT_PRF, DEFAULT_ITERATION_COUNT, 0, DEFAULT_DK_LENGTH);
    }

    /**
     * Instantiates a PBKDF2 secure hasher with the default number of iterations and the default PRF using the specified derived key length.
     *
     * @param dkLength Derived Key Length in bytes
     */
    public PBKDF2SecureHasher(final int dkLength) {
        this(DEFAULT_PRF, DEFAULT_ITERATION_COUNT, 0, dkLength);
    }

    /**
     * Instantiates a PBKDF2 secure hasher with the provided number of iterations and derived key (output) length in bytes, using the default PRF ({@code SHA512}).
     *
     * @param iterationCount the number of iterations
     * @param dkLength       the desired output length in bytes
     */
    public PBKDF2SecureHasher(int iterationCount, int dkLength) {
        this(DEFAULT_PRF, iterationCount, 0, dkLength);
    }

    /**
     * Instantiates a PBKDF2 secure hasher using the provided cost parameters. A unique
     * salt of the specified length will be generated on every hash request.
     * Currently supported PRFs are {@code MD5} (deprecated), {@code SHA1} (deprecated), {@code SHA256},
     * {@code SHA384}, and {@code SHA512}. Unknown PRFs will default to {@code SHA512}.
     *
     * @param prf            a String representation of the PRF name, e.g. "SHA256", "SHA-384" "sha_512"
     * @param iterationCount the number of iterations
     * @param saltLength     the salt length in bytes ({@code >= 8}, {@code 0} indicates a static salt)
     * @param dkLength       the output length in bytes ({@code 1 to (2^32 - 1) * hLen})
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
     * @param saltLength     the salt length in bytes {@code >= 8})
     * @param dkLength       the output length in bytes ({@code 1 to (2^32 - 1) * hLen})
     */
    private void validateParameters(String prf, Integer iterationCount, int saltLength, int dkLength) {
        logger.debug("Validating PBKDF2 secure hasher with prf {}, iteration count {}, salt length {} bytes, output length {} bytes", prf, iterationCount, saltLength, dkLength);

        if (!isIterationCountValid(iterationCount)) {
            logger.error("The provided iteration count {} is below the minimum {}.", iterationCount, MIN_ITERATION_COUNT);
            throw new IllegalArgumentException("Invalid iterationCount is not within iteration count boundary.");
        }
        initializeSalt(saltLength);

        // Calculate hLen based on PRF
        Digest prfType = resolvePRF(prf);
        int hLen = prfType.getDigestSize();
        logger.debug("The PRF is {}, with a digest size (hLen) of {} bytes", prfType.getAlgorithmName(), hLen);

        if (!isDKLengthValid(hLen, dkLength)) {
            logger.error("The provided dkLength {} bytes is outside the output boundary {} to {}.", dkLength, MIN_DK_LENGTH, getMaxDKLength(hLen));
            throw new IllegalArgumentException("Invalid dkLength is not within derived key length boundary.");
        }
    }

    /**
     * Returns the algorithm-specific name for logging and messages.
     *
     * @return the algorithm name
     */
    @Override
    String getAlgorithmName() {
        return "PBKDF2";
    }

    /**
     * Returns {@code true} if the algorithm can accept empty (non-{@code null}) inputs.
     *
     * @return the true if {@code ""} is allowable input
     */
    @Override
    boolean acceptsEmptyInput() {
        return true;
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
     * Returns whether the provided hash (derived key) length is within boundaries given the configured PRF. The lower bound >= 1 and the
     * upper bound <= ((2^32 - 1) * 32) * hLen.
     *
     * @param hLen     the PRF digest size in bytes
     * @param dkLength the output length in bytes
     * @return true if dkLength is within boundaries
     */
    public static boolean isDKLengthValid(int hLen, Integer dkLength) {
        final int MAX_DK_LENGTH = getMaxDKLength(hLen);
        logger.debug("The max dkLength is {} bytes for hLen {} bytes.", MAX_DK_LENGTH, hLen);

        return dkLength >= MIN_DK_LENGTH && dkLength <= MAX_DK_LENGTH;
    }

    /**
     * Returns the maximum length of the derived key in bytes given the digest length in bytes of the underlying PRF.
     * If the calculated maximum exceeds {@link Integer#MAX_VALUE}, that is returned instead, as RFC 8018 specifies
     * {@code keyLength INTEGER (1..MAX) OPTIONAL}.
     *
     * @param hLen the length of the PRF digest output in bytes
     * @return the maximum possible length of the derived key in bytes
     */
    private static int getMaxDKLength(int hLen) {
        final long MAX_LENGTH = ((Double.valueOf((Math.pow(2, 32)))).longValue() - 1) * hLen;
        return Long.valueOf(Math.min(MAX_LENGTH, Integer.MAX_VALUE)).intValue();
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
        logger.debug("Creating PBKDF2 hash with salt [{}] ({} bytes)", Hex.toHexString(rawSalt), rawSalt.length);

        if (!isSaltLengthValid(rawSalt.length)) {
            throw new IllegalArgumentException("The salt length (" + rawSalt.length + " bytes) is invalid");
        }

        final long startNanos = System.nanoTime();
        PKCS5S2ParametersGenerator gen = new PKCS5S2ParametersGenerator(this.prf);
        gen.init(input, rawSalt, iterationCount);
        // The generateDerivedParameters method expects the dkLength in bits
        byte[] hash = ((KeyParameter) gen.generateDerivedParameters(dkLength * 8)).getKey();
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
