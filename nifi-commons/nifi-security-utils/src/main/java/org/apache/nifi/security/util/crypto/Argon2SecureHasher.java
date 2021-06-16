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

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides an implementation of {@code Argon2} for secure password hashing. This class is
 * roughly based on Spring Security's implementation but does not include the full module
 * in this utility module. This implementation uses {@code Argon2id} which provides a
 * balance of protection against side-channel and memory attacks.
 * <p>
 * One <strong>critical</strong> difference is that this implementation uses a
 * <strong>static universal</strong> salt unless instructed otherwise, which provides
 * strict determinism across nodes in a cluster. The purpose for this is to allow for
 * blind equality comparison of sensitive values hashed on different nodes (with
 * potentially different {@code nifi.sensitive.props.key} values) during flow inheritance
 * (see {@code FingerprintFactory}).
 */
public class Argon2SecureHasher implements SecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(Argon2SecureHasher.class);

    private static final int DEFAULT_HASH_LENGTH = 32;
    private static final int DEFAULT_PARALLELISM = 1;
    private static final int DEFAULT_MEMORY = 1 << 12;
    private static final int DEFAULT_ITERATIONS = 3;
    private static final int DEFAULT_SALT_LENGTH = 16;
    private static final int MIN_MEMORY_SIZE_KB = 8;
    private static final int MIN_PARALLELISM = 1;
    private static final long MAX_PARALLELISM = Math.round(Math.pow(2, 24)) - 1;
    private static final int MIN_HASH_LENGTH = 4;
    private static final int MIN_ITERATIONS = 1;
    private static final int MIN_SALT_LENGTH = 8;

    // Using Integer vs. int to allow for unsigned 32b (values can exceed Integer.MAX_VALUE)
    private final Integer hashLength;
    private final Integer memory;
    private final int parallelism;
    private final Integer iterations;
    private final Integer saltLength;

    private boolean usingStaticSalt;

    // A 16 byte salt (nonce) is recommended for password hashing
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

    // Upper boundary for several cost parameters
    private static final long UPPER_BOUNDARY = Math.round(Math.pow(2, 32)) - 1;

    /**
     * Instantiates an Argon2 secure hasher using the default cost parameters
     * ({@code hashLength = }{@link #DEFAULT_HASH_LENGTH},
     * {@code memory = }{@link #DEFAULT_MEMORY},
     * {@code parallelism = }{@link #DEFAULT_PARALLELISM},
     * {@code iterations = }{@link #DEFAULT_ITERATIONS}). A static salt is also used.
     */
    public Argon2SecureHasher() {
        this(DEFAULT_HASH_LENGTH, DEFAULT_MEMORY, DEFAULT_PARALLELISM, DEFAULT_ITERATIONS, 0);
    }

    /**
     * Instantiates an Argon2 secure hasher using the provided cost parameters. A static
     * {@link #DEFAULT_SALT_LENGTH} byte salt will be generated on every hash request.
     * {@link Integer} is used instead of {@code int} for parameters which have a max value of {@code 2^32 - 1} to allow for unsigned integers exceeding {@link Integer#MAX_VALUE}.
     *
     * @param hashLength  the output length in bytes ({@code 4 to 2^32 - 1})
     * @param memory      the integer number of KB used ({@code 8p to 2^32 - 1})
     * @param parallelism degree of parallelism ({@code 1 to 2^24 - 1})
     * @param iterations  number of iterations ({@code 1 to 2^32 - 1})
     */
    public Argon2SecureHasher(Integer hashLength, Integer memory, int parallelism, Integer iterations) {
        this(hashLength, memory, parallelism, iterations, 0);
    }

    /**
     * Instantiates an Argon2 secure hasher using the provided cost parameters. A unique
     * salt of the specified length will be generated on every hash request.
     * {@link Integer} is used instead of {@code int} for parameters which have a max value of {@code 2^32 - 1} to allow for unsigned integers exceeding {@link Integer#MAX_VALUE}.
     *
     * @param hashLength  the output length in bytes ({@code 4 to 2^32 - 1})
     * @param memory      the integer number of KB used ({@code 8p to 2^32 - 1})
     * @param parallelism degree of parallelism ({@code 1 to 2^24 - 1})
     * @param iterations  number of iterations ({@code 1 to 2^32 - 1})
     * @param saltLength  the salt length in bytes {@code 8 to 2^32 - 1})
     */
    public Argon2SecureHasher(Integer hashLength, Integer memory, int parallelism, Integer iterations, Integer saltLength) {

        validateParameters(hashLength, memory, parallelism, iterations, saltLength);

        this.hashLength = hashLength;
        this.memory = memory;
        this.parallelism = parallelism;
        this.iterations = iterations;

        this.saltLength = saltLength;
    }

    /**
     * Enforces valid Argon2 secure hasher cost parameters are provided.
     *
     * @param hashLength  the output length in bytes ({@code 4 to 2^32 - 1})
     * @param memory      the integer number of KB used ({@code 8p to 2^32 - 1})
     * @param parallelism degree of parallelism ({@code 1 to 2^24 - 1})
     * @param iterations  number of iterations ({@code 1 to 2^32 - 1})
     * @param saltLength  the salt length in bytes {@code 8 to 2^32 - 1})
     */
    private void validateParameters(Integer hashLength, Integer memory, int parallelism, Integer iterations, Integer saltLength) {

        if (!isHashLengthValid(hashLength)) {
            logger.error("The provided hash length {} is outside the boundary of 4 to 2^32 - 1.", hashLength);
            throw new IllegalArgumentException("Invalid hash length is not within the hashLength boundary.");
        }
        if (!isMemorySizeValid(memory)) {
            logger.error("The provided memory size {} KiB is outside the boundary of 8p to 2^32 - 1.", memory);
            throw new IllegalArgumentException("Invalid memory size is not within the memory boundary.");
        }
        if (!isParallelismValid(parallelism)) {
            logger.error("The provided parallelization factor {} is outside the boundary of 1 to 2^24 - 1.", parallelism);
            throw new IllegalArgumentException("Invalid parallelization factor exceeds the parallelism boundary.");
        }
        if (!isIterationsValid(iterations)) {
            logger.error("The iteration count {} is outside the boundary of 1 to 2^32 - 1.", iterations);
            throw new IllegalArgumentException("Invalid iteration count exceeds the iterations boundary.");
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
     * Returns whether the provided hash length is within boundaries. The lower bound >= 4 and the
     * upper bound <= 2^32 - 1.
     * @param hashLength the output length in bytes
     * @return true if hashLength is within boundaries
     */
    public static boolean isHashLengthValid(Integer hashLength) {
        if (hashLength < DEFAULT_HASH_LENGTH) {
            logger.warn("The provided hash length {} is below the recommended minimum {}.", hashLength, DEFAULT_HASH_LENGTH);
        }
        return hashLength >= MIN_HASH_LENGTH && hashLength <= UPPER_BOUNDARY;
    }

    /**
     * Returns whether the provided memory size is within boundaries. The lower bound >= 8 and the
     * upper bound <= 2^32 - 1.
     * @param memory the integer number of KiB used
     * @return true if memory is within boundaries
     */
    public static boolean isMemorySizeValid(Integer memory) {
        if (memory < DEFAULT_MEMORY) {
            logger.warn("The provided memory size {} KiB is below the recommended minimum {} KiB.", memory, DEFAULT_MEMORY);
        }
        return memory >= MIN_MEMORY_SIZE_KB && memory <= UPPER_BOUNDARY;
    }

    /**
     * Returns whether the provided parallelization factor is within boundaries. The lower bound >= 1 and the
     * upper bound <= 2^24 - 1.
     * @param parallelism degree of parallelism
     * @return true if parallelism is within boundaries
     */
    public static boolean isParallelismValid(int parallelism) {
        if (parallelism < DEFAULT_PARALLELISM) {
            logger.warn("The provided parallelization factor {} is below the recommended minimum {}.", parallelism, DEFAULT_PARALLELISM);
        }
        return parallelism >= MIN_PARALLELISM && parallelism <= MAX_PARALLELISM;
    }

    /**
     * Returns whether the provided iteration count is within boundaries. The lower bound >= 1 and the
     * upper bound <= 2^32 - 1.
     * @param iterations number of iterations
     * @return true if iterations is within boundaries
     */
    public static boolean isIterationsValid(Integer iterations) {
        if (iterations < DEFAULT_ITERATIONS) {
            logger.warn("The provided iteration count {} is below the recommended minimum {}.", iterations, DEFAULT_ITERATIONS);
        }
        return iterations >= MIN_ITERATIONS && iterations <= UPPER_BOUNDARY;
    }

    /**
     * Returns whether the provided salt length (saltLength) is within boundaries. The lower bound >= 8 and the
     * upper bound <= 2^32 - 1.
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
        return saltLength >= MIN_SALT_LENGTH && saltLength <= UPPER_BOUNDARY;
    }

    /**
     * Returns a String representation of {@code Argon2(input)} in hex-encoded format.
     *
     * @param input the input
     * @return the hex-encoded hash
     */
    @Override
    public String hashHex(String input) {
        if (input == null) {
            logger.warn("Attempting to generate an Argon2 hash of null input; using empty input");
            input = "";
        }

        return Hex.toHexString(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of {@code Argon2(input)} in Base 64-encoded format.
     *
     * @param input the input
     * @return the Base 64-encoded hash
     */
    @Override
    public String hashBase64(String input) {
        if (input == null) {
            logger.warn("Attempting to generate an Argon2 hash of null input; using empty input");
            input = "";
        }

        return Base64.toBase64String(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a byte[] representation of {@code Argon2(input)}.
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
        byte[] salt = getSalt();
        byte[] hash = new byte[hashLength];
        logger.debug("Creating {} byte Argon2 hash with salt [{}]", hashLength, Hex.toHexString(salt));

        final long startNanos = System.nanoTime();

        Argon2Parameters params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
                .withSalt(salt)
                .withParallelism(parallelism)
                .withMemoryAsKB(memory)
                .withIterations(iterations)
                .build();
        Argon2BytesGenerator generator = new Argon2BytesGenerator();
        generator.init(params);

        final long initNanos = System.nanoTime();

        generator.generateBytes(input, hash);

        final long generateNanos = System.nanoTime();

        final long initDurationMicros = TimeUnit.NANOSECONDS.toMicros(initNanos - startNanos);
        final long generateDurationMicros = TimeUnit.NANOSECONDS.toMicros(generateNanos - initNanos);
        final long totalDurationMillis = TimeUnit.MICROSECONDS.toMillis(initDurationMicros + generateDurationMicros);

        logger.debug("Generated Argon2 hash in {} ms (init: {} µs, generate: {} µs)", totalDurationMillis, initDurationMicros, generateDurationMicros);

        return hash;
    }
}
