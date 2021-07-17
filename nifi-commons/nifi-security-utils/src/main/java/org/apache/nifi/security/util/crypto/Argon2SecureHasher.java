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

import java.util.concurrent.TimeUnit;
import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;
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
public class Argon2SecureHasher extends AbstractSecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(Argon2SecureHasher.class);

    private static final int DEFAULT_HASH_LENGTH = 32;
    public static final int DEFAULT_PARALLELISM = 8;
    public static final int DEFAULT_MEMORY = 1 << 16;
    public static final int DEFAULT_ITERATIONS = 5;
    private static final int DEFAULT_SALT_LENGTH = 16;
    private static final int MIN_MEMORY_SIZE_KB = 8;
    private static final int MIN_PARALLELISM = 1;
    private static final int MAX_PARALLELISM = Double.valueOf(Math.pow(2, 24)).intValue() - 1;
    private static final int MIN_HASH_LENGTH = 4;
    private static final int MIN_ITERATIONS = 1;
    private static final int MIN_SALT_LENGTH = 8;

    // Using Integer vs. int to allow for unsigned 32b (values can exceed Integer.MAX_VALUE)
    private final Integer hashLength;
    private final Integer memory;
    private final int parallelism;
    private final Integer iterations;

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
     * Instantiates an Argon2 secure hasher using the provided hash length and default cost parameters
     * ({@code memory = }{@link #DEFAULT_MEMORY},
     * {@code parallelism = }{@link #DEFAULT_PARALLELISM},
     * {@code iterations = }{@link #DEFAULT_ITERATIONS}). A static salt is also used.
     *
     * @param hashLength the desired hash output length in bytes
     */
    public Argon2SecureHasher(Integer hashLength) {
        this(hashLength, DEFAULT_MEMORY, DEFAULT_PARALLELISM, DEFAULT_ITERATIONS, 0);
    }

    /**
     * Instantiates an Argon2 secure hasher using the provided cost parameters. A static
     * {@link #DEFAULT_SALT_LENGTH} byte salt will be generated on every hash request.
     * {@link Integer} is used instead of {@code int} for parameters which have a max value of {@code 2^32 - 1} to allow for unsigned integers exceeding {@link Integer#MAX_VALUE}.
     *
     * @param hashLength  the output length in bytes ({@code 4 to 2^32 - 1})
     * @param memory      the integer number of KiB used ({@code 8p to 2^32 - 1})
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
     * @param memory      the integer number of KiB used ({@code 8p to 2^32 - 1})
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
     * @param memory      the integer number of KiB used ({@code 8p to 2^32 - 1})
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

        initializeSalt(saltLength);
    }

    /**
     * Returns the algorithm-specific name for logging and messages.
     *
     * @return the algorithm name
     */
    @Override
    String getAlgorithmName() {
        return "Argon2";
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
     * Returns whether the provided hash length is within boundaries. The lower bound >= 4 and the
     * upper bound <= 2^32 - 1.
     * @param hashLength the output length in bytes
     * @return true if hashLength is within boundaries
     */
    public static boolean isHashLengthValid(Integer hashLength) {
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
         logger.debug("Creating {} byte Argon2 hash with salt [{}]", hashLength, Hex.toHexString(rawSalt));

         if (!isSaltLengthValid(rawSalt.length)) {
             throw new IllegalArgumentException("The salt length (" + rawSalt.length + " bytes) is invalid");
         }

         byte[] hash = new byte[hashLength];

        final long startNanos = System.nanoTime();

        Argon2Parameters params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
                .withSalt(rawSalt)
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
