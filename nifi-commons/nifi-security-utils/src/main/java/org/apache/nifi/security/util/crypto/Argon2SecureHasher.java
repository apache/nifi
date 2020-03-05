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

    private final int hashLength;
    private final int memory;
    private final int parallelism;
    private final int iterations;
    private final int saltLength;

    private final boolean usingStaticSalt;

    // A 16 byte salt (nonce) is recommended for password hashing
    private static final byte[] staticSalt = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

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
     *
     * @param hashLength  the output length in bytes ({@code 4 to 2^32 - 1})
     * @param memory      the integer number of KB used ({@code 8p to 2^32 - 1})
     * @param parallelism degree of parallelism ({@code 1 to 2^24 - 1})
     * @param iterations  number of iterations ({@code 1 to 2^32 - 1})
     */
    public Argon2SecureHasher(int hashLength, int memory, int parallelism, int iterations) {
        this(hashLength, memory, parallelism, iterations, 0);
    }

    /**
     * Instantiates an Argon2 secure hasher using the provided cost parameters. A unique
     * salt of the specified length will be generated on every hash request.
     *
     * @param hashLength  the output length in bytes ({@code 4 to 2^32 - 1})
     * @param memory      the integer number of KB used ({@code 8p to 2^32 - 1})
     * @param parallelism degree of parallelism ({@code 1 to 2^24 - 1})
     * @param iterations  number of iterations ({@code 1 to 2^32 - 1})
     * @param saltLength  the salt length in bytes {@code 8 to 2^32 - 1})
     */
    public Argon2SecureHasher(int hashLength, int memory, int parallelism, int iterations, int saltLength) {
        // TODO: Implement boundary checking
        this.hashLength = hashLength;
        this.memory = memory;
        this.parallelism = parallelism;
        this.iterations = iterations;

        this.saltLength = saltLength;
        if (saltLength > 0) {
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
            return staticSalt;
        } else {
            SecureRandom sr = new SecureRandom();
            byte[] salt = new byte[saltLength];
            sr.nextBytes(salt);
            return salt;
        }
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
