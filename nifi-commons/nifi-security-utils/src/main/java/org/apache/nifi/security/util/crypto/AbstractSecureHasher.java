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

import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public abstract class AbstractSecureHasher implements SecureHasher {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSecureHasher.class);

    protected int saltLength;

    private boolean usingStaticSalt;

    // A 16 byte salt (nonce) is recommended for password hashing
    private static final byte[] STATIC_SALT = "NiFi Static Salt".getBytes(StandardCharsets.UTF_8);

    // Upper boundary for several cost parameters
    static final Integer UPPER_BOUNDARY = Double.valueOf(Math.pow(2, 32)).intValue() - 1;

    /**
     * Verifies the salt length is valid for this algorithm and if a static salt should be used.
     *
     * @param saltLength the salt length in bytes
     */
    protected void initializeSalt(Integer saltLength) {
        if (saltLength > 0) {
            if (!isSaltLengthValid(saltLength)) {
                logger.error("The salt length {} is outside the boundary of {} to {}.", saltLength, getMinSaltLength(), getMaxSaltLength());
                throw new IllegalArgumentException("Invalid salt length exceeds the saltLength boundary.");
            }
            this.usingStaticSalt = false;
        } else {
            this.usingStaticSalt = true;
            logger.debug("Configured to use static salt");
        }
    }

    /**
     * Returns whether the provided salt length (saltLength) is within boundaries. The lower bound >= (usually) 8 and the
     * upper bound <= (usually) {@link Integer#MAX_VALUE}. This method is not {@code static} because it depends on the
     * instantiation of the algorithm-specific concrete class.
     *
     * @param saltLength the salt length in bytes
     * @return true if saltLength is within boundaries
     */
    public boolean isSaltLengthValid(Integer saltLength) {
        final int SALT_LENGTH = getDefaultSaltLength();
        if (saltLength == 0) {
            logger.debug("The provided salt length 0 indicates a static salt of {} bytes", SALT_LENGTH);
            return true;
        }
        if (saltLength < SALT_LENGTH) {
            logger.warn("The provided dynamic salt length {} is below the recommended minimum {}", saltLength, SALT_LENGTH);
        }
        return saltLength >= getMinSaltLength() && saltLength <= getMaxSaltLength();
    }

    /**
     * Returns the algorithm-specific default salt length in bytes.
     *
     * @return the default salt length
     */
    abstract int getDefaultSaltLength();

    /**
     * Returns the algorithm-specific minimum salt length in bytes.
     *
     * @return the min salt length
     */
    abstract int getMinSaltLength();

    /**
     * Returns the algorithm-specific maximum salt length in bytes.
     *
     * @return the max salt length
     */
    abstract int getMaxSaltLength();

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
     * Returns the algorithm-specific name for logging and messages.
     *
     * @return the algorithm name
     */
    abstract String getAlgorithmName();

    /**
     * Returns {@code true} if the algorithm can accept empty (non-{@code null}) inputs.
     *
     * @return the true if {@code ""} is allowable input
     */
    abstract boolean acceptsEmptyInput();

    /**
     * Returns a String representation of the hash in hex-encoded format.
     *
     * @param input the non-empty input
     * @return the hex-encoded hash
     */
    @Override
    public String hashHex(String input) {
        try {
            input = validateInput(input);
        } catch (IllegalArgumentException e) {
            return "";
        }
        return Hex.toHexString(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of the hash in hex-encoded format.
     *
     * @param input the non-empty input
     * @param salt  the provided salt
     *
     * @return the hex-encoded hash
     */
    @Override
    public String hashHex(String input, String salt) {
        try {
            input = validateInput(input);
        } catch (IllegalArgumentException e) {
            return "";
        }

        return Hex.toHexString(hash(input.getBytes(StandardCharsets.UTF_8), salt.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of the hash in Base 64-encoded format.
     *
     * @param input the non-empty input
     * @return the Base 64-encoded hash
     */
    @Override
    public String hashBase64(String input) {
        try {
            input = validateInput(input);
        } catch (IllegalArgumentException e) {
            return "";
        }

        return CipherUtility.encodeBase64NoPadding(hash(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a String representation of the hash in Base 64-encoded format.
     *
     * @param input the non-empty input
     * @param salt  the provided salt
     *
     * @return the Base 64-encoded hash
     */
    @Override
    public String hashBase64(String input, String salt) {
        try {
            input = validateInput(input);
        } catch (IllegalArgumentException e) {
            return "";
        }

        return CipherUtility.encodeBase64NoPadding(hash(input.getBytes(StandardCharsets.UTF_8), salt.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns a byte[] representation of {@code SecureHasher.hash(input)}.
     *
     * @param input the input
     * @return the hash
     */
    public byte[] hashRaw(byte[] input) {
        return hash(input);
    }

    /**
     * Returns a byte[] representation of {@code SecureHasher.hash(input)}.
     *
     * @param input the input
     * @param salt  the provided salt
     *
     * @return the hash
     */
    @Override
    public byte[] hashRaw(byte[] input, byte[] salt) {
        return hash(input, salt);
    }

    /**
     * Returns the valid {@code input} String (if the algorithm accepts empty input, changes {@code null} to {@code ""}; if not, throws {@link IllegalArgumentException}).
     *
     * @param input the input to validate
     * @return a valid input string
     */
    private String validateInput(String input) {
        if (acceptsEmptyInput()) {
            if (input == null) {
                logger.warn("Attempting to generate a hash using {} of null input; using empty input", getAlgorithmName());
                return "";
            }
        } else {
            if (input == null || input.length() == 0) {
                logger.warn("Attempting to generate a hash using {} of null or empty input; returning 0 length string", getAlgorithmName());
                throw new IllegalArgumentException();
            }
        }

        return input;
    }

    /**
     * Returns the algorithm-specific calculated hash for the input and generates or retrieves the salt according to
     * the configured salt length.
     *
     * @param input the input in raw bytes
     * @return the hash in raw bytes
     */
    abstract byte[] hash(byte[] input);

    /**
     * Returns the algorithm-specific calculated hash for the input and salt.
     *
     * @param input the input in raw bytes
     * @param salt  the provided salt
     * @return the hash in raw bytes
     */
    abstract byte[] hash(byte[] input, byte[] salt);
}
