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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Argon2CipherProvider extends RandomIVPBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(Argon2CipherProvider.class);

    private static final int DEFAULT_PARALLELISM = Argon2SecureHasher.DEFAULT_PARALLELISM;
    private static final int DEFAULT_MEMORY = Argon2SecureHasher.DEFAULT_MEMORY;
    private static final int DEFAULT_ITERATIONS = Argon2SecureHasher.DEFAULT_ITERATIONS;
    private static final int DEFAULT_SALT_LENGTH = 16;

    // Using Integer vs. int to allow for unsigned 32b (values can exceed Integer.MAX_VALUE)
    private final Integer memory;
    private final int parallelism;
    private final Integer iterations;

    // Argon2 full salt pattern: $argon2id$v=19$m=1024,t=3,p=1$ftvICs8WpASuN3FnRIDcRA$
    private static final Pattern ARGON2_SALT_FORMAT = Pattern.compile("^\\$argon2id\\$v=19\\$m=\\d+,t=\\d+,p=\\d+\\$[\\w\\+\\/]{22}(?=\\$?)$");

    /**
     * Instantiates an Argon2 cipher provider using the default cost parameters
     * {@code memory = }{@link #DEFAULT_MEMORY},
     * {@code parallelism = }{@link #DEFAULT_PARALLELISM},
     * {@code iterations = }{@link #DEFAULT_ITERATIONS}). A static salt is also used.
     */
    public Argon2CipherProvider() {
        this(DEFAULT_MEMORY, DEFAULT_PARALLELISM, DEFAULT_ITERATIONS);
    }

    /**
     * Instantiates an Argon2 cipher provider using the provided cost parameters.
     *
     * @param memory      the integer number of KiB used ({@code 8p to 2^32 - 1})
     * @param parallelism degree of parallelism ({@code 1 to 2^24 - 1})
     * @param iterations  number of iterations ({@code 1 to 2^32 - 1})
     */
    public Argon2CipherProvider(Integer memory, int parallelism, Integer iterations) {
        this.memory = memory;
        this.parallelism = parallelism;
        this.iterations = iterations;
        if (memory < DEFAULT_MEMORY) {
            logger.warn("The provided memory size {} KiB is below the recommended minimum {} KiB", memory, DEFAULT_MEMORY);
        }
        if (parallelism < DEFAULT_PARALLELISM) {
            logger.warn("The provided parallelization factor {} is below the recommended minimum {}", parallelism, DEFAULT_PARALLELISM);
        }
        if (iterations < DEFAULT_ITERATIONS) {
            logger.warn("The provided iterations count {} is below the recommended minimum {}", iterations, DEFAULT_ITERATIONS);
        }
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key is derived by the KDF of the implementation. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$argon2id$v=19$m=1024,t=3,p=1$ftvICs8WpASuN3FnRIDcRA$eB912UtYgZjKdwK64V7pfmMiDbsaPK+hts6H6cSHl3I".getBytes(StandardCharsets.UTF_8)})
     * @param iv               the IV
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode) throws Exception {
        try {
            return getInitializedCipher(encryptionMethod, password, salt, iv, keyLength, encryptMode);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    @Override
    Logger getLogger() {
        return logger;
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
     * <p>
     * The IV can be retrieved by the calling method using {@link Cipher#getIV()}.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the complete salt (e.g. {@code "$argon2id$v=19$m=1024,t=3,p=1$ftvICs8WpASuN3FnRIDcRA$eB912UtYgZjKdwK64V7pfmMiDbsaPK+hts6H6cSHl3I".getBytes(StandardCharsets.UTF_8)})
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, int keyLength, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, password, salt, new byte[0], keyLength, encryptMode);
    }

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode) throws Exception {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("The encryption method must be specified");
        }
        if (!encryptionMethod.isCompatibleWithStrongKDFs()) {
            throw new IllegalArgumentException(encryptionMethod.name() + " is not compatible with Argon2");
        }

        if (StringUtils.isEmpty(password)) {
            throw new IllegalArgumentException("Encryption with an empty password is not supported");
        }

        String algorithm = encryptionMethod.getAlgorithm();

        final String cipherName = CipherUtility.parseCipherFromAlgorithm(algorithm);
        if (!CipherUtility.isValidKeyLength(keyLength, cipherName)) {
            throw new IllegalArgumentException(keyLength + " is not a valid key length for " + cipherName);
        }

        String saltString = new String(salt, StandardCharsets.UTF_8);
        byte[] rawSalt = new byte[getDefaultSaltLength()];
        int memory;
        int parallelism;
        int iterations;
        if (isArgon2FormattedSalt(saltString)) {
            List<Integer> params = new ArrayList<>(3);
            parseSalt(saltString, rawSalt, params);
            memory = params.get(0);
            iterations = params.get(1);
            parallelism = params.get(2);
        } else {
            rawSalt = salt;
            memory = getMemory();
            iterations = getIterations();
            parallelism = getParallelism();
        }

        Argon2SecureHasher argon2SecureHasher = new Argon2SecureHasher(keyLength / 8, memory, parallelism, iterations);
        try {
            byte[] keyBytes = argon2SecureHasher.hashRaw(password.getBytes(StandardCharsets.UTF_8), rawSalt);
            SecretKey tempKey = new SecretKeySpec(keyBytes, algorithm);

            KeyedCipherProvider keyedCipherProvider = new AESKeyedCipherProvider();
            return keyedCipherProvider.getCipher(encryptionMethod, tempKey, iv, encryptMode);
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("The salt length")) {
                throw new IllegalArgumentException("The raw salt must be greater than or equal to 8 bytes", e);
            } else {
                logger.error("Encountered an error generating the Argon2 hash", e);
                throw e;
            }
        }
    }

    /**
     * Returns the raw salt contained in the provided Argon2 salt string.
     *
     * @param argon2Salt the full Argon2 salt
     * @return the raw salt decoded from Base64
     */
    public static byte[] extractRawSaltFromArgon2Salt(String argon2Salt) {
        final String[] saltComponents = argon2Salt.split("\\$");
        // saltComponents = [(optional empty before leading $), argon2id, v=19, m=1024,t=3,p=1, ftvICs8WpASuN3FnRIDcRA]
        if (saltComponents.length < 4) {
            throw new IllegalArgumentException("Could not parse salt");
        }
        return Base64.decodeBase64(saltComponents[saltComponents.length - 1]);
    }

    /**
     * Returns {@code true} if the salt string is a valid Argon2 salt string ({@code $argon2id$v=19$m=4096,t=3,p=1$abcdefghi..{22}}).
     *
     * @param salt the salt string to evaluate
     * @return true if valid Argon2 salt
     */
    public static boolean isArgon2FormattedSalt(String salt) {
        if (salt == null || salt.length() == 0) {
            throw new IllegalArgumentException("The salt cannot be empty. To generate a salt, use Argon2CipherProvider#generateSalt()");
        }
        Matcher matcher = ARGON2_SALT_FORMAT.matcher(salt);
        return matcher.find();
    }

    private void parseSalt(String argon2Salt, byte[] rawSalt, List<Integer> params) {
        if (StringUtils.isEmpty(argon2Salt)) {
            throw new IllegalArgumentException("Cannot parse empty salt");
        }

        /** Salt format is $argon2id$v=19$m=memory,t=iterations,p=parallelism$saltB64$hash */
        byte[] salt = extractRawSaltFromArgon2Salt(argon2Salt);

        if (rawSalt.length < salt.length) {
            byte[] tempBytes = new byte[salt.length];
            System.arraycopy(rawSalt, 0, tempBytes, 0, rawSalt.length);
            rawSalt = tempBytes;
        }
        System.arraycopy(salt, 0, rawSalt, 0, rawSalt.length);

        if (params == null) {
            params = new ArrayList<>(3);
        }

        final String[] saltComponents = argon2Salt.split("\\$");
        // saltComponents = [, argon2id, v=19, m=1024,t=4,p=1, hiKyaQbZyQBmCmD1zGcyMw, rc+ec+/hQeBcwzjH+OEmUtaTUqhZYKN4ZKJtWzFZYjQ]
        final Map<String, String> saltParams = Arrays.stream(saltComponents[3].split(","))
                .collect(Collectors.toMap(
                        pair -> pair.split("=")[0],
                        pair -> pair.split("=")[1])
                );
        params.add(Integer.parseInt(saltParams.get("m")));
        params.add(Integer.parseInt(saltParams.get("t")));
        params.add(Integer.parseInt(saltParams.get("p")));
    }

    @Override
    public byte[] generateSalt() {
        byte[] rawSalt = new byte[DEFAULT_SALT_LENGTH];
        new SecureRandom().nextBytes(rawSalt);

        // TODO: Handle 2i and 2d implementations
        // Form the "full salt" containing the embedded algorithm, version, cost params, and raw salt
        return formSalt(rawSalt, getMemory(), getIterations(), getParallelism()).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns the formatted Argon2 salt string given the provided parameters.
     * <p>
     * {@code $argon2id$v=19$m=4096,t=3,p=1$abcdefABCDEF0123456789}
     *
     * @param rawSalt     the salt bytes
     * @param memory      the memory cost in KiB
     * @param iterations  the iterations
     * @param parallelism the parallelism factor
     * @return the formatted salt string
     */
    public static String formSalt(byte[] rawSalt, int memory, int iterations, int parallelism) {
        StringBuilder sb = new StringBuilder("$argon2id$");
        sb.append("v=19").append("$");
        sb.append("m=").append(memory).append(",");
        sb.append("t=").append(iterations).append(",");
        sb.append("p=").append(parallelism).append("$");
        sb.append(CipherUtility.encodeBase64NoPadding(rawSalt));
        return sb.toString();
    }

    @Override
    public int getDefaultSaltLength() {
        return DEFAULT_SALT_LENGTH;
    }

    protected int getMemory() {
        return memory;
    }

    protected int getParallelism() {
        return parallelism;
    }

    protected int getIterations() {
        return iterations;
    }
}