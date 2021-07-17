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
package org.apache.nifi.security.kms;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.repository.config.RepositoryEncryptionConfiguration;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.bouncycastle.util.encoders.DecoderException;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class CryptoUtils {
    private static final Logger logger = LoggerFactory.getLogger(CryptoUtils.class);
    public static final String STATIC_KEY_PROVIDER_CLASS_NAME = "org.apache.nifi.security.kms.StaticKeyProvider";
    public static final String FILE_BASED_KEY_PROVIDER_CLASS_NAME = "org.apache.nifi.security.kms.FileBasedKeyProvider";
    public static final String KEY_STORE_KEY_PROVIDER_CLASS_NAME = "org.apache.nifi.security.kms.KeyStoreKeyProvider";

    // TODO: Move to RepositoryEncryptionUtils in NIFI-6617
    public static final String LEGACY_SKP_FQCN = "org.apache.nifi.provenance.StaticKeyProvider";
    public static final String LEGACY_FBKP_FQCN = "org.apache.nifi.provenance.FileBasedKeyProvider";

    // TODO: Enforce even length
    private static final Pattern HEX_PATTERN = Pattern.compile("(?i)^[0-9a-f]+$");

    private static final List<Integer> UNLIMITED_KEY_LENGTHS = Arrays.asList(32, 48, 64);

    public static final String ENCRYPTED_FSR_CLASS_NAME = "org.apache.nifi.controller.repository.crypto.EncryptedFileSystemRepository";
    public static final String EWAFFR_CLASS_NAME = "org.apache.nifi.controller.repository.crypto.EncryptedWriteAheadFlowFileRepository";

    public static boolean isUnlimitedStrengthCryptoAvailable() {
        try {
            return Cipher.getMaxAllowedKeyLength("AES") > 128;
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Tried to determine if unlimited strength crypto is available but the AES algorithm is not available");
            return false;
        }
    }

    /**
     * Utility method which returns true if the string is null, empty, or entirely whitespace.
     *
     * @param src the string to evaluate
     * @return true if empty
     */
    public static boolean isEmpty(String src) {
        return src == null || src.trim().isEmpty();
    }

    /**
     * Concatenates multiple byte[] into a single byte[].
     *
     * @param arrays the component byte[] in order
     * @return a concatenated byte[]
     */
    public static byte[] concatByteArrays(byte[]... arrays) {
        int totalByteLength = 0;
        for (byte[] bytes : arrays) {
            totalByteLength += bytes.length;
        }
        byte[] totalBytes = new byte[totalByteLength];
        int currentLength = 0;
        for (byte[] bytes : arrays) {
            System.arraycopy(bytes, 0, totalBytes, currentLength, bytes.length);
            currentLength += bytes.length;
        }
        return totalBytes;
    }

    /**
     * Returns true if the provided configuration values are valid (shallow evaluation only; does not validate the keys
     * contained in a {@link FileBasedKeyProvider}).
     *
     * @param rec the configuration to validate
     * @return true if the config is valid
     */
    public static boolean isValidRepositoryEncryptionConfiguration(RepositoryEncryptionConfiguration rec) {
        return isValidKeyProvider(rec.getKeyProviderImplementation(), rec.getKeyProviderLocation(), rec.getEncryptionKeyId(), rec.getEncryptionKeys());

    }

    /**
     * Returns true if the provided configuration values successfully define the specified {@link KeyProvider}.
     *
     * @param keyProviderImplementation the FQ class name of the {@link KeyProvider} implementation
     * @param keyProviderLocation       the location of the definition (for {@link FileBasedKeyProvider}, etc.)
     * @param keyId                     the active key ID
     * @param encryptionKeys            a map of key IDs to key material in hex format
     * @return true if the provided configuration is valid
     */
    public static boolean isValidKeyProvider(String keyProviderImplementation, final String keyProviderLocation, final String keyId, final Map<String, String> encryptionKeys) {
        try {
            keyProviderImplementation = handleLegacyPackages(keyProviderImplementation);
        } catch (final KeyManagementException e) {
            logger.warn("Key Provider [{}] Validation Failed: {}", keyProviderImplementation, e.getMessage());
            return false;
        }

        switch (keyProviderImplementation) {
            case STATIC_KEY_PROVIDER_CLASS_NAME:
                if (encryptionKeys == null) {
                    return false;
                } else {
                    boolean everyKeyValid = encryptionKeys.values().stream().allMatch(CryptoUtils::keyIsValid);
                    return everyKeyValid && StringUtils.isNotEmpty(keyId);
                }
            case FILE_BASED_KEY_PROVIDER_CLASS_NAME:
            case KEY_STORE_KEY_PROVIDER_CLASS_NAME:
                final Path keyProviderPath = Paths.get(keyProviderLocation);
                return Files.isReadable(keyProviderPath) && StringUtils.isNotEmpty(keyId);
            default:
                logger.warn("Validation Failed: Key Provider [{}] Location [{}] Key ID [{}]", keyProviderImplementation, keyProviderLocation, keyId);
                return false;
        }
    }

    static String handleLegacyPackages(String implementationClassName) throws KeyManagementException {
        if (org.apache.nifi.util.StringUtils.isBlank(implementationClassName)) {
            throw new KeyManagementException("Invalid key provider implementation provided: " + implementationClassName);
        }
        if (implementationClassName.equalsIgnoreCase(LEGACY_SKP_FQCN)) {
            return StaticKeyProvider.class.getName();
        } else if (implementationClassName.equalsIgnoreCase(LEGACY_FBKP_FQCN)) {
            return FileBasedKeyProvider.class.getName();
        } else {
            return implementationClassName;
        }
    }

    /**
     * Returns true if the provided key is valid hex and is the correct length for the current system's JCE policies.
     *
     * @param encryptionKeyHex the key in hexadecimal
     * @return true if this key is valid
     */
    public static boolean keyIsValid(String encryptionKeyHex) {
        return isHexString(encryptionKeyHex)
                && (isUnlimitedStrengthCryptoAvailable()
                ? UNLIMITED_KEY_LENGTHS.contains(encryptionKeyHex.length())
                : encryptionKeyHex.length() == 32);
    }

    /**
     * Returns true if the input is valid hexadecimal (does not enforce length and is case-insensitive).
     *
     * @param hexString the string to evaluate
     * @return true if the string is valid hex
     */
    public static boolean isHexString(String hexString) {
        return StringUtils.isNotEmpty(hexString) && HEX_PATTERN.matcher(hexString).matches();
    }

    /**
     * Returns the root key from the {@code bootstrap.conf} file used to encrypt various sensitive properties and data encryption keys.
     *
     * @return the root key
     * @throws KeyManagementException if the key cannot be read
     */
    public static SecretKey getRootKey() throws KeyManagementException {
        try {
            // Get the root encryption key from bootstrap.conf
            String rootKeyHex = NiFiBootstrapUtils.extractKeyFromBootstrapFile();
            return new SecretKeySpec(Hex.decode(rootKeyHex), "AES");
        } catch (IOException | DecoderException e) {
            logger.error("Encountered an error: ", e);
            throw new KeyManagementException(e);
        }
    }

    /**
     * Returns true if the two parameters are equal. This method is null-safe and evaluates the
     * equality in constant-time rather than "short-circuiting" on the first inequality. This
     * prevents timing attacks (side channel attacks) when comparing passwords or hash values.
     *
     * @param a a String to compare
     * @param b a String to compare
     * @return true if the values are equal
     */
    public static boolean constantTimeEquals(String a, String b) {
        if (a == null) {
            return b == null;
        } else {
            // This returns true IFF b != null and the byte[] are equal; if b == null, a is not, and they are not equal
            return b != null && constantTimeEquals(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Returns true if the two parameters are equal. This method is null-safe and evaluates the
     * equality in constant-time rather than "short-circuiting" on the first inequality. This
     * prevents timing attacks (side channel attacks) when comparing passwords or hash values.
     * Does not convert the character arrays to {@code String}s when converting to {@code byte[]}
     * to avoid putting sensitive data in the String pool.
     *
     * @param a a char[] to compare
     * @param b a char[] to compare
     * @return true if the values are equal
     */
    public static boolean constantTimeEquals(char[] a, char[] b) {
        return constantTimeEquals(convertCharsToBytes(a), convertCharsToBytes(b));
    }


    /**
     * Returns true if the two parameters are equal. This method is null-safe and evaluates the
     * equality in constant-time rather than "short-circuiting" on the first inequality. This
     * prevents timing attacks (side channel attacks) when comparing passwords or hash values.
     *
     * @param a a byte[] to compare
     * @param b a byte[] to compare
     * @return true if the values are equal
     */
    public static boolean constantTimeEquals(byte[] a, byte[] b) {
        return MessageDigest.isEqual(a, b);
    }

    /**
     * Returns a {@code byte[]} containing the value of the provided {@code char[]} without using {@code new String(chars).getBytes()} which would put sensitive data (the password) in the String pool.
     *
     * @param chars the characters to convert
     * @return the byte[]
     */
    private static byte[] convertCharsToBytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        return Arrays.copyOfRange(byteBuffer.array(),
                byteBuffer.position(), byteBuffer.limit());
    }
}
