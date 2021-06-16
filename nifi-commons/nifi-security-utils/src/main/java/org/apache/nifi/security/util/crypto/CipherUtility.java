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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CipherUtility {

    public static final int BUFFER_SIZE = 65536;
    private static final Pattern KEY_LENGTH_PATTERN = Pattern.compile("([\\d]+)BIT");

    private static final Map<String, Integer> MAX_PASSWORD_LENGTH_BY_ALGORITHM;
    private static final int DEFAULT_MAX_ALLOWED_KEY_LENGTH = 128;

    static {
        Map<String, Integer> aMap = new HashMap<>();
        /**
         * These values were determined empirically by running {@link NiFiLegacyCipherProviderGroovyTest#testShouldDetermineDependenceOnUnlimitedStrengthCrypto()}
         *, which evaluates each algorithm in a try/catch harness with increasing password size until it throws an exception.
         * This was performed on a JVM without the Unlimited Strength Jurisdiction cryptographic policy files installed.
         */
        aMap.put("PBEWITHMD5AND128BITAES-CBC-OPENSSL", 16);
        aMap.put("PBEWITHMD5AND192BITAES-CBC-OPENSSL", 16);
        aMap.put("PBEWITHMD5AND256BITAES-CBC-OPENSSL", 16);
        aMap.put("PBEWITHMD5ANDDES", 16);
        aMap.put("PBEWITHMD5ANDRC2", 16);
        aMap.put("PBEWITHSHA1ANDRC2", 16);
        aMap.put("PBEWITHSHA1ANDDES", 16);
        aMap.put("PBEWITHSHAAND128BITAES-CBC-BC", 7);
        aMap.put("PBEWITHSHAAND192BITAES-CBC-BC", 7);
        aMap.put("PBEWITHSHAAND256BITAES-CBC-BC", 7);
        aMap.put("PBEWITHSHAAND40BITRC2-CBC", 7);
        aMap.put("PBEWITHSHAAND128BITRC2-CBC", 7);
        aMap.put("PBEWITHSHAAND40BITRC4", 7);
        aMap.put("PBEWITHSHAAND128BITRC4", 7);
        aMap.put("PBEWITHSHA256AND128BITAES-CBC-BC", 7);
        aMap.put("PBEWITHSHA256AND192BITAES-CBC-BC", 7);
        aMap.put("PBEWITHSHA256AND256BITAES-CBC-BC", 7);
        aMap.put("PBEWITHSHAAND2-KEYTRIPLEDES-CBC", 7);
        aMap.put("PBEWITHSHAAND3-KEYTRIPLEDES-CBC", 7);
        aMap.put("PBEWITHSHAANDTWOFISH-CBC", 7);
        MAX_PASSWORD_LENGTH_BY_ALGORITHM = Collections.unmodifiableMap(aMap);
    }

    /**
     * Returns the cipher algorithm from the full algorithm name. Useful for getting key lengths, etc.
     * <p/>
     * Ex: PBEWITHMD5AND128BITAES-CBC-OPENSSL -> AES
     *
     * @param algorithm the full algorithm name
     * @return the generic cipher name or the full algorithm if one cannot be extracted
     */
    public static String parseCipherFromAlgorithm(final String algorithm) {
        if (StringUtils.isEmpty(algorithm)) {
            return algorithm;
        }
        String formattedAlgorithm = algorithm.toUpperCase();

        // This is not optimal but the algorithms do not have a standard format
        final String AES = "AES";
        final String TDES = "TRIPLEDES";
        final String TDES_ALTERNATE = "DESEDE";
        final String DES = "DES";
        final String RC4 = "RC4";
        final String RC2 = "RC2";
        final String TWOFISH = "TWOFISH";
        final List<String> SYMMETRIC_CIPHERS = Arrays.asList(AES, TDES, TDES_ALTERNATE, DES, RC4, RC2, TWOFISH);

        // The algorithms contain "TRIPLEDES" but the cipher name is "DESede"
        final String ACTUAL_TDES_CIPHER = "DESede";

        for (String cipher : SYMMETRIC_CIPHERS) {
            if (formattedAlgorithm.contains(cipher)) {
                if (cipher.equals(TDES) || cipher.equals(TDES_ALTERNATE)) {
                    return ACTUAL_TDES_CIPHER;
                } else {
                    return cipher;
                }
            }
        }

        return algorithm;
    }

    /**
     * Returns the cipher key length from the full algorithm name. Useful for getting key lengths, etc.
     * <p/>
     * Ex: PBEWITHMD5AND128BITAES-CBC-OPENSSL -> 128
     *
     * @param algorithm the full algorithm name
     * @return the key length or -1 if one cannot be extracted
     */
    public static int parseKeyLengthFromAlgorithm(final String algorithm) {
        int keyLength = parseActualKeyLengthFromAlgorithm(algorithm);
        if (keyLength != -1) {
            return keyLength;
        } else {
            // Key length not explicitly named in algorithm
            String cipher = parseCipherFromAlgorithm(algorithm);
            return getDefaultKeyLengthForCipher(cipher);
        }
    }

    private static int parseActualKeyLengthFromAlgorithm(final String algorithm) {
        Matcher matcher = KEY_LENGTH_PATTERN.matcher(algorithm);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            return -1;
        }
    }

    /**
     * Returns true if the provided key length is a valid key length for the provided cipher family. Does not reflect if the Unlimited Strength Cryptography Jurisdiction Policies are installed.
     * Does not reflect if the key length is correct for a specific combination of cipher and PBE-derived key length.
     * <p/>
     * Ex:
     * <p/>
     * 256 is valid for {@code AES/CBC/PKCS7Padding} but not {@code PBEWITHMD5AND128BITAES-CBC-OPENSSL}. However, this method will return {@code true} for both because it only gets the cipher
     * family, {@code AES}.
     * <p/>
     * 64, AES -> false
     * [128, 192, 256], AES -> true
     *
     * @param keyLength the key length in bits
     * @param cipher    the cipher family
     * @return true if this key length is valid
     */
    public static boolean isValidKeyLength(int keyLength, final String cipher) {
        if (StringUtils.isEmpty(cipher)) {
            return false;
        }
        return getValidKeyLengthsForAlgorithm(cipher).contains(keyLength);
    }

    /**
     * Returns true if the provided key length is a valid key length for the provided algorithm. Does not reflect if the Unlimited Strength Cryptography Jurisdiction Policies are installed.
     * <p/>
     * Ex:
     * <p/>
     * 256 is valid for {@code AES/CBC/PKCS7Padding} but not {@code PBEWITHMD5AND128BITAES-CBC-OPENSSL}.
     * <p/>
     * 64, AES/CBC/PKCS7Padding -> false
     * [128, 192, 256], AES/CBC/PKCS7Padding -> true
     * <p/>
     * 128, PBEWITHMD5AND128BITAES-CBC-OPENSSL -> true
     * [192, 256], PBEWITHMD5AND128BITAES-CBC-OPENSSL -> false
     *
     * @param keyLength the key length in bits
     * @param algorithm the specific algorithm
     * @return true if this key length is valid
     */
    public static boolean isValidKeyLengthForAlgorithm(int keyLength, final String algorithm) {
        if (StringUtils.isEmpty(algorithm)) {
            return false;
        }
        return getValidKeyLengthsForAlgorithm(algorithm).contains(keyLength);
    }

    /**
     * Returns a list of valid key lengths in bits for this algorithm. If the algorithm cannot be parsed, an empty list is returned.
     *
     * @param algorithm the name of the algorithm
     * @return a list of valid key lengths
     */
    public static List<Integer> getValidKeyLengthsForAlgorithm(String algorithm) {
        List<Integer> validKeyLengths = new ArrayList<>();
        if (StringUtils.isEmpty(algorithm)) {
            return validKeyLengths;
        }

        // Some algorithms specify a single key size
        int keyLength = parseActualKeyLengthFromAlgorithm(algorithm);
        if (keyLength != -1) {
            validKeyLengths.add(keyLength);
            return validKeyLengths;
        }

        // The algorithm does not specify a key size
        String cipher = parseCipherFromAlgorithm(algorithm);
        switch (cipher.toUpperCase()) {
            case "DESEDE":
                // 3DES keys have the cryptographic strength of 7/8 because of parity bits, but are often represented with n*8 bytes
                return Arrays.asList(56, 64, 112, 128, 168, 192);
            case "DES":
                return Arrays.asList(56, 64);
            case "RC2":
            case "RC4":
            case "RC5":
                /** These ciphers can have arbitrary length keys but that's a really bad idea, {@see http://crypto.stackexchange.com/a/9963/12569}.
                 * Also, RC* is deprecated and should be considered insecure */
                for (int i = 40; i <= 2048; i++) {
                    validKeyLengths.add(i);
                }
                return validKeyLengths;
            case "AES":
            case "TWOFISH":
                return Arrays.asList(128, 192, 256);
            default:
                return validKeyLengths;
        }
    }

    private static int getDefaultKeyLengthForCipher(String cipher) {
        if (StringUtils.isEmpty(cipher)) {
            return -1;
        }
        cipher = cipher.toUpperCase();
        switch (cipher) {
            case "DESEDE":
                return 112;
            case "DES":
                return 64;
            case "RC2":
            case "RC4":
            case "RC5":
            default:
                return 128;
        }
    }

    public static void processStreams(Cipher cipher, InputStream in, OutputStream out) {
        try {
            final byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] transformedBytes = cipher.update(buffer, 0, len);
                if (transformedBytes != null) {
                    out.write(transformedBytes);
                }
            }

            out.write(cipher.doFinal());
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    public static byte[] readBytesFromInputStream(InputStream in, String label, int limit, byte[] delimiter) throws IOException, ProcessException {
        if (in == null) {
            throw new IllegalArgumentException("Cannot read " + label + " from null InputStream");
        }

        // If the value is not detected within the first n bytes, throw an exception
        in.mark(limit);

        // The first n bytes of the input stream contain the value up to the custom delimiter
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        byte[] stoppedBy = StreamUtils.copyExclusive(in, bytesOut, limit + delimiter.length, delimiter);

        if (stoppedBy != null) {
            return bytesOut.toByteArray();
        }

        // If no delimiter was found, reset the cursor
        in.reset();
        return null;
    }

    public static void writeBytesToOutputStream(OutputStream out, byte[] value, String label, byte[] delimiter) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("Cannot write " + label + " to null OutputStream");
        }
        out.write(value);
        out.write(delimiter);
    }

    public static String encodeBase64NoPadding(final byte[] bytes) {
        String base64UrlNoPadding = Base64.encodeBase64URLSafeString(bytes);
        base64UrlNoPadding = base64UrlNoPadding.replaceAll("-", "+");
        base64UrlNoPadding = base64UrlNoPadding.replaceAll("_", "/");
        return base64UrlNoPadding;
    }

    public static boolean passwordLengthIsValidForAlgorithmOnLimitedStrengthCrypto(final int passwordLength, EncryptionMethod encryptionMethod) {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("Cannot evaluate an empty encryption method algorithm");
        }

        return passwordLength <= getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod);
    }

    public static int getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(EncryptionMethod encryptionMethod) {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("Cannot evaluate an empty encryption method algorithm");
        }
        return MAX_PASSWORD_LENGTH_BY_ALGORITHM.getOrDefault(encryptionMethod.getAlgorithm(), -1);
    }

    public static boolean isUnlimitedStrengthCryptoSupported() {
        try {
            return (Cipher.getMaxAllowedKeyLength("AES") > DEFAULT_MAX_ALLOWED_KEY_LENGTH);
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }

    /**
     * Returns the salt length for various PBE algorithms. These values were determined empirically from configured/chosen legacy values from the earlier version of the project.
     *
     * @param algorithm the {@link EncryptionMethod#getAlgorithm()}
     * @return the salt length in bytes. Default is 16.
     */
    public static int getSaltLengthForAlgorithm(String algorithm) {
        int saltLength = 16;
        // DES/RC* algorithms use custom iteration counts
        if (algorithm.contains("DES") || algorithm.contains("RC")) {
            saltLength = 8;
        }
        return saltLength;
    }

    /**
     * Returns the current timestamp in a default format. Used by many encryption operations for logging/debugging.
     *
     * @return the current timestamp in 'yyyy-MM-dd HH:mm:ss.SSS Z' format
     */
    public static String getTimestampString() {
        Locale currentLocale = Locale.getDefault();
        String pattern = "yyyy-MM-dd HH:mm:ss.SSS Z";
        SimpleDateFormat formatter = new SimpleDateFormat(pattern, currentLocale);
        Date now = new Date();
        return formatter.format(now);
    }

    public static ByteCountingInputStream wrapStreamForCounting(InputStream inputStream) {
        // Wrap the streams for byte counting if necessary
        ByteCountingInputStream bcis;
        if (!(inputStream instanceof ByteCountingInputStream)) {
            bcis = new ByteCountingInputStream(inputStream);
        } else {
            bcis = (ByteCountingInputStream) inputStream;
        }

        return bcis;
    }

    public static ByteCountingOutputStream wrapStreamForCounting(OutputStream outputStream) {
        // Wrap the streams for byte counting if necessary
        ByteCountingOutputStream bcos;
        if (!(outputStream instanceof ByteCountingOutputStream)) {
            bcos = new ByteCountingOutputStream(outputStream);
        } else {
            bcos = (ByteCountingOutputStream) outputStream;
        }

        return bcos;
    }

    /**
     * Returns the calculated cipher text length given the plaintext length and salt length, if any. If the salt length is > 0, the salt delimiter length ({@code 8}) is included as well.
     *
     * @param ptLength   the plaintext length
     * @param saltLength the salt length
     * @return the complete cipher text, salt (optional), IV, and delimiter(s) length
     */
    public static int calculateCipherTextLength(int ptLength, int saltLength) {
        int ctBlocks = Double.valueOf(Math.ceil(ptLength / 16.0)).intValue();
        int ctLength = (ptLength % 16 == 0 ? ctBlocks + 1 : ctBlocks) * 16;
        // IV length, Salt delimiter length, IV delimiter length
        return ctLength + saltLength + 16 + (saltLength > 0 ? 8 : 0) + 6;
    }

    /**
     * Returns the array index of {@code haystack} if {@code needle} is found within it. This is a sequence scanner.
     *
     * @param haystack the search space byte[]
     * @param needle   the sequence to find
     * @return the first index of the sequence or -1 if it does not exist
     */
    public static int findSequence(byte[] haystack, byte[] needle) {
        for (int i = 0; i < haystack.length - needle.length; i++) {
            boolean match = true;
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Returns the raw salt from the provided "full salt" which could be KDF-specific.
     *
     * Examples:
     *
     * Argon2 -> {@code $argon2id$v=19$m=4096,t=3,p=1$abcdefABCDEF0123456789}
     * Bcrypt -> {@code $2a$10$abcdefABCDEF0123456789}
     * Scrypt -> {@code $s0$e0801$abcdefABCDEF0123456789}
     *
     * If the KDF does not have a custom encoding for the salt, the provided "full salt" is returned intact.
     *
     * @param fullSalt the KDF-formatted salt
     * @param kdf the KDF used
     * @return the raw salt
     */
    public static byte[] extractRawSalt(byte[] fullSalt, KeyDerivationFunction kdf) {
        final String saltString = new String(fullSalt, StandardCharsets.UTF_8);
        switch (kdf) {
            case ARGON2:
                return Argon2CipherProvider.isArgon2FormattedSalt(saltString) ? Argon2CipherProvider.extractRawSaltFromArgon2Salt(saltString) : fullSalt;
            case BCRYPT:
                return BcryptCipherProvider.isBcryptFormattedSalt(saltString) ? BcryptCipherProvider.extractRawSalt(saltString) : fullSalt;
            case SCRYPT:
                return ScryptCipherProvider.isScryptFormattedSalt(saltString) ? ScryptCipherProvider.extractRawSaltFromScryptSalt(saltString) : fullSalt;
            // case PBKDF2:
            // case NONE:
            // case NIFI_LEGACY:
            // case OPENSSL_EVP_BYTES_TO_KEY:
            default:
                return fullSalt;
        }
    }
}