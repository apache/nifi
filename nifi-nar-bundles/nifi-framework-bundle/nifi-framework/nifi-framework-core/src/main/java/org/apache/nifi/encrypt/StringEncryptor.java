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
package org.apache.nifi.encrypt;

import java.nio.charset.StandardCharsets;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.security.util.crypto.CipherProvider;
import org.apache.nifi.security.util.crypto.CipherProviderFactory;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.apache.nifi.security.util.crypto.KeyedCipherProvider;
import org.apache.nifi.security.util.crypto.PBECipherProvider;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An application specific string encryptor that collects configuration from the
 * application properties, system properties, and/or system environment.
 * </p>
 * <p>
 * <p>
 * Instance of this class are thread-safe</p>
 * <p>
 * <p>
 * The encryption provider and algorithm is configured using the application
 * properties:
 * <ul>
 * <li>nifi.sensitive.props.provider</li>
 * <li>nifi.sensitive.props.algorithm</li>
 * </ul>
 * </p>
 * <p>
 * <p>
 * The encryptor's password may be set by configuring the below property:
 * <ul>
 * <li>nifi.sensitive.props.key</li>
 * </ul>
 * </p>
 */
@SuppressWarnings("deprecation")
public class StringEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(StringEncryptor.class);

    private static final List<String> SUPPORTED_ALGORITHMS = new ArrayList<>();
    private static final List<String> SUPPORTED_PROVIDERS = new ArrayList<>();

    private final String algorithm;
    private final String provider;
    private final PBEKeySpec password;
    private final SecretKeySpec key;

    private String encoding = "HEX";

    private CipherProvider cipherProvider;

    static {
        Security.addProvider(new BouncyCastleProvider());

        for (EncryptionMethod em : EncryptionMethod.values()) {
            SUPPORTED_ALGORITHMS.add(em.getAlgorithm());
        }
        logger.debug("Supported encryption algorithms: " + StringUtils.join(SUPPORTED_ALGORITHMS, "\n"));

        for (Provider provider : Security.getProviders()) {
            SUPPORTED_PROVIDERS.add(provider.getName());
        }
        logger.debug("Supported providers: " + StringUtils.join(SUPPORTED_PROVIDERS, "\n"));
    }

    public static final String NF_SENSITIVE_PROPS_KEY = "nifi.sensitive.props.key";
    public static final String NF_SENSITIVE_PROPS_ALGORITHM = "nifi.sensitive.props.algorithm";
    public static final String NF_SENSITIVE_PROPS_PROVIDER = "nifi.sensitive.props.provider";
    private static final String DEFAULT_SENSITIVE_PROPS_KEY = "nififtw!";

    /**
     * This constructor creates an encryptor using <em>Password-Based Encryption</em> (PBE). The <em>key</em> value is the direct value provided in <code>nifi.sensitive.props.key</code> in
     * <code>nifi.properties</code>, which is a <em>PASSWORD</em> rather than a <em>KEY</em>, but is named such for backward/legacy logical compatibility throughout the rest of the codebase.
     * <p>
     * For actual raw key provision, see {@link #StringEncryptor(String, String, byte[])}.
     *
     * @param algorithm the PBE cipher algorithm ({@link EncryptionMethod#algorithm})
     * @param provider  the JCA Security provider ({@link EncryptionMethod#provider})
     * @param key       the UTF-8 characters from nifi.properties -- nifi.sensitive.props.key
     */
    public StringEncryptor(final String algorithm, final String provider, final String key) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.key = null;
        this.password = new PBEKeySpec(key == null
                ? DEFAULT_SENSITIVE_PROPS_KEY.toCharArray()
                : key.toCharArray());
        initialize();
    }

    /**
     * This constructor creates an encryptor using <em>Keyed Encryption</em>. The <em>key</em> value is the raw byte value of a symmetric encryption key
     * (usually expressed for human-readability/transmission in hexadecimal or Base64 encoded format).
     *
     * @param algorithm the PBE cipher algorithm ({@link EncryptionMethod#algorithm})
     * @param provider  the JCA Security provider ({@link EncryptionMethod#provider})
     * @param key       a raw encryption key in bytes
     */
    public StringEncryptor(final String algorithm, final String provider, final byte[] key) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.key = new SecretKeySpec(key, extractKeyTypeFromAlgorithm(algorithm));
        this.password = null;
        initialize();
    }

    /**
     * A default constructor for mocking during testing.
     */
    protected StringEncryptor() {
        this.algorithm = null;
        this.provider = null;
        this.key = null;
        this.password = null;
    }

    /**
     * Extracts the cipher "family" (i.e. "AES", "DES", "RC4") from the full algorithm name.
     *
     * @param algorithm the algorithm ({@link EncryptionMethod#algorithm})
     * @return the cipher family
     * @throws EncryptionException if the algorithm is null/empty or not supported
     */
    private String extractKeyTypeFromAlgorithm(String algorithm) throws EncryptionException {
        if (StringUtils.isBlank(algorithm)) {
            throw new EncryptionException("The algorithm cannot be null or empty");
        }
        String parsedCipher = CipherUtility.parseCipherFromAlgorithm(algorithm);
        if (parsedCipher.equals(algorithm)) {
            throw new EncryptionException("No supported algorithm detected");
        } else {
            return parsedCipher;
        }
    }

    /**
     * Creates an instance of the NiFi sensitive property encryptor.
     *
     * @param niFiProperties properties
     * @return encryptor
     * @throws EncryptionException if any issues arise initializing or
     *                             validating the encryptor
     * @see #createEncryptor(String, String, String)
     * @deprecated as of NiFi 1.4.0 because the entire {@link NiFiProperties} object is not necessary to generate the encryptor.
     */
    @Deprecated
    public static StringEncryptor createEncryptor(final NiFiProperties niFiProperties) throws EncryptionException {

        // Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        final String sensitivePropAlgorithmVal = niFiProperties.getProperty(NF_SENSITIVE_PROPS_ALGORITHM);
        final String sensitivePropProviderVal = niFiProperties.getProperty(NF_SENSITIVE_PROPS_PROVIDER);
        String sensitivePropValueNifiPropVar = niFiProperties.getProperty(NF_SENSITIVE_PROPS_KEY);
        // TODO: This method should be removed in 2.0.0 and replaced globally with the String, String, String method
        if (StringUtils.isBlank(sensitivePropValueNifiPropVar)) {
            printBlankKeyWarning();
            sensitivePropValueNifiPropVar = DEFAULT_SENSITIVE_PROPS_KEY;
        }

        return createEncryptor(sensitivePropAlgorithmVal, sensitivePropProviderVal, sensitivePropValueNifiPropVar);
    }

    /**
     * Creates an instance of the NiFi sensitive property encryptor. If the password is blank, the default will be used and an error will be printed to the log.
     *
     * @param algorithm the encryption (and key derivation) algorithm ({@link EncryptionMethod#algorithm})
     * @param provider  the JCA Security provider ({@link EncryptionMethod#provider})
     * @param password  the UTF-8 characters from nifi.properties -- nifi.sensitive.props.key
     * @return the initialized encryptor
     */
    public static StringEncryptor createEncryptor(String algorithm, String provider, String password) {
        if (StringUtils.isBlank(algorithm)) {
            throw new EncryptionException(NF_SENSITIVE_PROPS_ALGORITHM + " must be set");
        }

        if (StringUtils.isBlank(provider)) {
            throw new EncryptionException(NF_SENSITIVE_PROPS_PROVIDER + " must be set");
        }

        // Can't throw an exception because users who have not populated a key expect fallback to default.
        // TODO: This should be removed in 2.0.0 and replaced with strict enforcement of a explicit unique key
        if (StringUtils.isBlank(password)) {
            printBlankKeyWarning();
            password = DEFAULT_SENSITIVE_PROPS_KEY;
        }

        return new StringEncryptor(algorithm, provider, password);
    }

    private static void printBlankKeyWarning() {
        logger.error(StringUtils.repeat("*", 80));
        logger.error(centerString("A blank sensitive properties key was provided"));
        logger.error(centerString("Specify a unique key in nifi.properties"));
        logger.error(centerString("for nifi.sensitive.props.key"));
        logger.error(centerString(""));
        logger.error(centerString("The Encrypt Config Tool in NiFi Toolkit can be used to"));
        logger.error(centerString("migrate the flow to the new key"));
        logger.error(StringUtils.repeat("*", 80));
    }

    private static String centerString(String msg) {
        return "*" + StringUtils.center(msg, 78, " ") + "*";
    }

    protected void initialize() {
        if (isInitialized()) {
            logger.debug("Attempted to initialize an already-initialized StringEncryptor");
            return;
        }

        if (paramsAreValid()) {
            if (CipherUtility.isPBECipher(algorithm)) {
                cipherProvider = CipherProviderFactory.getCipherProvider(KeyDerivationFunction.NIFI_LEGACY);
            } else {
                cipherProvider = CipherProviderFactory.getCipherProvider(KeyDerivationFunction.NONE);
            }
        } else {
            throw new EncryptionException("Cannot initialize the StringEncryptor because some configuration values are invalid");
        }
    }

    private boolean paramsAreValid() {
        boolean algorithmAndProviderValid = algorithmIsValid(algorithm) && providerIsValid(provider);
        boolean secretIsValid = false;
        if (CipherUtility.isPBECipher(algorithm)) {
            secretIsValid = passwordIsValid(password);
        } else if (CipherUtility.isKeyedCipher(algorithm)) {
            secretIsValid = keyIsValid(key, algorithm);
        }

        return algorithmAndProviderValid && secretIsValid;
    }

    private boolean keyIsValid(SecretKeySpec key, String algorithm) {
        return key != null && CipherUtility.getValidKeyLengthsForAlgorithm(algorithm).contains(key.getEncoded().length * 8);
    }

    private boolean passwordIsValid(PBEKeySpec password) {
        try {
            return password.getPassword().length > 0;
        } catch (IllegalStateException | NullPointerException e) {
            return false;
        }
    }

    public void setEncoding(String base) {
        if ("HEX".equalsIgnoreCase(base)) {
            this.encoding = "HEX";
        } else if ("BASE64".equalsIgnoreCase(base)) {
            this.encoding = "BASE64";
        } else {
            throw new IllegalArgumentException("The encoding base must be 'HEX' or 'BASE64'");
        }
    }

    /**
     * Encrypts the given clear text.
     *
     * @param clearText the message to encrypt
     * @return the cipher text
     * @throws EncryptionException if the encrypt fails
     */
    public String encrypt(String clearText) throws EncryptionException {
        try {
            if (isInitialized()) {
                byte[] rawBytes;
                if (CipherUtility.isPBECipher(algorithm)) {
                    rawBytes = encryptPBE(clearText);
                } else {
                    rawBytes = encryptKeyed(clearText);
                }
                return encode(rawBytes);
            } else {
                throw new EncryptionException("The encryptor is not initialized");
            }
        } catch (final Exception e) {
            throw new EncryptionException(e);
        }
    }

    private byte[] encryptPBE(String plaintext) {
        PBECipherProvider pbecp = (PBECipherProvider) cipherProvider;
        final EncryptionMethod encryptionMethod = EncryptionMethod.forAlgorithm(algorithm);

        // Generate salt
        byte[] salt;
        // NiFi legacy code determined the salt length based on the cipher block size
        if (pbecp instanceof org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) {
            salt = ((org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) pbecp).generateSalt(encryptionMethod);
        } else {
            salt = pbecp.generateSalt();
        }

        // Determine necessary key length
        int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(algorithm);

        // Generate cipher
        try {
            Cipher cipher = pbecp.getCipher(encryptionMethod, new String(password.getPassword()), salt, keyLength, true);

            // Write IV if necessary (allows for future use of PBKDF2, Bcrypt, or Scrypt)
            // byte[] iv = new byte[0];
            // if (cipherProvider instanceof RandomIVPBECipherProvider) {
            //     iv = cipher.getIV();
            // }

            // Encrypt the plaintext
            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // Combine the output
            // byte[] rawBytes = CryptoUtils.concatByteArrays(salt, iv, cipherBytes);
            return CryptoUtils.concatByteArrays(salt, cipherBytes);
        } catch (Exception e) {
            throw new EncryptionException("Could not encrypt sensitive value", e);
        }
    }

    private byte[] encryptKeyed(String plaintext) {
        KeyedCipherProvider keyedcp = (KeyedCipherProvider) cipherProvider;

        // Generate cipher
        try {
            SecureRandom sr = new SecureRandom();
            byte[] iv = new byte[16];
            sr.nextBytes(iv);

            Cipher cipher = keyedcp.getCipher(EncryptionMethod.forAlgorithm(algorithm), key, iv, true);

            // Encrypt the plaintext
            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // Combine the output
            return CryptoUtils.concatByteArrays(iv, cipherBytes);
        } catch (Exception e) {
            throw new EncryptionException("Could not encrypt sensitive value", e);
        }
    }

    private String encode(byte[] rawBytes) {
        if (this.encoding.equalsIgnoreCase("HEX")) {
            return Hex.encodeHexString(rawBytes);
        } else {
            return Base64.toBase64String(rawBytes);
        }
    }

    /**
     * Decrypts the given cipher text.
     *
     * @param cipherText the message to decrypt
     * @return the clear text
     * @throws EncryptionException if the decrypt fails
     */
    public String decrypt(String cipherText) throws EncryptionException {
        try {
            if (isInitialized()) {
                byte[] plainBytes;
                byte[] cipherBytes = decode(cipherText);
                if (CipherUtility.isPBECipher(algorithm)) {
                    plainBytes = decryptPBE(cipherBytes);
                } else {
                    plainBytes = decryptKeyed(cipherBytes);
                }
                return new String(plainBytes, StandardCharsets.UTF_8);
            } else {
                throw new EncryptionException("The encryptor is not initialized");
            }
        } catch (final Exception e) {
            throw new EncryptionException(e);
        }
    }

    private byte[] decryptPBE(byte[] cipherBytes) {
        PBECipherProvider pbecp = (PBECipherProvider) cipherProvider;
        final EncryptionMethod encryptionMethod = EncryptionMethod.forAlgorithm(algorithm);

        // Extract salt
        int saltLength = CipherUtility.getSaltLengthForAlgorithm(algorithm);
        byte[] salt = new byte[saltLength];
        System.arraycopy(cipherBytes, 0, salt, 0, saltLength);

        byte[] actualCipherBytes = Arrays.copyOfRange(cipherBytes, saltLength, cipherBytes.length);

        // Determine necessary key length
        int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(algorithm);

        // Generate cipher
        try {
            Cipher cipher = pbecp.getCipher(encryptionMethod, new String(password.getPassword()), salt, keyLength, false);

            // Write IV if necessary (allows for future use of PBKDF2, Bcrypt, or Scrypt)
            // byte[] iv = new byte[0];
            // if (cipherProvider instanceof RandomIVPBECipherProvider) {
            //     iv = cipher.getIV();
            // }

            // Decrypt the plaintext
            return cipher.doFinal(actualCipherBytes);
        } catch (Exception e) {
            throw new EncryptionException("Could not decrypt sensitive value", e);
        }
    }

    private byte[] decryptKeyed(byte[] cipherBytes) {
        KeyedCipherProvider keyedcp = (KeyedCipherProvider) cipherProvider;

        // Generate cipher
        try {
            int ivLength = 16;
            byte[] iv = new byte[ivLength];
            System.arraycopy(cipherBytes, 0, iv, 0, ivLength);

            byte[] actualCipherBytes = Arrays.copyOfRange(cipherBytes, ivLength, cipherBytes.length);

            Cipher cipher = keyedcp.getCipher(EncryptionMethod.forAlgorithm(algorithm), key, iv, false);

            // Encrypt the plaintext
            return cipher.doFinal(actualCipherBytes);
        } catch (Exception e) {
            throw new EncryptionException("Could not decrypt sensitive value", e);
        }
    }

    private byte[] decode(String encoded) throws DecoderException {
        if (this.encoding.equalsIgnoreCase("HEX")) {
            return Hex.decodeHex(encoded.toCharArray());
        } else {
            return Base64.decode(encoded);
        }
    }

    public boolean isInitialized() {
        return this.cipherProvider != null;
    }

    protected static boolean algorithmIsValid(String algorithm) {
        return SUPPORTED_ALGORITHMS.contains(algorithm);
    }

    protected static boolean providerIsValid(String provider) {
        return SUPPORTED_PROVIDERS.contains(provider);
    }

    /**
     * Returns {@code true} if the two {@code StringEncryptor} objects are logically equivalent.
     * This requires the same {@code algorithm}, {@code provider}, {@code encoding}, and
     * {@code key}/{@code password}.
     * <p>
     * A {@code ciphertext} generated by one object can be decrypted by a separate object if they are equal as determined by this method.
     *
     * @param o the other StringEncryptor
     * @return true if these instances are equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringEncryptor that = (StringEncryptor) o;
        return Objects.equals(algorithm, that.algorithm)
                && Objects.equals(provider, that.provider)
                && Objects.equals(encoding, that.encoding)
                && secretsAreEqual(that.password, that.key);
    }

    /**
     * Returns true if the provided {@code password} and {@code key} match those contained in this {@code StringEncryptor}. This method does <strong>not</strong> compare {@code password == key}.
     * <p>
     * Internally, uses {@link #isPBEKeySpecEqual(PBEKeySpec, PBEKeySpec)} and {@link SecretKeySpec#equals(Object)}.
     *
     * @param otherPassword the password {@link PBEKeySpec}
     * @param otherKey      the key {@link SecretKeySpec}
     * @return true if the passwords match and the keys match
     */
    private boolean secretsAreEqual(PBEKeySpec otherPassword, SecretKeySpec otherKey) {
        // SecretKeySpec implements null-safe equals(), but PBEKeySpec does not
        return isPBEKeySpecEqual(this.password, otherPassword) && Objects.equals(this.key, otherKey);
    }

    /**
     * Returns true if the two {@link PBEKeySpec} objects are logically equivalent (same params and password).
     *
     * @param a a PBEKeySpec to compare
     * @param b a PBEKeySpec to compare
     * @return true if they can be used for encryption interchangeably
     */
    private static boolean isPBEKeySpecEqual(PBEKeySpec a, PBEKeySpec b) {
        if (a != null) {
            if (b == null) {
                return false;
            } else {
                // Compare all the accessors that will not throw exceptions
                boolean nonNullsEqual = a.getIterationCount() == b.getIterationCount()
                        && a.getKeyLength() == b.getKeyLength()
                        && Arrays.equals(a.getSalt(), b.getSalt());

                // Compare the passwords using constant-time equality while catching exceptions
                boolean passwordsEqual;
                try {
                    passwordsEqual = CryptoUtils.constantTimeEquals(a.getPassword(), b.getPassword());
                } catch (IllegalStateException e) {
                    logger.warn("Encountered an error trying to compare password equality (one or more passwords have been cleared)");
                    // Assume any key spec with password cleared is unusable; return false
                    return false;
                }

                // Logging for debug assistance
                if (logger.isDebugEnabled()) {
                    logger.debug("The PBEKeySpec objects have equal non-null elements ({}) and equal passwords ({})", new Object[]{String.valueOf(nonNullsEqual), String.valueOf(passwordsEqual)});
                }
                return nonNullsEqual && passwordsEqual;
            }
        } else {
            // If here, a == null
            return b == null;
        }
    }


    /**
     * Returns the hashcode of this object. Does not include {@code cipherProvider} in hashcode calculations.
     *
     * @return the hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(algorithm, provider, encoding, password, key);
    }

    /**
     * Returns a String containing the {@code algorithm}, {@code provider}, {@code encoding}, and {@code cipherProvider} class name.
     *
     * @return a String representation of the object state
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("StringEncryptor using ").append(algorithm)
                .append(" from ").append(provider)
                .append(" with ").append(encoding).append(" encoding and cipher provider ")
                .append(cipherProvider.getClass().getName());
        return sb.toString();
    }
}
