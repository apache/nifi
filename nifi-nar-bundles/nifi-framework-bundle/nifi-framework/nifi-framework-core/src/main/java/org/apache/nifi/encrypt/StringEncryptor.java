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

import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import javax.crypto.Cipher;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.CipherUtility;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jasypt.exceptions.EncryptionInitializationException;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
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
public class StringEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(StringEncryptor.class);

    private static final List<String> SUPPORTED_ALGORITHMS = new ArrayList<>();
    private static final List<String> SUPPORTED_PROVIDERS = new ArrayList<>();

    private final String algorithm;
    private final String provider;
    private Cipher encryptCipher;
    private Cipher decryptCipher;

    static {
        Security.addProvider(new BouncyCastleProvider());
        // Set<String> unsortedAlgorithmsSet = new HashSet<>();
        //
        // for (Provider provider : Security.getProviders()) {
        //     for (Provider.Service service : provider.getServices()) {
        //         unsortedAlgorithmsSet.add(service.getAlgorithm());
        //     }
        // }
        // SUPPORTED_ALGORITHMS = new ArrayList<>(unsortedAlgorithmsSet);
        // Collections.sort(SUPPORTED_ALGORITHMS);

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
    private static final String TEST_PLAINTEXT = "this is a test";


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
    protected StringEncryptor(final String algorithm, final String provider, final String key) {
        this.algorithm = algorithm;
        this.provider = provider;
        initialize(key);
    }

    public StringEncryptor(final String algorithm, final String provider, final byte[] key) {
        this.algorithm = algorithm;
        this.provider = provider;
        initialize(key);
    }

    protected StringEncryptor() {
        this.algorithm = null;
        this.provider = null;
    }

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
        final String sensitivePropValueNifiPropVar = niFiProperties.getProperty(NF_SENSITIVE_PROPS_KEY, DEFAULT_SENSITIVE_PROPS_KEY);

        return createEncryptor(sensitivePropAlgorithmVal, sensitivePropProviderVal, sensitivePropValueNifiPropVar);
    }

    /**
     * Creates an instance of the NiFi sensitive property encryptor.
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

        if (StringUtils.isBlank(password)) {
            throw new EncryptionException(NF_SENSITIVE_PROPS_KEY + " must be set");
        }

        return new StringEncryptor(algorithm, provider, password);
    }

    protected void initialize(String password) {
        if (isInitialized()) {
            return;
        }

        if (paramsAreValid()) {
            // try {
            //     int iterationCount = CipherUtility.getIterationCountForAlgorithm(algorithm);
            //
            //     this.encryptCipher = CipherUtility.initPBECipher(algorithm, provider, password, )
            //
            // } catch (NoSuchPaddingException | NoSuchAlgorithmException | NoSuchProviderException e) {
            //     logger.error("Encountered an error: ", e);
            //     throw new EncryptionException("There was an error initializing the StringEncryptor", e);
            // }
        }
    }

    protected void initialize(byte[] key) {
        if (isInitialized()) {
            return;
        }
    }

    private boolean paramsAreValid() {
        return algorithmIsValid(algorithm) && providerIsValid(provider);
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
                // return encryptor.encrypt(clearText);
                // TODO: Replace encryptor
                return clearText;
            } else {
                throw new EncryptionException("The encryptor is not initialized");
            }
        } catch (final EncryptionOperationNotPossibleException | EncryptionInitializationException eonpe) {
            throw new EncryptionException(eonpe);
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
                // return encryptor.decrypt(cipherText);
                // TODO: Replace encryptor
                return cipherText;
            } else {
                throw new EncryptionException("The encryptor is not initialized");
            }
        } catch (final EncryptionOperationNotPossibleException | EncryptionInitializationException eonpe) {
            throw new EncryptionException(eonpe);
        }
    }

    public boolean isInitialized() {
        return this.encryptCipher != null && this.decryptCipher != null;
    }

    protected static boolean algorithmIsValid(String algorithm) {
        return SUPPORTED_ALGORITHMS.contains(algorithm);
    }

    protected static boolean providerIsValid(String provider) {
        return SUPPORTED_PROVIDERS.contains(provider);
    }
}
