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

import java.security.Security;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.exceptions.EncryptionInitializationException;
import org.jasypt.exceptions.EncryptionOperationNotPossibleException;

/**
 * <p>
 * An application specific string encryptor that collects configuration from the
 * application properties, system properties, and/or system environment.
 * </p>
 *
 * <p>
 * Instance of this class are thread-safe</p>
 *
 * <p>
 * The encryption provider and algorithm is configured using the application
 * properties:
 * <ul>
 * <li>nifi.sensitive.props.provider</li>
 * <li>nifi.sensitive.props.algorithm</li>
 * </ul>
 * </p>
 *
 * <p>
 * The encryptor's password may be set by configuring the below property:
 * <ul>
 * <li>nifi.sensitive.props.key</li>
 * </ul>
 * </p>
 *
 */
public final class StringEncryptor {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public static final String NF_SENSITIVE_PROPS_KEY = "nifi.sensitive.props.key";
    public static final String NF_SENSITIVE_PROPS_ALGORITHM = "nifi.sensitive.props.algorithm";
    public static final String NF_SENSITIVE_PROPS_PROVIDER = "nifi.sensitive.props.provider";
    private static final String DEFAULT_SENSITIVE_PROPS_KEY = "nififtw!";
    private static final String TEST_PLAINTEXT = "this is a test";
    private final StandardPBEStringEncryptor encryptor;

    private StringEncryptor(final String aglorithm, final String provider, final String key) {
        encryptor = new StandardPBEStringEncryptor();
        encryptor.setAlgorithm(aglorithm);
        encryptor.setProviderName(provider);
        encryptor.setPassword(key);
        encryptor.setStringOutputType("hexadecimal");
        encryptor.initialize();
    }

    /**
     * Creates an instance of the nifi sensitive property encryptor. Validates
     * that the encryptor is actually working.
     *
     * @param niFiProperties properties
     * @return encryptor
     * @throws EncryptionException if any issues arise initializing or
     * validating the encryptor
     */
    public static StringEncryptor createEncryptor(final NiFiProperties niFiProperties) throws EncryptionException {

        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

        final String sensitivePropAlgorithmVal = niFiProperties.getProperty(NF_SENSITIVE_PROPS_ALGORITHM);
        final String sensitivePropProviderVal = niFiProperties.getProperty(NF_SENSITIVE_PROPS_PROVIDER);
        final String sensitivePropValueNifiPropVar = niFiProperties.getProperty(NF_SENSITIVE_PROPS_KEY, DEFAULT_SENSITIVE_PROPS_KEY);

        if (StringUtils.isBlank(sensitivePropAlgorithmVal)) {
            throw new EncryptionException(NF_SENSITIVE_PROPS_ALGORITHM + "must bet set");
        }

        if (StringUtils.isBlank(sensitivePropProviderVal)) {
            throw new EncryptionException(NF_SENSITIVE_PROPS_PROVIDER + "must bet set");
        }

        if (StringUtils.isBlank(sensitivePropValueNifiPropVar)) {
            throw new EncryptionException(NF_SENSITIVE_PROPS_KEY + "must bet set");
        }

        final StringEncryptor nifiEncryptor;
        try {
            nifiEncryptor = new StringEncryptor(sensitivePropAlgorithmVal, sensitivePropProviderVal, sensitivePropValueNifiPropVar);
            //test that we can infact encrypt and decrypt something
            if (!nifiEncryptor.decrypt(nifiEncryptor.encrypt(TEST_PLAINTEXT)).equals(TEST_PLAINTEXT)) {
                throw new EncryptionException("NiFi property encryptor does appear to be working - decrypt/encrypt return invalid results");
            }

        } catch (final EncryptionInitializationException | EncryptionOperationNotPossibleException ex) {
            throw new EncryptionException("Cannot initialize sensitive property encryptor", ex);

        }
        return nifiEncryptor;
    }

    /**
     * Encrypts the given clear text.
     *
     * @param clearText the message to encrypt
     *
     * @return the cipher text
     *
     * @throws EncryptionException if the encrypt fails
     */
    public String encrypt(String clearText) throws EncryptionException {
        try {
            return encryptor.encrypt(clearText);
        } catch (final EncryptionOperationNotPossibleException | EncryptionInitializationException eonpe) {
            throw new EncryptionException(eonpe);
        }
    }

    /**
     * Decrypts the given cipher text.
     *
     * @param cipherText the message to decrypt
     *
     * @return the clear text
     *
     * @throws EncryptionException if the decrypt fails
     */
    public String decrypt(String cipherText) throws EncryptionException {
        try {
            return encryptor.decrypt(cipherText);
        } catch (final EncryptionOperationNotPossibleException | EncryptionInitializationException eonpe) {
            throw new EncryptionException(eonpe);
        }
    }

}
