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

package org.apache.nifi.security.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyStoreUtils {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtils.class);

    public static final String SUN_PROVIDER_NAME = "SUN";

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Returns the provider that will be used for the given keyStoreType
     *
     * @param keyStoreType the keyStoreType
     * @return the provider that will be used
     */
    public static String getKeyStoreProvider(String keyStoreType) {
        if (KeystoreType.PKCS12.toString().equalsIgnoreCase(keyStoreType)) {
            return BouncyCastleProvider.PROVIDER_NAME;
        } else if (KeystoreType.JKS.toString().equalsIgnoreCase(keyStoreType)) {
            return SUN_PROVIDER_NAME;
        }
        return null;
    }

    /**
     * Returns an empty KeyStore backed by the appropriate provider
     *
     * @param keyStoreType the keyStoreType
     * @return an empty KeyStore
     * @throws KeyStoreException if a KeyStore of the given type cannot be instantiated
     */
    public static KeyStore getKeyStore(String keyStoreType) throws KeyStoreException {
        String keyStoreProvider = getKeyStoreProvider(keyStoreType);
        if (StringUtils.isNotEmpty(keyStoreProvider)) {
            try {
                return KeyStore.getInstance(keyStoreType, keyStoreProvider);
            } catch (Exception e) {
                logger.error("Unable to load " + keyStoreProvider + " " + keyStoreType
                        + " keystore.  This may cause issues getting trusted CA certificates as well as Certificate Chains for use in TLS.", e);
            }
        }
        return KeyStore.getInstance(keyStoreType);
    }

    /**
     * Returns an empty KeyStore intended for use as a TrustStore backed by the appropriate provider
     *
     * @param trustStoreType the trustStoreType
     * @return an empty KeyStore
     * @throws KeyStoreException if a KeyStore of the given type cannot be instantiated
     */
    public static KeyStore getTrustStore(String trustStoreType) throws KeyStoreException {
        if (KeystoreType.PKCS12.toString().equalsIgnoreCase(trustStoreType)) {
            logger.warn(trustStoreType + " truststores are deprecated.  " + KeystoreType.JKS.toString() + " is preferred.");
        }
        return getKeyStore(trustStoreType);
    }

    /**
     * Returns true if the given keystore can be loaded using the given keystore type and password. Returns false otherwise.
     *
     * @param keystore     the keystore to validate
     * @param keystoreType the type of the keystore
     * @param password     the password to access the keystore
     * @return true if valid; false otherwise
     */
    public static boolean isStoreValid(final URL keystore, final KeystoreType keystoreType, final char[] password) {

        if (keystore == null) {
            throw new IllegalArgumentException("Keystore may not be null");
        } else if (keystoreType == null) {
            throw new IllegalArgumentException("Keystore type may not be null");
        } else if (password == null) {
            throw new IllegalArgumentException("Password may not be null");
        }

        BufferedInputStream bis = null;
        final KeyStore ks;
        try {

            // Load the keystore
            bis = new BufferedInputStream(keystore.openStream());
            ks = KeyStoreUtils.getKeyStore(keystoreType.name());
            ks.load(bis, password);

            return true;

        } catch (Exception e) {
            return false;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close input stream", ioe);
                }
            }
        }
    }

    /**
     * Returns true if the given keystore can be loaded using the given keystore type and password and the default
     * (first) alias can be retrieved with the key-specific password. Returns false otherwise.
     *
     * @param keystore     the keystore to validate
     * @param keystoreType the type of the keystore
     * @param password     the password to access the keystore
     * @param keyPassword  the password to access the specific key
     * @return true if valid; false otherwise
     */
    public static boolean isKeyPasswordCorrect(final URL keystore, final KeystoreType keystoreType, final char[] password, final char[] keyPassword) {

        if (keystore == null) {
            throw new IllegalArgumentException("Keystore may not be null");
        } else if (keystoreType == null) {
            throw new IllegalArgumentException("Keystore type may not be null");
        } else if (password == null) {
            throw new IllegalArgumentException("Password may not be null");
        }

        BufferedInputStream bis = null;
        final KeyStore ks;
        try {

            // Load the keystore
            bis = new BufferedInputStream(keystore.openStream());
            ks = KeyStoreUtils.getKeyStore(keystoreType.name());
            ks.load(bis, password);

            // Determine the default alias
            String alias = ks.aliases().nextElement();
            try {
                Key privateKeyEntry = ks.getKey(alias, keyPassword);
                return true;
            } catch (UnrecoverableKeyException e) {
                logger.warn("Tried to access a key in keystore " + keystore + " with a key password that failed");
                return false;
            }
        } catch (Exception e) {
            return false;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close input stream", ioe);
                }
            }
        }
    }
}
