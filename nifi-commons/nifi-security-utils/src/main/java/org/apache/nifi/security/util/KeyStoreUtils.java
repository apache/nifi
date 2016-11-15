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

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Security;

public class KeyStoreUtils {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtils.class);

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
}
