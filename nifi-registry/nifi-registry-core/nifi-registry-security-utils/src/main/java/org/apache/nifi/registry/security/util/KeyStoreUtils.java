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

package org.apache.nifi.registry.security.util;

import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

public class KeyStoreUtils {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtils.class);

    private static final String SUN_SECURITY_PROVIDER = "SUN";

    private static final Map<String, String> KEY_STORE_TYPE_PROVIDERS = new HashMap<>();

    static {
        Security.addProvider(new BouncyCastleProvider());

        KEY_STORE_TYPE_PROVIDERS.put(KeystoreType.JKS.toString(), SUN_SECURITY_PROVIDER);
        KEY_STORE_TYPE_PROVIDERS.put(KeystoreType.PKCS12.toString(), BouncyCastleProvider.PROVIDER_NAME);
        KEY_STORE_TYPE_PROVIDERS.put(KeystoreType.BCFKS.toString(), BouncyCastleProvider.PROVIDER_NAME);
    }

    /**
     * Returns the provider that will be used for the given keyStoreType
     *
     * @param keyStoreType the keyStoreType
     * @return the provider that will be used
     */
    public static String getKeyStoreProvider(final String keyStoreType) {
        final String storeType = StringUtils.upperCase(keyStoreType);
        return KEY_STORE_TYPE_PROVIDERS.get(storeType);
    }

    /**
     * Returns an empty KeyStore backed by the appropriate provider
     *
     * @param keyStoreType the keyStoreType
     * @return an empty KeyStore
     * @throws KeyStoreException if a KeyStore of the given type cannot be instantiated
     */
    public static KeyStore getKeyStore(final String keyStoreType) throws KeyStoreException {
        final String keyStoreProvider = getKeyStoreProvider(keyStoreType);
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
}
