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

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.KeyStore;
import java.security.KeyStoreException;

public class KeyStoreUtils {
    private static final BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

    /**
     * Returns an empty KeyStore backed by the appropriate provider
     *
     * @param keyStoreType the keyStoreType
     * @return an empty KeyStore
     * @throws KeyStoreException if a KeyStore of the given type cannot be instantiated
     */
    public static KeyStore getKeyStore(final String keyStoreType) throws KeyStoreException {
        if (KeystoreType.BCFKS.toString().equals(keyStoreType)) {
            return KeyStore.getInstance(keyStoreType, BOUNCY_CASTLE_PROVIDER);
        } else {
            return KeyStore.getInstance(keyStoreType);
        }
    }
}
