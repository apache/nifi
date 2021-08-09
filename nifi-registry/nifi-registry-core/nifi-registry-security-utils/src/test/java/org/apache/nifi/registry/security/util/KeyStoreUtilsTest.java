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
import org.junit.Assert;
import org.junit.Test;

import java.security.KeyStore;
import java.security.KeyStoreException;

public class KeyStoreUtilsTest {

    @Test
    public void testGetKeyStore() throws KeyStoreException {
        for (final KeystoreType keystoreType : KeystoreType.values()) {
            final KeyStore keyStore = KeyStoreUtils.getKeyStore(keystoreType.toString());
            Assert.assertNotNull(String.format("KeyStore not found for Keystore Type [%s]", keystoreType), keyStore);
            Assert.assertEquals(keystoreType.name(), keyStore.getType());
        }
    }

    @Test
    public void testGetKeyStoreProviderNullType() {
        final String keyStoreProvider = KeyStoreUtils.getKeyStoreProvider(null);
        Assert.assertNull(keyStoreProvider);
    }

    @Test
    public void testGetKeyStoreProviderBouncyCastleProvider() {
        final String keyStoreProvider = KeyStoreUtils.getKeyStoreProvider(KeystoreType.PKCS12.name());
        Assert.assertEquals(BouncyCastleProvider.PROVIDER_NAME, keyStoreProvider);
    }
}
