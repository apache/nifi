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

import org.apache.nifi.security.kms.util.SecretKeyUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KeyStoreKeyProviderTest {
    private static final String KEY_ID = UUID.randomUUID().toString();

    private static final SecretKey SECRET_KEY = SecretKeyUtils.getSecretKey();

    private static final char[] PASSWORD = UUID.randomUUID().toString().toCharArray();

    private static final String KEY_STORE_TYPE = "PKCS12";

    private static KeyStore keyStore;

    @BeforeClass
    public static void setKeyStore() throws GeneralSecurityException, IOException {
        keyStore = getKeyStore();
    }

    @Test
    public void testGetKey() throws KeyManagementException {
        final KeyStoreKeyProvider provider = new KeyStoreKeyProvider(keyStore, PASSWORD);

        final SecretKey secretKeyFound = provider.getKey(KEY_ID);
        assertEquals(SECRET_KEY, secretKeyFound);
    }

    @Test
    public void testKeyExists() {
        final KeyStoreKeyProvider provider = new KeyStoreKeyProvider(keyStore, PASSWORD);

        assertTrue(provider.keyExists(KEY_ID));
    }

    @Test
    public void testGetAvailableKeys() {
        final KeyStoreKeyProvider provider = new KeyStoreKeyProvider(keyStore, PASSWORD);

        final List<String> keyIds = provider.getAvailableKeyIds();
        assertTrue(keyIds.contains(KEY_ID));
    }

    @Test
    public void testGetKeyNotFoundManagementException() {
        final KeyStoreKeyProvider provider = new KeyStoreKeyProvider(keyStore, PASSWORD);
        assertThrows(KeyManagementException.class, () -> provider.getKey(SecretKey.class.getName()));
    }

    private static KeyStore getKeyStore() throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(KEY_STORE_TYPE);
        keyStore.load(null, null);
        keyStore.setEntry(KEY_ID, new KeyStore.SecretKeyEntry(SECRET_KEY), new KeyStore.PasswordProtection(PASSWORD));
        return keyStore;
    }
}
