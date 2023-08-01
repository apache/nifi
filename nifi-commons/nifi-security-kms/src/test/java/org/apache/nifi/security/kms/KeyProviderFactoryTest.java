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

import org.apache.nifi.security.kms.configuration.KeyProviderConfiguration;
import org.apache.nifi.security.kms.configuration.KeyStoreKeyProviderConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyProviderFactoryTest {

    @Test
    public void testGetUnsupportedKeyProvider() {
        final KeyProviderConfiguration<?> configuration = new UnsupportedKeyProviderConfiguration();
        assertThrows(UnsupportedOperationException.class, () -> KeyProviderFactory.getKeyProvider(configuration));
    }

    @Test
    public void testGetKeyStoreKeyProvider() throws GeneralSecurityException, IOException {
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        final char[] password = String.class.getSimpleName().toCharArray();
        final KeyProviderConfiguration<?> configuration = new KeyStoreKeyProviderConfiguration(keyStore, password);
        final KeyProvider keyProvider = KeyProviderFactory.getKeyProvider(configuration);
        assertEquals(KeyStoreKeyProvider.class, keyProvider.getClass());
    }

    private static class UnsupportedKeyProviderConfiguration implements KeyProviderConfiguration<KeyProvider> {

        @Override
        public Class<KeyProvider> getKeyProviderClass() {
            return KeyProvider.class;
        }
    }
}
