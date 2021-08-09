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
import org.junit.Test;

import javax.crypto.SecretKey;
import java.security.KeyManagementException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StaticKeyProviderTest {
    private static final String KEY_ID = UUID.randomUUID().toString();

    private static final SecretKey SECRET_KEY = SecretKeyUtils.getSecretKey();

    @Test
    public void testGetKey() throws KeyManagementException {
        final StaticKeyProvider provider = new StaticKeyProvider(Collections.singletonMap(KEY_ID, SECRET_KEY));

        final SecretKey secretKeyFound = provider.getKey(KEY_ID);
        assertEquals(SECRET_KEY, secretKeyFound);
    }

    @Test
    public void testKeyExists() {
        final StaticKeyProvider provider = new StaticKeyProvider(Collections.singletonMap(KEY_ID, SECRET_KEY));

        assertTrue(provider.keyExists(KEY_ID));
    }

    @Test
    public void testGetAvailableKeys() {
        final StaticKeyProvider provider = new StaticKeyProvider(Collections.singletonMap(KEY_ID, SECRET_KEY));

        final List<String> keyIds = provider.getAvailableKeyIds();
        assertTrue(keyIds.contains(KEY_ID));
    }

    @Test
    public void testGetKeyNotFoundManagementException() {
        final StaticKeyProvider provider = new StaticKeyProvider(Collections.emptyMap());
        assertThrows(KeyManagementException.class, () -> provider.getKey(SecretKey.class.getName()));
    }
}
