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

import org.junit.Before;
import org.junit.Test;

import javax.crypto.SecretKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class StandardPropertySecretKeyProviderTest {
    private static final String SEED = String.class.getName();

    private static final String SHORT_SEED = String.class.getSimpleName();

    private StandardPropertySecretKeyProvider provider;

    @Before
    public void setUp() {
        provider = new StandardPropertySecretKeyProvider();
    }

    @Test
    public void testPropertyEncryptionMethodSupported() {
        for (final PropertyEncryptionMethod propertyEncryptionMethod : PropertyEncryptionMethod.values()) {
            final SecretKey secretKey = provider.getSecretKey(propertyEncryptionMethod, SEED);
            final int secretKeyLength = secretKey.getEncoded().length;
            final String message = String.format("Method [%s] Key Length not matched", propertyEncryptionMethod);
            assertEquals(message, propertyEncryptionMethod.getHashLength(), secretKeyLength);
        }
    }

    @Test
    public void testPasswordLengthLessThanRequired() {
        assertThrows(EncryptionException.class, () -> provider.getSecretKey(PropertyEncryptionMethod.NIFI_ARGON2_AES_GCM_256, SHORT_SEED));
    }
}
