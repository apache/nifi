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
package org.apache.nifi.security.crypto.key.scrypt;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ScryptDerivedKeyProviderTest {
    private static final String PASSWORD = "secret";

    private static final int COST = 16384;

    private static final int BLOCK_SIZE = 8;

    private static final int PARALLELIZATION = 1;

    private static final String SALT_ENCODED = "epIxT/h6HbbwHaehFnh/bw";

    private static final String HASH = "7H0vsXlY8UxxyW/BWx/9GuY7jEvGjT71GFd6O4SZND0";

    private static final String SERIALIZED = String.format("$s0$e0801$%s$%s", SALT_ENCODED, HASH);

    private static final int DERIVED_KEY_LENGTH = 32;

    private static final String ALGORITHM = "AES";

    private static final Base64.Decoder decoder = Base64.getDecoder();

    ScryptDerivedKeyProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new ScryptDerivedKeyProvider();
    }

    @Test
    void testGetDerivedKey() {
        final byte[] salt = decoder.decode(SALT_ENCODED);
        final ScryptDerivedKeyParameterSpec parameterSpec = new ScryptDerivedKeyParameterSpec(COST, BLOCK_SIZE, PARALLELIZATION, salt);

        final DerivedKeySpec<ScryptDerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(
                PASSWORD.toCharArray(),
                DERIVED_KEY_LENGTH,
                ALGORITHM,
                parameterSpec
        );

        final DerivedKey derivedKey = provider.getDerivedKey(derivedKeySpec);

        assertNotNull(derivedKey);
        assertEquals(ALGORITHM, derivedKey.getAlgorithm());
        assertEquals(SERIALIZED, derivedKey.getSerialized());

        final byte[] hashBytes = decoder.decode(HASH);
        assertArrayEquals(hashBytes, derivedKey.getEncoded());
    }
}
