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
package org.apache.nifi.security.crypto.key.detection;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.apache.nifi.security.crypto.key.argon2.Argon2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.bcrypt.BcryptDerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.pbkdf2.Pbkdf2DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.scrypt.ScryptDerivedKeyParameterSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class DetectedDerivedKeyProviderTest {

    private static final char[] PASSWORD = {'w', 'o', 'r', 'd'};

    private static final int DERIVED_KEY_LENGTH = 16;

    private static final String ALGORITHM = "AES";

    private static final byte[] SALT = {'N', 'i', 'F', 'i', 'S', 'A', 'L', 'T'};

    private static final int MINIMUM_ITERATIONS = 1;

    private static final int MINIMUM_ARGON2_MEMORY = 2;

    private static final int MINIMUM_PARALLELISM = 1;

    private static final int MINIMUM_BCRYPT_COST = 4;

    private static final byte[] BCRYPT_SALT = {'S', 'I', 'X', 'T', 'E', 'E', 'N', 'B', 'Y', 'T', 'E', 'S', 'S', 'A', 'L', 'T'};

    private static final int MINIMUM_SCRYPT_COST = 2;

    private static final int SCRYPT_BLOCK_SIZE = 16;

    @Mock
    DerivedKeyParameterSpec unsupportedDerivedKeyParameterSpec;

    DetectedDerivedKeyProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new DetectedDerivedKeyProvider();
    }

    @Test
    void testArgon2() {
        final Argon2DerivedKeyParameterSpec parameterSpec = new Argon2DerivedKeyParameterSpec(MINIMUM_ARGON2_MEMORY, MINIMUM_ITERATIONS, MINIMUM_PARALLELISM, SALT);
        assertDerivedKey(parameterSpec);
    }

    @Test
    void testBcrypt() {
        final BcryptDerivedKeyParameterSpec parameterSpec = new BcryptDerivedKeyParameterSpec(MINIMUM_BCRYPT_COST, BCRYPT_SALT);
        assertDerivedKey(parameterSpec);
    }

    @Test
    void testPbkdf2() {
        final Pbkdf2DerivedKeyParameterSpec parameterSpec = new Pbkdf2DerivedKeyParameterSpec(MINIMUM_ITERATIONS, SALT);
        assertDerivedKey(parameterSpec);
    }

    @Test
    void testScrypt() {
        final ScryptDerivedKeyParameterSpec parameterSpec = new ScryptDerivedKeyParameterSpec(MINIMUM_SCRYPT_COST, SCRYPT_BLOCK_SIZE, MINIMUM_PARALLELISM, SALT);
        assertDerivedKey(parameterSpec);
    }

    @Test
    void testUnsupported() {
        final DerivedKeySpec<DerivedKeyParameterSpec> derivedKeySpec = getDerivedKeySpec(unsupportedDerivedKeyParameterSpec);
        assertThrows(UnsupportedOperationException.class, () -> provider.getDerivedKey(derivedKeySpec));
    }

    private <T extends DerivedKeyParameterSpec> DerivedKeySpec<T> getDerivedKeySpec(final T parameterSpec) {
        return new StandardDerivedKeySpec<>(PASSWORD, DERIVED_KEY_LENGTH, ALGORITHM, parameterSpec);
    }

    private <T extends DerivedKeyParameterSpec> void assertDerivedKey(final T parameterSpec) {
        final DerivedKeySpec<DerivedKeyParameterSpec> derivedKeySpec = getDerivedKeySpec(parameterSpec);

        final DerivedKey derivedKey = provider.getDerivedKey(derivedKeySpec);

        assertNotNull(derivedKey);
    }
}
