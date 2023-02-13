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
package org.apache.nifi.security.crypto.key.bcrypt;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BcryptDerivedKeyProviderTest {
    private static final String PASSWORD = "abc123xyz";

    private static final int COST = 12;

    private static final String SALT_ENCODED = "R9h/cIPz0gi.URNNX3kh2O";

    private static final String HASH = "PST9/PgBkqquzi.Ss7KIUgO2t0jWMUW";

    private static final String SERIALIZED = String.format("$2a$%d$%s%s", COST, SALT_ENCODED, HASH);

    private static final Charset SERIALIZED_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final int DERIVED_KEY_LENGTH = 16;

    private static final String ALGORITHM = "AES";

    private static final String DIGEST_ALGORITHM = "SHA-512";

    BcryptDerivedKeyProvider provider;

    @BeforeEach
    void setProvider() {
        provider = new BcryptDerivedKeyProvider();
    }

    @Test
    void testGetDerivedKey() {
        final byte[] salt = BcryptBase64Decoder.decode(SALT_ENCODED);
        final BcryptDerivedKeyParameterSpec parameterSpec = new BcryptDerivedKeyParameterSpec(COST, salt);

        final DerivedKeySpec<BcryptDerivedKeyParameterSpec> derivedKeySpec = new StandardDerivedKeySpec<>(
                PASSWORD.toCharArray(),
                DERIVED_KEY_LENGTH,
                ALGORITHM,
                parameterSpec
        );

        final DerivedKey derivedKey = provider.getDerivedKey(derivedKeySpec);

        assertNotNull(derivedKey);
        assertEquals(ALGORITHM, derivedKey.getAlgorithm());
        assertEquals(SERIALIZED, derivedKey.getSerialized());

        final byte[] encodedHashBytes = HASH.getBytes(SERIALIZED_CHARACTER_SET);
        final byte[] digestedDerivedKey = getDerivedKeyBytes(encodedHashBytes);
        assertArrayEquals(digestedDerivedKey, derivedKey.getEncoded());
    }

    private byte[] getDerivedKeyBytes(final byte[] hash) {
        final MessageDigest messageDigest = getMessageDigest();
        final byte[] digested = messageDigest.digest(hash);
        return Arrays.copyOf(digested, DERIVED_KEY_LENGTH);
    }

    private MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new UnsupportedOperationException(DIGEST_ALGORITHM, e);
        }
    }
}
