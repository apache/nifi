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
import org.apache.nifi.security.crypto.key.DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.DerivedSecretKey;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * bcrypt implementation of Derived Key Provider based on Bouncy Castle bcrypt components with SHA-512 digest for derived key
 */
public class BcryptDerivedKeyProvider implements DerivedKeyProvider<BcryptDerivedKeyParameterSpec> {
    private static final Charset SERIALIZED_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final int SERIALIZED_HASH_START_INDEX = 29;

    private static final String DIGEST_ALGORITHM = "SHA-512";

    private static final String BCRYPT_VERSION = "2a";

    /**
     * Get Derived Key using bcrypt version 2a and provided specification with SHA-512 digest of encoded raw hash bytes
     *
     * @param derivedKeySpec Derived Key Specification
     * @return Derived Secret Key
     */
    @Override
    public DerivedKey getDerivedKey(final DerivedKeySpec<BcryptDerivedKeyParameterSpec> derivedKeySpec) {
        final String serialized = getHashMessage(derivedKeySpec);
        final byte[] hashMessage = serialized.getBytes(SERIALIZED_CHARACTER_SET);

        final byte[] encodedRawHash = Arrays.copyOfRange(hashMessage, SERIALIZED_HASH_START_INDEX, hashMessage.length);
        final byte[] derivedKeyBytes = getDerivedKeyBytes(encodedRawHash, derivedKeySpec.getDerivedKeyLength());
        return new DerivedSecretKey(derivedKeyBytes, derivedKeySpec.getAlgorithm(), serialized);
    }

    private String getHashMessage(final DerivedKeySpec<BcryptDerivedKeyParameterSpec> derivedKeySpec) {
        final BcryptDerivedKeyParameterSpec parameterSpec = derivedKeySpec.getParameterSpec();

        final int cost = parameterSpec.getCost();
        final byte[] salt = parameterSpec.getSalt();

        final char[] password = derivedKeySpec.getPassword();

        return OpenBSDBCrypt.generate(BCRYPT_VERSION, password, salt, cost);
    }

    private byte[] getDerivedKeyBytes(final byte[] hash, final int derivedKeyLength) {
        final MessageDigest messageDigest = getMessageDigest();
        final byte[] digested = messageDigest.digest(hash);
        return Arrays.copyOf(digested, derivedKeyLength);
    }

    private MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new UnsupportedOperationException(DIGEST_ALGORITHM, e);
        }
    }
}
