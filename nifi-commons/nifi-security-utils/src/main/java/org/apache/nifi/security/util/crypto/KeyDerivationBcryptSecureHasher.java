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
package org.apache.nifi.security.util.crypto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Extension of Bcrypt Secure Hasher used for Key Derivation support. Allows specifying a Derived Key Length in bytes.
 */
public class KeyDerivationBcryptSecureHasher extends BcryptSecureHasher {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyDerivationBcryptSecureHasher.class);

    private static final String DIGEST_ALGORITHM = "SHA-512";

    private static final int HASH_START_INDEX = 29;

    private final int derivedKeyLength;

    private final boolean digestBcryptHash;

    /**
     * Key Deriviation Bcrypt Secure Hasher with specified Derived Key Length
     *
     * @param derivedKeyLength Derived Key Length in bytes
     */
    public KeyDerivationBcryptSecureHasher(final int derivedKeyLength) {
        this.derivedKeyLength = derivedKeyLength;
        this.digestBcryptHash = false;
    }

    /**
     * Key Deriviation Bcrypt Secure Hasher with specified Derived Key Length and Cost Parameters
     *
     * @param derivedKeyLength Derived Key Length in bytes
     * @param cost Cost Parameter for calculation
     * @param digestBcryptHash Enable to disable digesting of bcrypt hash to support legacy derivation functions
     */
    public KeyDerivationBcryptSecureHasher(final int derivedKeyLength, final int cost, final boolean digestBcryptHash) {
        super(cost);
        this.derivedKeyLength = derivedKeyLength;
        this.digestBcryptHash = digestBcryptHash;
    }

    /**
     * Hash raw bytes using provided salt and then leverage SHA-512 to digest the results and truncate to length requested
     *
     * @param input Raw bytes to be hashed
     * @param rawSalt Raw salt bytes to be hashed
     * @return Hash bytes digested using SHA-512 and truncated to derived key length configured
     */
    @Override
    byte[] hash(final byte[] input, final byte[] rawSalt) {
        final byte[] costSaltBcryptHash = super.hash(input, rawSalt);

        final MessageDigest messageDigest = getMessageDigest();
        byte[] digest;
        if (digestBcryptHash) {
            LOGGER.warn("Using Legacy Key Derivation on bcrypt hash including cost and salt");
            digest = messageDigest.digest(costSaltBcryptHash);
        } else {
            // Remove cost and salt from bcrypt function results and retain bcrypt hash
            byte[] hash = Arrays.copyOfRange(costSaltBcryptHash, HASH_START_INDEX, costSaltBcryptHash.length);
            digest = messageDigest.digest(hash);
        }

        return Arrays.copyOf(digest, derivedKeyLength);
    }

    private MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new UnsupportedOperationException(DIGEST_ALGORITHM, e);
        }
    }
}
