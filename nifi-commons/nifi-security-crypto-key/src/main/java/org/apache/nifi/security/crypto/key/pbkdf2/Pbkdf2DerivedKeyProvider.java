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
package org.apache.nifi.security.crypto.key.pbkdf2;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.DerivedSecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * PBKDF2 implementation of Derived Key Provider with HMAC SHA-512 pseudorandom function
 */
public class Pbkdf2DerivedKeyProvider implements DerivedKeyProvider<Pbkdf2DerivedKeyParameterSpec> {
    private static final String ALGORITHM = "PBKDF2WithHmacSHA512";

    private static final int BITS = 8;

    private static final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    /**
     * Get Derived Key using PBKDF2 with HMAC SHA-512 and provided specification
     *
     * @param derivedKeySpec Derived Key Specification
     * @return Derived Secret Key
     */
    @Override
    public DerivedKey getDerivedKey(final DerivedKeySpec<Pbkdf2DerivedKeyParameterSpec> derivedKeySpec) {
        final byte[] derivedKeyBytes = getDerivedKeyBytes(derivedKeySpec);
        final String serialized = encoder.encodeToString(derivedKeyBytes);
        return new DerivedSecretKey(derivedKeyBytes, derivedKeySpec.getAlgorithm(), serialized);
    }

    private byte[] getDerivedKeyBytes(final DerivedKeySpec<Pbkdf2DerivedKeyParameterSpec> derivedKeySpec) {
        final Pbkdf2DerivedKeyParameterSpec parameterSpec = derivedKeySpec.getParameterSpec();
        final byte[] salt = parameterSpec.getSalt();
        final int iterations = parameterSpec.getIterations();
        final int derivedKeyLengthBits = derivedKeySpec.getDerivedKeyLength() * BITS;

        final PBEKeySpec keySpec = new PBEKeySpec(derivedKeySpec.getPassword(), salt, iterations, derivedKeyLengthBits);
        final SecretKeyFactory secretKeyFactory = getSecretKeyFactory();

        try {
            final SecretKey secretKey = secretKeyFactory.generateSecret(keySpec);
            return secretKey.getEncoded();
        } catch (final InvalidKeySpecException e) {
            throw new IllegalStateException("PBKDF2 key generation failed", e);
        }
    }

    private SecretKeyFactory getSecretKeyFactory() {
        try {
            return SecretKeyFactory.getInstance(ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("PBKDF2 algorithm not found", e);
        }
    }
}
