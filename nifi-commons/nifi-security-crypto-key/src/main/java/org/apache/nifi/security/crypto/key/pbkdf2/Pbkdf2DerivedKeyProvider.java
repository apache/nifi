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
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA512Digest;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.params.KeyParameter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * PBKDF2 implementation of Derived Key Provider based on Bouncy Castle components with HMAC SHA-512 pseudorandom function
 */
public class Pbkdf2DerivedKeyProvider implements DerivedKeyProvider<Pbkdf2DerivedKeyParameterSpec> {
    private static final Charset PASSWORD_CHARACTER_SET = StandardCharsets.UTF_8;

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
        final Digest digest = new SHA512Digest();
        final PKCS5S2ParametersGenerator generator = new PKCS5S2ParametersGenerator(digest);

        final byte[] password = new String(derivedKeySpec.getPassword()).getBytes(PASSWORD_CHARACTER_SET);
        final Pbkdf2DerivedKeyParameterSpec parameterSpec = derivedKeySpec.getParameterSpec();
        final byte[] salt = parameterSpec.getSalt();
        final int iterations = parameterSpec.getIterations();
        generator.init(password, salt, iterations);

        final int derivedKeyLengthBits = derivedKeySpec.getDerivedKeyLength() * BITS;
        final CipherParameters cipherParameters = generator.generateDerivedParameters(derivedKeyLengthBits);
        final KeyParameter keyParameter = (KeyParameter) cipherParameters;
        return keyParameter.getKey();
    }
}
