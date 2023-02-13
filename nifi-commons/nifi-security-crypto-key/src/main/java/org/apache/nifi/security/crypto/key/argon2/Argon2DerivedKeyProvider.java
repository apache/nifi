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
package org.apache.nifi.security.crypto.key.argon2;

import org.apache.nifi.security.crypto.key.DerivedKey;
import org.apache.nifi.security.crypto.key.DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.DerivedSecretKey;
import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;

import java.util.Base64;

/**
 * Argon2 implementation of Derived Key Provider based on Bouncy Castle Argon2 components described in RFC 9016
 */
public class Argon2DerivedKeyProvider implements DerivedKeyProvider<Argon2DerivedKeyParameterSpec> {
    private static final int HYBRID_VERSION = Argon2Parameters.ARGON2_id;

    private static final int CURRENT_VERSION = Argon2Parameters.ARGON2_VERSION_13;

    private static final String SERIALIZED_FORMAT = "$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s";

    private static final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    /**
     * Get Derived Key using Argon2id version 1.3 and provided specification
     *
     * @param derivedKeySpec Derived Key Specification
     * @return Derived Secret Key
     */
    @Override
    public DerivedKey getDerivedKey(final DerivedKeySpec<Argon2DerivedKeyParameterSpec> derivedKeySpec) {
        final Argon2DerivedKeyParameterSpec parameterSpec = derivedKeySpec.getParameterSpec();
        final Argon2Parameters parameters = getParameters(parameterSpec);
        final Argon2BytesGenerator generator = new Argon2BytesGenerator();
        generator.init(parameters);

        final int derivedKeyLength = derivedKeySpec.getDerivedKeyLength();
        final byte[] derivedKeyBytes = new byte[derivedKeyLength];
        final char[] password = derivedKeySpec.getPassword();
        generator.generateBytes(password, derivedKeyBytes);

        final String serialized = getSerialized(derivedKeyBytes, parameterSpec);
        return new DerivedSecretKey(derivedKeyBytes, derivedKeySpec.getAlgorithm(), serialized);
    }

    private Argon2Parameters getParameters(final Argon2DerivedKeyParameterSpec parameterSpec) {
        return new Argon2Parameters.Builder(HYBRID_VERSION)
                .withVersion(CURRENT_VERSION)
                .withMemoryAsKB(parameterSpec.getMemory())
                .withIterations(parameterSpec.getIterations())
                .withParallelism(parameterSpec.getParallelism())
                .withSalt(parameterSpec.getSalt())
                .build();
    }

    private String getSerialized(final byte[] derivedKeyBytes, final Argon2DerivedKeyParameterSpec parameterSpec) {
        final String derivedKeyEncoded = encoder.encodeToString(derivedKeyBytes);
        final String saltEncoded = encoder.encodeToString(parameterSpec.getSalt());
        return String.format(
                SERIALIZED_FORMAT,
                CURRENT_VERSION,
                parameterSpec.getMemory(),
                parameterSpec.getIterations(),
                parameterSpec.getParallelism(),
                saltEncoded,
                derivedKeyEncoded
        );
    }
}
