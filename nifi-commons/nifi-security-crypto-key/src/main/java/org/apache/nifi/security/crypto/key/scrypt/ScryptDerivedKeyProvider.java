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
import org.apache.nifi.security.crypto.key.DerivedKeyProvider;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.DerivedSecretKey;
import org.bouncycastle.crypto.generators.SCrypt;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * scrypt implementation of Derived Key Provider based on Bouncy Castle scrypt components described in RFC 7914
 */
public class ScryptDerivedKeyProvider implements DerivedKeyProvider<ScryptDerivedKeyParameterSpec> {
    private static final String SERIALIZED_FORMAT = "$s0$%s$%s$%s";

    private static final int HEXADECIMAL_RADIX = 16;

    private static final int LOG_BASE_2 = 2;

    private static final int COST_BITS = 16;

    private static final int SIZE_BITS = 8;

    private static final Charset PASSWORD_CHARACTER_SET = StandardCharsets.UTF_8;

    private static final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();

    /**
     * Get Derived Key using scrypt and provided specification
     *
     * @param derivedKeySpec Derived Key Specification
     * @return Derived Secret Key
     */
    @Override
    public DerivedKey getDerivedKey(final DerivedKeySpec<ScryptDerivedKeyParameterSpec> derivedKeySpec) {
        final byte[] password = new String(derivedKeySpec.getPassword()).getBytes(PASSWORD_CHARACTER_SET);

        final ScryptDerivedKeyParameterSpec parameterSpec = derivedKeySpec.getParameterSpec();
        final byte[] salt = parameterSpec.getSalt();
        final int cost = parameterSpec.getCost();
        final int blockSize = parameterSpec.getBlockSize();
        final int parallelization = parameterSpec.getParallelization();
        final int derivedKeyLength = derivedKeySpec.getDerivedKeyLength();

        final byte[] derivedKeyBytes = SCrypt.generate(password, salt, cost, blockSize, parallelization, derivedKeyLength);

        final String serialized = getSerialized(derivedKeyBytes, parameterSpec);
        return new DerivedSecretKey(derivedKeyBytes, derivedKeySpec.getAlgorithm(), serialized);
    }

    private String getSerialized(final byte[] derivedKeyBytes, final ScryptDerivedKeyParameterSpec parameterSpec) {
        final String parametersEncoded = getParametersEncoded(parameterSpec);
        final String derivedKeyEncoded = encoder.encodeToString(derivedKeyBytes);
        final String saltEncoded = encoder.encodeToString(parameterSpec.getSalt());
        return String.format(
                SERIALIZED_FORMAT,
                parametersEncoded,
                saltEncoded,
                derivedKeyEncoded
        );
    }

    private String getParametersEncoded(final ScryptDerivedKeyParameterSpec parameterSpec) {
        final long cost = log2(parameterSpec.getCost()) << COST_BITS;
        final long blockSize = parameterSpec.getBlockSize() << SIZE_BITS;
        final long parallelization = parameterSpec.getParallelization();
        final long parameters = cost | blockSize | parallelization;
        return Long.toString(parameters, HEXADECIMAL_RADIX);
    }

    private long log2(final int number) {
        final double log = Math.log(number);
        final double logBase2 = Math.log(LOG_BASE_2);
        final double log2 = log / logBase2;
        return Math.round(log2);
    }
}
