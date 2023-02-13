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
package org.apache.nifi.security.crypto.key;

import java.util.Objects;

/**
 * Standard implementation of Derived Key Specification with required properties
 */
public class StandardDerivedKeySpec<T extends DerivedKeyParameterSpec> implements DerivedKeySpec<T> {
    private final char[] password;

    private final int derivedKeyLength;

    private final String algorithm;

    private final T parameterSpec;

    public StandardDerivedKeySpec(
            final char[] password,
            final int derivedKeyLength,
            final String algorithm,
            final T parameterSpec
    ) {
        this.password = Objects.requireNonNull(password, "Password required");
        this.derivedKeyLength = derivedKeyLength;
        this.algorithm = Objects.requireNonNull(algorithm, "Algorithm required");
        this.parameterSpec = Objects.requireNonNull(parameterSpec, "Parameter Specification required");
    }

    @Override
    public char[] getPassword() {
        return password;
    }

    @Override
    public int getDerivedKeyLength() {
        return derivedKeyLength;
    }

    @Override
    public String getAlgorithm() {
        return algorithm;
    }

    @Override
    public T getParameterSpec() {
        return parameterSpec;
    }
}
