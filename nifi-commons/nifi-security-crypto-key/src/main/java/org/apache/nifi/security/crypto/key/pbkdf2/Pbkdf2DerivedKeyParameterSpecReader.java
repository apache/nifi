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

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpecReader;

import java.util.Objects;

/**
 * PBKDF2 implementation uses the serialized parameters as the salt with a hard-coded number of iterations as defined in NiFi 0.5.0
 */
public class Pbkdf2DerivedKeyParameterSpecReader implements DerivedKeyParameterSpecReader<Pbkdf2DerivedKeyParameterSpec> {
    protected static final int VERSION_0_5_0_ITERATIONS = 160000;

    /**
     * Read serialized parameters and return as salt bytes with 160,000 iterations as defined in NiFi 0.5.0
     *
     * @param serializedParameters Serialized parameters
     * @return PBKDF2 Parameter Specification
     */
    @Override
    public Pbkdf2DerivedKeyParameterSpec read(final byte[] serializedParameters) {
        Objects.requireNonNull(serializedParameters, "Parameters required");
        return new Pbkdf2DerivedKeyParameterSpec(VERSION_0_5_0_ITERATIONS, serializedParameters);
    }
}
