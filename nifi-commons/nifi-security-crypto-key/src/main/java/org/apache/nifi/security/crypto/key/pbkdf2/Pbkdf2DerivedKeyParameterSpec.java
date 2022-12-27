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

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;

/**
 * PBKDF2 key derivation function parameter specification
 */
public class Pbkdf2DerivedKeyParameterSpec implements DerivedKeyParameterSpec {
    private final int iterations;

    private final byte[] salt;

    /**
     * PBKDF2 Parameter Specification constructor with required properties
     *
     * @param iterations Cost parameter
     * @param salt Array of random salt bytes
     */
    public Pbkdf2DerivedKeyParameterSpec(
            final int iterations,
            final byte[] salt
    ) {
        this.iterations = iterations;
        this.salt = salt;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    public int getIterations() {
        return iterations;
    }
}
