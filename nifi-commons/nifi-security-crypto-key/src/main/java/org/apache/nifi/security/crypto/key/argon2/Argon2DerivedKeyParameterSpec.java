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

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;

/**
 * Argon2 key derivation function parameter specification
 */
public class Argon2DerivedKeyParameterSpec implements DerivedKeyParameterSpec {
    private final int memory;

    private final int iterations;

    private final int parallelism;

    private final byte[] salt;

    /**
     * Argon2 Parameter Specification constructor with required properties
     *
     * @param memory Size of memory in kilobytes for processing
     * @param iterations Number of iterations to perform
     * @param parallelism Number of threads for parallel processing
     * @param salt Array of random salt bytes
     */
    public Argon2DerivedKeyParameterSpec(
            final int memory,
            final int iterations,
            final int parallelism,
            final byte[] salt
    ) {
        this.memory = memory;
        this.iterations = iterations;
        this.parallelism = parallelism;
        this.salt = salt;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    public int getMemory() {
        return memory;
    }

    public int getIterations() {
        return iterations;
    }

    public int getParallelism() {
        return parallelism;
    }
}
