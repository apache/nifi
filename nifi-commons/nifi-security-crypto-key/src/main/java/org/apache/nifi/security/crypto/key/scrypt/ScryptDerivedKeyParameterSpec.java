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

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;

/**
 * scrypt key derivation function parameter specification
 */
public class ScryptDerivedKeyParameterSpec implements DerivedKeyParameterSpec {
    private final int cost;

    private final int blockSize;

    private final int parallelization;

    private final byte[] salt;

    /**
     * scrypt Parameter Specification constructor with required properties
     *
     * @param cost CPU and memory cost parameter
     * @param blockSize Block size parameter
     * @param parallelization Parallelization parameter
     * @param salt Array of random salt bytes
     */
    public ScryptDerivedKeyParameterSpec(
            final int cost,
            final int blockSize,
            final int parallelization,
            final byte[] salt
    ) {
        this.cost = cost;
        this.blockSize = blockSize;
        this.parallelization = parallelization;
        this.salt = salt;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    public int getCost() {
        return cost;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getParallelization() {
        return parallelization;
    }
}
