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
package org.apache.nifi.security.crypto.key.bcrypt;

import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;

/**
 * bcrypt key derivation function parameter specification
 */
public class BcryptDerivedKeyParameterSpec implements DerivedKeyParameterSpec {
    private final int cost;

    private final byte[] salt;

    /**
     * bcrypt Parameter Specification constructor with required properties
     *
     * @param cost Cost parameter
     * @param salt Array of random salt bytes
     */
    public BcryptDerivedKeyParameterSpec(
            final int cost,
            final byte[] salt
    ) {
        this.cost = cost;
        this.salt = salt;
    }

    @Override
    public byte[] getSalt() {
        return salt;
    }

    public int getCost() {
        return cost;
    }
}
