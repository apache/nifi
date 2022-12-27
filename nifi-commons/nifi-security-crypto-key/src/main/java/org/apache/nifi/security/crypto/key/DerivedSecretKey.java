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

import javax.crypto.spec.SecretKeySpec;

/**
 * Derived Secret Key containing encoded key bytes and serialized representation of key with parameters
 */
public class DerivedSecretKey extends SecretKeySpec implements DerivedKey {
    private final String serialized;

    /**
     * Derived Secret Key constructor with encoded key and serialized representation
     *
     * @param key Encoded key bytes
     * @param algorithm Cipher algorithm for which the key will be used
     * @param serialized Serialized representation of the key with parameters depending on derivation function
     */
    public DerivedSecretKey(final byte[] key, final String algorithm, final String serialized) {
        super(key, algorithm);
        this.serialized = serialized;
    }

    /**
     * Get serialized key with parameters
     *
     * @return Serialized key with parameters
     */
    @Override
    public String getSerialized() {
        return serialized;
    }
}
