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
package org.apache.nifi.security.kms;

import java.security.KeyManagementException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.crypto.SecretKey;

/**
 * Static Key Provider stores Secret Keys in memory based on initialized configuration properties
 */
public class StaticKeyProvider implements KeyProvider {
    private final Map<String, SecretKey> keys;

    /**
     * Static Key Provider constructor of Map of Key Identifier to Secret Key
     *
     * @param keys Map of Key Identifier to Secret Key
     */
    public StaticKeyProvider(final Map<String, SecretKey> keys) {
        this.keys = Collections.unmodifiableMap(Objects.requireNonNull(keys, "Keys required"));
    }

    /**
     * Returns the key identified by this ID or throws an exception if one is not available.
     *
     * @param keyId the key identifier
     * @return the key
     * @throws KeyManagementException Thrown when Secret Key not found for Key Identifier
     */
    @Override
    public SecretKey getKey(final String keyId) throws KeyManagementException {
        final SecretKey secretKey = keys.get(keyId);
        if (secretKey == null) {
            throw new KeyManagementException(String.format("Secret Key [%s] not found", keyId));
        }
        return secretKey;
    }

    /**
     * Returns true if the key exists and is available. Null or empty IDs will return false.
     *
     * @param keyId the key identifier
     * @return true if the key can be used
     */
    @Override
    public boolean keyExists(final String keyId) {
        return keys.containsKey(keyId);
    }

    /**
     * Returns a singleton list of the available key identifier.
     *
     * @return a List containing the {@code KEY_ID}
     */
    @Override
    public List<String> getAvailableKeyIds() {
        return new ArrayList<>(keys.keySet());
    }
}
