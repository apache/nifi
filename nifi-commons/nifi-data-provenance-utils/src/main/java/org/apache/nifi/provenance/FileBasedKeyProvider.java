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
package org.apache.nifi.provenance;

import java.security.KeyManagementException;
import java.util.Collections;
import java.util.List;
import javax.crypto.SecretKey;
import javax.naming.OperationNotSupportedException;

public class FileBasedKeyProvider implements KeyProvider {

    /**
     * Returns the key identified by this ID or throws an exception if one is not available.
     *
     * @param keyId the key identifier
     * @return the key
     * @throws KeyManagementException if the key cannot be retrieved
     */
    @Override
    public SecretKey getKey(String keyId) throws KeyManagementException {
        // TODO: Implement
        return null;
    }

    /**
     * Returns true if the key exists and is available. Null or empty IDs will return false.
     *
     * @param keyId the key identifier
     * @return true if the key can be used
     */
    @Override
    public boolean keyExists(String keyId) {
        // TODO: Implement
        return false;
    }

    @Override
    public List<String> getAvailableKeyIds() {
        return Collections.emptyList();
    }

    /**
     * Adds the key to the provider and associates it with the given ID. Some implementations may not allow this operation.
     *
     * @param keyId the key identifier
     * @param key the key
     * @return true if the key was successfully added
     * @throws OperationNotSupportedException if this implementation doesn't support adding keys
     * @throws KeyManagementException if the key is invalid, the ID conflicts, etc.
     */
    @Override
    public boolean addKey(String keyId, SecretKey key) throws OperationNotSupportedException, KeyManagementException {
        // TODO: Implement
        return false;
    }
}
