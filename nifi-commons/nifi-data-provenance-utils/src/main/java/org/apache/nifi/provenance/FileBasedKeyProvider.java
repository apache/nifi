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

import java.io.IOException;
import java.security.KeyManagementException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.naming.OperationNotSupportedException;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedKeyProvider implements KeyProvider {
    private static final Logger logger = LoggerFactory.getLogger(FileBasedKeyProvider.class);

    private String filepath;
    private Map<String, SecretKey> keys = new HashMap<>();

    FileBasedKeyProvider(String location) throws KeyManagementException {
        this(location, getMasterKey());
    }

    FileBasedKeyProvider(String location, SecretKey masterKey) throws KeyManagementException {
        this.filepath = location;
        this.keys = CryptoUtils.readKeys(this.filepath, masterKey);
    }

    private static SecretKey getMasterKey() throws KeyManagementException {
        try {
            // Get the master encryption key from bootstrap.conf
            String masterKeyHex = NiFiPropertiesLoader.extractKeyFromBootstrapFile();
            return new SecretKeySpec(Hex.decode(masterKeyHex), "AES");
        } catch (IOException e) {
            logger.error("Encountered an error: ", e);
            throw new KeyManagementException(e);
        }
    }

    /**
     * Returns the key identified by this ID or throws an exception if one is not available.
     *
     * @param keyId the key identifier
     * @return the key
     * @throws KeyManagementException if the key cannot be retrieved
     */
    @Override
    public SecretKey getKey(String keyId) throws KeyManagementException {
        if (keyExists(keyId)) {
            return keys.get(keyId);
        } else {
            throw new KeyManagementException("No key available for ID: " + keyId);
        }
    }

    /**
     * Returns true if the key exists and is available. Null or empty IDs will return false.
     *
     * @param keyId the key identifier
     * @return true if the key can be used
     */
    @Override
    public boolean keyExists(String keyId) {
        return keys.containsKey(keyId);
    }

    /**
     * Returns a list of available key identifiers (useful for encryption, as retired keys may not be listed here even if they are available for decryption for legacy/BC reasons).
     *
     * @return a List of keyIds (empty list if none are available)
     */
    @Override
    public List<String> getAvailableKeyIds() {
        return new ArrayList<>(keys.keySet());
    }

    /**
     * Adds the key to the provider and associates it with the given ID. Some implementations may not allow this operation.
     *
     * @param keyId the key identifier
     * @param key   the key
     * @return true if the key was successfully added
     * @throws OperationNotSupportedException if this implementation doesn't support adding keys
     * @throws KeyManagementException         if the key is invalid, the ID conflicts, etc.
     */
    @Override
    public boolean addKey(String keyId, SecretKey key) throws OperationNotSupportedException, KeyManagementException {
        throw new OperationNotSupportedException("This implementation does not allow adding keys. Modify the file backing this provider at " + filepath);
    }
}
