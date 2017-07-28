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
import javax.crypto.SecretKey;
import javax.naming.OperationNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedKeyProvider extends StaticKeyProvider {
    private static final Logger logger = LoggerFactory.getLogger(FileBasedKeyProvider.class);

    private String filepath;

    public FileBasedKeyProvider(String location, SecretKey masterKey) throws KeyManagementException {
        super(CryptoUtils.readKeys(location, masterKey));
        this.filepath = location;
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
