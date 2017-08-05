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
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(KeyProviderFactory.class);

    public static KeyProvider buildKeyProvider(String implementationClassName, String keyProviderLocation, String keyId, Map<String, String> encryptionKeys,
                                               SecretKey masterKey) throws KeyManagementException {
        KeyProvider keyProvider;

       implementationClassName = CryptoUtils.handleLegacyPackages(implementationClassName);

        if (StaticKeyProvider.class.getName().equals(implementationClassName)) {
            // Get all the keys (map) from config
            if (CryptoUtils.isValidKeyProvider(implementationClassName, keyProviderLocation, keyId, encryptionKeys)) {
                Map<String, SecretKey> formedKeys = encryptionKeys.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> {
                                    try {
                                        return CryptoUtils.formKeyFromHex(e.getValue());
                                    } catch (KeyManagementException e1) {
                                        // This should never happen because the hex has already been validated
                                        logger.error("Encountered an error: ", e1);
                                        return null;
                                    }
                                }));
                keyProvider = new StaticKeyProvider(formedKeys);
            } else {
                final String msg = "The StaticKeyProvider definition is not valid";
                logger.error(msg);
                throw new KeyManagementException(msg);
            }
        } else if (FileBasedKeyProvider.class.getName().equals(implementationClassName)) {
            keyProvider = new FileBasedKeyProvider(keyProviderLocation, masterKey);
            if (!keyProvider.keyExists(keyId)) {
                throw new KeyManagementException("The specified key ID " + keyId + " is not in the key definition file");
            }
        } else {
            throw new KeyManagementException("Invalid key provider implementation provided: " + implementationClassName);
        }

        return keyProvider;
    }

    public static boolean requiresMasterKey(String implementationClassName) throws KeyManagementException {
        implementationClassName = CryptoUtils.handleLegacyPackages(implementationClassName);
        return FileBasedKeyProvider.class.getName().equals(implementationClassName);
    }
}
