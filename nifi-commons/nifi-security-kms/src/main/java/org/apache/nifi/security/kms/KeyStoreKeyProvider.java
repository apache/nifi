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

import org.apache.nifi.security.kms.reader.KeyReaderException;

import javax.crypto.SecretKey;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * KeyStore implementation of Key Provider
 */
public class KeyStoreKeyProvider extends StaticKeyProvider {
    /**
     * KeyStore Key Provider constructor with KeyStore and password used to read Secret Key entries
     *
     * @param keyStore KeyStore
     * @param keyPassword Password for reading Secret Key entries
     */
    public KeyStoreKeyProvider(final KeyStore keyStore, final char[] keyPassword) {
        super(readSecretKeys(requireNonNull(keyStore, "KeyStore required"), requireNonNull(keyPassword, "Password required")));
    }

    private static Map<String, SecretKey> readSecretKeys(final KeyStore keyStore, final char[] keyPassword) throws KeyReaderException {
        final Map<String, SecretKey> secretKeys = new HashMap<>();

        try {
            final Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                final String alias = aliases.nextElement();
                final Key key = keyStore.getKey(alias, keyPassword);
                if (key instanceof SecretKey) {
                    final SecretKey secretKey = (SecretKey) key;
                    secretKeys.put(alias, secretKey);
                }
            }
        } catch (final GeneralSecurityException e) {
            throw new KeyReaderException("Reading KeyStore failed", e);
        }

        return secretKeys;
    }
}
