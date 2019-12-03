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
package org.apache.nifi.properties.sensitive;

import org.apache.nifi.properties.sensitive.property.provider.keystore.KeyStoreProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * Key Stores backed by byte arrays.  This class is used only by the KeyStore Sensitive Property Provider Integration Tests.
 *
 */
public class ByteArrayKeyStoreProvider implements KeyStoreProvider {
    private final byte[] source;
    private final String storeType;
    private final String storePassword;

    public ByteArrayKeyStoreProvider(byte[] source, String storeType, String storePassword) {
        this.source = source;
        this.storeType = storeType;
        this.storePassword = storePassword;
    }

    /**
     * Reads, loads, and returns a KeyStore from the configured byte array.
     *
     * @return new KeyStore
     * @throws IOException if the KeyStore cannot be opened or read
     */
    public KeyStore getKeyStore() throws IOException {
        KeyStore store;
        try {
            store = KeyStore.getInstance(storeType.toUpperCase());
            store.load(new ByteArrayInputStream(source), storePassword.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new IOException("Error loading Key Store.", e);
        }
        return store;
    }
}
