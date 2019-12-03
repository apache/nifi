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
package org.apache.nifi.properties.sensitive.keystore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

/**
 * KeyStores read from the file system.
 *
 */
public class StandardKeyStoreProvider implements KeyStoreProvider {
    private final String filename;
    private final String storeType;
    private final String storePassword;

    /**
     * Creates a StandardKeyStoreProvider.
     *
     * @param filename  key store filename
     * @param storeType key store type, e.g., JCEKS
     * @param storePassword key store password
     */
    public StandardKeyStoreProvider(String filename, String storeType, String storePassword) {
        this.filename = filename;
        this.storeType = storeType;
        this.storePassword = storePassword;
    }

    /**
     * Reads, loads, and returns a KeyStore from configured filename.
     *
     * @return new KeyStore
     * @throws IOException if the KeyStore cannot be opened or read
     */
    public KeyStore getKeyStore() throws IOException {
        KeyStore store;
        File file = new File(filename);

        try {
            store = KeyStore.getInstance(storeType.toUpperCase());
            store.load(new FileInputStream(file), storePassword.toCharArray());

        } catch (IOException | NoSuchAlgorithmException | CertificateException | KeyStoreException e) {
            throw new IOException("Error loading Key Store.", e);
        }

        return store;
    }
}
