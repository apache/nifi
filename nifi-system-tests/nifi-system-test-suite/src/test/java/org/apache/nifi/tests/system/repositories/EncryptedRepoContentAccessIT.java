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

package org.apache.nifi.tests.system.repositories;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EncryptedRepoContentAccessIT extends ContentAccessIT {
    private static final String KEYSTORE_CREDENTIALS = UUID.randomUUID().toString();

    private static final String KEYSTORE_NAME = "repository.p12";

    private static final String KEY_ID = "primary-key";

    private static final String KEYSTORE_TYPE = "PKCS12";

    private static final int KEY_LENGTH = 32;

    private static final String KEY_ALGORITHM = "AES";

    private static Path keyStorePath;

    @BeforeAll
    public static void setRepositoryKeystore(@TempDir final Path temporaryDirectory) throws GeneralSecurityException, IOException {
        keyStorePath = temporaryDirectory.resolve(KEYSTORE_NAME);

        final SecureRandom secureRandom = new SecureRandom();
        final byte[] key = new byte[KEY_LENGTH];
        secureRandom.nextBytes(key);
        final SecretKeySpec secretKeySpec = new SecretKeySpec(key, KEY_ALGORITHM);

        final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
        keyStore.load(null);

        final KeyStore.SecretKeyEntry secretKeyEntry = new KeyStore.SecretKeyEntry(secretKeySpec);
        final KeyStore.PasswordProtection passwordProtection = new KeyStore.PasswordProtection(KEYSTORE_CREDENTIALS.toCharArray());
        keyStore.setEntry(KEY_ID, secretKeyEntry, passwordProtection);

        try (final OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
            keyStore.store(outputStream, KEYSTORE_CREDENTIALS.toCharArray());
        }
    }

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        final Map<String, String> encryptedRepoProperties = new HashMap<>();
        encryptedRepoProperties.put("nifi.repository.encryption.protocol.version", "1");
        encryptedRepoProperties.put("nifi.repository.encryption.key.id", KEY_ID);
        encryptedRepoProperties.put("nifi.repository.encryption.key.provider", "KEYSTORE");
        encryptedRepoProperties.put("nifi.repository.encryption.key.provider.keystore.location", keyStorePath.toString());
        encryptedRepoProperties.put("nifi.repository.encryption.key.provider.keystore.password", KEYSTORE_CREDENTIALS);
        return encryptedRepoProperties;
    }
}
