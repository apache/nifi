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

import org.apache.nifi.security.kms.util.SecretKeyUtils;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileBasedKeyProviderTest {
    private static final String KEYS_EXTENSION = ".keys";

    private static final String KEY_ID = UUID.randomUUID().toString();

    @Test
    public void testGetKey() throws GeneralSecurityException, IOException {
        final SecretKey rootKey = SecretKeyUtils.getSecretKey();
        final SecretKey secretKey = SecretKeyUtils.getSecretKey();
        final Path secretKeysPath = getSecretKeysPath(rootKey, Collections.singletonMap(KEY_ID, secretKey));
        final FileBasedKeyProvider provider = new FileBasedKeyProvider(secretKeysPath, rootKey);

        final SecretKey secretKeyFound = provider.getKey(KEY_ID);
        assertEquals(secretKey, secretKeyFound);
        assertTrue(provider.keyExists(KEY_ID));
        assertFalse(provider.getAvailableKeyIds().isEmpty());
    }

    private Path getSecretKeysPath(final SecretKey rootKey, final Map<String, SecretKey> secretKeys) throws IOException, GeneralSecurityException {
        final Path path = Files.createTempFile(FileBasedKeyProviderTest.class.getSimpleName(), KEYS_EXTENSION);
        path.toFile().deleteOnExit();

        final Properties properties = SecretKeyUtils.getEncryptedSecretKeys(rootKey, secretKeys);
        try (final OutputStream outputStream = new FileOutputStream(path.toFile())) {
            properties.store(outputStream, null);
        }

        return path;
    }
}
