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
package org.apache.nifi.security.kms.reader;

import org.apache.nifi.security.kms.util.SecretKeyUtils;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class StandardFileBasedKeyReaderTest {
    private static final String KEYS_EXTENSION = ".keys";

    private static final SecretKey ROOT_KEY = SecretKeyUtils.getSecretKey();

    @Test
    public void testReadSecretKeys() throws Exception {
        final StandardFileBasedKeyReader reader = new StandardFileBasedKeyReader();

        final SecretKey secretKey = SecretKeyUtils.getSecretKey();
        final String keyId = SecretKey.class.getSimpleName();

        final Path path = getSecretKeysPath(ROOT_KEY, Collections.singletonMap(keyId, secretKey));
        final Map<String, SecretKey> secretKeys = reader.readSecretKeys(path, ROOT_KEY);
        final SecretKey readSecretKey = secretKeys.get(keyId);
        assertEquals("Secret Key not matched", secretKey, readSecretKey);
    }

    private Path getSecretKeysPath(final SecretKey rootKey, final Map<String, SecretKey> secretKeys) throws Exception {
        final Path path = Files.createTempFile(StandardFileBasedKeyReaderTest.class.getSimpleName(), KEYS_EXTENSION);
        path.toFile().deleteOnExit();

        final Properties properties = SecretKeyUtils.getEncryptedSecretKeys(rootKey, secretKeys);
        try (final OutputStream outputStream = new FileOutputStream(path.toFile())) {
            properties.store(outputStream, null);
        }

        return path;
    }
}
