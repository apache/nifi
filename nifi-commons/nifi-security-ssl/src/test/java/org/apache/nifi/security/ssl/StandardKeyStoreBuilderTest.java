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
package org.apache.nifi.security.ssl;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.UUID;

import static java.nio.file.Files.createTempFile;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardKeyStoreBuilderTest {

    private static final String TYPE = KeyStore.getDefaultType();

    private static final char[] PASSWORD = UUID.randomUUID().toString().toCharArray();

    @Test
    void testBuild() throws Exception {
        final Path path = createTempFile(StandardKeyStoreBuilderTest.class.getSimpleName(), TYPE);
        path.toFile().deleteOnExit();

        final KeyStore sourceKeyStore = KeyStore.getInstance(TYPE);
        sourceKeyStore.load(null);
        try (final OutputStream outputStream = Files.newOutputStream(path)) {
            sourceKeyStore.store(outputStream, PASSWORD);
        }

        final StandardKeyStoreBuilder builder = new StandardKeyStoreBuilder();
        builder.type(TYPE);
        builder.password(PASSWORD);
        try (final InputStream inputStream = Files.newInputStream(path)) {
            builder.inputStream(inputStream);
            final KeyStore keyStore = builder.build();
            assertNotNull(keyStore);
        }
    }
}
