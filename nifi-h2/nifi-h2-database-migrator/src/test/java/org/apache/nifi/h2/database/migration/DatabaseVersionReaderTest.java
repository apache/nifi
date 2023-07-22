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
package org.apache.nifi.h2.database.migration;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatabaseVersionReaderTest {

    @Test
    void testReadDatabaseVersionFileNotFound() {
        final Path databaseFilePath = Paths.get("not-found");

        final DatabaseVersion databaseVersion = DatabaseVersionReader.readDatabaseVersion(databaseFilePath);

        assertEquals(DatabaseVersion.UNKNOWN, databaseVersion);
    }

    @Test
    void testReadDatabaseVersion14() {
        final Path databaseFilePath = getResourcePath("/1.4/nifi-flow-audit.mv.db");

        final DatabaseVersion databaseVersion = DatabaseVersionReader.readDatabaseVersion(databaseFilePath);

        assertEquals(DatabaseVersion.VERSION_1_4, databaseVersion);
    }

    @Test
    void testReadDatabaseVersion21() {
        final Path databaseFilePath = getResourcePath("/2.1/nifi-flow-audit.mv.db");

        final DatabaseVersion databaseVersion = DatabaseVersionReader.readDatabaseVersion(databaseFilePath);

        assertEquals(DatabaseVersion.VERSION_2_1, databaseVersion);
    }

    @Test
    void testReadDatabaseVersion22() {
        final Path databaseFilePath = getResourcePath("/2.2/nifi-flow-audit.mv.db");

        final DatabaseVersion databaseVersion = DatabaseVersionReader.readDatabaseVersion(databaseFilePath);

        assertEquals(DatabaseVersion.VERSION_2_2, databaseVersion);
    }

    private Path getResourcePath(final String relativePath) {
        final URL resourceUrl = getClass().getResource(relativePath);
        if (resourceUrl == null) {
            throw new IllegalStateException(String.format("Resource Path [%s] not found", relativePath));
        }
        try {
            final URI resourceUri = resourceUrl.toURI();
            return Paths.get(resourceUri);
        } catch (final URISyntaxException e) {
            throw new IllegalStateException(String.format("Resource URL [%s] conversion failed", resourceUrl));
        }
    }
}
