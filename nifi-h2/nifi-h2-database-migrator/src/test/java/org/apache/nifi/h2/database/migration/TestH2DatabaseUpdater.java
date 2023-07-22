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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestH2DatabaseUpdater {

    private static final String DATABASE_NAME = "nifi-flow-audit";

    private static final String VERSION_1_4_PATH = String.format("/1.4/%s", DATABASE_NAME);

    private static final String VERSION_2_1_PATH = String.format("/2.1/%s", DATABASE_NAME);

    private static final String VERSION_2_2_PATH = String.format("/2.2/%s", DATABASE_NAME);

    private static final String FILE_NAME_FORMAT = "%s.mv.db";

    private static final String URL_FORMAT = "jdbc:h2:%s;LOCK_MODE=3";

    private static final String CREDENTIALS = "nf";

    @Test
    public void testMigrationFrom14(@TempDir final File temporaryDirectory) throws Exception {
        runUpdater(temporaryDirectory, VERSION_1_4_PATH);
        assertFilesFound(2, temporaryDirectory);
    }

    @Test
    public void testMigrationFrom21(@TempDir final File temporaryDirectory) throws Exception {
        runUpdater(temporaryDirectory, VERSION_2_1_PATH);
        assertFilesFound(2, temporaryDirectory);
    }

    @Test
    public void testMigrationFrom22(@TempDir final File temporaryDirectory) throws Exception {
        runUpdater(temporaryDirectory, VERSION_2_2_PATH);
        assertFilesFound(1, temporaryDirectory);
    }

    private void assertFilesFound(final int filesExpected, final File temporaryDirectory) {
        final File[] files = temporaryDirectory.listFiles();

        assertNotNull(files);
        assertEquals(filesExpected, files.length);
    }

    private void runUpdater(final File temporaryDirectory, final String sourceRelativePath) throws Exception {
        final Path temporaryDirectoryPath = temporaryDirectory.toPath();

        final String sourceRelativeFilePath = String.format(FILE_NAME_FORMAT, sourceRelativePath);
        final Path sourceFilePath = getResourcePath(sourceRelativeFilePath);

        final Path temporarySourceFilePath = temporaryDirectoryPath.resolve(sourceFilePath.getFileName());
        Files.copy(sourceFilePath, temporarySourceFilePath, REPLACE_EXISTING);

        final Path temporaryDatabasePath = temporaryDirectoryPath.resolve(DATABASE_NAME);
        final String databaseUrl = String.format(URL_FORMAT, temporaryDatabasePath);

        H2DatabaseUpdater.checkAndPerformMigration(temporaryDatabasePath.toString(), databaseUrl, CREDENTIALS, CREDENTIALS);
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