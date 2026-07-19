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

package org.apache.nifi.minifi.c2.command;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.nio.charset.Charset.defaultCharset;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.readAllLines;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpdateAssetCommandHelperTest {

    private static final String[] INVALID_ASSET_FILE_NAMES = new String[]{
            "~",
            "./",
            "../",
            "./../",
            "../sibling-directory",
            "/tmp"
    };

    private static final String ASSET_DIRECTORY = "asset_directory";
    private static final String ASSET_FILE = "asset-file";
    private static final byte[] CONTENT = String.class.getSimpleName().getBytes(StandardCharsets.UTF_8);

    @TempDir
    private File tempDir;

    private Path assetDirectory;
    private UpdateAssetCommandHelper updateAssetCommandHelper;

    @BeforeEach
    void setUp() {
        assetDirectory = Paths.get(tempDir.getAbsolutePath(), ASSET_DIRECTORY);
        updateAssetCommandHelper = new UpdateAssetCommandHelper(assetDirectory.toString());
    }

    @Test
    void testAssetDirectoryShouldBeCreated() {
        updateAssetCommandHelper.createAssetDirectory();

        assertTrue(exists(assetDirectory));
        assertTrue(isDirectory(assetDirectory));
    }

    @Test
    void testAssetFileDoesNotExist() {
        updateAssetCommandHelper.createAssetDirectory();

        boolean result = updateAssetCommandHelper.assetUpdatePrecondition(ASSET_FILE, FALSE);

        assertTrue(result);
    }

    @Test
    void testAssetFileExistsAndForceDownloadOff() {
        updateAssetCommandHelper.createAssetDirectory();
        touchAssetFile();

        boolean result = updateAssetCommandHelper.assetUpdatePrecondition(ASSET_FILE, FALSE);

        assertFalse(result);
    }

    @Test
    void testAssetFileExistsAndForceDownloadOn() {
        updateAssetCommandHelper.createAssetDirectory();
        touchAssetFile();

        boolean result = updateAssetCommandHelper.assetUpdatePrecondition(ASSET_FILE, TRUE);

        assertTrue(result);
    }

    @Test
    void testAssetPersistedCorrectly() throws IOException {
        updateAssetCommandHelper.createAssetDirectory();
        String testAssetContent = "test file content";

        boolean result = updateAssetCommandHelper.assetPersistFunction(ASSET_FILE, testAssetContent.getBytes(defaultCharset()));

        assertTrue(result);
        assertIterableEquals(singletonList(testAssetContent), readAllLines(assetDirectory.resolve(ASSET_FILE), defaultCharset()));
    }

    @Test
    void testAssetDirectoryDoesNotExistWhenPersistingAsset() {
        String testAssetContent = "test file content";

        boolean result = updateAssetCommandHelper.assetPersistFunction(ASSET_FILE, testAssetContent.getBytes(defaultCharset()));

        assertFalse(result);
        assertFalse(exists(assetDirectory.resolve(ASSET_FILE)));
    }

    @ParameterizedTest
    @FieldSource("INVALID_ASSET_FILE_NAMES")
    void testAssetUpdatePreconditionDirectoriesDisallowed(final String assetName) {
        final boolean completed = updateAssetCommandHelper.assetUpdatePrecondition(assetName, true);

        assertFalse(completed);
    }

    @ParameterizedTest
    @FieldSource("INVALID_ASSET_FILE_NAMES")
    void testAssetPersistFunctionDirectoriesDisallowed(final String assetName) {
        final boolean completed = updateAssetCommandHelper.assetPersistFunction(assetName, CONTENT);

        assertFalse(completed);
    }

    private void touchAssetFile() {
        try {
            Files.writeString(Paths.get(assetDirectory.toString(), ASSET_FILE), EMPTY);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to touch file", e);
        }
    }
}
