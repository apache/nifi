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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.nio.charset.Charset.defaultCharset;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.Files.write;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class UpdateAssetCommandHelperTest {

    private static final String ASSET_DIRECTORY = "asset_directory";
    private static final String ASSET_FILE = "asset.file";

    @TempDir
    private File tempDir;

    private Path assetDirectory;
    private UpdateAssetCommandHelper updateAssetCommandHelper;

    @BeforeEach
    public void setUp() {
        assetDirectory = Paths.get(tempDir.getAbsolutePath(), ASSET_DIRECTORY);
        updateAssetCommandHelper = new UpdateAssetCommandHelper(assetDirectory.toString());
    }

    @Test
    public void testAssetDirectoryShouldBeCreated() {
        // given + when
        updateAssetCommandHelper.createAssetDirectory();

        // then
        assertTrue(exists(assetDirectory));
        assertTrue(isDirectory(assetDirectory));
    }

    @Test
    public void testAssetFileDoesNotExist() {
        // given
        updateAssetCommandHelper.createAssetDirectory();

        // when
        boolean result = updateAssetCommandHelper.assetUpdatePrecondition(ASSET_FILE, FALSE);

        // then
        assertTrue(result);
    }

    @Test
    public void testAssetFileExistsAndForceDownloadOff() {
        // given
        updateAssetCommandHelper.createAssetDirectory();
        touchAssetFile();

        // when
        boolean result = updateAssetCommandHelper.assetUpdatePrecondition(ASSET_FILE, FALSE);

        // then
        assertFalse(result);
    }

    @Test
    public void testAssetFileExistsAndForceDownloadOn() {
        // given
        updateAssetCommandHelper.createAssetDirectory();
        touchAssetFile();

        // when
        boolean result = updateAssetCommandHelper.assetUpdatePrecondition(ASSET_FILE, TRUE);

        // then
        assertTrue(result);
    }

    @Test
    public void testAssetPersistedCorrectly() throws IOException {
        // given
        updateAssetCommandHelper.createAssetDirectory();
        String testAssetContent = "test file content";

        // when
        boolean result = updateAssetCommandHelper.assetPersistFunction(ASSET_FILE, testAssetContent.getBytes(defaultCharset()));

        // then
        assertTrue(result);
        assertIterableEquals(singletonList(testAssetContent), readAllLines(assetDirectory.resolve(ASSET_FILE), defaultCharset()));
    }

    @Test
    public void testAssetDirectoryDoesNotExistWhenPersistingAsset() {
        // given
        String testAssetContent = "test file content";

        // when
        boolean result = updateAssetCommandHelper.assetPersistFunction(ASSET_FILE, testAssetContent.getBytes(defaultCharset()));

        // then
        assertFalse(result);
        assertFalse(exists(assetDirectory.resolve(ASSET_FILE)));
    }

    private void touchAssetFile() {
        try {
            write(Paths.get(assetDirectory.toString(), ASSET_FILE), EMPTY.getBytes(UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to touch file", e);
        }
    }
}
