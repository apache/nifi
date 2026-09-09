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

package org.apache.nifi.registry.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFileUtils {
    @Test
    public void testSanitizeFilename() {
        String filename = "This / is / a test";
        final String sanitizedFilename = FileUtils.sanitizeFilename(filename);
        assertEquals("This___is___a_test", sanitizedFilename);
    }

    @Test
    public void testGetChildLocationAcceptsContainedPath(@TempDir final Path tempDir) {
        final File parentDir = tempDir.toFile();
        final File child = FileUtils.getChildLocation(parentDir, Paths.get("bucket", "group", "artifact"));
        final Path parentPath = parentDir.toPath().toAbsolutePath().normalize();
        final Path childPath = child.toPath().toAbsolutePath().normalize();
        assertTrue(childPath.startsWith(parentPath));
        assertEquals(parentPath.resolve(Paths.get("bucket", "group", "artifact")), childPath);
    }

    @Test
    public void testGetChildLocationRejectsEscapeAndIdentity(@TempDir final Path tempDir) {
        final File parentDir = tempDir.toFile();
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, Paths.get("..")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, Paths.get("..", "1.0.0")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, Paths.get("..", "..")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, Paths.get(".")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, Paths.get("")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, tempDir.resolve("other")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(null, Paths.get("child")));
        assertThrows(IllegalArgumentException.class, () -> FileUtils.getChildLocation(parentDir, null));
    }
}
