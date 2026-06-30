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
package org.apache.nifi.util.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestFileUtils {

    @Test
    public void testGetSanitizedFilenameNullAndEmpty() {
        assertNull(FileUtils.getSanitizedFilename(null));
        assertEquals("", FileUtils.getSanitizedFilename(""));
    }

    @Test
    public void testGetSanitizedFilenameReplacesInvalidCharacters() {
        assertEquals("a_b_c", FileUtils.getSanitizedFilename("a/b\\c"));
        assertEquals("name_", FileUtils.getSanitizedFilename("name:"));
        assertEquals("a_b", FileUtils.getSanitizedFilename("a\tb"));
        assertEquals("_", FileUtils.getSanitizedFilename("*"));
    }

    @Test
    public void testGetSanitizedFilenamePreservesSpaces() {
        assertEquals("driver (1).jar", FileUtils.getSanitizedFilename("driver (1).jar"));
        assertEquals("my report.txt", FileUtils.getSanitizedFilename("my report.txt"));
        assertEquals("driver   (1).jar", FileUtils.getSanitizedFilename("driver   (1).jar"));
        assertEquals(" driver (1).jar ", FileUtils.getSanitizedFilename(" driver (1).jar "));
    }

    @Test
    public void testGetSanitizedFilenamePreservesDots() {
        assertEquals("report...", FileUtils.getSanitizedFilename("report..."));
        assertEquals(".env", FileUtils.getSanitizedFilename(".env"));
    }
}
