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
package org.apache.nifi.processors.standard.ftp;


import org.apache.nifi.processors.standard.ftp.filesystem.DefaultVirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestVirtualFileSystem {

    private VirtualFileSystem fileSystem;
    private static final List<VirtualPath> ORIGINAL_DIRECTORY_LIST = Arrays.asList(
            new VirtualPath("/"),
            new VirtualPath("/Directory1"),
            new VirtualPath("/Directory1/SubDirectory1"),
            new VirtualPath("/Directory1/SubDirectory1/SubSubDirectory"),
            new VirtualPath("/Directory1/SubDirectory2"),
            new VirtualPath("/Directory2"),
            new VirtualPath("/Directory2/SubDirectory3"),
            new VirtualPath("/Directory2/SubDirectory4")
    );

    @BeforeEach
    public void setup() {
        setupVirtualDirectoryStructure();
    }

    private void setupVirtualDirectoryStructure() {
        fileSystem = new DefaultVirtualFileSystem();
        for (VirtualPath directory : ORIGINAL_DIRECTORY_LIST) {
            if (!directory.equals(VirtualFileSystem.ROOT)) {
                fileSystem.mkdir(directory);
            }
        }
    }

    @Test
    public void testTryToCreateDirectoryWithNonExistentParents() {
        VirtualPath newDirectory = new VirtualPath("/Directory3/SubDirectory5/SubSubDirectory");

        boolean directoryCreated = fileSystem.mkdir(newDirectory);

        assertFalse(directoryCreated);
        assertAllDirectoriesAre(ORIGINAL_DIRECTORY_LIST);
    }

    @Test
    public void testListContentsOfDirectory() {
        VirtualPath parent = new VirtualPath("/Directory1");
        VirtualPath[] expectedSubDirectories = {
                new VirtualPath("/Directory1/SubDirectory1"),
                new VirtualPath("/Directory1/SubDirectory2")
        };

        List<VirtualPath> subDirectories = fileSystem.listChildren(parent);

        assertTrue(subDirectories.containsAll(Arrays.asList(expectedSubDirectories)));
    }

    @Test
    public void testListContentsOfRoot() {
        VirtualPath parent = new VirtualPath("/");
        VirtualPath[] expectedSubDirectories = {
                new VirtualPath("/Directory1"),
                new VirtualPath("/Directory2")
        };

        List<VirtualPath> subDirectories = fileSystem.listChildren(parent);

        assertTrue(subDirectories.containsAll(Arrays.asList(expectedSubDirectories)));
    }

    @Test
    public void testListContentsOfEmptyDirectory() {
        VirtualPath parent = new VirtualPath("/Directory2/SubDirectory3");

        List<VirtualPath> subDirectories = fileSystem.listChildren(parent);

        assertEquals(0, subDirectories.size());
    }

    @Test
    public void testTryToDeleteNonEmptyDirectory() {
        boolean success = fileSystem.delete(new VirtualPath("/Directory1"));

        assertFalse(success);
        assertAllDirectoriesAre(ORIGINAL_DIRECTORY_LIST);
    }

    @Test
    public void testDeleteEmptyDirectory() {
        List<VirtualPath> expectedRemainingDirectories = Arrays.asList(
                new VirtualPath("/"),
                new VirtualPath("/Directory1"),
                new VirtualPath("/Directory1/SubDirectory1"),
                new VirtualPath("/Directory1/SubDirectory1/SubSubDirectory"),
                new VirtualPath("/Directory1/SubDirectory2"),
                new VirtualPath("/Directory2"),
                new VirtualPath("/Directory2/SubDirectory4")
        );

        boolean success = fileSystem.delete(new VirtualPath("/Directory2/SubDirectory3"));

        assertTrue(success);
        assertAllDirectoriesAre(expectedRemainingDirectories);
    }

    @Test
    public void testDeleteRoot() {
        boolean success = fileSystem.delete(VirtualFileSystem.ROOT);

        assertFalse(success);
        assertAllDirectoriesAre(ORIGINAL_DIRECTORY_LIST);
    }

    @Test
    public void testDeleteNonExistentDirectory() {
        boolean success = fileSystem.delete(new VirtualPath("/Directory3"));

        assertFalse(success);
        assertAllDirectoriesAre(ORIGINAL_DIRECTORY_LIST);
    }

    private void assertAllDirectoriesAre(List<VirtualPath> expectedDirectories) {
        if (expectedDirectories.size() != fileSystem.getTotalNumberOfFiles()) {
            fail();
        } else {
            for (VirtualPath path : expectedDirectories) {
                if (!fileSystem.exists(path)) {
                    fail();
                }
            }
        }
    }

}
