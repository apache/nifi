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

package org.apache.nifi.kafka.connect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.io.CleanupMode.ALWAYS;

public class WorkingDirectoryUtilsTest {

    @Test
    public void testDeleteNonexistentFile(@TempDir(cleanup = ALWAYS) File tempDir) {
        File nonexistentFile = new File(tempDir, "testFile");

        WorkingDirectoryUtils.purgeDirectory(nonexistentFile);

        assertFalse(nonexistentFile.exists());
    }

    @Test
    public void testDeleteFlatFile(@TempDir(cleanup = ALWAYS) File tempDir) throws IOException {
        File file = new File(tempDir, "testFile");
        file.createNewFile();

        WorkingDirectoryUtils.purgeDirectory(file);

        assertFalse(file.exists());
    }

    @Test
    public void testDeleteDirectoryWithContents(@TempDir(cleanup = ALWAYS) File tempDir) throws IOException {
        File directory = new File(tempDir, "directory");
        File subDirectory = new File(directory, "subDirectory");
        File subDirectoryContent = new File(subDirectory, "subDirectoryContent");
        File directoryContent = new File(directory, "directoryContent");

        directory.mkdir();
        subDirectory.mkdir();
        subDirectoryContent.createNewFile();
        directoryContent.createNewFile();

        WorkingDirectoryUtils.purgeDirectory(directory);

        assertFalse(directory.exists());
    }

    @Test
    public void testPurgeUnpackedNarsEmptyRootDirectory(@TempDir(cleanup = ALWAYS) File tempDir) {
        File rootDirectory = new File(tempDir, "rootDirectory");

        rootDirectory.mkdir();

        WorkingDirectoryUtils.purgeIncompleteUnpackedNars(rootDirectory);

        assertTrue(rootDirectory.exists());
    }

    @Test
    public void testPurgeUnpackedNarsRootDirectoryWithFilesOnly(@TempDir(cleanup = ALWAYS) File tempDir) throws IOException {
        File rootDirectory = new File(tempDir, "rootDirectory");
        File directoryContent1 = new File(rootDirectory, "file1");
        File directoryContent2 = new File(rootDirectory, "file2");

        rootDirectory.mkdir();
        directoryContent1.createNewFile();
        directoryContent2.createNewFile();

        WorkingDirectoryUtils.purgeIncompleteUnpackedNars(rootDirectory);

        assertTrue(rootDirectory.exists() && directoryContent1.exists() && directoryContent2.exists());
    }

    @Test
    public void testPurgeUnpackedNars(@TempDir(cleanup = ALWAYS) File tempDir) throws IOException {
        File rootDirectory = new File(tempDir, "rootDirectory");
        rootDirectory.mkdir();
        TestDirectoryStructure testDirectoryStructure = new TestDirectoryStructure(rootDirectory);

        WorkingDirectoryUtils.purgeIncompleteUnpackedNars(testDirectoryStructure.getRootDirectory());

        assertTrue(testDirectoryStructure.isConsistent());
    }

    @Test
    public void testWorkingDirectoryIntegrityRestored(@TempDir(cleanup = ALWAYS) File tempDir) throws IOException {
        /*
        workingDirectory
            - nar
                - extensions
                    - *TestDirectoryStructure*
                - narDirectory
                - narFile
            - extensions
                - *TestDirectoryStructure*
            - additionalDirectory
            - workingDirectoryFile
         */
        File workingDirectory = new File(tempDir, "workingDirectory");
        File nar = new File(workingDirectory, "nar");
        File narExtensions = new File(nar, "extensions");
        File narDirectory = new File(nar, "narDirectory");
        File narFile = new File(nar, "narFile");
        File extensions = new File(workingDirectory, "extensions");
        File additionalDirectory = new File(workingDirectory, "additionalDirectory");
        File workingDirectoryFile = new File(workingDirectory, "workingDirectoryFile");

        workingDirectory.mkdir();
        nar.mkdir();
        narExtensions.mkdir();
        narDirectory.mkdir();
        narFile.createNewFile();
        extensions.mkdir();
        additionalDirectory.mkdir();
        workingDirectoryFile.createNewFile();

        TestDirectoryStructure narExtensionsStructure = new TestDirectoryStructure(narExtensions);
        TestDirectoryStructure extensionsStructure = new TestDirectoryStructure(extensions);

        WorkingDirectoryUtils.reconcileWorkingDirectory(workingDirectory);

        assertTrue(workingDirectory.exists()
                && nar.exists()
                && narExtensionsStructure.isConsistent()
                && narDirectory.exists()
                && narFile.exists()
                && extensionsStructure.isConsistent()
                && additionalDirectory.exists()
                && workingDirectoryFile.exists()
        );
    }

    private class TestDirectoryStructure {
        /*
            rootDirectory
                - subDirectory1-nar-unpacked
                    - subDirectory1File1
                    - nar-digest
                - subDirectory2
                    - subDirectory2File1
                - subDirectory3-nar-unpacked
                    - subDirectory3Dir1
                        - subDirectory3Dir1File1
                    - subDirectory3File1
                - fileInRoot
         */
        File rootDirectory;
        File subDirectory1;
        File subDirectory2;
        File subDirectory3;
        File fileInRoot;
        File subDirectory1File1;
        File subDirectory1File2;
        File subDirectory2File1;
        File subDirectory3Dir1;
        File subDirectory3File1;
        File subDirectory3Dir1File1;

        public TestDirectoryStructure(final File rootDirectory) throws IOException {
            this.rootDirectory = rootDirectory;
            subDirectory1 = new File(rootDirectory, "subDirectory1-" + WorkingDirectoryUtils.NAR_UNPACKED_SUFFIX);
            subDirectory2 = new File(rootDirectory, "subDirector2");
            subDirectory3 = new File(rootDirectory, "subDirector3-" + WorkingDirectoryUtils.NAR_UNPACKED_SUFFIX);
            fileInRoot = new File(rootDirectory, "fileInRoot");
            subDirectory1File1 = new File(subDirectory1, "subDirectory1File1");
            subDirectory1File2 = new File(subDirectory1, WorkingDirectoryUtils.HASH_FILENAME);
            subDirectory2File1 = new File(subDirectory2, "subDirectory2File1");
            subDirectory3Dir1 = new File(subDirectory3, "subDirectory3Dir1");
            subDirectory3File1 = new File(subDirectory3, "subDirectory3File1");
            subDirectory3Dir1File1 = new File(subDirectory3Dir1, "subDirectory3Dir1File1");

            subDirectory1.mkdir();
            subDirectory2.mkdir();
            subDirectory3.mkdir();
            fileInRoot.createNewFile();
            subDirectory1File1.createNewFile();
            subDirectory1File2.createNewFile();
            subDirectory2File1.createNewFile();
            subDirectory3File1.createNewFile();
            subDirectory3Dir1.mkdir();
            subDirectory3Dir1File1.createNewFile();
        }

        public File getRootDirectory() {
            return rootDirectory;
        }

        /**
         * Checks if all directories ending in 'nar-unpacked' that have a file named 'nar-digest' within still exist,
         * and the directory ending in 'nar-unpacked' without 'nar-digest' has been removed with all of its contents.
         * @return true if the above is met.
         */
        public boolean isConsistent() {
            return (rootDirectory.exists()
                    && subDirectory1.exists() && subDirectory1File1.exists() && subDirectory1File2.exists()
                    && subDirectory2.exists() && subDirectory2File1.exists()
                    && !(subDirectory3.exists() || subDirectory3Dir1.exists() || subDirectory3File1.exists() || subDirectory3Dir1File1.exists())
                    && fileInRoot.exists());
        }
    }

}
