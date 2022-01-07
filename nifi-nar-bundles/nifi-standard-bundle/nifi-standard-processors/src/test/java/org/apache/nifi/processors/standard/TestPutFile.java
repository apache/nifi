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
package org.apache.nifi.processors.standard;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPutFile {

    private static final String TARGET_DIRECTORY = "target/put-file";
    private static final String ARCHIVE_DIRECTORY = "target/put-file-archive";
    private static final String TARGET_FILENAME = "targetFile.txt";
    private File targetDir;
    private File archiveDir;

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void prepDestDirectory() throws IOException {
        targetDir = setupDirectory(TARGET_DIRECTORY);
        archiveDir = setupDirectory(ARCHIVE_DIRECTORY);
    }

    private File setupDirectory(String directoryName) throws IOException {
        File targetDir = new File(directoryName);
        if (!targetDir.exists()) {
            Files.createDirectories(targetDir.toPath());
            return targetDir;
        }

        targetDir.setReadable(true);
        deleteDirectoryContent(targetDir);

        return targetDir;
    }

    private void deleteDirectoryContent(File directory) throws IOException {
        for (final File file : directory.listFiles()) {
            if (file.isDirectory()) {
                deleteDirectoryContent(file);
            }
            Files.delete(file.toPath());
        }
    }

    @Test
    public void testCreateDirectory() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        String newDir = targetDir.getAbsolutePath()+"/new-folder";
        runner.setProperty(PutFile.DIRECTORY, newDir);
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(newDir, TARGET_FILENAME, "Hello world!!"));
    }

    @Test
    public void testCreateRelativeDirectory() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        String newDir = TARGET_DIRECTORY + "/new-folder";
        runner.setProperty(PutFile.DIRECTORY, newDir);
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("Hello world!!".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(newDir, TARGET_FILENAME, "Hello world!!"));
    }

    @Test
    public void testCreateEmptyStringDirectory() {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        String newDir = "";
        runner.setProperty(PutFile.DIRECTORY, newDir);
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);
        runner.assertNotValid();

        runner.setProperty(PutFile.DIRECTORY, "not-empty");
        runner.assertValid();
    }

    @Test
    public void testReplaceConflictResolution() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));

        //Second file
        runner.enqueue("Second file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "Second file"));
    }

    @Test
    public void testIgnoreConflictResolution() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.IGNORE_RESOLUTION);

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));

        //Second file
        runner.enqueue("Second file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));
    }

    @Test
    public void testFailConflictResolution() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.FAIL_RESOLUTION);

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));

        //Second file
        runner.enqueue("Second file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(PutFile.REL_SUCCESS, 1);
        runner.assertTransferCount(PutFile.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
        // Confirm file was not overwritten
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));
    }

    @Test
    public void testMaxFileLimitReach() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);
        runner.setProperty(PutFile.MAX_DESTINATION_FILES, "1");

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));

        //Second file
        attributes.put(CoreAttributes.FILENAME.key(), "secondFile.txt");
        runner.enqueue("Second file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(PutFile.REL_FAILURE, 1);
        runner.assertPenalizeCount(1);
        // Confirm file was not overwritten; verifyOutput also confirms there is only 1 file in TARGET_DIRECTORY
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));
    }

    @Test
    public void testReplaceAndMaxFileLimitReach() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.REPLACE_RESOLUTION);
        runner.setProperty(PutFile.MAX_DESTINATION_FILES, "1");

        Map<String,String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "First file"));

        //Second file
        runner.enqueue("Second file".getBytes(), attributes);
        runner.run();
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME, "Second file"));
    }

    @Test
    public void testArchiveFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.ARCHIVE_RESOLUTION);
        // Not valid yet because archive directory has not been configured
        runner.assertNotValid();

        runner.setProperty(PutFile.ARCHIVE_DIRECTORY, archiveDir.getAbsolutePath());
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.enqueue("Second file".getBytes(), attributes);
        runner.run(2);
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);

        // Check first file now in archive directory
        assertTrue(verifyOutput(ARCHIVE_DIRECTORY, TARGET_FILENAME, "First file"));

        // Check second file now in target directory
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "Second file"));
    }

    @Test
    public void testArchiveFileWithExtension() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.ARCHIVE_RESOLUTION);
        runner.setProperty(PutFile.ARCHIVE_DIRECTORY, archiveDir.getAbsolutePath());
        // use expression language evaluation
        runner.setProperty(PutFile.ARCHIVE_FILENAME_EXTENSION, "${archive.extension}");
        runner.setProperty(PutFile.GUARANTEE_UNIQUE_ARCHIVE_FILENAME, "false");
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        // required attribute for expression language evaluation
        attributes.put("archive.extension", ".arch");
        runner.enqueue("First file".getBytes(), attributes);
        runner.enqueue("Second file".getBytes(), attributes);
        runner.enqueue("Third file".getBytes(), attributes);
        runner.run(3);

        // Third file fails because filename already exists in archive directory and 'Guarantee Archive Filename Uniqueness' is false
        runner.assertTransferCount(FetchFile.REL_FAILURE, 1);
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);

        // Check first file now in archive directory
        assertTrue(verifyOutput(ARCHIVE_DIRECTORY, TARGET_FILENAME + ".arch", "First file"));

        // Check second file now in target directory
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "Second file"));
    }

    @Test
    public void testArchiveFileWhenArchiveFileExistsAndUniqueGuarantee() throws IOException {
        // PutFile.GUARANTEE_UNIQUE_ARCHIVE_FILENAME is 'true' by default
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.ARCHIVE_RESOLUTION);
        runner.setProperty(PutFile.ARCHIVE_DIRECTORY, archiveDir.getAbsolutePath());
        runner.setProperty(PutFile.ARCHIVE_FILENAME_EXTENSION, ".arch");
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.enqueue("Second file".getBytes(), attributes);
        runner.enqueue("Third file".getBytes(), attributes);
        runner.run(3);

        // Third file adds UUID to filename because filename already exists in archive directory
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 3);

        // Check that the first two files now in archive directory; both have ".arch" extension, and the second file archived has UUID appended
        String[] dirList = new File(ARCHIVE_DIRECTORY).list();
        assertNotNull(dirList);
        assertEquals(2, dirList.length);

        String uuidPattern = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
        // Check the first two files, both now in the archive directory
        for (String actualFilename : dirList) {
            Path path = Paths.get(String.join("/", ARCHIVE_DIRECTORY, actualFilename));
            String actualContent = new String(Files.readAllBytes(path));
            boolean filenameIncludesUUID = Pattern.matches(TARGET_FILENAME + ".arch-" + uuidPattern, actualFilename);
            if (filenameIncludesUUID) {
                // the filename including the UUID is the second file to reach the archive directory, or equivalently the second file processed in this unit test
                assertEquals("Second file", actualContent);
            } else {
                // the filename without the UUID is the first file to reach the archive directory, or equivalently the first file processed in this unit test
                assertEquals(TARGET_FILENAME + ".arch", actualFilename);
                assertEquals("First file", actualContent);
            }
        }

        // Check third file now in target directory
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "Third file"));
    }

    @Test
    public void testArchiveDirDoesNotExist() throws IOException {
        // ensure archive directory does not exist (remove it if necessary)
        removeDirectoryNonRecursive("target/put-file-archive-dir-does-not-exist");
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.ARCHIVE_RESOLUTION);
        runner.setProperty(PutFile.CREATE_DIRS, "false");
        runner.setProperty(PutFile.ARCHIVE_DIRECTORY, "target/put-file-archive-dir-does-not-exist");
        runner.assertValid();

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.run();

        // First file succeeds because archive directory is not needed (filename does not exist in target directory)
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 1);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "First file"));

        runner.enqueue("Second file".getBytes(), attributes);
        runner.run();

        // Second file fails because archive directory is needed (filename exists in target directory) and 'Create Missing Directories' is set to false
        runner.assertTransferCount(FetchFile.REL_FAILURE, 1);
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 1);
        // Check first file still in target directory
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "First file"));

        // Allow archive directory to be created
        runner.setProperty(PutFile.CREATE_DIRS, "true");
        runner.enqueue("Third file".getBytes(), attributes);
        runner.run();

        // Check first file now in archive directory and third file now in target directory
        runner.assertTransferCount(FetchFile.REL_FAILURE, 1);
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "Third file"));
        assertTrue(verifyOutput("target/put-file-archive-dir-does-not-exist", TARGET_FILENAME,  "First file"));

        // Cleanup by removing the directory that should not exist
        removeDirectoryNonRecursive("target/put-file-archive-dir-does-not-exist");
    }

    @Test
    public void testArchiveDirMaxFileLimit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new PutFile());
        runner.setProperty(PutFile.DIRECTORY, targetDir.getAbsolutePath());
        runner.setProperty(PutFile.CONFLICT_RESOLUTION, PutFile.ARCHIVE_RESOLUTION);
        runner.setProperty(PutFile.ARCHIVE_DIRECTORY, archiveDir.getAbsolutePath());
        runner.setProperty(PutFile.MAX_ARCHIVE_FILES, "1");

        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TARGET_FILENAME);
        runner.enqueue("First file".getBytes(), attributes);
        runner.enqueue("Second file".getBytes(), attributes);
        runner.enqueue("Third file".getBytes(), attributes);
        runner.run(3);

        // Third file fails because archive directory is full
        runner.assertTransferCount(FetchFile.REL_FAILURE, 1);
        runner.assertTransferCount(FetchFile.REL_SUCCESS, 2);

        // Check first file now in archive directory
        assertTrue(verifyOutput(ARCHIVE_DIRECTORY, TARGET_FILENAME, "First file"));

        // Check second file now in target directory
        assertTrue(verifyOutput(TARGET_DIRECTORY, TARGET_FILENAME,  "Second file"));
    }

    private TestRunner putFileRunner;

    private final String testFile = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "hello.txt";

    @Before
    public void setup() {

        putFileRunner = TestRunners.newTestRunner(PutFile.class);
        putFileRunner.setProperty(PutFile.CHANGE_OWNER, System.getProperty("user.name"));
        putFileRunner.setProperty(PutFile.CHANGE_PERMISSIONS, "rw-r-----");
        putFileRunner.setProperty(PutFile.CREATE_DIRS, "true");
        putFileRunner.setProperty(PutFile.DIRECTORY, "target/test/data/out/PutFile/1/2/3/4/5");

        putFileRunner.setValidateExpressionUsage(false);
    }

    @After
    public void tearDown() throws IOException {
        emptyTestDirectory();
    }

    @Test
    public void testPutFile() throws IOException {
        emptyTestDirectory();

        Map<String,String> attributes = new HashMap<>();
        attributes.put("filename", "testfile.txt");

        putFileRunner.enqueue(Paths.get(testFile), attributes);
        putFileRunner.run();

        putFileRunner.assertTransferCount(PutSFTP.REL_SUCCESS, 1);

        //verify directory exists
        Path newDirectory = Paths.get("target/test/data/out/PutFile/1/2/3/4/5");
        Path newFile = newDirectory.resolve("testfile.txt");
        assertTrue("New directory not created.", newDirectory.toAbsolutePath().toFile().exists());
        assertTrue("New File not created.", newFile.toAbsolutePath().toFile().exists());

        PosixFileAttributeView filePosixAttributeView = Files.getFileAttributeView(newFile.toAbsolutePath(), PosixFileAttributeView.class);
        assertEquals(System.getProperty("user.name"), filePosixAttributeView.getOwner().getName());
        Set<PosixFilePermission> filePermissions = filePosixAttributeView.readAttributes().permissions();
        assertTrue(filePermissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(filePermissions.contains(PosixFilePermission.OWNER_WRITE));
        assertFalse(filePermissions.contains(PosixFilePermission.OWNER_EXECUTE));
        assertTrue(filePermissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(filePermissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(filePermissions.contains(PosixFilePermission.GROUP_EXECUTE));
        assertFalse(filePermissions.contains(PosixFilePermission.OTHERS_READ));
        assertFalse(filePermissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertFalse(filePermissions.contains(PosixFilePermission.OTHERS_EXECUTE));

        PosixFileAttributeView dirPosixAttributeView = Files.getFileAttributeView(newDirectory.toAbsolutePath(), PosixFileAttributeView.class);
        assertEquals(System.getProperty("user.name"), dirPosixAttributeView.getOwner().getName());
        Set<PosixFilePermission> dirPermissions = dirPosixAttributeView.readAttributes().permissions();
        assertTrue(dirPermissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(dirPermissions.contains(PosixFilePermission.OWNER_WRITE));
        assertTrue(dirPermissions.contains(PosixFilePermission.OWNER_EXECUTE));
        assertTrue(dirPermissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(dirPermissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(dirPermissions.contains(PosixFilePermission.GROUP_EXECUTE));
        assertFalse(dirPermissions.contains(PosixFilePermission.OTHERS_READ));
        assertFalse(dirPermissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertFalse(dirPermissions.contains(PosixFilePermission.OTHERS_EXECUTE));

        putFileRunner.clearTransferState();
    }


    private void emptyTestDirectory() throws IOException {
        Files.walkFileTree(Paths.get("target/test/data/out/PutFile"), new FileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void removeDirectoryNonRecursive(String directoryName) throws IOException {
        // This method does not recurse subdirectories
        if (Files.exists(Paths.get(directoryName))) {
            FileUtils.deleteFilesInDir(new File(directoryName), null, null);
            Files.delete(Paths.get(directoryName));
        }
    }

    /**
     * This method verifies the following:
     * 1) target directory contains exactly 1 file
     * 2) expected name of the file in the target directory
     * 3) contents of the file in the target directory
     *
     * @param directoryName   name of the target directory
     * @param filename        name of the file in the target directory
     * @param expectedContent content of the file in the target directory
     * @return true if the above conditions are true
     * @throws IOException when there is a failure accessing a file
     */
    private boolean verifyOutput(final String directoryName, final String filename, final String expectedContent) throws IOException {
        File directory = new File(directoryName);
        String[] dirList = directory.list();
        if (dirList == null) {
            fail("Directory is null; expected directory to exist and contain 1 file: " + directoryName);
            return false;
        }
        assertEquals(1, dirList.length);
        Path path = Paths.get(String.join("/", directoryName, filename));
        if (!Files.exists(path)) {
            fail("File does not exist in expected location: " + directoryName + "/" + filename);
            return false;
        }
        String actualContent = new String(Files.readAllBytes(path));
        assertEquals(expectedContent, actualContent);
        return true;
    }
}
