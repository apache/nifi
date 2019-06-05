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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestGetFile {

    @Test
    public void testWithInaccessibleDir() throws IOException {
        // Some systems don't support POSIX (Windows) and will fail if run. Should ignore the test in that event
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return;
        }
        File inaccessibleDir = new File("target/inaccessible");
        inaccessibleDir.deleteOnExit();
        inaccessibleDir.mkdir();
        Set<PosixFilePermission> posixFilePermissions = new HashSet<>();
        Files.setPosixFilePermissions(inaccessibleDir.toPath(), posixFilePermissions);
        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, inaccessibleDir.getAbsolutePath());
        try {
            runner.run();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getCause().getMessage()
                    .endsWith("does not have sufficient permissions (i.e., not writable and readable)"));
        }
    }

    @Test
    public void testWithUnreadableDir() throws IOException {
        // Some systems don't support POSIX (Windows) and will fail if run. Should ignore the test in that event
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return;
        }
        File unreadableDir = new File("target/unreadable");
        unreadableDir.deleteOnExit();
        unreadableDir.mkdir();
        Set<PosixFilePermission> posixFilePermissions = new HashSet<>();
        posixFilePermissions.add(PosixFilePermission.GROUP_EXECUTE);
        posixFilePermissions.add(PosixFilePermission.GROUP_WRITE);
        posixFilePermissions.add(PosixFilePermission.OTHERS_EXECUTE);
        posixFilePermissions.add(PosixFilePermission.OTHERS_WRITE);
        posixFilePermissions.add(PosixFilePermission.OWNER_EXECUTE);
        posixFilePermissions.add(PosixFilePermission.OWNER_WRITE);
        Files.setPosixFilePermissions(unreadableDir.toPath(), posixFilePermissions);
        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, unreadableDir.getAbsolutePath());
        try {
            runner.run();
            fail();
        } catch (AssertionError e) {
            Throwable ex = e.getCause();
            assertTrue(e.getCause().getMessage()
                    .endsWith("does not have sufficient permissions (i.e., not writable and readable)"));
        }
    }

    @Test
    public void testWithUnwritableDir() throws IOException {
        // Some systems don't support POSIX (Windows) and will fail if run. Should ignore the test in that event
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return;
        }
        File unwritableDir = new File("target/unwritable");
        unwritableDir.deleteOnExit();
        unwritableDir.mkdir();
        Set<PosixFilePermission> posixFilePermissions = new HashSet<>();
        posixFilePermissions.add(PosixFilePermission.GROUP_EXECUTE);
        posixFilePermissions.add(PosixFilePermission.GROUP_READ);
        posixFilePermissions.add(PosixFilePermission.OTHERS_EXECUTE);
        posixFilePermissions.add(PosixFilePermission.OTHERS_READ);
        posixFilePermissions.add(PosixFilePermission.OWNER_EXECUTE);
        posixFilePermissions.add(PosixFilePermission.OWNER_READ);
        Files.setPosixFilePermissions(unwritableDir.toPath(), posixFilePermissions);
        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, unwritableDir.getAbsolutePath());
        try {
            runner.run();
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getCause().getMessage()
                    .endsWith("does not have sufficient permissions (i.e., not writable and readable)"));
        }
    }

    @Test
    public void testFilePickedUp() throws IOException {
        final File directory = new File("target/test/data/in");
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        final Path absTargetPath = targetPath.toAbsolutePath();
        final String absTargetPathStr = absTargetPath.getParent() + "/";
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, directory.getAbsolutePath());
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals("/", path);
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
        assertEquals(absTargetPathStr, absolutePath);
    }

    private void deleteDirectory(final File directory) throws IOException {
        if (directory != null && directory.exists()) {
            for (final File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                }

                assertTrue("Could not delete " + file.getAbsolutePath(), file.delete());
            }
        }
    }

    @Test
    public void testTodaysFilesPickedUp() throws IOException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.US);
        final String dirStruc = sdf.format(new Date());

        final File directory = new File("target/test/data/in/" + dirStruc);
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in/${now():format('yyyy/MM/dd')}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));
    }

    @Test
    public void testMinAgeFilesPickedUp() throws IOException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.US);
        final String dirStruc = sdf.format(new Date());

        final File directory = new File("target/test/data/in/" + dirStruc);
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        // Because it's a file copy, initial modified date is set to the original files. Update to today
        Calendar c = Calendar.getInstance();
        Files.setAttribute(targetPath, "lastModifiedTime", FileTime.fromMillis(c.getTimeInMillis()));

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        // Keep source file to simplify age testing
        runner.setProperty(GetFile.KEEP_SOURCE_FILE, "true");
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in/${now():format('yyyy/MM/dd')}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));

        runner.clearTransferState();

        // Set Minimum age to 20 hours, verify no file picked up
        runner.setProperty(GetFile.MIN_AGE, "20 hr");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 0);

        runner.clearTransferState();

        //Change File create/modified time, check again
        c.add(Calendar.DAY_OF_YEAR, -1);
        final FileTime oneDayAgo = FileTime.fromMillis(c.getTimeInMillis());
        Files.setAttribute(targetPath, "lastModifiedTime", oneDayAgo);

        runner.run();
        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);

        runner.clearTransferState();
    }

    @Test
    public void testMinAgeWithFutureDateFiles() throws IOException, InterruptedException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.US);
        final String dirStruc = sdf.format(new Date());

        final File directory = new File("target/test/data/in/" + dirStruc);
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        // Keep source file to simplify age testing
        runner.setProperty(GetFile.KEEP_SOURCE_FILE, "true");
        // Set min age to 100 msec
        runner.setProperty(GetFile.MIN_AGE, "100 millis");
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in/${now():format('yyyy/MM/dd')}");
        // Wait 200 msec so file is old enough
        Thread.sleep(200);
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));

        runner.clearTransferState();

        //Change File create/modified time, check again. Should be excluded because it's in the future
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_YEAR, 1);
        final FileTime oneDayAgo = FileTime.fromMillis(c.getTimeInMillis());
        Files.setAttribute(targetPath, "lastModifiedTime", oneDayAgo);

        runner.run();
        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 0);

        runner.clearTransferState();

        // Clear Min Age, should find the file now
        runner.setProperty(GetFile.MIN_AGE, "0 sec");
        runner.run();
        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);

        runner.clearTransferState();
    }

    @Test
    public void testPath() throws IOException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/", Locale.US);
        final String dirStruc = sdf.format(new Date());

        final File directory = new File("target/test/data/in/" + dirStruc);
        deleteDirectory(new File("target/test/data/in"));
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        final Path absTargetPath = targetPath.toAbsolutePath();
        final String absTargetPathStr = absTargetPath.getParent().toString() + "/";
        Files.copy(inPath, targetPath);

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);
        successFiles.get(0).assertContentEquals("Hello, World!".getBytes("UTF-8"));

        final String path = successFiles.get(0).getAttribute("path");
        assertEquals(dirStruc, path.replace('\\', '/'));
        final String absolutePath = successFiles.get(0).getAttribute(CoreAttributes.ABSOLUTE_PATH.key());
        assertEquals(absTargetPathStr, absolutePath);
    }

    @Test
    public void testAttributes() throws IOException {
        final File directory = new File("target/test/data/in/");
        deleteDirectory(directory);
        assertTrue("Unable to create test data directory " + directory.getAbsolutePath(), directory.exists() || directory.mkdirs());

        final File inFile = new File("src/test/resources/hello.txt");
        final Path inPath = inFile.toPath();
        final File destFile = new File(directory, inFile.getName());
        final Path targetPath = destFile.toPath();
        Files.copy(inPath, targetPath);

        boolean verifyLastModified = false;
        try {
            destFile.setLastModified(1000000000);
            verifyLastModified = true;
        } catch (Exception doNothing) {
        }

        boolean verifyPermissions = false;
        try {
            /* If you mount an NTFS partition in Linux, you are unable to change the permissions of the files,
            * because every file has the same permissions, controlled by the 'fmask' and 'dmask' mount options.
            * Executing a chmod command will not fail, but it does not change the file's permissions.
            * From Java perspective the NTFS mount point, as a FileStore supports the 'unix' and 'posix' file
            * attribute views, but the setPosixFilePermissions() has no effect.
            *
            * If you set verifyPermissions to true without the following extra check, the test case will fail
            * on a file system, where Nifi source is located on a NTFS mount point in Linux.
            * The purpose of the extra check is to ensure, that setPosixFilePermissions() changes the file's
            * permissions, and set verifyPermissions, after we are convinced.
            */
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("r--r-----");
            Files.setPosixFilePermissions(targetPath, perms);
            Set<PosixFilePermission> permsAfterSet = Files.getPosixFilePermissions(targetPath);
            if (perms.equals(permsAfterSet)) {
                verifyPermissions = true;
            }

        } catch (Exception doNothing) {
        }

        final TestRunner runner = TestRunners.newTestRunner(new GetFile());
        runner.setProperty(GetFile.DIRECTORY, "target/test/data/in");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetFile.REL_SUCCESS, 1);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(GetFile.REL_SUCCESS);

        if (verifyLastModified) {
            try {
                final DateFormat formatter = new SimpleDateFormat(GetFile.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                final Date fileModifyTime = formatter.parse(successFiles.get(0).getAttribute("file.lastModifiedTime"));
                assertEquals(new Date(1000000000), fileModifyTime);
            } catch (ParseException e) {
                fail();
            }
        }
        if (verifyPermissions) {
            successFiles.get(0).assertAttributeEquals("file.permissions", "r--r-----");
        }
    }
}
