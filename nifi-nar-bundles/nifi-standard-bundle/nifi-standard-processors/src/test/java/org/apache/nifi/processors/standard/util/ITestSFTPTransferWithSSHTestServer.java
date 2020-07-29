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
package org.apache.nifi.processors.standard.util;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockPropertyContext;
import org.apache.nifi.util.file.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ITestSFTPTransferWithSSHTestServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ITestSFTPTransferWithSSHTestServer.class);

    private static final String SFTP_ROOT_DIR = "target/test-sftp-transfer-vfs";

    private static final String DIR_1 = "dir1";
    private static final String DIR_2 = "dir2";
    private static final String DIR_3 = "dir3";
    private static final String DIR_4 = "dir4";

    private static final String DIR_1_CHILD_1 = "child1";
    private static final String DIR_1_CHILD_2 = "child2";

    private static final String FILE_1 = "file1.txt";
    private static final String FILE_2 = "file2.txt";
    private static final String DOT_FILE = ".foo.txt";

    private static SSHTestServer sshTestServer;

    @BeforeClass
    public static void setupClass() throws IOException {
        sshTestServer = new SSHTestServer();
        sshTestServer.setVirtualFileSystemPath(SFTP_ROOT_DIR);
        sshTestServer.startServer();
    }

    @AfterClass
    public static void cleanupClass() throws IOException {
        sshTestServer.stopServer();
    }

    @Before
    public void setupFiles() throws IOException {
        final File sftpRootDir = new File(SFTP_ROOT_DIR);
        FileUtils.deleteFilesInDir(sftpRootDir, null, LOGGER, true, true);

        // create and initialize dir1/child1
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_1 + "/" + DIR_1_CHILD_1, FILE_1, "dir1 child1 file1");
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_1 + "/" + DIR_1_CHILD_1, FILE_2, "dir1 child1 file2");
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_1 + "/" + DIR_1_CHILD_1, DOT_FILE, "dir1 child1 foo");

        // create and initialize dir1/child2
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_1 + "/" + DIR_1_CHILD_2, FILE_1, "dir1 child2 file1");
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_1 + "/" + DIR_1_CHILD_2, FILE_2, "dir1 child2 file2");
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_1 + "/" + DIR_1_CHILD_2, DOT_FILE, "dir1 child2 foo");

        // create and initialize dir2
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_2, FILE_1, "dir2 file1");
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_2, FILE_2, "dir2 file2");
        initializeFile(SFTP_ROOT_DIR + "/" + DIR_2, DOT_FILE, "dir2 foo");

        // Create a symbolic link so that dir3/dir1 links to dir1 so we can test following links
        final Path targetPath = Paths.get("../" + DIR_1);

        final String dir3Path = SFTP_ROOT_DIR + "/" + DIR_3;
        FileUtils.ensureDirectoryExistAndCanAccess(new File(dir3Path));
        final Path linkPath = Paths.get(dir3Path + "/" + DIR_1);

        Files.createSymbolicLink(linkPath, targetPath);

        // create dir4 for writing files
        final File dir4File = new File(SFTP_ROOT_DIR + "/" + DIR_4);
        FileUtils.ensureDirectoryExistAndCanAccess(dir4File);
    }

    private void initializeFile(final String path, final String filename, final String content) throws IOException {
        final File parent = new File(path);
        if (!parent.exists()) {
            assertTrue("Failed to create parent directory: " + path, parent.mkdirs());
        }

        final File file = new File(parent, filename);
        try (final OutputStream out = new FileOutputStream(file);
             final Writer writer = new OutputStreamWriter(out)) {
            writer.write(content);
            writer.flush();
        }
    }

    @Test
    public void testGetListingSimple() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            final FileInfo file1Info = listing.stream().filter(f -> f.getFileName().equals(FILE_1)).findFirst().orElse(null);
            assertNotNull(file1Info);
            assertFalse(file1Info.isDirectory());
            assertEquals("rw-r--r--", file1Info.getPermissions());

            final FileInfo file2Info = listing.stream().filter(f -> f.getFileName().equals(FILE_2)).findFirst().orElse(null);
            assertNotNull(file2Info);
            assertFalse(file2Info.isDirectory());
            assertEquals("rw-r--r--", file2Info.getPermissions());
        }
    }

    @Test
    public void testGetListingSimpleWithDotFiles() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);
        properties.put(SFTPTransfer.IGNORE_DOTTED_FILES, "false");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(3, listing.size());

            final FileInfo dotFileInfo = listing.stream().filter(f -> f.getFileName().equals(DOT_FILE)).findFirst().orElse(null);
            assertNotNull(dotFileInfo);
        }
    }

    @Test
    public void testGetListingWithoutRecursiveSearch() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_1);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "false");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testGetListingWithRecursiveSearch() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_1);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(4, listing.size());
        }
    }

    @Test
    public void testGetListingWithoutSymlinks() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_3);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");
        properties.put(SFTPTransfer.FOLLOW_SYMLINK, "false");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testGetListingWithSymlinks() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_3);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");
        properties.put(SFTPTransfer.FOLLOW_SYMLINK, "true");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(4, listing.size());
        }
    }

    @Test
    public void testGetListingWithBatchSize() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_1);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");

        // first listing is without batch size and shows 4 results
        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(4, listing.size());
        }

        // set a batch size of 2 and ensure we get 2 results
        properties.put(SFTPTransfer.REMOTE_POLL_BATCH_SIZE, "2");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());
        }
    }

    @Test
    public void testGetListingWithFileFilter() throws IOException {
        final String fileFilterRegex = "file1.*";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_1);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");
        properties.put(SFTPTransfer.FILE_FILTER_REGEX, fileFilterRegex);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            listing.forEach(f -> assertTrue(f.getFileName().matches(fileFilterRegex)));
        }
    }

    @Test
    public void testGetListingWithPathFilter() throws IOException {
        final String remotePath = ".";
        final String pathFilterRegex = "dir1/child1";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");
        properties.put(SFTPTransfer.PATH_FILTER_REGEX, pathFilterRegex);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            // a listing will have fullPathFileName like "./dir1/child1/file1.txt" so to verify the path pattern
            // we need to remove the file part and relativize based on the remote path to get "dir1/child1"
            listing.forEach(f -> {
                final String filename = f.getFileName();
                final String path = f.getFullPathFileName().replace(filename, "");

                final Path fullPath = Paths.get(path);
                final Path relPath = Paths.get(remotePath).relativize(fullPath);
                assertTrue(relPath.toString().matches(pathFilterRegex));
            });
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetListingWhenRemotePathDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, "DOES-NOT-EXIST");
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            transfer.getListing();
        }
    }

    @Test
    public void testDeleteFileWithoutPath() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory has two files
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            // issue deletes for the two files
            for (final FileInfo fileInfo : listing) {
                transfer.deleteFile(null, null, fileInfo.getFullPathFileName());
            }

            // verify there are now zero files
            final List<FileInfo> listingAfterDelete = transfer.getListing();
            assertNotNull(listingAfterDelete);
            assertEquals(0, listingAfterDelete.size());
        }
    }

    @Test
    public void testDeleteFileWithPath() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory has two files
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            // issue deletes for the two files
            for (final FileInfo fileInfo : listing) {
                final String filename = fileInfo.getFileName();
                final String path = fileInfo.getFullPathFileName().replace(filename, "");
                transfer.deleteFile(null, path, filename);
            }

            // verify there are now zero files
            final List<FileInfo> listingAfterDelete = transfer.getListing();
            assertNotNull(listingAfterDelete);
            assertEquals(0, listingAfterDelete.size());
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testDeleteFileWhenDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            transfer.deleteFile(null, null, "foo/bar/does-not-exist.txt");
        }
    }

    @Test
    public void testDeleteDirectory() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_4);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory exists
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(0, listing.size());

            transfer.deleteDirectory(null, DIR_4);

            // verify the directory no longer exists
            try {
                transfer.getListing();
                Assert.fail("Should have thrown exception");
            } catch (FileNotFoundException e) {
                // nothing to do, expected
            }
        }
    }

    @Test(expected = IOException.class)
    public void testDeleteDirectoryWhenDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            transfer.deleteDirectory(null, "DOES-NOT-EXIST");
        }
    }

    @Test
    public void testEnsureDirectoryExistsSimple() throws IOException {
        final String remotePath = "DOES-NOT-EXIST";
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory does not exist
            try {
                transfer.getListing();
                Assert.fail("Should have failed");
            } catch (FileNotFoundException e) {
                // Nothing to do, expected
            }

            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));

            // verify the directory now exists
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testEnsureDirectoryExistsMultipleLevels() throws IOException {
        final String remotePath = "A/B/C";
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory does not exist
            try {
                transfer.getListing();
                Assert.fail("Should have failed");
            } catch (FileNotFoundException e) {
                // Nothing to do, expected
            }

            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));

            // verify the directory now exists
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testEnsureDirectoryExistsWhenAlreadyExists() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory already exists
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            final String absolutePath = transfer.getAbsolutePath(null, DIR_2);
            transfer.ensureDirectoryExists(null, new File(absolutePath));
        }
    }

    @Test
    public void testEnsureDirectoryExistsWithDirectoryListingDisabled() throws IOException {
        final String remotePath = "DOES-NOT-EXIST";
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);
        properties.put(SFTPTransfer.DISABLE_DIRECTORY_LISTING, "true");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory does not exist
            try {
                transfer.getListing();
                Assert.fail("Should have failed");
            } catch (FileNotFoundException e) {
                // Nothing to do, expected
            }

            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));

            // verify the directory now exists
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test(expected = IOException.class)
    public void testEnsureDirectoryExistsWithDirectoryListingDisabledAndAlreadyExists() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);
        properties.put(SFTPTransfer.DISABLE_DIRECTORY_LISTING, "true");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory already exists
            final List<FileInfo> listing = transfer.getListing();
            assertNotNull(listing);
            assertEquals(2, listing.size());

            final String absolutePath = transfer.getAbsolutePath(null, DIR_2);
            transfer.ensureDirectoryExists(null, new File(absolutePath));
        }
    }

    @Test
    public void testEnsureDirectoryExistsWithDirectoryListingDisabledAndParentDoesNotExist() throws IOException {
        final String remotePath = "A/B/C";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);
        properties.put(SFTPTransfer.DISABLE_DIRECTORY_LISTING, "true");

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory does not exist
            try {
                transfer.getListing();
                Assert.fail("Should have failed");
            } catch (FileNotFoundException e) {
                // Nothing to do, expected
            }

            // Should swallow exception here
            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));
        }
    }

    @Test
    public void testGetRemoteFileInfo() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, DIR_2, FILE_1);
            assertNotNull(fileInfo);
            assertEquals(FILE_1, fileInfo.getFileName());
        }
    }

    @Test
    public void testGetRemoteFileInfoWhenPathDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, "DOES-NOT-EXIST", FILE_1);
            assertNull(fileInfo);
        }
    }

    @Test
    public void testGetRemoteFileInfoWhenFileDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, DIR_2, "DOES-NOT-EXIST");
            assertNull(fileInfo);
        }
    }

    @Test
    public void testGetRemoteFileInfoWhenFileIsADirectory() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, DIR_1, DIR_1_CHILD_1);
            assertNull(fileInfo);
        }
    }

    @Test
    public void testRename() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final String source = DIR_2 + "/" + FILE_1;
            final String target = DIR_2 + "/" + FILE_1 + "-RENAMED";

            final FileInfo targetInfoBefore = transfer.getRemoteFileInfo(null, DIR_2, FILE_1 + "-RENAMED");
            assertNull(targetInfoBefore);

            transfer.rename(null, source, target);

            final FileInfo targetInfoAfter = transfer.getRemoteFileInfo(null, DIR_2, FILE_1 + "-RENAMED");
            assertNotNull(targetInfoAfter);
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testRenameWhenSourceDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final String source = DIR_2 + "/DOES-NOT-EXIST";
            final String target = DIR_2 + "/" + FILE_1 + "-RENAMED";
            transfer.rename(null, source, target);
        }
    }

    @Test(expected = IOException.class)
    public void testRenameWhenTargetAlreadyExists() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try(final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final String source = DIR_2 + "/" + FILE_1;
            final String target = DIR_2 + "/" + FILE_2;

            final FileInfo targetInfoBefore = transfer.getRemoteFileInfo(null, DIR_2, FILE_2);
            assertNotNull(targetInfoBefore);

            transfer.rename(null, source, target);
        }
    }

    @Test
    public void testPutWithPermissions() throws IOException {
        final String permissions = "rw-rw-rw-";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);

        final String filename = "test-put-simple.txt";
        final String fileContent = "this is a test";

        try(final SFTPTransfer transfer = createSFTPTransfer(properties);
            final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file does not already exist
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, DIR_4, filename);
            assertNull(fileInfoBefore);

            final String fullPath = transfer.put(null, DIR_4, filename, in);
            assertNotNull(fullPath);

            // Verify file now exists
            final FileInfo fileInfoAfter = transfer.getRemoteFileInfo(null, DIR_4, filename);
            assertNotNull(fileInfoAfter);
            assertEquals(permissions, fileInfoAfter.getPermissions());

            // Verify correct content was written
            final File writtenFile = new File(SFTP_ROOT_DIR + "/" + DIR_4 + "/" + filename);
            final String retrievedContent = IOUtils.toString(writtenFile.toURI(), StandardCharsets.UTF_8);
            assertEquals(fileContent, retrievedContent);
        }
    }

    @Test
    public void testPutWithTempFilename() throws IOException {
        final String permissions = "rw-rw-rw-";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);
        properties.put(SFTPTransfer.TEMP_FILENAME, "temp-file.txt");

        final String filename = "test-put-simple.txt";
        final String fileContent = "this is a test";

        try(final SFTPTransfer transfer = createSFTPTransfer(properties);
            final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file does not already exist
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, DIR_4, filename);
            assertNull(fileInfoBefore);

            final String fullPath = transfer.put(null, DIR_4, filename, in);
            assertNotNull(fullPath);

            // Verify file now exists
            final FileInfo fileInfoAfter = transfer.getRemoteFileInfo(null, DIR_4, filename);
            assertNotNull(fileInfoAfter);
            assertEquals(permissions, fileInfoAfter.getPermissions());
        }
    }

    @Test
    public void testPutWithLastModifiedTime() throws IOException, ParseException {
        final String permissions = "rw-rw-rw-";
        final String lastModifiedTime = "2019-09-01T11:11:11-0500";

        final DateFormat formatter = new SimpleDateFormat(SFTPTransfer.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
        final long expectedLastModifiedTime = formatter.parse(lastModifiedTime).getTime();

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);
        properties.put(SFTPTransfer.LAST_MODIFIED_TIME, lastModifiedTime);

        final String filename = "test-put-simple.txt";
        final String fileContent = "this is a test";

        try(final SFTPTransfer transfer = createSFTPTransfer(properties);
            final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file does not already exist
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, DIR_4, filename);
            assertNull(fileInfoBefore);

            final String fullPath = transfer.put(null, DIR_4, filename, in);
            assertNotNull(fullPath);

            // Verify file now exists
            final FileInfo fileInfoAfter = transfer.getRemoteFileInfo(null, DIR_4, filename);
            assertNotNull(fileInfoAfter);
            assertEquals(permissions, fileInfoAfter.getPermissions());
            assertEquals(expectedLastModifiedTime, fileInfoAfter.getLastModifiedTime());
        }
    }

    @Test(expected = IOException.class)
    public void testPutWhenFileAlreadyExists() throws IOException {
        final String permissions = "rw-rw-rw-";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);

        final String fileContent = "this is a test";

        try(final SFTPTransfer transfer = createSFTPTransfer(properties);
            final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file already exists
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, DIR_2, FILE_1);
            assertNotNull(fileInfoBefore);

            // Should fail because file already exists
            transfer.put(null, DIR_2, FILE_1, in);
        }
    }

    @Test(expected = IOException.class)
    public void testPutWhenDirectoryDoesNotExist() throws IOException {
        final String permissions = "rw-rw-rw-";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);

        final String fileContent = "this is a test";

        try(final SFTPTransfer transfer = createSFTPTransfer(properties);
            final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {
            transfer.put(null, "DOES-NOT-EXIST", FILE_1, in);
        }
    }

    private Map<PropertyDescriptor, String> createBaseProperties() {
        final Map<PropertyDescriptor,String> properties = new HashMap<>();
        properties.put(SFTPTransfer.HOSTNAME, "localhost");
        properties.put(SFTPTransfer.PORT, Integer.toString(sshTestServer.getSSHPort()));
        properties.put(SFTPTransfer.USERNAME, sshTestServer.getUsername());
        properties.put(SFTPTransfer.PASSWORD, sshTestServer.getPassword());
        properties.put(SFTPTransfer.STRICT_HOST_KEY_CHECKING, "false");
        return properties;
    }

    private SFTPTransfer createSFTPTransfer(final Map<PropertyDescriptor, String> properties) {
        final PropertyContext propertyContext = new MockPropertyContext(properties);
        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        return new SFTPTransfer(propertyContext, logger);
    }
}
