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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.util.MockPropertyContext;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestServerSFTPTransfer {

    private static final String LOCALHOST = "127.0.0.1";

    private static final String USERNAME = "user";

    private static final String PASSWORD = UUID.randomUUID().toString();

    private static final String DIR_1 = "dir1";
    private static final String DIR_2 = "dir2";
    private static final String LINKED_DIRECTORY = "linked-directory";
    private static final String LINKED_FILE = "linked-file";
    private static final String EMPTY_DIRECTORY = "dir4";

    private static final String DIR_1_CHILD_1 = "child1";
    private static final String DIR_1_CHILD_2 = "child2";

    private static final String FILE_1 = "file1.txt";
    private static final String FILE_2 = "file2.txt";
    private static final String DOT_FILE = ".foo.txt";

    private static final boolean FILTERING_ENABLED = true;

    @TempDir
    File serverDirectory;

    private SshServer sshServer;

    @BeforeEach
    public void setupFiles() throws IOException {
        writeFile(DIR_1, DIR_1_CHILD_1, FILE_1);
        writeFile(DIR_1, DIR_1_CHILD_1, FILE_2);
        writeFile(DIR_1, DIR_1_CHILD_1, DOT_FILE);

        writeFile(DIR_1, DIR_1_CHILD_2, FILE_1);
        writeFile(DIR_1, DIR_1_CHILD_2, FILE_2);
        writeFile(DIR_1, DIR_1_CHILD_2, DOT_FILE);

        writeFile(DIR_2, FILE_1);
        writeFile(DIR_2, FILE_2);
        writeFile(DIR_2, DOT_FILE);

        final File linkedDirectory = new File(serverDirectory, LINKED_DIRECTORY);
        final File linkedDirectoryTarget = new File(serverDirectory.getAbsolutePath(), DIR_1);
        Files.createSymbolicLink(linkedDirectory.toPath(), linkedDirectoryTarget.toPath());

        final File secondDirectory = new File(serverDirectory, DIR_2);
        final File linkedFile = new File(serverDirectory, LINKED_FILE);
        final File linkedFileTarget = new File(secondDirectory, FILE_1);
        Files.createSymbolicLink(linkedFile.toPath(), linkedFileTarget.toPath());

        final File emptyDirectory = new File(serverDirectory, EMPTY_DIRECTORY);
        assertTrue(emptyDirectory.mkdirs());

        startServer();
    }

    @AfterEach
    public void stopServer() throws IOException {
        sshServer.stop(true);
    }

    @Test
    public void testGetListingSimple() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(2, listing.size());

            final FileInfo file1Info = listing.stream().filter(f -> f.getFileName().equals(FILE_1)).findFirst().orElse(null);
            assertNotNull(file1Info);
            assertFalse(file1Info.isDirectory());

            final FileInfo file2Info = listing.stream().filter(f -> f.getFileName().equals(FILE_2)).findFirst().orElse(null);
            assertNotNull(file2Info);
            assertFalse(file2Info.isDirectory());
        }
    }

    @Test
    public void testGetListingSimpleWithDotFiles() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);
        properties.put(SFTPTransfer.IGNORE_DOTTED_FILES, "false");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
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

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testGetListingWithRecursiveSearch() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_1);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(4, listing.size());
        }
    }

    @Test
    public void testGetListingWithoutSymlinks() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");
        properties.put(SFTPTransfer.FOLLOW_SYMLINK, "false");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(6, listing.size());
        }
    }

    @Test
    public void testGetListingWithSymlinks() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");
        properties.put(SFTPTransfer.FOLLOW_SYMLINK, "true");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(11, listing.size());
        }
    }

    @Test
    public void testGetListingWithBatchSize() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_1);
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");

        // first listing is without batch size and shows 4 results
        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(4, listing.size());
        }

        // set a batch size of 2 and ensure we get 2 results
        properties.put(SFTPTransfer.REMOTE_POLL_BATCH_SIZE, "2");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
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

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
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

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(2, listing.size());
        }
    }

    @Test
    public void testGetListingWhenRemotePathDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, "DOES-NOT-EXIST");
        properties.put(SFTPTransfer.RECURSIVE_SEARCH, "true");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            assertThrows(FileNotFoundException.class, () -> transfer.getListing(FILTERING_ENABLED));
        }
    }

    @Test
    public void testDeleteFileWithoutPath() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory has two files
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(2, listing.size());

            // issue deletes for the two files
            for (final FileInfo fileInfo : listing) {
                transfer.deleteFile(null, null, fileInfo.getFullPathFileName());
            }

            // verify there are now zero files
            final List<FileInfo> listingAfterDelete = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listingAfterDelete);
            assertEquals(0, listingAfterDelete.size());
        }
    }

    @Test
    public void testDeleteFileWithPath() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory has two files
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(2, listing.size());

            // issue deletes for the two files
            for (final FileInfo fileInfo : listing) {
                final String filename = fileInfo.getFileName();
                final String path = fileInfo.getFullPathFileName().replace(filename, "");
                transfer.deleteFile(null, path, filename);
            }

            // verify there are now zero files
            final List<FileInfo> listingAfterDelete = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listingAfterDelete);
            assertEquals(0, listingAfterDelete.size());
        }
    }

    @Test
    public void testDeleteFileWhenDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            assertThrows(FileNotFoundException.class, () -> transfer.deleteFile(null, null, "foo/bar/does-not-exist.txt"));
        }
    }

    @Test
    public void testDeleteDirectory() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, EMPTY_DIRECTORY);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory exists
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(0, listing.size());

            transfer.deleteDirectory(null, EMPTY_DIRECTORY);

            assertThrows(FileNotFoundException.class, () -> transfer.getListing(FILTERING_ENABLED));
        }
    }

    @Test
    public void testDeleteDirectoryWhenDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            assertThrows(IOException.class, () -> transfer.deleteDirectory(null, "DOES-NOT-EXIST"));
        }
    }

    @Test
    public void testEnsureDirectoryExistsSimple() throws IOException {
        final String remotePath = "DOES-NOT-EXIST";
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory does not exist
            assertThrows(FileNotFoundException.class, () -> transfer.getListing(FILTERING_ENABLED));

            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));

            // verify the directory now exists
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testEnsureDirectoryExistsMultipleLevels() throws IOException {
        final String remotePath = "A/B/C";
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            assertThrows(FileNotFoundException.class, () -> transfer.getListing(FILTERING_ENABLED));

            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));

            // verify the directory now exists
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testEnsureDirectoryExistsWhenAlreadyExists() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory already exists
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
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

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            assertThrows(FileNotFoundException.class, () -> transfer.getListing(FILTERING_ENABLED));

            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));

            // verify the directory now exists
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(0, listing.size());
        }
    }

    @Test
    public void testEnsureDirectoryExistsWithDirectoryListingDisabledAndAlreadyExists() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, DIR_2);
        properties.put(SFTPTransfer.DISABLE_DIRECTORY_LISTING, "true");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            // verify the directory already exists
            final List<FileInfo> listing = transfer.getListing(FILTERING_ENABLED);
            assertNotNull(listing);
            assertEquals(2, listing.size());

            final String absolutePath = transfer.getAbsolutePath(null, DIR_2);
            assertThrows(IOException.class, () -> transfer.ensureDirectoryExists(null, new File(absolutePath)));
        }
    }

    @Test
    public void testEnsureDirectoryExistsWithDirectoryListingDisabledAndParentDoesNotExist() throws IOException {
        final String remotePath = "A/B/C";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.REMOTE_PATH, remotePath);
        properties.put(SFTPTransfer.DISABLE_DIRECTORY_LISTING, "true");

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            assertThrows(FileNotFoundException.class, () -> transfer.getListing(FILTERING_ENABLED));

            // Should swallow exception here
            final String absolutePath = transfer.getAbsolutePath(null, remotePath);
            transfer.ensureDirectoryExists(null, new File(absolutePath));
        }
    }

    @Test
    public void testGetRemoteFileInfo() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, DIR_2, FILE_1);
            assertNotNull(fileInfo);
            assertEquals(FILE_1, fileInfo.getFileName());
        }
    }

    @Test
    public void testGetRemoteFileInfoWhenPathDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, "DOES-NOT-EXIST", FILE_1);
            assertNull(fileInfo);
        }
    }

    @Test
    public void testGetRemoteFileInfoWhenFileDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, DIR_2, "DOES-NOT-EXIST");
            assertNull(fileInfo);
        }
    }

    @Test
    public void testGetRemoteFileInfoWhenFileIsADirectory() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final FileInfo fileInfo = transfer.getRemoteFileInfo(null, DIR_1, DIR_1_CHILD_1);
            assertNull(fileInfo);
        }
    }

    @Test
    public void testRename() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final String source = DIR_2 + "/" + FILE_1;
            final String target = DIR_2 + "/" + FILE_1 + "-RENAMED";

            final FileInfo targetInfoBefore = transfer.getRemoteFileInfo(null, DIR_2, FILE_1 + "-RENAMED");
            assertNull(targetInfoBefore);

            transfer.rename(null, source, target);

            final FileInfo targetInfoAfter = transfer.getRemoteFileInfo(null, DIR_2, FILE_1 + "-RENAMED");
            assertNotNull(targetInfoAfter);
        }
    }

    @Test
    public void testRenameWhenSourceDoesNotExist() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final String source = DIR_2 + "/DOES-NOT-EXIST";
            final String target = DIR_2 + "/" + FILE_1 + "-RENAMED";
            assertThrows(FileNotFoundException.class, () -> transfer.rename(null, source, target));
        }
    }

    @Test
    public void testRenameWhenTargetAlreadyExists() throws IOException {
        final Map<PropertyDescriptor, String> properties = createBaseProperties();

        try (final SFTPTransfer transfer = createSFTPTransfer(properties)) {
            final String source = DIR_2 + "/" + FILE_1;
            final String target = DIR_2 + "/" + FILE_2;

            final FileInfo targetInfoBefore = transfer.getRemoteFileInfo(null, DIR_2, FILE_2);
            assertNotNull(targetInfoBefore);

            assertThrows(IOException.class, () -> transfer.rename(null, source, target));
        }
    }

    @Test
    public void testPutWithPermissions() throws IOException {
        final String permissions = "rw-rw-rw-";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);

        final String filename = "test-put-simple.txt";
        final String fileContent = "this is a test";

        try (final SFTPTransfer transfer = createSFTPTransfer(properties);
             final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file does not already exist
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, EMPTY_DIRECTORY, filename);
            assertNull(fileInfoBefore);

            final String fullPath = transfer.put(null, EMPTY_DIRECTORY, filename, in);
            assertNotNull(fullPath);

            // Verify file now exists
            final FileInfo fileInfoAfter = transfer.getRemoteFileInfo(null, EMPTY_DIRECTORY, filename);
            assertNotNull(fileInfoAfter);
            assertEquals(permissions, fileInfoAfter.getPermissions());

            // Verify correct content was written
            final File writtenFile = new File(serverDirectory, EMPTY_DIRECTORY + "/" + filename);
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

        try (final SFTPTransfer transfer = createSFTPTransfer(properties);
             final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file does not already exist
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, EMPTY_DIRECTORY, filename);
            assertNull(fileInfoBefore);

            final String fullPath = transfer.put(null, EMPTY_DIRECTORY, filename, in);
            assertNotNull(fullPath);

            // Verify file now exists
            final FileInfo fileInfoAfter = transfer.getRemoteFileInfo(null, EMPTY_DIRECTORY, filename);
            assertNotNull(fileInfoAfter);
            assertEquals(permissions, fileInfoAfter.getPermissions());
        }
    }

    @Test
    public void testPutWithLastModifiedTime() throws IOException {
        final String permissions = "rw-rw-rw-";
        final String lastModifiedTime = "2019-09-01T11:11:11-0500";
        final long expectedLastModifiedTime = 1567354271000L;

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);
        properties.put(SFTPTransfer.LAST_MODIFIED_TIME, lastModifiedTime);

        final String filename = "test-put-simple.txt";
        final String fileContent = "this is a test";

        try (final SFTPTransfer transfer = createSFTPTransfer(properties);
             final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {

            // Verify file does not already exist
            final FileInfo fileInfoBefore = transfer.getRemoteFileInfo(null, EMPTY_DIRECTORY, filename);
            assertNull(fileInfoBefore);

            final String fullPath = transfer.put(null, EMPTY_DIRECTORY, filename, in);
            assertNotNull(fullPath);

            // Verify file now exists
            final FileInfo fileInfoAfter = transfer.getRemoteFileInfo(null, EMPTY_DIRECTORY, filename);
            assertNotNull(fileInfoAfter);
            assertEquals(permissions, fileInfoAfter.getPermissions());
            assertEquals(expectedLastModifiedTime, fileInfoAfter.getLastModifiedTime());
        }
    }

    @Test
    public void testPutWhenDirectoryDoesNotExist() throws IOException {
        final String permissions = "rw-rw-rw-";

        final Map<PropertyDescriptor, String> properties = createBaseProperties();
        properties.put(SFTPTransfer.PERMISSIONS, permissions);

        final String fileContent = "this is a test";

        try (final SFTPTransfer transfer = createSFTPTransfer(properties);
             final InputStream in = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8))) {
            assertThrows(IOException.class, () -> transfer.put(null, "DOES-NOT-EXIST", FILE_1, in));
        }
    }

    private Map<PropertyDescriptor, String> createBaseProperties() {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(SFTPTransfer.HOSTNAME, LOCALHOST);
        properties.put(SFTPTransfer.PORT, Integer.toString(sshServer.getPort()));
        properties.put(SFTPTransfer.USERNAME, USERNAME);
        properties.put(SFTPTransfer.PASSWORD, PASSWORD);
        properties.put(SFTPTransfer.STRICT_HOST_KEY_CHECKING, Boolean.FALSE.toString());
        return properties;
    }

    private SFTPTransfer createSFTPTransfer(final Map<PropertyDescriptor, String> properties) {
        final PropertyContext propertyContext = new MockPropertyContext(properties);
        final ComponentLog logger = Mockito.mock(ComponentLog.class);
        return new SFTPTransfer(propertyContext, logger);
    }

    private void startServer() throws IOException {
        sshServer = SshServer.setUpDefaultServer();
        sshServer.setHost(LOCALHOST);
        sshServer.setPasswordAuthenticator((username, password, serverSession) -> USERNAME.equals(username) && PASSWORD.equals(password));
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
        sshServer.setFileSystemFactory(new VirtualFileSystemFactory(serverDirectory.toPath()));
        sshServer.setSubsystemFactories(Collections.singletonList(new SftpSubsystemFactory()));
        sshServer.start();
    }

    private void writeFile(final String... pathElements) throws IOException {
        final Path path = Paths.get(serverDirectory.getAbsolutePath(), pathElements);
        final File parentFile = path.toFile().getParentFile();
        FileUtils.forceMkdir(parentFile);
        Files.writeString(path, path.toFile().getAbsolutePath());
    }
}
