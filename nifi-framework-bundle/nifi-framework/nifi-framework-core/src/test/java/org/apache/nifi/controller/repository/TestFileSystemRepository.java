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
package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.util.DiskUtils;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(OS.WINDOWS)
public class TestFileSystemRepository {

    public static final File helloWorldFile = new File("src/test/resources/hello.txt");
    private static final Logger logger = LoggerFactory.getLogger(TestFileSystemRepository.class);

    private FileSystemRepository repository = null;
    private StandardResourceClaimManager claimManager = null;
    private final File rootFile = new File("target/content_repository");
    private NiFiProperties nifiProperties;

    @BeforeEach
    public void setup() throws IOException {
        nifiProperties = NiFiProperties.createBasicNiFiProperties(TestFileSystemRepository.class.getResource("/conf/nifi.properties").getFile());
        if (rootFile.exists()) {
            DiskUtils.deleteRecursively(rootFile);
        }
        repository = new FileSystemRepository(nifiProperties);
        claimManager = new StandardResourceClaimManager();
        repository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP));
        repository.purge();
    }

    @AfterEach
    public void shutdown() throws IOException {
        repository.shutdown();
    }

    @Test
    @Disabled("Intended for manual testing only, in order to judge changes to performance")
    public void testWritePerformance() throws IOException {
        final long bytesToWrite = 1_000_000_000L;
        final int contentSize = 100;

        final int iterations = (int) (bytesToWrite / contentSize);
        final byte[] content = new byte[contentSize];
        final Random random = new Random();
        random.nextBytes(content);

        //        final ContentClaimWriteCache cache = new ContentClaimWriteCache(repository);
        final long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            final ContentClaim claim = repository.create(false);
            try (final OutputStream out = repository.write(claim)) {
                out.write(content);
            }
        }
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        final long mb = bytesToWrite / (1024 * 1024);
        final long seconds = millis / 1000L;
        final double mbps = (double) mb / (double) seconds;
        logger.info("Took {} millis to write {} bytes {} times (total of {} bytes) for a write rate of {} MB/s",
                millis, contentSize, iterations, NumberFormat.getNumberInstance(Locale.US).format(bytesToWrite), mbps);
    }

    @Test
    public void testIsArchived() {
        assertFalse(repository.isArchived(Paths.get("1.txt")));
        assertFalse(repository.isArchived(Paths.get("a/1.txt")));
        assertFalse(repository.isArchived(Paths.get("a/b/1.txt")));
        assertFalse(repository.isArchived(Paths.get("a/archive/b/c/1.txt")));

        assertTrue(repository.isArchived(Paths.get("archive/1.txt")));
        assertTrue(repository.isArchived(Paths.get("a/archive/1.txt")));
        assertTrue(repository.isArchived(Paths.get("a/b/c/archive/1.txt")));
    }


    @Test
    @Timeout(30)
    public void testClaimsArchivedWhenMarkedDestructable() throws IOException, InterruptedException {
        final ContentClaim contentClaim = repository.create(false);
        final long configuredAppendableClaimLength = DataUnit.parseDataSize(nifiProperties.getMaxAppendableClaimSize(), DataUnit.B).longValue();
        final Map<String, Path> containerPaths = nifiProperties.getContentRepositoryPaths();
        assertEquals(1, containerPaths.size());
        final String containerName = containerPaths.keySet().iterator().next();

        try (final OutputStream out = repository.write(contentClaim)) {
            long bytesWritten = 0L;
            final byte[] bytes = "Hello World".getBytes(StandardCharsets.UTF_8);

            while (bytesWritten <= configuredAppendableClaimLength) {
                out.write(bytes);
                bytesWritten += bytes.length;
            }
        }

        assertEquals(0, repository.getArchiveCount(containerName));
        assertEquals(0, claimManager.decrementClaimantCount(contentClaim.getResourceClaim()));
        claimManager.markDestructable(contentClaim.getResourceClaim());

        // The claim should become archived but it may take a few seconds, as it's handled by background threads
        while (repository.getArchiveCount(containerName) != 1) {
            Thread.sleep(50L);
        }
    }

    @Test
    @Timeout(value = 30)
    public void testArchivedClaimRemovedDueToAge() throws IOException, InterruptedException {
        // Recreate Repository with specific properties
        final Map<String, String> propertyOverrides = new HashMap<>();
        propertyOverrides.put(NiFiProperties.CONTENT_ARCHIVE_MAX_RETENTION_PERIOD, "2 sec");
        propertyOverrides.put(NiFiProperties.CONTENT_ARCHIVE_CLEANUP_FREQUENCY, "1 sec");
        propertyOverrides.put(NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "99%");
        recreateRepositoryWithPropertyOverrides(propertyOverrides);

        final Map<String, Path> containerPaths = nifiProperties.getContentRepositoryPaths();
        assertEquals(1, containerPaths.size());
        final Path containerPath = containerPaths.values().iterator().next();

        // Perform a few iterations to ensure that it works not just the first time, since there is a lot of logic on initialization.
        for (int i = 0; i < 3; i++) {
            final File archiveDir = containerPath.resolve(String.valueOf(i)).resolve("archive").toFile();
            final File archivedFile = new File(archiveDir, "1234");

            try (final OutputStream fos = new FileOutputStream(archivedFile)) {
                fos.write("Hello World".getBytes());
            }

            while (archivedFile.exists()) {
                Thread.sleep(50L);
            }
        }
    }

    @Test
    @Timeout(value = 30)
    public void testArchivedClaimRemovedDueToDiskUsage() throws IOException, InterruptedException {
        // Recreate Repository with specific properties
        final Map<String, String> propertyOverrides = new HashMap<>();
        propertyOverrides.put(NiFiProperties.CONTENT_ARCHIVE_MAX_RETENTION_PERIOD, "555 days");
        propertyOverrides.put(NiFiProperties.CONTENT_ARCHIVE_CLEANUP_FREQUENCY, "1 sec");
        propertyOverrides.put(NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "1%");
        recreateRepositoryWithPropertyOverrides(propertyOverrides);

        final Map<String, Path> containerPaths = nifiProperties.getContentRepositoryPaths();
        assertEquals(1, containerPaths.size());
        final Path containerPath = containerPaths.values().iterator().next();

        // Perform a few iterations to ensure that it works not just the first time, since there is a lot of logic on initialization.
        for (int i = 0; i < 3; i++) {
            final File archiveDir = containerPath.resolve(String.valueOf(i)).resolve("archive").toFile();
            final File archivedFile = new File(archiveDir, "1234");

            try (final OutputStream fos = new FileOutputStream(archivedFile)) {
                fos.write("Hello World".getBytes());
            }

            while (archivedFile.exists()) {
                Thread.sleep(50L);
            }
        }
    }

    private void recreateRepositoryWithPropertyOverrides(final Map<String, String> propertyOverrides) throws IOException {
        repository.shutdown();
        nifiProperties = NiFiProperties.createBasicNiFiProperties(TestFileSystemRepository.class.getResource("/conf/nifi.properties").getFile(), propertyOverrides);

        repository = new FileSystemRepository(nifiProperties);
        claimManager = new StandardResourceClaimManager();
        repository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP));
        repository.purge();
    }


    @Test
    public void testUnreferencedFilesAreArchivedOnCleanup() throws IOException {
        final Map<String, Path> containerPaths = nifiProperties.getContentRepositoryPaths();
        assertTrue(containerPaths.size() > 0);

        for (final Map.Entry<String, Path> entry : containerPaths.entrySet()) {
            final String containerName = entry.getKey();
            final Path containerPath = entry.getValue();

            final Path section1 = containerPath.resolve("1");
            final Path file1 = section1.resolve("file-1");
            Files.write(file1, "hello".getBytes(), StandardOpenOption.CREATE);

            // Should be nothing in the archive at this point
            assertEquals(0, repository.getArchiveCount(containerName));

            // When we cleanup, we should see one file moved to archive
            repository.cleanup();
            assertEquals(1, repository.getArchiveCount(containerName));
        }
    }

    @Test
    public void testAlreadyArchivedFilesCounted() throws IOException {
        // We want to make sure that the initialization code counts files in archive, so we need to create a new FileSystemRepository to do this.
        repository.shutdown();

        final Map<String, Path> containerPaths = nifiProperties.getContentRepositoryPaths();
        assertTrue(containerPaths.size() > 0);

        for (final Path containerPath : containerPaths.values()) {
            final Path section1 = containerPath.resolve("1");
            final Path archive = section1.resolve("archive");
            Files.createDirectories(archive);

            for (int i = 0; i < 3; i++) {
                final Path file1 = archive.resolve("file-" + i);
                Files.write(file1, "hello".getBytes(), StandardOpenOption.CREATE);
            }
        }

        repository = new FileSystemRepository(nifiProperties);

        for (final String containerName : containerPaths.keySet()) {
            assertEquals(3, repository.getArchiveCount(containerName));
        }
    }


    @Test
    public void testContentNotFoundExceptionThrownIfResourceClaimTooShort() throws IOException {
        final File contentFile = new File("target/content_repository/0/0.bin");
        try (final OutputStream fos = new FileOutputStream(contentFile)) {
            fos.write("Hello World".getBytes(StandardCharsets.UTF_8));
        }

        final ResourceClaim resourceClaim = new StandardResourceClaim(claimManager, "default", "0", "0.bin", false);
        final StandardContentClaim existingContentClaim = new StandardContentClaim(resourceClaim, 0);
        existingContentClaim.setLength(11);

        try (final InputStream in = repository.read(existingContentClaim)) {
            final byte[] buff = new byte[11];
            StreamUtils.fillBuffer(in, buff);
            assertEquals("Hello World", new String(buff, StandardCharsets.UTF_8));
        }

        final StandardContentClaim halfContentClaim = new StandardContentClaim(resourceClaim, 6);
        halfContentClaim.setLength(5);

        try (final InputStream in = repository.read(halfContentClaim)) {
            final byte[] buff = new byte[5];
            StreamUtils.fillBuffer(in, buff);
            assertEquals("World", new String(buff, StandardCharsets.UTF_8));
        }

        final StandardContentClaim emptyContentClaim = new StandardContentClaim(resourceClaim, 11);
        existingContentClaim.setLength(0);

        try (final InputStream in = repository.read(emptyContentClaim)) {
            assertEquals(-1, in.read());
        }

        final StandardContentClaim missingContentClaim = new StandardContentClaim(resourceClaim, 12);
        missingContentClaim.setLength(1);

        assertThrows(ContentNotFoundException.class, () -> repository.read(missingContentClaim));
    }

    @Test
    public void testBogusFile() throws IOException {
        repository.shutdown();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestFileSystemRepository.class.getResource("/conf/nifi.properties").getFile());

        File bogus = new File(rootFile, "bogus");
        try {
            bogus.mkdir();
            bogus.setReadable(false);

            repository = new FileSystemRepository(nifiProperties);
            repository.initialize(new StandardContentRepositoryContext(new StandardResourceClaimManager(), EventReporter.NO_OP));
        } finally {
            bogus.setReadable(true);
            assertTrue(bogus.delete());
        }
    }

    @Test
    public void testCreateContentClaim() throws IOException {
        // value passed to #create is irrelevant because the FileSystemRepository does not currently support loss tolerance.
        final ContentClaim claim = repository.create(true);
        assertNotNull(claim);
        assertEquals(1, repository.getClaimantCount(claim));
    }

    @Test
    public void testReadClaimThenWriteThenReadMore() throws IOException {
        final ContentClaim claim = repository.create(false);

        final OutputStream out = repository.write(claim);
        out.write("hello".getBytes());
        out.flush();

        final InputStream in = repository.read(claim);
        final byte[] buffer = new byte[5];
        StreamUtils.fillBuffer(in, buffer);

        assertEquals("hello", new String(buffer));

        out.write("good-bye".getBytes());
        out.close();

        final byte[] buffer2 = new byte[8];
        StreamUtils.fillBuffer(in, buffer2);
        assertEquals("good-bye", new String(buffer2));
    }

    @Test
    public void testClaimantCounts() throws IOException {
        final ContentClaim claim = repository.create(true);
        assertNotNull(claim);
        assertEquals(1, repository.getClaimantCount(claim));
        assertEquals(2, repository.incrementClaimaintCount(claim));
        assertEquals(3, repository.incrementClaimaintCount(claim));
        assertEquals(4, repository.incrementClaimaintCount(claim));
        assertEquals(5, repository.incrementClaimaintCount(claim));
        repository.decrementClaimantCount(claim);
        assertEquals(4, repository.getClaimantCount(claim));
        repository.decrementClaimantCount(claim);
        assertEquals(3, repository.getClaimantCount(claim));
        repository.decrementClaimantCount(claim);
        assertEquals(2, repository.getClaimantCount(claim));
        repository.decrementClaimantCount(claim);
        assertEquals(1, repository.getClaimantCount(claim));
        repository.decrementClaimantCount(claim);
        assertEquals(0, repository.getClaimantCount(claim));
        repository.remove(claim);
    }

    @Test
    public void testResourceClaimReused() throws IOException {
        final ContentClaim claim1 = repository.create(false);
        final ContentClaim claim2 = repository.create(false);

        // should not be equal because claim1 may still be in use
        assertNotSame(claim1.getResourceClaim(), claim2.getResourceClaim());

        try (final OutputStream ignored = repository.write(claim1)) {
        }

        final ContentClaim claim3 = repository.create(false);
        assertEquals(claim1.getResourceClaim(), claim3.getResourceClaim());
    }

    @Test
    public void testResourceClaimNotReusedAfterRestart() throws IOException, InterruptedException {
        final ContentClaim claim1 = repository.create(false);
        try (final OutputStream ignored = repository.write(claim1)) {
        }

        repository.shutdown();
        Thread.sleep(1000L);

        repository = new FileSystemRepository(nifiProperties);
        repository.initialize(new StandardContentRepositoryContext(new StandardResourceClaimManager(), EventReporter.NO_OP));
        repository.purge();

        final ContentClaim claim2 = repository.create(false);
        assertNotSame(claim1.getResourceClaim(), claim2.getResourceClaim());
    }

    @Test
    public void testWriteWithNoContent() throws IOException {
        final ContentClaim claim1 = repository.create(false);
        try (final OutputStream out = repository.write(claim1)) {
            out.write("Hello".getBytes());
        }

        final ContentClaim claim2 = repository.create(false);
        assertEquals(claim1.getResourceClaim(), claim2.getResourceClaim());
        try (final OutputStream ignored = repository.write(claim2)) {

        }

        final ContentClaim claim3 = repository.create(false);
        assertEquals(claim1.getResourceClaim(), claim3.getResourceClaim());
        try (final OutputStream out = repository.write(claim3)) {
            out.write(" World".getBytes());
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final InputStream in = repository.read(claim1)) {
            StreamUtils.copy(in, baos);
        }

        assertEquals("Hello", baos.toString());

        baos.reset();
        try (final InputStream in = repository.read(claim2)) {
            StreamUtils.copy(in, baos);
        }
        assertEquals("", baos.toString());
        assertEquals(0, baos.size());

        baos.reset();
        try (final InputStream in = repository.read(claim3)) {
            StreamUtils.copy(in, baos);
        }
        assertEquals(" World", baos.toString());
    }

    @Test
    public void testRemoveDeletesFileIfNoClaimants() throws IOException {
        final ContentClaim claim = repository.create(true);
        assertNotNull(claim);
        assertEquals(1, repository.getClaimantCount(claim));
        repository.incrementClaimaintCount(claim);

        final Path claimPath = getPath(claim);
        final String maxAppendableClaimLength = nifiProperties.getMaxAppendableClaimSize();
        final int maxClaimLength = DataUnit.parseDataSize(maxAppendableClaimLength, DataUnit.B).intValue();

        // Create the file.
        try (final OutputStream out = repository.write(claim)) {
            out.write(new byte[maxClaimLength]);
        }

        int count = repository.decrementClaimantCount(claim);
        assertEquals(1, count);
        assertTrue(Files.exists(claimPath));
        // ensure that no Exception is thrown here.
        repository.remove(null);
        assertTrue(Files.exists(claimPath));

        count = repository.decrementClaimantCount(claim);
        assertEquals(0, count);
        repository.remove(claim);
        assertFalse(Files.exists(claimPath));
    }

    private Path getPath(final ContentClaim claim) {
        try {
            final Method m = repository.getClass().getDeclaredMethod("getPath", ContentClaim.class);
            m.setAccessible(true);
            return (Path) m.invoke(repository, claim);
        } catch (final Exception e) {
            throw new RuntimeException("Could not invoke #getPath on FileSystemRepository due to " + e);
        }
    }

    @Test
    public void testImportFromFile() throws IOException {
        final ContentClaim claim = repository.create(false);
        final File testFile = new File("src/test/resources/hello.txt");
        final File file1 = new File("target/testFile1");
        final Path path1 = file1.toPath();
        final File file2 = new File("target/testFile2");
        final Path path2 = file2.toPath();

        Files.copy(testFile.toPath(), path1, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(testFile.toPath(), path2, StandardCopyOption.REPLACE_EXISTING);

        repository.importFrom(path1, claim);
        assertTrue(file1.exists());
        assertTrue(file2.exists());

        // try to read the data back out.
        final Path path = getPath(claim);
        final byte[] data = Files.readAllBytes(path);
        final byte[] expected = Files.readAllBytes(testFile.toPath());
        assertArrayEquals(expected, data);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final InputStream in = repository.read(claim)) {
            StreamUtils.copy(in, baos);
        }

        assertArrayEquals(expected, baos.toByteArray());
    }

    @Test
    public void testImportFromStream() throws IOException {
        final ContentClaim claim = repository.create(false);
        final byte[] data = "hello".getBytes();
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        repository.importFrom(bais, claim);

        final Path claimPath = getPath(claim);
        assertArrayEquals(data, Files.readAllBytes(claimPath));
    }

    @Test
    public void testExportToOutputStream() throws IOException {
        final ContentClaim claim = repository.create(true);

        try (final OutputStream out = repository.write(claim)) {
            Files.copy(helloWorldFile.toPath(), out);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        repository.exportTo(claim, baos);
        final byte[] data = baos.toByteArray();
        assertArrayEquals(Files.readAllBytes(helloWorldFile.toPath()), data);
    }

    @Test
    public void testExportToFile() throws IOException {
        final ContentClaim claim = repository.create(true);
        try (final OutputStream out = repository.write(claim)) {
            Files.copy(helloWorldFile.toPath(), out);
        }

        final File outFile = new File("target/testExportToFile");
        final Path outPath = outFile.toPath();
        Files.deleteIfExists(outPath);

        final byte[] expected = Files.readAllBytes(helloWorldFile.toPath());

        repository.exportTo(claim, outPath, false);
        assertArrayEquals(expected, Files.readAllBytes(outPath));

        repository.exportTo(claim, outPath, true);
        final byte[] doubleExpected = new byte[expected.length * 2];
        System.arraycopy(expected, 0, doubleExpected, 0, expected.length);
        System.arraycopy(expected, 0, doubleExpected, expected.length, expected.length);
        assertArrayEquals(doubleExpected, Files.readAllBytes(outPath));
    }

    @Test
    public void testSize() throws IOException {
        final ContentClaim claim = repository.create(true);
        final Path path = getPath(claim);

        Files.createDirectories(path.getParent());
        final byte[] data = "The quick brown fox jumps over the lazy dog".getBytes();
        try (final OutputStream out = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            out.write(data);
        }

        assertEquals(data.length, repository.size(claim));
    }

    @Test
    public void testSizeWithNoContent() {
        final ContentClaim claim =
         new StandardContentClaim(new StandardResourceClaim(claimManager,
                 "container1", "section 1", "1", false), 0L);

        assertThrows(ContentNotFoundException.class, () -> repository.size(claim));
    }

    @Test
    public void testReadWithNoContent() {
        final ContentClaim claim = new StandardContentClaim(new StandardResourceClaim(claimManager, "container1", "section 1", "1", false), 0L);

        assertThrows(ContentNotFoundException.class,
                () -> repository.read(claim));
    }

    @Test
    public void testReadWithContent() throws IOException {
        final ContentClaim claim = repository.create(true);
        final Path path = getPath(claim);

        Files.createDirectories(path.getParent());
        final byte[] data = "The quick brown fox jumps over the lazy dog".getBytes();
        try (final OutputStream out = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            out.write(data);
        }

        try (final InputStream inStream = repository.read(claim)) {
            assertNotNull(inStream);
            final byte[] dataRead = readFully(inStream, data.length);
            assertArrayEquals(data, dataRead);
        }
    }

    @Test
    public void testReadWithContentArchived() throws IOException {
        final ContentClaim claim = repository.create(true);
        final Path path = getPath(claim);
        Files.deleteIfExists(path);

        Path archivePath = FileSystemRepository.getArchivePath(path);

        Files.createDirectories(archivePath.getParent());
        final byte[] data = "The quick brown fox jumps over the lazy dog".getBytes();
        try (final OutputStream out = Files.newOutputStream(archivePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            out.write(data);
        }

        try (final InputStream inStream = repository.read(claim)) {
            assertNotNull(inStream);
            final byte[] dataRead = readFully(inStream, data.length);
            assertArrayEquals(data, dataRead);
        }
    }

    private boolean isWindowsEnvironment() {
        return System.getProperty("os.name").toLowerCase().startsWith("windows");
    }

    @Test
    public void testReadWithNoContentArchived() throws IOException {
        final ContentClaim claim = repository.create(true);
        final Path path = getPath(claim);
        Files.deleteIfExists(path);

        Path archivePath = FileSystemRepository.getArchivePath(path);
        Files.deleteIfExists(archivePath);

        assertThrows(ContentNotFoundException.class, () -> repository.read(claim).close());
    }

    @Test
    public void testWrite() throws IOException {
        final ContentClaim claim = repository.create(true);
        final byte[] data = "The quick brown fox jumps over the lazy dog".getBytes();
        try (final OutputStream out = repository.write(claim)) {
            out.write(data);
        }

        final Path path = getPath(claim);
        assertArrayEquals(data, Files.readAllBytes(path));
    }

    @Test
    public void testRemoveWhileWritingToClaim() throws IOException {
        final ContentClaim claim = repository.create(false);
        final OutputStream out = repository.write(claim);

        // write at least 1 MB to the output stream so that when we close the output stream
        // the repo won't keep the stream open.
        final String maxAppendableClaimLength = nifiProperties.getMaxAppendableClaimSize();
        final int maxClaimLength = DataUnit.parseDataSize(maxAppendableClaimLength, DataUnit.B).intValue();
        final byte[] buff = new byte[maxClaimLength];
        out.write(buff);
        out.write(buff);

        // false because claimant count is still 1, so the resource claim was not removed
        assertFalse(repository.remove(claim));

        assertEquals(0, repository.decrementClaimantCount(claim));

        // false because claimant count is 0 but there is an 'active' stream for the claim
        assertFalse(repository.remove(claim));

        out.close();
        assertTrue(repository.remove(claim));
    }

    @Test
    public void testMarkDestructableDoesNotArchiveIfStreamOpenAndWrittenTo() throws IOException, InterruptedException {
        FileSystemRepository repository = null;
        try {
            final List<Path> archivedPaths = Collections.synchronizedList(new ArrayList<>());

            // We are creating our own 'local' repository in this test so shut down the one created in the setup() method
            shutdown();

            repository = new FileSystemRepository(nifiProperties) {
                @Override
                protected boolean archive(Path curPath) {
                    archivedPaths.add(curPath);
                    return true;
                }
            };

            final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
            repository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP));
            repository.purge();

            final ContentClaim claim = repository.create(false);

            // Create a stream and write a bit to it, then close it. This will cause the
            // claim to be put back onto the 'writableClaimsQueue'
            try (final OutputStream out = repository.write(claim)) {
                assertEquals(1, claimManager.getClaimantCount(claim.getResourceClaim()));
                out.write("1\n".getBytes());
            }

            assertEquals(1, claimManager.getClaimantCount(claim.getResourceClaim()));

            int claimantCount = claimManager.decrementClaimantCount(claim.getResourceClaim());
            assertEquals(0, claimantCount);
            assertTrue(archivedPaths.isEmpty());

            claimManager.markDestructable(claim.getResourceClaim());

            // Wait for the archive thread to have a chance to run
            Thread.sleep(2000L);

            // Should still be empty because we have a stream open to the file.
            assertTrue(archivedPaths.isEmpty());
            assertEquals(0, claimManager.getClaimantCount(claim.getResourceClaim()));
        } finally {
            if (repository != null) {
                repository.shutdown();
            }
        }
    }

    @Test
    public void testWriteCannotProvideNullOutput() throws IOException {
        FileSystemRepository repository = null;
        try {
            final List<Path> archivedPathsWithOpenStream = Collections.synchronizedList(new ArrayList<>());

            // We are creating our own 'local' repository in this test so shut down the one created in the setup() method
            shutdown();

            repository = new FileSystemRepository(nifiProperties) {
                @Override
                protected boolean archive(Path curPath) {
                    if (getOpenStreamCount() > 0) {
                        archivedPathsWithOpenStream.add(curPath);
                    }

                    return true;
                }
            };

            final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();
            repository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP));
            repository.purge();

            final ContentClaim claim = repository.create(false);

            assertEquals(1, claimManager.getClaimantCount(claim.getResourceClaim()));

            int claimantCount = claimManager.decrementClaimantCount(claim.getResourceClaim());
            assertEquals(0, claimantCount);
            assertTrue(archivedPathsWithOpenStream.isEmpty());

            OutputStream out = repository.write(claim);
            out.close();
            repository.decrementClaimantCount(claim);

            ContentClaim claim2 = repository.create(false);
            assertEquals(claim.getResourceClaim(), claim2.getResourceClaim());
            out = repository.write(claim2);

            final boolean archived = repository.archive(claim.getResourceClaim());
            assertFalse(archived);
        } finally {
            if (repository != null) {
                repository.shutdown();
            }
        }
    }

    /**
     * We have encountered a situation where the File System Repo is moving
     * files to archive and then eventually aging them off while there is still
     * an open file handle. This test is meant to replicate the conditions under
     * which this would happen and verify that it is fixed.
     *
     * The condition that caused this appears to be that a Process Session
     * created a Content Claim and then did not write to it. It then decremented
     * the claimant count (which reduced the count to 0). This was likely due to
     * creating the claim in ProcessSession.write(FlowFile, StreamCallback) and
     * then having an Exception thrown when the Process Session attempts to read
     * the current Content Claim. In this case, it would not ever get to the
     * point of calling FileSystemRepository.write().
     *
     * The above sequence of events is problematic because calling
     * FileSystemRepository.create() will remove the Resource Claim from the
     * 'writable claims queue' and expects that we will write to it. When we
     * call FileSystemRepository.write() with that Resource Claim, we return an
     * OutputStream that, when closed, will take care of adding the Resource
     * Claim back to the 'writable claims queue' or otherwise close the
     * FileOutputStream that is open for that Resource Claim. If
     * FileSystemRepository.write() is never called, or if the OutputStream
     * returned by that method is never closed, but the Content Claim is then
     * decremented to 0, we can get into a situation where we do archive the
     * content (because the claimant count is 0 and it is not in the 'writable
     * claims queue') and then eventually age it off, without ever closing the
     * OutputStream. We need to ensure that we do always close that Output
     * Stream.
     */
    @Test
    public void testMarkDestructableDoesNotArchiveIfStreamOpenAndNotWrittenTo() throws IOException, InterruptedException {
        FileSystemRepository repository = null;
        try {
            final List<Path> archivedPathsWithOpenStream = Collections.synchronizedList(new ArrayList<>());

            // We are creating our own 'local' repository in this test so shut down the one created in the setup() method
            shutdown();

            repository = new FileSystemRepository(nifiProperties) {
                @Override
                protected boolean archive(Path curPath) {
                    if (getOpenStreamCount() > 0) {
                        archivedPathsWithOpenStream.add(curPath);
                    }

                    return true;
                }
            };

            final StandardResourceClaimManager claimManager = new StandardResourceClaimManager();

            repository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP));
            repository.purge();

            final ContentClaim claim = repository.create(false);

            assertEquals(1, claimManager.getClaimantCount(claim.getResourceClaim()));

            int claimantCount = claimManager.decrementClaimantCount(claim.getResourceClaim());
            assertEquals(0, claimantCount);
            assertTrue(archivedPathsWithOpenStream.isEmpty());

            // This would happen when FlowFile repo is checkpointed, if Resource Claim has claimant count of 0.
            // Since the Resource Claim of interest is still 'writable', we should not archive it.
            claimManager.markDestructable(claim.getResourceClaim());

            // Wait for the archive thread to have a chance to run
            long totalSleepMillis = 0;
            final long startTime = System.nanoTime();
            while (archivedPathsWithOpenStream.isEmpty() && totalSleepMillis < 5000) {
                Thread.sleep(100L);
                totalSleepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            }

            // Should still be empty because we have a stream open to the file so we should
            // not actually try to archive the data.
            assertTrue(archivedPathsWithOpenStream.isEmpty());
            assertEquals(0, claimManager.getClaimantCount(claim.getResourceClaim()));
        } finally {
            if (repository != null) {
                repository.shutdown();
            }
        }
    }


    private byte[] readFully(final InputStream inStream, final int size) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
        int len;
        final byte[] buffer = new byte[size];
        while ((len = inStream.read(buffer)) >= 0) {
            baos.write(buffer, 0, len);
        }

        return baos.toByteArray();
    }
}
