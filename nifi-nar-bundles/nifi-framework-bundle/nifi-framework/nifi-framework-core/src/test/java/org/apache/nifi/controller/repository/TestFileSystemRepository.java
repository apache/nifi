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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.util.DiskUtils;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

public class TestFileSystemRepository {

    public static final int NUM_REPO_SECTIONS = 1;

    public static final File helloWorldFile = new File("src/test/resources/hello.txt");

    private FileSystemRepository repository = null;
    private final File rootFile = new File("target/content_repository");

    @Before
    public void setup() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
        if (rootFile.exists()) {
            DiskUtils.deleteRecursively(rootFile);
        }
        repository = new FileSystemRepository();
        repository.initialize(new StandardResourceClaimManager());
        repository.purge();
    }

    @After
    public void shutdown() throws IOException {
        repository.shutdown();
    }

    @Test
    public void testMinimalArchiveCleanupIntervalHonoredAndLogged() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> testAppender = new ListAppender<>();
        testAppender.setName("Test");
        testAppender.start();
        root.addAppender(testAppender);
        NiFiProperties.getInstance().setProperty(NiFiProperties.CONTENT_ARCHIVE_CLEANUP_FREQUENCY, "1 millis");
        repository = new FileSystemRepository();
        repository.initialize(new StandardResourceClaimManager());
        repository.purge();


        boolean messageFound = false;
        String message = "The value of nifi.content.repository.archive.cleanup.frequency property "
                + "is set to '1 millis' which is below the allowed minimum of 1 second (1000 milliseconds). "
                + "Minimum value of 1 sec will be used as scheduling interval for archive cleanup task.";
        for (ILoggingEvent event : testAppender.list) {
            String actualMessage = event.getFormattedMessage();
            if (actualMessage.equals(message)) {
                assertEquals(event.getLevel(), Level.WARN);
                messageFound = true;
                break;
            }
        }
        assertTrue(messageFound);
    }

    @Test
    public void testBogusFile() throws IOException {
        repository.shutdown();
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");

        File bogus = new File(rootFile, "bogus");
        try {
            bogus.mkdir();
            bogus.setReadable(false);

            repository = new FileSystemRepository();
            repository.initialize(new StandardResourceClaimManager());
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

        try (final OutputStream out = repository.write(claim1)) {
        }

        final ContentClaim claim3 = repository.create(false);
        assertEquals(claim1.getResourceClaim(), claim3.getResourceClaim());
    }

    @Test
    public void testResourceClaimNotReusedAfterRestart() throws IOException, InterruptedException {
        final ContentClaim claim1 = repository.create(false);
        try (final OutputStream out = repository.write(claim1)) {
        }

        repository.shutdown();
        Thread.sleep(1000L);

        repository = new FileSystemRepository();
        repository.initialize(new StandardResourceClaimManager());
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
        try (final OutputStream out = repository.write(claim2)) {

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

        // Create the file.
        try (final OutputStream out = Files.newOutputStream(claimPath, StandardOpenOption.CREATE)) {
            out.write("Hello".getBytes());
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
            throw new RuntimeException("Could not invoke #getPath on FileSystemRepository due to " + e.toString());
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
        assertTrue(Arrays.equals(expected, data));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final InputStream in = repository.read(claim)) {
            StreamUtils.copy(in, baos);
        }

        assertTrue(Arrays.equals(expected, baos.toByteArray()));
    }


    @Test
    public void testImportFromStream() throws IOException {
        final ContentClaim claim = repository.create(false);
        final byte[] data = "hello".getBytes();
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        repository.importFrom(bais, claim);

        final Path claimPath = getPath(claim);
        assertTrue(Arrays.equals(data, Files.readAllBytes(claimPath)));
    }

    @Test
    public void testExportToOutputStream() throws IOException {
        final ContentClaim claim = repository.create(true);
        final Path path = getPath(claim);

        Files.createDirectories(path.getParent());
        Files.copy(helloWorldFile.toPath(), path, StandardCopyOption.REPLACE_EXISTING);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        repository.exportTo(claim, baos);
        final byte[] data = baos.toByteArray();
        assertTrue(Arrays.equals(Files.readAllBytes(helloWorldFile.toPath()), data));
    }

    @Test
    public void testExportToFile() throws IOException {
        final ContentClaim claim = repository.create(true);
        final Path path = getPath(claim);

        Files.createDirectories(path.getParent());
        Files.copy(helloWorldFile.toPath(), path, StandardCopyOption.REPLACE_EXISTING);
        final File outFile = new File("target/testExportToFile");
        final Path outPath = outFile.toPath();
        Files.deleteIfExists(outPath);

        final byte[] expected = Files.readAllBytes(helloWorldFile.toPath());

        repository.exportTo(claim, outPath, false);
        assertTrue(Arrays.equals(expected, Files.readAllBytes(outPath)));

        repository.exportTo(claim, outPath, true);
        final byte[] doubleExpected = new byte[expected.length * 2];
        System.arraycopy(expected, 0, doubleExpected, 0, expected.length);
        System.arraycopy(expected, 0, doubleExpected, expected.length, expected.length);
        assertTrue(Arrays.equals(doubleExpected, Files.readAllBytes(outPath)));
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

    @Test(expected = ContentNotFoundException.class)
    public void testSizeWithNoContent() throws IOException {
        final ContentClaim claim = repository.create(true);
        assertEquals(0L, repository.size(claim));
    }

    @Test(expected = ContentNotFoundException.class)
    public void testReadWithNoContent() throws IOException {
        final ContentClaim claim = repository.create(true);
        final InputStream in = repository.read(claim);
        in.close();
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
            assertTrue(Arrays.equals(data, dataRead));
        }
    }

    @Test
    public void testWrite() throws IOException {
        final ContentClaim claim = repository.create(true);
        final byte[] data = "The quick brown fox jumps over the lazy dog".getBytes();
        try (final OutputStream out = repository.write(claim)) {
            out.write(data);
        }

        final Path path = getPath(claim);
        assertTrue(Arrays.equals(data, Files.readAllBytes(path)));
    }

    @Test
    public void testRemoveWhileWritingToClaim() throws IOException {
        final ContentClaim claim = repository.create(false);
        final OutputStream out = repository.write(claim);

        // write at least 1 MB to the output stream so that when we close the output stream
        // the repo won't keep the stream open.
        final byte[] buff = new byte[1024 * 1024];
        out.write(buff);
        out.write(buff);

        // true because claimant count is still 1.
        assertTrue(repository.remove(claim));

        assertEquals(0, repository.decrementClaimantCount(claim));

        // false because claimant count is 0 but there is an 'active' stream for the claim
        assertFalse(repository.remove(claim));

        out.close();
        assertTrue(repository.remove(claim));
    }

    @Test
    public void testMergeWithHeaderFooterDemarcator() throws IOException {
        testMerge("HEADER", "FOOTER", "DEMARCATOR");
    }

    @Test
    public void testMergeWithHeaderFooter() throws IOException {
        testMerge("HEADER", "FOOTER", null);
    }

    @Test
    public void testMergeWithHeaderOnly() throws IOException {
        testMerge("HEADER", null, null);
    }

    @Test
    public void testMergeWithFooterOnly() throws IOException {
        testMerge(null, "FOOTER", null);
    }

    @Test
    public void testMergeWithDemarcator() throws IOException {
        testMerge(null, null, "DEMARCATOR");
    }

    @Test
    public void testWithHeaderDemarcator() throws IOException {
        testMerge("HEADER", null, "DEMARCATOR");
    }

    @Test
    public void testMergeWithFooterDemarcator() throws IOException {
        testMerge(null, "FOOTER", "DEMARCATOR");
    }

    @Test
    public void testMergeWithoutHeaderFooterDemarcator() throws IOException {
        testMerge(null, null, null);
    }

    private void testMerge(final String header, final String footer, final String demarcator) throws IOException {
        final int count = 4;
        final String content = "The quick brown fox jumps over the lazy dog";
        final List<ContentClaim> claims = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final ContentClaim claim = repository.create(true);
            claims.add(claim);
            try (final OutputStream out = repository.write(claim)) {
                out.write(content.getBytes());
            }
        }

        final ContentClaim destination = repository.create(true);
        final byte[] headerBytes = header == null ? null : header.getBytes();
        final byte[] footerBytes = footer == null ? null : footer.getBytes();
        final byte[] demarcatorBytes = demarcator == null ? null : demarcator.getBytes();
        repository.merge(claims, destination, headerBytes, footerBytes, demarcatorBytes);

        final StringBuilder sb = new StringBuilder();
        if (header != null) {
            sb.append(header);
        }
        for (int i = 0; i < count; i++) {
            sb.append(content);
            if (demarcator != null && i != count - 1) {
                sb.append(demarcator);
            }
        }
        if (footer != null) {
            sb.append(footer);
        }
        final String expectedText = sb.toString();
        final byte[] expected = expectedText.getBytes();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) destination.getLength());
        try (final InputStream in = repository.read(destination)) {
            StreamUtils.copy(in, baos);
        }
        final byte[] actual = baos.toByteArray();
        assertTrue(Arrays.equals(expected, actual));
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
