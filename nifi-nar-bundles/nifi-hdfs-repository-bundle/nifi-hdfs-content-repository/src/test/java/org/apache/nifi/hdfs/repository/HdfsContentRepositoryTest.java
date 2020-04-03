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
package org.apache.nifi.hdfs.repository;

import static org.apache.nifi.hdfs.repository.HdfsContentRepository.ARCHIVE_DIR_NAME;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.ARCHIVE_GROUP_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.CORE_SITE_DEFAULT_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FAILURE_TIMEOUT_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.OPERATING_MODE_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.PRIMARY_GROUP_PROPERTY;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.SECTIONS_PER_CONTAINER;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.prop;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.props;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_ENABLED;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE;
import static org.apache.nifi.util.NiFiProperties.REPOSITORY_CONTENT_PREFIX;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.nimbusds.jose.util.StandardCharset;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaim;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class HdfsContentRepositoryTest {

    NiFiProperties basic = props(prop(REPOSITORY_CONTENT_PREFIX + "disk1", "file:target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "file:target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "99%"),
            prop(CONTENT_ARCHIVE_ENABLED, "true"));

    NiFiProperties archiveProps = props(prop(REPOSITORY_CONTENT_PREFIX + "disk1", "file:target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "file:target/test-repo2"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"),
            prop(OPERATING_MODE_PROPERTY, "Archive"), prop(ARCHIVE_GROUP_PROPERTY, "disk2"),
            prop(CONTENT_ARCHIVE_ENABLED, "true"));

    NiFiProperties fallback = props(prop(REPOSITORY_CONTENT_PREFIX + "disk1", "file:target/test-repo1"),
            prop(REPOSITORY_CONTENT_PREFIX + "disk2", "file:target/test-repo2"), prop(PRIMARY_GROUP_PROPERTY, "disk1"),
            prop(OPERATING_MODE_PROPERTY, "FailureFallback"), prop(FAILURE_TIMEOUT_PROPERTY, "15 seconds"),
            prop(CORE_SITE_DEFAULT_PROPERTY, "src/test/resources/empty-core-site.xml"));

    @Before
    public void setup() throws IOException {
        for (int i = 1; i <= 2; i++) {
            File repo = new File("target/test-repo" + i);
            cleanRepo(repo);
        }
    }

    /**
     * Deletes all files but keeps the driectory structure for the specified repository
     */
    protected static void cleanRepo(File repo) {
        if (!repo.isDirectory()) {
            repo.mkdirs();
        }

        for (int s = 0; s < SECTIONS_PER_CONTAINER; s++) {
            File section = new File(repo, "" + s);
            if (!section.isDirectory()) {
                // it's faster to do this here than the let hdfs raw file system do it
                new File(section, ARCHIVE_DIR_NAME).mkdirs();
                continue;
            }
            for (File file : section.listFiles()) {
                if (file.getName().equals(ARCHIVE_DIR_NAME)) {
                    continue;
                } else if (!file.delete()) {
                    throw new RuntimeException("Failed to delete previous file: " + file);
                }
            }
            for (File file : new File(section, ARCHIVE_DIR_NAME).listFiles()) {
                if (!file.delete()) {
                    throw new RuntimeException("Failed to delete previous file: " + file);
                }
            }
        }
    }

    @Test
    public void createTest() throws IOException {
        ResourceClaimManager claimManager = mockClaimManager();
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(claimManager);

            ContentClaim claim1 = repo.create(true);

            assertNotNull(claim1);
            assertNotNull(claim1.getResourceClaim());
            assertEquals("disk2", claim1.getResourceClaim().getContainer());
            assertTrue(claim1.getResourceClaim().getId(), claim1.getResourceClaim().getId().endsWith("-1"));
            assertEquals("1", claim1.getResourceClaim().getSection());
            assertEquals(0L, claim1.getOffset());
            assertEquals(-1, claim1.getLength());

            ContentClaim claim2 = repo.create(true);

            assertNotNull(claim2);
            assertNotNull(claim2.getResourceClaim());
            assertEquals("disk1", claim2.getResourceClaim().getContainer());
            assertTrue(claim2.getResourceClaim().getId(), claim2.getResourceClaim().getId().endsWith("-2"));
            assertEquals("2", claim2.getResourceClaim().getSection());
            assertEquals(0L, claim2.getOffset());
            assertEquals(-1, claim2.getLength());

            assertEquals(2, repo.getWritableClaimStreams().size());

            verify(claimManager, times(2)).newResourceClaim(anyString(), anyString(), anyString(), anyBoolean(),
                    anyBoolean());
            verify(claimManager, times(1)).incrementClaimantCount(eq(claim1.getResourceClaim()), eq(true));
            verify(claimManager, times(1)).incrementClaimantCount(eq(claim2.getResourceClaim()), eq(true));
            verify(claimManager, times(2)).incrementClaimantCount(any(), anyBoolean());
            verify(claimManager, times(0)).incrementClaimantCount(any());

            assertTrue(new File("target/test-repo2/1/" + claim1.getResourceClaim().getId()).isFile());
            assertTrue(new File("target/test-repo1/2/" + claim2.getResourceClaim().getId()).isFile());
        }
    }

    @Test
    public void spaceTest() throws IOException {
        File workDir = new File("target/test-repo1");

        // sanity check
        assertTrue(workDir.getTotalSpace() > 0);
        assertTrue(workDir.getFreeSpace() > 0);

        // make sure it works for the primary and secondary container groups
        try (HdfsContentRepository repo = new HdfsContentRepository(fallback)) {
            assertEquals(workDir.getTotalSpace(), repo.getContainerCapacity("disk1"));
            assertEquals(workDir.getFreeSpace(), repo.getContainerUsableSpace("disk1"));

            assertEquals(workDir.getTotalSpace(), repo.getContainerCapacity("disk2"));
            assertEquals(workDir.getFreeSpace(), repo.getContainerUsableSpace("disk2"));
        }
    }

    @Test
    public void normalGetActiveContainerTest() throws Exception {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            long duration = System.currentTimeMillis();

            Container one = repo.getActiveContainer(0L);
            Container two = repo.getActiveContainer(1L);

            assertNotNull(one);
            assertNotNull(two);
            assertNotEquals(one, two);

            one.setFull(true);

            // we should only get 'two' back now since 'one' is full
            assertEquals(two, repo.getActiveContainer(0L));
            assertEquals(two, repo.getActiveContainer(1L));
            assertEquals(two, repo.getActiveContainer(2L));
            assertEquals(two, repo.getActiveContainer(3L));

            duration = System.currentTimeMillis() - duration;
            assertTrue("getActiveContainer calls shouldn't take long because "
                    + "at least one container is active, but it took: " + duration + " ms", duration < 500);

        }
    }

    @Test
    public void delayedGetActiveContainerTest() throws Exception {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {

            Container one = repo.getActiveContainer(0L);
            Container two = repo.getActiveContainer(1L);

            one.setFull(true);
            two.setFull(true);

            //
            // this is kinda tough to test, but we want to make sure
            // getActiveContainer blocks if there are no containers active
            //
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                AtomicLong ready = new AtomicLong(0);

                // have a separate thread try to acquire an active container
                // it should get blocked because there aren't any currently active.
                Future<Long> future = executor.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        long waitTime = System.currentTimeMillis();
                        ready.set(waitTime);
                        assertEquals(one, repo.getActiveContainer(0L));
                        return System.currentTimeMillis() - waitTime;
                    }
                });

                // wait for the thread to be ready
                while (ready.get() == 0) {
                    Thread.sleep(10);
                }

                // block the thread for awhile, then make one of the containers active
                Thread.sleep(400);
                one.setFull(false);

                // get how long getActiveContainer blocked for
                long duration = future.get();

                // make sure it blocked for pretty close to a half a second,
                // it's possible it takes longer (maybe a second) depending on thread scheduling
                assertTrue("" + duration, duration >= 450 && duration < 2000);

            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void writeTest() throws IOException {
        try (HdfsContentRepository repo = spy(new HdfsContentRepository(basic))) {
            repo.initialize(mockClaimManager());

            ContentClaim claim = repo.create(true);

            byte[] testData = "this is test data!".getBytes(StandardCharset.UTF_8);
            ClaimOutputStream claimStream;
            try (OutputStream outStream = repo.write(claim)) {
                claimStream = (ClaimOutputStream) outStream;
                outStream.write(testData);
            }

            verify(repo, times(1)).claimClosed(eq(claimStream));

            assertEquals(0, claim.getOffset());
            assertEquals(testData.length, claim.getLength());

            ResourceClaim resource = claim.getResourceClaim();
            File claimFile = new File("target/test-repo2/" + resource.getSection() + "/" + resource.getId());
            assertTrue(claimFile.isFile());
            assertArrayEquals(testData, FileUtils.readFileToByteArray(claimFile));
            assertTrue(resource.isWritable());
            assertEquals(1, repo.getWritableClaimStreams().size());
        }
    }

    @Test
    public void writeEmptyClaimTest() throws IOException {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager(false));

            ContentClaim claim = repo.create(true);

            repo.write(claim).close();

            assertEquals(0, claim.getLength());

            File claimFile = new File("target/test-repo2/1/" + claim.getResourceClaim().getId());
            assertTrue(claimFile.isFile());
            assertEquals(0, claimFile.length());

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            try (InputStream inStream = repo.read(claim)) {
                IOUtils.copy(inStream, bytesOut);
            }
            assertEquals(0, bytesOut.size());
        }
    }

    @Test
    public void reuseClaimTest() throws IOException {
        ResourceClaimManager claimManager = mockClaimManager();
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(claimManager);

            ContentClaim claim1 = repo.create(true);

            byte[] testData1 = "this is test data!".getBytes(StandardCharset.UTF_8);
            byte[] testData2 = " more test data!".getBytes(StandardCharset.UTF_8);
            byte[] combined = "this is test data! more test data!".getBytes(StandardCharset.UTF_8);

            try (OutputStream outStream = repo.write(claim1)) {
                outStream.write(testData1);
            }

            ContentClaim claim2 = repo.create(true);

            try (OutputStream outStream = repo.write(claim2)) {
                outStream.write(testData2);
            }

            assertEquals(0, claim1.getOffset());
            assertEquals(testData1.length, claim1.getLength());

            assertEquals(testData1.length, claim2.getOffset());
            assertEquals(testData2.length, claim2.getLength());

            ResourceClaim resource1 = claim1.getResourceClaim();
            ResourceClaim resource2 = claim2.getResourceClaim();

            assertEquals(1, repo.getWritableClaimStreams().size());

            assertEquals(resource1, resource2);
            assertEquals(resource1.getContainer(), resource2.getContainer());
            assertEquals(resource1.getSection(), resource2.getSection());
            assertEquals(resource1.getId(), resource2.getId());

            File claimFile = new File("target/test-repo2/" + resource1.getSection() + "/" + resource1.getId());
            assertTrue(claimFile.isFile());
            assertArrayEquals(combined, FileUtils.readFileToByteArray(claimFile));

            verify(claimManager, times(1)).incrementClaimantCount(eq(resource1), eq(true));
            verify(claimManager, times(1)).incrementClaimantCount(eq(resource1), eq(false));
        }
    }

    @Test
    public void readNullClaimTest() throws IOException {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            try (InputStream inStream = repo.read(null)) {
                IOUtils.copy(inStream, bytesOut);
            }
            assertEquals(0, bytesOut.size());
        }
    }

    @Test
    public void readEmptyClaimTest() throws IOException {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager(false));

            ContentClaim claim = repo.create(true);

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            try (InputStream inStream = repo.read(claim)) {
                IOUtils.copy(inStream, bytesOut);
            }
            assertEquals(0, bytesOut.size());
        }
    }

    @Test
    public void readActiveClaimTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "test read data!";
        byte[] data = dataStr.getBytes(StandardCharset.UTF_8);
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(
                new StandardResourceClaim(null, "disk1", "0", "claim-0", true), 0);
        claim.setLength(data.length);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            byte[] readBytes;
            try (InputStream inStream = repo.read(claim)) {
                readBytes = IOUtils.toByteArray(inStream);
            }

            assertArrayEquals(new String(readBytes), data, readBytes);
        }
    }

    @Test
    public void readArchivedClaimTest() throws IOException {
        File outFile = new File("target/test-repo1/0/archive/claim-0");
        String dataStr = "test archived data!";
        byte[] data = dataStr.getBytes(StandardCharset.UTF_8);
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(
                new StandardResourceClaim(null, "disk1", "0", "claim-0", true), 0);
        claim.setLength(data.length);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            byte[] readBytes;
            try (InputStream inStream = repo.read(claim)) {
                readBytes = IOUtils.toByteArray(inStream);
            }

            assertArrayEquals(new String(readBytes), data, readBytes);
        }
    }

    @Test
    public void readMultiClaimTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "onetwothreefour";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        ResourceClaim resource = new StandardResourceClaim(null, "disk1", "0", "claim-0", true);

        StandardContentClaim claim1 = new StandardContentClaim(resource, 0);
        claim1.setLength(3);

        StandardContentClaim claim2 = new StandardContentClaim(resource, 3);
        claim2.setLength(3);

        StandardContentClaim claim3 = new StandardContentClaim(resource, 6);
        claim3.setLength(5);

        StandardContentClaim claim4 = new StandardContentClaim(resource, 11);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            byte[] readBytes1;
            try (InputStream inStream = repo.read(claim1)) {
                readBytes1 = IOUtils.toByteArray(inStream);
            }

            byte[] readBytes2;
            try (InputStream inStream = repo.read(claim2)) {
                readBytes2 = IOUtils.toByteArray(inStream);
            }

            byte[] readBytes3;
            try (InputStream inStream = repo.read(claim3)) {
                readBytes3 = IOUtils.toByteArray(inStream);
            }

            byte[] readBytes4;
            try (InputStream inStream = repo.read(claim4)) {
                readBytes4 = IOUtils.toByteArray(inStream);
            }

            assertEquals("one", new String(readBytes1));
            assertEquals("two", new String(readBytes2));
            assertEquals("three", new String(readBytes3));
            assertEquals("four", new String(readBytes4));
        }
    }

    @Test
    public void readThenWriteThenReadTest() throws IOException {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager());
            ContentClaim claim = repo.create(false);

            InputStream in = null;
            try {
                try (OutputStream outStream = repo.write(claim)) {
                    outStream.write("hello".getBytes());
                    outStream.flush();

                    in = repo.read(claim);
                    byte[] buffer = new byte[5];
                    StreamUtils.fillBuffer(in, buffer);

                    assertEquals("hello", new String(buffer));

                    outStream.write("good-bye".getBytes());
                }

                final byte[] buffer = new byte[8];
                StreamUtils.fillBuffer(in, buffer);
                assertEquals("good-bye", new String(buffer));
            } finally {
                if (in != null) {
                    in.close();
                }
            }
        }
    }

    @Test
    public void importTest() throws IOException {
        File outFile = new File("target/test-file");
        String dataStr = "test import data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager());

            ContentClaim claim = repo.create(true);

            assertEquals(dataStr.length(), repo.importFrom(outFile.toPath(), claim));

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            try (InputStream inStream = repo.read(claim)) {
                IOUtils.copy(inStream, bytesOut);
            }

            assertEquals(dataStr, bytesOut.toString());
        }
    }

    @Test
    public void exportTest() throws IOException {
        File sourceFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "onetwothreefour";
        FileUtils.writeStringToFile(sourceFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(createClaim("disk1", "0", "claim-0", true, false), 0);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            assertEquals(dataStr.length(), repo.exportTo(claim, bytesOut));
            assertEquals(dataStr, bytesOut.toString());

            bytesOut.reset();
            assertEquals(8, repo.exportTo(claim, bytesOut, 3, 8));
            assertEquals(dataStr.substring(3, 11), bytesOut.toString());

            File outFile = new File("target/test-export-file");
            if (outFile.isFile()) {
                assertTrue(outFile.delete());
            }

            assertEquals(dataStr.length(), repo.exportTo(claim, outFile.toPath(), false));
            assertTrue(outFile.isFile());
            assertEquals(dataStr, FileUtils.readFileToString(outFile, StandardCharsets.UTF_8));

            assertTrue(outFile.delete());

            assertEquals(8, repo.exportTo(claim, outFile.toPath(), false, 3, 8));
            assertTrue(outFile.isFile());
            assertEquals(dataStr.substring(3, 11), FileUtils.readFileToString(outFile, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void validateClaimTest() {
        try {
            HdfsContentRepository.validateContentClaimForWriting(null);
            fail("Expected NPE to be thrown because claim is null.");
        } catch (NullPointerException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("ContentClaim cannot be null"));
        }

        try {
            HdfsContentRepository.validateContentClaimForWriting(Mockito.mock(ContentClaim.class));
            fail("Expected exception because the claim is not a StandardContentClaim.");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("Content Claim does belong to this Content Repository"));
        }

        try {
            StandardContentClaim claim = new StandardContentClaim(null, 0);
            claim.setLength(10);
            HdfsContentRepository.validateContentClaimForWriting(claim);
            fail("Expected exception because the claim is already written to (has a length).");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("already been written to"));
        }

        assertNotNull(HdfsContentRepository.validateContentClaimForWriting(new StandardContentClaim(null, 0)));
    }

    @Test
    public void mergeTest() throws IOException {
        File sourceFile1 = new File("target/test-repo2/5/claim-5");
        FileUtils.writeStringToFile(sourceFile1, "one", StandardCharset.UTF_8);
        StandardContentClaim claim1 = new StandardContentClaim(createClaim("disk2", "5", "claim-5", true, false), 0);
        claim1.setLength(3);

        File sourceFile2 = new File("target/test-repo1/6/claim-6");
        FileUtils.writeStringToFile(sourceFile2, "two", StandardCharset.UTF_8);
        StandardContentClaim claim2 = new StandardContentClaim(createClaim("disk1", "6", "claim-6", true, false), 0);
        claim2.setLength(3);

        File sourceFile3 = new File("target/test-repo2/7/claim-7");
        FileUtils.writeStringToFile(sourceFile3, "three", StandardCharset.UTF_8);
        StandardContentClaim claim3 = new StandardContentClaim(createClaim("disk2", "7", "claim-7", true, false), 0);
        claim3.setLength(5);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager());

            ContentClaim dest = repo.create(true);

            byte[] header = "--HEAD--".getBytes(StandardCharset.UTF_8);
            byte[] footer = "--FOOT--".getBytes(StandardCharset.UTF_8);
            byte[] separator = "--SEP--".getBytes(StandardCharset.UTF_8);

            String expected = "--HEAD--one--SEP--two--SEP--three--FOOT--";

            assertEquals(expected.length(), repo.merge(Arrays.asList(claim1, claim2, claim3), dest, header, footer, separator));

            File outFile = new File("target/test-repo2/1/" + dest.getResourceClaim().getId());
            assertEquals(expected, FileUtils.readFileToString(outFile, StandardCharset.UTF_8));
        }
    }

    @Test
    public void cloneTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "test clone data!";
        byte[] data = dataStr.getBytes(StandardCharset.UTF_8);
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        ResourceClaimManager claimManager = mockClaimManager();

        StandardContentClaim claim = new StandardContentClaim(
                claimManager.newResourceClaim("disk1", "0", "claim-0", true, true), 0);
        claim.setLength(data.length);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(claimManager);

            assertEquals(0, repo.getWritableClaimStreams().size());

            ContentClaim cloned = repo.clone(claim, true);

            assertEquals(1, repo.getWritableClaimStreams().size());

            assertNotNull(cloned);

            assertTrue(outFile.isFile());

            File clonedFile = new File("target/test-repo2/1/" + cloned.getResourceClaim().getId());

            assertTrue(clonedFile.isFile());

            // this isn't really necessary or true in all cases, but it will be for this
            // test
            assertNotEquals(clonedFile.getPath(), outFile.getPath());

            String clonedText = FileUtils.readFileToString(clonedFile, StandardCharsets.UTF_8);
            assertEquals(dataStr, clonedText);
        }
    }

    @Test
    public void archiveInactiveTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "test more archive data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardResourceClaim claim = createClaim("disk1", "0", "claim-0", true, false);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            assertTrue(repo.archiveClaim(claim));

            assertFalse(outFile.isFile());

            File archived = new File(new File(outFile.getParentFile(), ARCHIVE_DIR_NAME), "claim-0");
            assertTrue(archived.isFile());

            String archivedStr = FileUtils.readFileToString(archived, StandardCharsets.UTF_8);
            assertEquals(dataStr, archivedStr);
        }
    }

    @Test
    public void archiveActiveTest() throws IOException {

        ResourceClaimManager claimManager = mockClaimManager(false);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(claimManager);

            ContentClaim claim = repo.create(true);

            File claimFile = new File("target/test-repo2/1/" + claim.getResourceClaim().getId());

            assertEquals(1, repo.getWritableClaimStreams().size());
            assertTrue(claimFile.isFile());

            // this should close the write stream and move the claim to the archive directory.
            assertTrue(repo.archiveClaim(claim.getResourceClaim()));

            assertEquals(0, repo.getWritableClaimStreams().size());
            assertFalse(claimFile.isFile());

            File archived = new File(new File(claimFile.getParentFile(), ARCHIVE_DIR_NAME), claim.getResourceClaim().getId());
            assertTrue(archived.isFile());
        }
    }

    @Test
    public void normalGetStatusTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "test get normal status data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(
                new StandardResourceClaim(null, "disk1", "0", "claim-0", true), 0);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            ClaimStatus status = repo.getStatus(claim, false);

            assertNotNull(status);
            assertEquals("disk1", status.getContainer().getName());
            assertEquals(dataStr.length(), status.getSize());
            assertTrue(new File(status.getPath().toUri()).isFile());
        }
    }

    @Test
    public void archivedGetStatusTest() throws IOException {
        File outFile = new File("target/test-repo1/0/archive/claim-0");
        String dataStr = "test get archived status data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(
                new StandardResourceClaim(null, "disk1", "0", "claim-0", true), 0);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            ClaimStatus status = repo.getStatus(claim, false);

            assertNotNull(status);
            assertEquals("disk1", status.getContainer().getName());
            assertEquals(dataStr.length(), status.getSize());
            assertTrue(new File(status.getPath().toUri()).isFile());
        }
    }

    @Test
    public void archivedContainerGetStatusTest() throws IOException {
        File outFile = new File("target/test-repo2/0/archive/claim-0");
        String dataStr = "test get archived container status data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(
                new StandardResourceClaim(null, "disk1", "0", "claim-0", true), 0);

        try (HdfsContentRepository repo = new HdfsContentRepository(archiveProps)) {
            ClaimStatus status = repo.getStatus(claim, false);

            assertNotNull(status);
            assertEquals("disk2", status.getContainer().getName());
            assertEquals(dataStr.length(), status.getSize());
            assertTrue(new File(status.getPath().toUri()).isFile());
        }
    }

    @Test
    public void sizeUnknownLengthTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "test get size data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardContentClaim claim = new StandardContentClaim(
                new StandardResourceClaim(null, "disk1", "0", "claim-0", true), 0);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            assertEquals(dataStr.length(), repo.size(claim));
        }
    }

    @Test
    public void removeNormalTest() throws IOException {
        File outFile = new File("target/test-repo1/0/claim-0");
        String dataStr = "test remove normal data!";
        FileUtils.writeStringToFile(outFile, dataStr, StandardCharset.UTF_8);

        StandardResourceClaim claim = createClaim("disk1", "0", "claim-0", true, false);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            assertTrue(repo.remove(claim));
        }

        assertFalse(outFile.isFile());
    }

    @Test
    public void removeNonExistentClaimTest() throws IOException {
        StandardResourceClaim claim = createClaim("disk1", "0", "claim-0", true, false);

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            assertTrue(repo.remove(claim));
        }
    }

    @Test
    public void removeRecentlyCreatedTest() throws IOException {
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager(false));

            ContentClaim claim = repo.create(true);

            assertEquals(1, repo.getWritableClaimStreams().size());

            assertTrue(repo.remove(claim));

            assertEquals(0, repo.getWritableClaimStreams().size());
        }
    }

    @Test
    public void cleanupTest() throws IOException {
        ResourceClaimManager claimManager = mockClaimManager();

        // every other section is going to be considered a no longer
        // necessary content claim that should be removed/archived
        when(claimManager.getClaimantCount(any())).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ResourceClaim claim = invocation.getArgument(0);
                if (Integer.parseInt(claim.getSection()) % 2 == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });

        List<File> allClaimFiles = new ArrayList<>();

        // create 100 fake content claims
        for (int i = 0; i < 100; i++) {
            File file = new File("target/test-repo" + ((i % 2) + 1) + "/" + i + "/claim-" + i);
            FileUtils.touch(file);
            allClaimFiles.add(file);

            // sanity check
            assertTrue(file.isFile());
        }

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(claimManager);

            repo.cleanup();

            // every other file should be archived
            for (int i = 0; i < 100; i++) {
                File checkFile = allClaimFiles.get(i);
                if (i % 2 == 0) {
                    assertFalse(checkFile.isFile());
                    File archiveFile = new File(new File(checkFile.getParentFile(), ARCHIVE_DIR_NAME),
                            checkFile.getName());
                    assertTrue(archiveFile.isFile());
                } else {
                    assertTrue(checkFile.isFile());
                }
            }
        }
    }

    @Test
    public void getActiveResourceClaimsTest() throws IOException {
        ResourceClaimManager claimManager = mockClaimManager();

        // create 100 fake content claims
        for (int i = 0; i < 100; i++) {
            File file = new File("target/test-repo" + ((i % 2) + 1) + "/" + i + "/claim-" + i);
            FileUtils.touch(file);

            // sanity check
            assertTrue(file.isFile());
        }

        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(claimManager);

            Set<ResourceClaim> claims1 = repo.getActiveResourceClaims("disk1");
            assertEquals(50, claims1.size());
            for (ResourceClaim claim : claims1) {
                assertEquals("claim-" + claim.getSection(), claim.getId());
                assertTrue(Integer.parseInt(claim.getSection()) % 2 == 0);
            }

            Set<ResourceClaim> claims2 = repo.getActiveResourceClaims("disk2");
            assertEquals(50, claims2.size());
            for (ResourceClaim claim : claims2) {
                assertEquals("claim-" + claim.getSection(), claim.getId());
                assertTrue(Integer.parseInt(claim.getSection()) % 2 == 1);
            }

        }
    }

    @Test
    public void shutdownTest() throws IOException {
        // make sure open write streams are closed when shutdown is called
        try (HdfsContentRepository repo = new HdfsContentRepository(basic)) {
            repo.initialize(mockClaimManager());

            ContentClaim claim1 = repo.create(true);
            ContentClaim claim2 = repo.create(true);
            ContentClaim claim3 = repo.create(true);

            ResourceClaim resource1 = claim1.getResourceClaim();
            ResourceClaim resource2 = claim2.getResourceClaim();
            ResourceClaim resource3 = claim3.getResourceClaim();

            List<ResourceClaim> resources = Arrays.asList(resource1, resource2, resource3);

            assertEquals(3, repo.getWritableClaimStreams().size());

            for (ResourceClaim resource : resources) {
                repo.getWritableClaimStreams().put(resource, spy(repo.getWritableClaimStreams().get(resource)));
                verify(repo.getWritableClaimStreams().get(resource), times(0)).close();
            }

            repo.shutdown();

            for (ResourceClaim resource : resources) {
                verify(repo.getWritableClaimStreams().get(resource), times(1)).close();
            }
        }
    }

    private ResourceClaimManager mockClaimManager() {
        return mockClaimManager(true);
    }
    private ResourceClaimManager mockClaimManager(boolean inUse) {
        ResourceClaimManager claimManager = mock(ResourceClaimManager.class);
        when(claimManager.newResourceClaim(anyString(), anyString(), anyString(), anyBoolean(), anyBoolean()))
                .thenAnswer(new NewResourceClaim(inUse));
        return claimManager;
    }

    @Ignore
    private static class NewResourceClaim implements Answer<ResourceClaim> {

        private final boolean inUse;

        public NewResourceClaim(boolean inUse) {
            this.inUse = inUse;
        }

        /** Args are:
         * String container, String section, String id, boolean lossTolerant, boolean writable
         */
        @Override
        public ResourceClaim answer(InvocationOnMock invocation) throws Throwable {
            String container = invocation.getArgument(0);
            String section = invocation.getArgument(1);
            String id = invocation.getArgument(2);
            boolean lossTolerant = invocation.getArgument(3);

            return createClaim(container, section, id, lossTolerant, inUse);
        }
    }

    private static StandardResourceClaim createClaim(String container, String section, String id, boolean lossTolerant, boolean inUse) {
        return new StandardResourceClaim(null, container, section, id, true) {
            @Override
            public boolean isInUse() {
                return inUse;
            }
        };
    }

}
