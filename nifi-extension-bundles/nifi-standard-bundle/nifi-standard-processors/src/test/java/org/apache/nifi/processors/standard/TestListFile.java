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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListProcessorTestWatcher;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Test only runs on *nix")
public class TestListFile {

    private static boolean isMillisecondSupported = false;

    private final String TESTDIR = "target/test/data/in";
    private final File testDir = new File(TESTDIR);
    private ListFile processor;
    private TestRunner runner;
    private ProcessContext context;

    // Testing factors in milliseconds for file ages that are configured on each run by resetAges()
    // age#millis are relative time references
    // time#millis are absolute time references
    // age#filter are filter label strings for the filter properties
    private Long syncTime = getTestModifiedTime();
    private Long time0millis, time1millis, time2millis, time3millis, time4millis, time5millis;
    private String age0, age2, age4, age5;

    @RegisterExtension
    private final ListProcessorTestWatcher dumpState = new ListProcessorTestWatcher(
            () -> {
                try {
                    return runner.getStateManager().getState(Scope.LOCAL).toMap();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to retrieve state", e);
                }
            },
            () -> listFiles(testDir).stream()
                    .map(f -> new FileInfo.Builder().filename(f.getName()).lastModifiedTime(f.lastModified()).build()).collect(Collectors.toList()),
            () -> runner.getFlowFilesForRelationship(AbstractListProcessor.REL_SUCCESS).stream().map(m -> (FlowFile) m).collect(Collectors.toList())
    );

    @BeforeAll
    public static void setupClass() throws Exception {
        // This only has to be done once.
        final File file = Files.createTempFile(Paths.get("target/"), "TestListFile", null).toFile();
        file.setLastModified(325990917351L);
        isMillisecondSupported = file.lastModified() % 1_000 > 0;
    }

    @BeforeEach
    public void setUp() throws Exception {
        processor = new ListFile();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION, AbstractListProcessor.PRECISION_SECONDS.getValue());
        context = runner.getProcessContext();
        deleteDirectory(testDir);
        assertTrue(testDir.exists() || testDir.mkdirs(), "Unable to create test data directory " + testDir.getAbsolutePath());
        resetAges();
    }

    public void tearDown() {
        deleteDirectory(testDir);
    }

    private List<File> listFiles(final File file) {
        if (file.isDirectory()) {
            final List<File> result = new ArrayList<>();
            Optional.ofNullable(file.listFiles()).ifPresent(files -> Arrays.stream(files).forEach(f -> result.addAll(listFiles(f))));
            return result;
        } else {
            return Collections.singletonList(file);
        }
    }

    /**
     * This method ensures runner clears transfer state,
     * and sleeps the current thread for specific period defined at {@link AbstractListProcessor#LISTING_LAG_MILLIS}
     * based on local filesystem timestamp precision before executing runner.run().
     */
    private void runNext() throws InterruptedException {
        runner.clearTransferState();

        listFiles(testDir);
        final Long lagMillis;
        if (isMillisecondSupported) {
            lagMillis = AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.MILLISECONDS);
        } else {
            // Filesystems such as Mac OS X HFS (Hierarchical File System) or EXT3 are known that only support timestamp in seconds precision.
            lagMillis = AbstractListProcessor.LISTING_LAG_MILLIS.get(TimeUnit.SECONDS);
        }
        Thread.sleep(lagMillis * 2);

        final long startedAtMillis = System.currentTimeMillis();
        runner.run();
        dumpState.dumpState(startedAtMillis);
    }

    @Test
    public void testGetPath() {
        runner.setProperty(ListFile.DIRECTORY, "/dir/test1");
        assertEquals("/dir/test1", processor.getPath(context));
        runner.setProperty(ListFile.DIRECTORY, "${literal(\"/DIR/TEST2\"):toLower()}");
        assertEquals("/dir/test2", processor.getPath(context));
    }

    @Test
    public void testPerformListingBlankDirectoryFailed() throws Exception {
        runner.setProperty(ListFile.DIRECTORY, "${literal('')}");
        runNext();
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);

        final MockComponentLog logger = runner.getLogger();
        final List<LogMessage> errorMessages = logger.getErrorMessages();
        assertFalse(errorMessages.isEmpty());

        final LogMessage firstLogMessage = errorMessages.getFirst();
        assertTrue(firstLogMessage.getMsg().contains("Blank"));
    }

    @Test
    public void testPerformListing() throws Exception {

        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runNext();
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);

        // create first file
        final File file1 = new File(TESTDIR + "/listing1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(time4millis));

        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 1 object.  Of that, 1 matches the filter.");

        // process first file and set new timestamp
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles1.size());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 1 object.  Of that, 1 matches the filter.");

        // create second file
        final File file2 = new File(TESTDIR + "/listing2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(time2millis));
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 2 objects.  Of those, 2 match the filter.");

        // process second file after timestamp
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 2 objects.  Of those, 2 match the filter.");

        // create third file
        final File file3 = new File(TESTDIR + "/listing3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(time4millis));
        // 0 are new because the timestamp is before the min listed timestamp
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");

        // process third file before timestamp
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(0, successFiles3.size());

        // force state to reset and process all files
        runner.removeProperty(ListFile.DIRECTORY);
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles4.size());

        runNext();
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");
    }

    @Test
    public void testPathFilterOnlyPicksUpMatchingFiles() throws IOException {
        final File aaa = new File(TESTDIR, "aaa");
        assertTrue(aaa.mkdirs() || aaa.exists());

        final File bbb = new File(TESTDIR, "bbb");
        assertTrue(bbb.mkdirs() || bbb.exists());

        final File file1 = new File(aaa, "1.txt");
        final File file2 = new File(bbb, "2.txt");
        final File file3 = new File(TESTDIR, "3.txt");

        final long tenSecondsAgo = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10L);
        for (final File file : Arrays.asList(file1, file2, file3)) {
            assertTrue(file.createNewFile() || file.exists());
            assertTrue(file.setLastModified(tenSecondsAgo));
        }

        runner.setProperty(ListFile.DIRECTORY, TESTDIR);
        runner.setProperty(ListFile.PATH_FILTER, ".+");
        runner.run();

        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS, 2);
    }

    @Test
    public void testFilterAge() throws Exception {
        final File file0 = new File(TESTDIR + "/age0.txt");
        assertTrue(file0.createNewFile());

        final File file2 = new File(TESTDIR + "/age2.txt");
        assertTrue(file2.createNewFile());

        final File file4 = new File(TESTDIR + "/age4.txt");
        assertTrue(file4.createNewFile());

        final Function<Boolean, Object> runNext = resetAges -> {
            if (resetAges) {
                resetAges();
                assertTrue(file0.setLastModified(time0millis));
                assertTrue(file2.setLastModified(time2millis));
                assertTrue(file4.setLastModified(time4millis));
            }

            assertTrue(file0.lastModified() > time3millis && file0.lastModified() <= time0millis);
            assertTrue(file2.lastModified() > time3millis && file2.lastModified() < time1millis);
            assertTrue(file4.lastModified() < time3millis);

            try {
                runNext();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        };

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runNext.apply(true);
        runner.assertTransferCount(ListFile.REL_SUCCESS, 3);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());
        assertEquals(file4.getName(), successFiles1.get(0).getAttribute("filename"));
        assertEquals(file2.getName(), successFiles1.get(1).getAttribute("filename"));
        assertEquals(file0.getName(), successFiles1.get(2).getAttribute("filename"));
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");

        // processor updates internal state, it shouldn't pick the same ones.
        runNext.apply(false);
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);

        // exclude oldest
        runner.setProperty(ListFile.MIN_AGE, age0);
        runner.setProperty(ListFile.MAX_AGE, age4);
        runNext.apply(true);
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());
        assertEquals(file2.getName(), successFiles2.get(0).getAttribute("filename"));
        assertEquals(file0.getName(), successFiles2.get(1).getAttribute("filename"));
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 2 match the filter.");

        // exclude newest
        runner.setProperty(ListFile.MIN_AGE, age2);
        runner.setProperty(ListFile.MAX_AGE, age5);
        runNext.apply(true);
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles3.size());
        assertEquals(file4.getName(), successFiles3.get(0).getAttribute("filename"));
        assertEquals(file2.getName(), successFiles3.get(1).getAttribute("filename"));

        // exclude oldest and newest
        runner.setProperty(ListFile.MIN_AGE, age2);
        runner.setProperty(ListFile.MAX_AGE, age4);
        runNext.apply(true);
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles4.size());
        assertEquals(file2.getName(), successFiles4.get(0).getAttribute("filename"));
    }

    @Test
    public void testFilterSize() throws Exception {
        final byte[] bytes1000 = new byte[1000];
        final byte[] bytes5000 = new byte[5000];
        final byte[] bytes10000 = new byte[10000];
        FileOutputStream fos;

        final File file1 = new File(TESTDIR + "/size1.txt");
        assertTrue(file1.createNewFile());
        fos = new FileOutputStream(file1);
        fos.write(bytes10000);
        fos.close();

        final File file2 = new File(TESTDIR + "/size2.txt");
        assertTrue(file2.createNewFile());
        fos = new FileOutputStream(file2);
        fos.write(bytes5000);
        fos.close();

        final File file3 = new File(TESTDIR + "/size3.txt");
        assertTrue(file3.createNewFile());
        fos = new FileOutputStream(file3);
        fos.write(bytes1000);
        fos.close();

        final long now = getTestModifiedTime();
        assertTrue(file1.setLastModified(now));
        assertTrue(file2.setLastModified(now));
        assertTrue(file3.setLastModified(now));

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");

        // exclude largest
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "0 b");
        runner.setProperty(ListFile.MAX_SIZE, "7500 b");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 2 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 2 match the filter.");

        // exclude smallest
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "2500 b");
        runner.removeProperty(ListFile.MAX_SIZE);
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 2 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles3.size());

        // exclude oldest and newest
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "2500 b");
        runner.setProperty(ListFile.MAX_SIZE, "7500 b");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 1 matches the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles4.size());
    }

    @Test
    public void testFilterHidden() throws Exception {
        final long now = getTestModifiedTime();

        FileOutputStream fos;

        final File file1 = new File(TESTDIR + "/hidden1.txt");
        assertTrue(file1.createNewFile());
        fos = new FileOutputStream(file1);
        fos.close();

        final File file2 = new File(TESTDIR + "/.hidden2.txt");
        assertTrue(file2.createNewFile());
        fos = new FileOutputStream(file2);
        fos.close();
        FileStore store = Files.getFileStore(file2.toPath());
        if (store.supportsFileAttributeView("dos")) {
            Files.setAttribute(file2.toPath(), "dos:hidden", true);
        }

        assertTrue(file1.setLastModified(now));
        assertTrue(file2.setLastModified(now));

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ".*");
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.removeProperty(ListFile.MIN_SIZE);
        runner.removeProperty(ListFile.MAX_SIZE);
        runner.setProperty(ListFile.IGNORE_HIDDEN_FILES, "false");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 2 objects.  Of those, 2 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles1.size());

        // exclude hidden
        runner.setProperty(ListFile.IGNORE_HIDDEN_FILES, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 2 objects.  Of those, 1 matches the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());
    }

    @Test
    public void testListWithUnreadableFiles() throws Exception {
        final File file1 = new File(TESTDIR + "/unreadable.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setReadable(false));

        final File file2 = new File(TESTDIR + "/readable.txt");
        assertTrue(file2.createNewFile());

        final long now = getTestModifiedTime();
        assertTrue(file1.setLastModified(now));
        assertTrue(file2.setLastModified(now));

        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ".*");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 2 objects.  Of those, 1 matches the filter.");
        runNext();

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles.size());
    }

    @Test
    public void testListWithinUnreadableDirectory() throws Exception {
        final File subdir = new File(TESTDIR + "/subdir");
        assertTrue(subdir.mkdir());
        assertTrue(subdir.setReadable(false));

        try {
            final File file1 = new File(TESTDIR + "/subdir/unreadable.txt");
            assertTrue(file1.createNewFile());
            assertTrue(file1.setReadable(false));

            final File file2 = new File(TESTDIR + "/subdir/readable.txt");
            assertTrue(file2.createNewFile());

            final File file3 = new File(TESTDIR + "/secondReadable.txt");
            assertTrue(file3.createNewFile());

            final long now = getTestModifiedTime();
            assertTrue(file1.setLastModified(now));
            assertTrue(file2.setLastModified(now));
            assertTrue(file3.setLastModified(now));

            runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
            runner.setProperty(ListFile.FILE_FILTER, ".*");
            assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 1 object.  Of that, 1 matches the filter.");
            runNext();

            final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
            assertEquals(1, successFiles.size());
            assertEquals("secondReadable.txt", successFiles.get(0).getAttribute("filename"));
        } finally {
            subdir.setReadable(true);
        }
    }

    @Test
    public void testListingNeedsSufficientPrivilegesAndFittingFilter() throws Exception {
        final File file = new File(TESTDIR + "/file.txt");
        assertTrue(file.createNewFile());
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());

        // Run with privileges but without fitting filter
        runner.setProperty(ListFile.FILE_FILTER, "willBeFilteredOut");
        assertTrue(file.setLastModified(getTestModifiedTime()));
        runNext();

        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(0, successFiles1.size());

        // Run with privileges and with fitting filter
        runner.setProperty(ListFile.FILE_FILTER, "file.*");
        assertTrue(file.setLastModified(getTestModifiedTime()));
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 1 object.  Of that, 1 matches the filter.");
        runNext();

        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());

        // Run without privileges and with fitting filter
        assertTrue(file.setReadable(false));
        assertTrue(file.setLastModified(getTestModifiedTime()));
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 1 object.  Of that, 0 match the filter.");
        runNext();

        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(0, successFiles3.size());
    }


    @Test
    public void testFilterFilePattern() throws Exception {

        final long now = getTestModifiedTime();

        final File file1 = new File(TESTDIR + "/file1-abc-apple.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(now));

        final File file2 = new File(TESTDIR + "/file2-xyz-apple.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(now));

        final File file3 = new File(TESTDIR + "/file3-xyz-banana.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(now));

        final File file4 = new File(TESTDIR + "/file4-pdq-banana.txt");
        assertTrue(file4.createNewFile());
        assertTrue(file4.setLastModified(now));

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ListFile.FILE_FILTER.getDefaultValue());
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 4 objects.  Of those, 4 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(4, successFiles1.size());

        // filter file on pattern
        // Modifying FILE_FILTER property reset listing status, so these files will be listed again.
        runner.setProperty(ListFile.FILE_FILTER, ".*-xyz-.*");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 4 objects.  Of those, 2 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS, 2);

        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 4 objects.  Of those, 2 match the filter.");
        runNext();
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);
    }

    @Test
    public void testFilterPathPattern() throws Exception {
        final long now = getTestModifiedTime();

        final File subdir1 = new File(TESTDIR + "/subdir1");
        assertTrue(subdir1.mkdirs());

        final File subdir2 = new File(TESTDIR + "/subdir1/subdir2");
        assertTrue(subdir2.mkdirs());

        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(now));

        final File file2 = new File(TESTDIR + "/subdir1/file2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(now));

        final File file3 = new File(TESTDIR + "/subdir1/subdir2/file3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(now));

        final File file4 = new File(TESTDIR + "/subdir1/file4.txt");
        assertTrue(file4.createNewFile());
        assertTrue(file4.setLastModified(now));

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ListFile.FILE_FILTER.getDefaultValue());
        runner.setProperty(ListFile.RECURSE, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 4 objects.  Of those, 4 match the filter.");
        runNext();

        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(4, successFiles1.size());

        // filter path on pattern subdir1
        runner.setProperty(ListFile.PATH_FILTER, "subdir1.*");
        runner.setProperty(ListFile.RECURSE, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 4 objects.  Of those, 3 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles2.size());

        // filter path on pattern subdir2
        runner.setProperty(ListFile.PATH_FILTER, ".*/subdir2");
        runner.setProperty(ListFile.RECURSE, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 4 objects.  Of those, 1 matches the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles3.size());
    }

    @Test
    public void testFilterPathPatternNegative() throws Exception {
        final long now = getTestModifiedTime();

        final File subdirA = new File(TESTDIR + "/AAA");
        assertTrue(subdirA.mkdirs());

        final File subdirL = new File(TESTDIR + "/LOG");
        assertTrue(subdirL.mkdirs());

        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(now));

        final File file2 = new File(TESTDIR + "/AAA/file2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(now));

        final File file3 = new File(TESTDIR + "/LOG/file3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(now));

        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ListFile.FILE_FILTER.getDefaultValue());
        runner.setProperty(ListFile.PATH_FILTER, "^((?!LOG).)*$");
        runner.setProperty(ListFile.RECURSE, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 2 match the filter.");
        runNext();

        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles1.size());
    }

    @Test
    public void testRecurse() throws Exception {
        final long now = getTestModifiedTime();

        final File subdir1 = new File(TESTDIR + "/subdir1");
        assertTrue(subdir1.mkdirs());

        final File subdir2 = new File(TESTDIR + "/subdir1/subdir2");
        assertTrue(subdir2.mkdirs());

        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(now));

        final File file2 = new File(TESTDIR + "/subdir1/file2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(now));

        final File file3 = new File(TESTDIR + "/subdir1/subdir2/file3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(now));

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.RECURSE, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS, 3);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        for (final MockFlowFile mff : successFiles1) {
            final String filename = mff.getAttribute(CoreAttributes.FILENAME.key());
            final String path = mff.getAttribute(CoreAttributes.PATH.key());

            switch (filename) {
                case "file1.txt":
                    assertEquals("." + File.separator, path);
                    mff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), file1.getParentFile().getAbsolutePath() + File.separator);
                    break;
                case "file2.txt":
                    assertEquals("subdir1" + File.separator, path);
                    mff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), file2.getParentFile().getAbsolutePath() + File.separator);
                    break;
                case "file3.txt":
                    assertEquals("subdir1" + File.separator + "subdir2" + File.separator, path);
                    mff.assertAttributeEquals(CoreAttributes.ABSOLUTE_PATH.key(), file3.getParentFile().getAbsolutePath() + File.separator);
                    break;
            }
        }
        assertEquals(3, successFiles1.size());

        // don't recurse
        runner.setProperty(ListFile.RECURSE, "false");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 1 object.  Of that, 1 matches the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());
    }

    @Test
    public void testReadable() throws Exception {
        final long now = getTestModifiedTime();

        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(now));

        final File file2 = new File(TESTDIR + "/file2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(now));

        final File file3 = new File(TESTDIR + "/file3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(now));

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.RECURSE, "true");
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        runner.assertTransferCount(ListFile.REL_SUCCESS, 3);
    }

    @Test
    public void testAttributesSet() throws Exception {
        // create temp file and time constant
        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        FileOutputStream fos = new FileOutputStream(file1);
        fos.write(new byte[1234]);
        fos.close();
        assertTrue(file1.setLastModified(time3millis));
        Long time3rounded = time3millis - time3millis % 1000;
        String userName = System.getProperty("user.name");

        // validate the file transferred
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles1.size());

        // get attribute check values
        final Path file1Path = file1.toPath();
        final Path directoryPath = new File(TESTDIR).toPath();
        final Path relativePath = directoryPath.relativize(file1.toPath().getParent());
        String relativePathString = relativePath.toString();
        relativePathString = relativePathString.isEmpty() ? "." + File.separator : relativePathString + File.separator;
        final Path absolutePath = file1.toPath().toAbsolutePath();
        final String absolutePathString = absolutePath.getParent().toString() + File.separator;
        final FileStore store = Files.getFileStore(file1Path);
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(ListFile.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
        final String time3Formatted = formatter.format(Instant.ofEpochMilli(time3rounded).atZone(ZoneId.systemDefault()));

        // check standard attributes
        MockFlowFile mock1 = successFiles1.get(0);
        assertEquals(relativePathString, mock1.getAttribute(CoreAttributes.PATH.key()));
        assertEquals("file1.txt", mock1.getAttribute(CoreAttributes.FILENAME.key()));
        assertEquals(absolutePathString, mock1.getAttribute(CoreAttributes.ABSOLUTE_PATH.key()));
        assertEquals("1234", mock1.getAttribute(ListFile.FILE_SIZE_ATTRIBUTE));

        // check attributes dependent on views supported
        if (store.supportsFileAttributeView("basic")) {
            assertEquals(time3Formatted, mock1.getAttribute(ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE));
            assertNotNull(mock1.getAttribute(ListFile.FILE_CREATION_TIME_ATTRIBUTE));
            assertNotNull(mock1.getAttribute(ListFile.FILE_LAST_ACCESS_TIME_ATTRIBUTE));
        }
        if (store.supportsFileAttributeView("owner")) {
            // look for username containment to handle Windows domains as well as Unix user names
            // org.junit.ComparisonFailure: expected:<[]username> but was:<[DOMAIN\]username>
            assertTrue(mock1.getAttribute(ListFile.FILE_OWNER_ATTRIBUTE).contains(userName));
        }
        if (store.supportsFileAttributeView("posix")) {
            assertNotNull(mock1.getAttribute(ListFile.FILE_GROUP_ATTRIBUTE), "Group name should be set");
            assertNotNull(mock1.getAttribute(ListFile.FILE_PERMISSIONS_ATTRIBUTE), "File permissions should be set");
        }
    }

    @Test
    public void testIsListingResetNecessary() throws Exception {
        assertTrue(processor.isListingResetNecessary(ListFile.DIRECTORY));
        assertTrue(processor.isListingResetNecessary(ListFile.RECURSE));
        assertTrue(processor.isListingResetNecessary(ListFile.FILE_FILTER));
        assertTrue(processor.isListingResetNecessary(ListFile.PATH_FILTER));
        assertTrue(processor.isListingResetNecessary(ListFile.MIN_AGE));
        assertTrue(processor.isListingResetNecessary(ListFile.MAX_AGE));
        assertTrue(processor.isListingResetNecessary(ListFile.MIN_SIZE));
        assertTrue(processor.isListingResetNecessary(ListFile.MAX_SIZE));
        assertTrue(processor.isListingResetNecessary(ListFile.IGNORE_HIDDEN_FILES));
        assertFalse(processor.isListingResetNecessary(new PropertyDescriptor.Builder().name("x").build()));
    }

    private void makeTestFile(final String name, final long millis, final Map<String, Long> fileTimes) throws IOException {
        final File file = new File(TESTDIR + name);
        assertTrue(file.createNewFile());
        assertTrue(file.setLastModified(millis));
        fileTimes.put(file.getName(), file.lastModified());
    }

    @Test
    public void testFilterRunMidFileWrites() throws Exception {
        final Map<String, Long> fileTimes = new HashMap<>();

        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());

        makeTestFile("/batch1-age3.txt", time3millis, fileTimes);
        makeTestFile("/batch1-age4.txt", time4millis, fileTimes);
        makeTestFile("/batch1-age5.txt", time5millis, fileTimes);

        // check files
        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 3 objects.  Of those, 3 match the filter.");
        runNext();

        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        runner.assertTransferCount(ListFile.REL_SUCCESS, 3);
        assertEquals(3, runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS).size());

        // should be picked since it's newer than age3
        makeTestFile("/batch2-age2.txt", time2millis, fileTimes);
        // should be picked even if it has the same age3 timestamp, because it wasn't there at the previous cycle.
        makeTestFile("/batch2-age3.txt", time3millis, fileTimes);
        // should be ignored since it's older than age3
        makeTestFile("/batch2-age4.txt", time4millis, fileTimes);

        assertVerificationOutcome(Outcome.SUCCESSFUL, "Successfully listed .* Found 6 objects.  Of those, 6 match the filter.");
        runNext();

        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        runner.assertTransferCount(ListFile.REL_SUCCESS, 2);
        assertEquals(2, runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS).size());
    }

    /*
     * HFS+, default for OS X, only has granularity to one second, accordingly, we go back in time to establish consistent test cases
     *
     * Provides "now" minus 1 second in millis
    */
    private static long getTestModifiedTime() {
        final long nowMillis = System.currentTimeMillis();
        // Subtract a second to avoid possible rounding issues
        final long nowSeconds = TimeUnit.SECONDS.convert(nowMillis, TimeUnit.MILLISECONDS) - 1;
        return TimeUnit.MILLISECONDS.convert(nowSeconds, TimeUnit.SECONDS);
    }

    private void resetAges() {
        syncTime = getTestModifiedTime();

        final long age0millis = 0L;
        final long age1millis = 20000L;
        final long age2millis = 40000L;
        final long age3millis = 60000L;
        final long age4millis = 80000L;
        final long age5millis = 200000L;

        time0millis = syncTime - age0millis;
        time1millis = syncTime - age1millis;
        time2millis = syncTime - age2millis;
        time3millis = syncTime - age3millis;
        time4millis = syncTime - age4millis;
        time5millis = syncTime - age5millis;

        age0 = Long.toString(age0millis) + " millis";
        age2 = Long.toString(age2millis) + " millis";
        age4 = Long.toString(age4millis) + " millis";
        age5 = Long.toString(age5millis) + " millis";
    }

    private void deleteDirectory(final File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (final File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    }
                    assertTrue(file.delete(), "Could not delete " + file.getAbsolutePath());
                }
            }
        }
    }

    private void assertVerificationOutcome(final Outcome expectedOutcome, final String expectedExplanationRegex) {
        final List<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap());

        assertEquals(1, results.size());
        final ConfigVerificationResult result = results.get(0);
        assertEquals(expectedOutcome, result.getOutcome());
        assertTrue(result.getExplanation().matches(expectedExplanationRegex),
                String.format("Expected verification result to match pattern [%s].  Actual explanation was: %s", expectedExplanationRegex, result.getExplanation()));
    }
}
