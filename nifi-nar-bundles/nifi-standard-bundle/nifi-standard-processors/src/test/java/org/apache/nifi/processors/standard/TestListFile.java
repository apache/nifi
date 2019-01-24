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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListProcessorTestWatcher;
import org.apache.nifi.processors.standard.util.FileInfo;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestListFile {

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
    private Long age0millis, age1millis, age2millis, age3millis, age4millis, age5millis;
    private String age0, age1, age2, age3, age4, age5;

    @Rule
    public ListProcessorTestWatcher dumpState = new ListProcessorTestWatcher(
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
    ) {
        @Override
        protected void finished(Description description) {
            try {
                // In order to refer files in testDir, we want to execute this rule before tearDown, because tearDown removes files.
                // And @After is always executed before @Rule.
                tearDown();
            } catch (Exception e) {
                throw new RuntimeException("Failed to tearDown.", e);
            }
        }
    };

    @Before
    public void setUp() throws Exception {
        processor = new ListFile();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION, AbstractListProcessor.PRECISION_SECONDS.getValue());
        context = runner.getProcessContext();
        deleteDirectory(testDir);
        assertTrue("Unable to create test data directory " + testDir.getAbsolutePath(), testDir.exists() || testDir.mkdirs());
        resetAges();
    }

    public void tearDown() throws Exception {
        deleteDirectory(testDir);
        File tempFile = processor.getPersistenceFile();
        if (tempFile.exists()) {
            File[] stateFiles = tempFile.getParentFile().listFiles();
            if (stateFiles != null) {
                for (File stateFile : stateFiles) {
                    assertTrue(stateFile.delete());
                }
            }
        }
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

        final List<File> files = listFiles(testDir);
        final boolean isMillisecondSupported = files.stream().anyMatch(file -> file.lastModified() % 1_000 > 0);
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
    @Ignore("Intended only for manual testing, as is very expensive to run as a unit test. Performs listing of 1,000,000 files (doesn't actually create the files, though - injects them in) to " +
        "ensure performance is not harmed")
    public void testPerformanceOnLargeListing() {
        final List<Path> paths = new ArrayList<>(1_000_000);
        final File base = new File("target");

        for (int firstLevel=0; firstLevel < 1000; firstLevel++) {
            final File dir = new File(base, String.valueOf(firstLevel));

            for (int secondLevel = 0; secondLevel < 1000; secondLevel++) {
                final File file = new File(dir, String.valueOf(secondLevel));
                paths.add(file.toPath());
            }
        }

        final BasicFileAttributes basicFileAttributes = new BasicFileAttributes() {
            @Override
            public FileTime lastModifiedTime() {
                return FileTime.fromMillis(System.currentTimeMillis());
            }

            @Override
            public FileTime lastAccessTime() {
                return FileTime.fromMillis(System.currentTimeMillis());
            }

            @Override
            public FileTime creationTime() {
                return FileTime.fromMillis(System.currentTimeMillis());
            }

            @Override
            public boolean isRegularFile() {
                return false;
            }

            @Override
            public boolean isDirectory() {
                return false;
            }

            @Override
            public boolean isSymbolicLink() {
                return false;
            }

            @Override
            public boolean isOther() {
                return false;
            }

            @Override
            public long size() {
                return 0;
            }

            @Override
            public Object fileKey() {
                return null;
            }
        };

        processor = new ListFile() {
            @Override
            protected Stream<Path> getPathStream(final Path basePath, final int maxDepth, final BiPredicate<Path, BasicFileAttributes> matcher) throws IOException {
                return paths.stream()
                    .filter(path -> matcher.test(path, basicFileAttributes));
            }
        };

        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION, AbstractListProcessor.PRECISION_SECONDS.getValue());
        runner.setProperty(ListFile.TRACK_PERFORMANCE, "true");
        runner.setProperty(ListFile.MAX_TRACKED_FILES, "100000");
        runner.setProperty(ListFile.DIRECTORY, "target");

        runner.run();

        final ListFile.PerformanceTracker tracker = processor.getPerformanceTracker();
        assertEquals(100_000, tracker.getTrackedFileCount());

        final ListFile.MonitorActiveTasks monitorActiveTasks = new ListFile.MonitorActiveTasks(tracker, runner.getLogger(), 1000, 1000, 1);

        while (tracker.getTrackedFileCount() > 0) {
            monitorActiveTasks.run();
        }

        assertEquals(0, tracker.getTrackedFileCount());
    }


    @Test
    public void testGetPath() {
        runner.setProperty(ListFile.DIRECTORY, "/dir/test1");
        assertEquals("/dir/test1", processor.getPath(context));
        runner.setProperty(ListFile.DIRECTORY, "${literal(\"/DIR/TEST2\"):toLower()}");
        assertEquals("/dir/test2", processor.getPath(context));
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

        // process first file and set new timestamp
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles1.size());

        // create second file
        final File file2 = new File(TESTDIR + "/listing2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(time2millis));

        // process second file after timestamp
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());

        // create third file
        final File file3 = new File(TESTDIR + "/listing3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(time4millis));

        // process third file before timestamp
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(0, successFiles3.size());

        // force state to reset and process all files
        runner.removeProperty(ListFile.DIRECTORY);
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles4.size());

        runNext();
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);
    }

    @Test
    public void testFilterAge() throws Exception {

        final File file1 = new File(TESTDIR + "/age1.txt");
        assertTrue(file1.createNewFile());

        final File file2 = new File(TESTDIR + "/age2.txt");
        assertTrue(file2.createNewFile());

        final File file3 = new File(TESTDIR + "/age3.txt");
        assertTrue(file3.createNewFile());

        final Function<Boolean, Object> runNext = resetAges -> {
            if (resetAges) {
                resetAges();
                assertTrue(file1.setLastModified(time0millis));
                assertTrue(file2.setLastModified(time2millis));
                assertTrue(file3.setLastModified(time4millis));
            }
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

        // processor updates internal state, it shouldn't pick the same ones.
        runNext.apply(false);
        runner.assertTransferCount(ListFile.REL_SUCCESS, 0);

        // exclude oldest
        runner.setProperty(ListFile.MIN_AGE, age0);
        runner.setProperty(ListFile.MAX_AGE, age3);
        runNext.apply(true);
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());
        assertEquals(file2.getName(), successFiles2.get(0).getAttribute("filename"));
        assertEquals(file1.getName(), successFiles2.get(1).getAttribute("filename"));

        // exclude newest
        runner.setProperty(ListFile.MIN_AGE, age1);
        runner.setProperty(ListFile.MAX_AGE, age5);
        runNext.apply(true);
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles3.size());
        assertEquals(file3.getName(), successFiles3.get(0).getAttribute("filename"));
        assertEquals(file2.getName(), successFiles3.get(1).getAttribute("filename"));

        // exclude oldest and newest
        runner.setProperty(ListFile.MIN_AGE, age1);
        runner.setProperty(ListFile.MAX_AGE, age3);
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
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());

        // exclude largest
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "0 b");
        runner.setProperty(ListFile.MAX_SIZE, "7500 b");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());

        // exclude smallest
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "2500 b");
        runner.removeProperty(ListFile.MAX_SIZE);
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles3.size());

        // exclude oldest and newest
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "2500 b");
        runner.setProperty(ListFile.MAX_SIZE, "7500 b");
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
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles1.size());

        // exclude hidden
        runner.setProperty(ListFile.IGNORE_HIDDEN_FILES, "true");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());
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
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(4, successFiles1.size());

        // filter file on pattern
        // Modifying FILE_FILTER property reset listing status, so these files will be listed again.
        runner.setProperty(ListFile.FILE_FILTER, ".*-xyz-.*");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS, 2);

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
        runNext();

        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(4, successFiles1.size());

        // filter path on pattern subdir1
        runner.setProperty(ListFile.PATH_FILTER, "subdir1");
        runner.setProperty(ListFile.RECURSE, "true");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles2.size());

        // filter path on pattern subdir2
        runner.setProperty(ListFile.PATH_FILTER, "subdir2");
        runner.setProperty(ListFile.RECURSE, "true");
        runNext();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles3.size());
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

        // exclude hidden
        runner.setProperty(ListFile.RECURSE, "false");
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
        final DateFormat formatter = new SimpleDateFormat(ListFile.FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
        final String time3Formatted = formatter.format(time3rounded);

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
            assertNotNull("Group name should be set", mock1.getAttribute(ListFile.FILE_GROUP_ATTRIBUTE));
            assertNotNull("File permissions should be set", mock1.getAttribute(ListFile.FILE_PERMISSIONS_ATTRIBUTE));
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
        assertEquals(true, processor.isListingResetNecessary(ListFile.IGNORE_HIDDEN_FILES));
        assertEquals(false, processor.isListingResetNecessary(new PropertyDescriptor.Builder().name("x").build()));
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

        age0millis = 0L;
        age1millis = 5000L;
        age2millis = 10000L;
        age3millis = 15000L;
        age4millis = 20000L;
        age5millis = 100000L;

        time0millis = syncTime - age0millis;
        time1millis = syncTime - age1millis;
        time2millis = syncTime - age2millis;
        time3millis = syncTime - age3millis;
        time4millis = syncTime - age4millis;
        time5millis = syncTime - age5millis;

        age0 = Long.toString(age0millis) + " millis";
        age1 = Long.toString(age1millis) + " millis";
        age2 = Long.toString(age2millis) + " millis";
        age3 = Long.toString(age3millis) + " millis";
        age4 = Long.toString(age4millis) + " millis";
        age5 = Long.toString(age5millis) + " millis";
    }

    private void deleteDirectory(final File directory) throws IOException {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (final File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    }
                    assertTrue("Could not delete " + file.getAbsolutePath(), file.delete());
                }
            }
        }
    }
}
