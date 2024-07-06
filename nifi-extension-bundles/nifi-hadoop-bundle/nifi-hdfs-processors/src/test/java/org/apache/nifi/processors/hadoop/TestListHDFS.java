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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.processors.hadoop.ListHDFS.LATEST_TIMESTAMP_KEY;
import static org.apache.nifi.processors.hadoop.ListHDFS.REL_SUCCESS;
import static org.apache.nifi.processors.hadoop.util.FilterMode.FILTER_DIRECTORIES_AND_FILES;
import static org.apache.nifi.processors.hadoop.util.FilterMode.FILTER_MODE_FILES_ONLY;
import static org.apache.nifi.processors.hadoop.util.FilterMode.FILTER_MODE_FULL_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class TestListHDFS {

    private TestRunner runner;
    private ListHDFSWithMockedFileSystem proc;
    private MockComponentLog mockLogger;

    @BeforeEach
    public void setup() throws InitializationException {
        proc = new ListHDFSWithMockedFileSystem();
        mockLogger = spy(new MockComponentLog(UUID.randomUUID().toString(), proc));
        runner = TestRunners.newTestRunner(proc, mockLogger);

        runner.setProperty(ListHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
        runner.setProperty(ListHDFS.DIRECTORY, "/test");
    }

    @Test
    void testListingWithValidELFunction() {
        addFileStatus("/test", "testFile.txt", false);

        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }

    @Test
    void testListingWithFilter() {
        addFileStatus("/test", "testFile.txt", false);

        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");
        runner.setProperty(ListHDFS.FILE_FILTER, "[^test].*");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
    }

    @Test
    void testListingWithInvalidELFunction() {
        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):foo()}");
        runner.assertNotValid();
    }

    @Test
    void testListingWithUnrecognizedELFunction() {
        addFileStatus("/test", "testFile.txt", false);

        runner.setProperty(ListHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        assertEquals(IllegalArgumentException.class, assertionError.getCause().getClass());

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
    }

    @Test
    void testListingHasCorrectAttributes() {
        addFileStatus("/test", "testFile.txt", false);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    void testRecursiveWithDefaultFilterAndFilterMode() {
        addFileStatus("/test", ".testFile.txt", false);
        addFileStatus("/test", "testFile.txt", false);

        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("testFile.txt")) {
                ff.assertAttributeEquals("path", "/test");
            } else if (filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir");
            } else {
                fail("filename was " + filename);
            }
        }
    }

    @Test
    void testRecursiveWithCustomFilterDirectoriesAndFiles() {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, ".*txt.*");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_DIRECTORIES_AND_FILES.getValue());

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir", "anotherDir", true);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);
        addFileStatus("/test", "txtDir", true);
        addFileStatus("/test/txtDir", "3.out", false);
        addFileStatus("/test/txtDir", "3.txt", false);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("testFile.txt")) {
                ff.assertAttributeEquals("path", "/test");
            } else if (filename.equals("3.txt")) {
                ff.assertAttributeEquals("path", "/test/txtDir");
            } else {
                fail("filename was " + filename);
            }
        }
    }

    @Test
    void testRecursiveWithCustomFilterFilesOnly() {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, "[^\\.].*\\.txt");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_MODE_FILES_ONLY.getValue());

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", ".partfile.txt", false);
        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir", "anotherDir", true);
        addFileStatus("/test/testDir/anotherDir", ".txt", false);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            switch (filename) {
                case "testFile.txt":
                    ff.assertAttributeEquals("path", "/test");
                    break;
                case "1.txt":
                    ff.assertAttributeEquals("path", "/test/testDir");
                    break;
                case "2.txt":
                    ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
                    break;
                default:
                    fail("filename was " + filename);
                    break;
            }
        }
    }

    @Test
    void testRecursiveWithCustomFilterFullPathWithoutSchemeAndAuthority() {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, "(/.*/)*anotherDir/1\\..*");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_MODE_FULL_PATH.getValue());

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir", "anotherDir", true);
        addFileStatus("/test/testDir/anotherDir", "1.out", false);
        addFileStatus("/test/testDir/anotherDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);
        addFileStatus("/test/testDir", "someDir", true);
        addFileStatus("/test/testDir/someDir", "1.out", false);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("1.out")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else if (filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else {
                fail("filename was " + filename);
            }
        }
    }

    @Test
    void testRecursiveWithCustomFilterFullPathWithSchemeAndAuthority() {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, "hdfs://hdfscluster:8020(/.*/)*anotherDir/1\\..*");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_MODE_FULL_PATH.getValue());

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir", "anotherDir", true);
        addFileStatus("/test/testDir/anotherDir", "1.out", false);
        addFileStatus("/test/testDir/anotherDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);
        addFileStatus("/test/testDir", "someDir", true);
        addFileStatus("/test/testDir/someDir", "1.out", false);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("1.out")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else if (filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else {
                fail("filename was " + filename);
            }
        }
    }

    @Test
    void testNotRecursive() {
        runner.setProperty(ListHDFS.RECURSE_SUBDIRS, "false");
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    void testNoListUntilUpdateFromRemoteOnPrimaryNodeChange() throws IOException {
        addFileStatus("/test", "testFile.txt", false, 1999L, 0L);


        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");

        runner.clearTransferState();

        // add new file to pull
        addFileStatus("/test", "testFile2.txt", false, 2000L, 0L);

        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, true);

        // Should fail to perform @OnScheduled methods.
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        // Should fail to perform @OnScheduled methods.
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, false);

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        Map<String, String> newState = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertEquals("2000", newState.get(LATEST_TIMESTAMP_KEY));
    }

    @Test
    void testEntriesWithSameTimestampOnlyAddedOnce() {
        addFileStatus("/test", "testFile.txt", false, 1L, 0L);
        addFileStatus("/test", "testFile2.txt", false, 1L, 8L);

        // this is a directory, so it won't be counted toward the entries
        addFileStatus("/test", "testDir", true, 1L, 8L);
        addFileStatus("/test/testDir", "1.txt", false, 1L, 100L);

        // The first iteration should pick up 3 files with the smaller timestamps.
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);

        addFileStatus("/test/testDir", "2.txt", false, 1L, 100L);

        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 4);
    }

    @Test
    void testMinAgeMaxAge() throws IOException {

        final long now = new Date().getTime();
        final long oneHourAgo = now - 3600000;
        final long twoHoursAgo = now - 2 * 3600000;

        addFileStatus("/test", "testFile.txt", false, now - 5, now - 5);
        addFileStatus("/test", "testFile1.txt", false, oneHourAgo, oneHourAgo);
        addFileStatus("/test", "testFile2.txt", false, twoHoursAgo, twoHoursAgo);

        // all files
        runner.run();
        runner.assertValid();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // invalid min_age > max_age
        runner.setProperty(ListHDFS.MINIMUM_FILE_AGE, "30 sec");
        runner.setProperty(ListHDFS.MAXIMUM_FILE_AGE, "1 sec");
        runner.assertNotValid();

        // only one file (one hour ago)
        runner.setProperty(ListHDFS.MINIMUM_FILE_AGE, "30 sec");
        runner.setProperty(ListHDFS.MAXIMUM_FILE_AGE, "90 min");
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0).assertAttributeEquals("filename", "testFile1.txt");
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // two files (one hour ago and two hours ago)
        runner.setProperty(ListHDFS.MINIMUM_FILE_AGE, "30 sec");
        runner.removeProperty(ListHDFS.MAXIMUM_FILE_AGE);
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // two files (now and one hour ago)
        runner.setProperty(ListHDFS.MINIMUM_FILE_AGE, "0 sec");
        runner.setProperty(ListHDFS.MAXIMUM_FILE_AGE, "90 min");
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);
    }

    @Test
    void testListAfterDirectoryChange() {
        addFileStatus("/test1", "testFile-1_1.txt", false, 100L, 0L);
        addFileStatus("/test1", "testFile-1_2.txt", false, 200L, 0L);
        addFileStatus("/test2", "testFile-2_1.txt", false, 150L, 0L);

        runner.setProperty(ListHDFS.DIRECTORY, "/test1");

        runner.run(); // Initial run, latest file from /test1 will be ignored

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        runner.setProperty(ListHDFS.DIRECTORY, "/test2"); // Changing directory should reset the state

        runner.run(); // Since state has been reset, testFile-2_1.txt from /test2 should be picked up
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);
    }

    @Test
    void testListingEmptyDir() {
        runner.setProperty(ListHDFS.DIRECTORY, "/test/emptyDir");

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "emptyDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "1.out", false);
        addFileStatus("/test/testDir/anotherDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);
        addFileStatus("/test/testDir/someDir", "1.out", false);

        runner.run();

        // verify that no messages were logged at the error level
        verify(mockLogger, never()).error(anyString());
        final ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockLogger, atLeast(0)).error(anyString(), throwableArgumentCaptor.capture());
        // if error.(message, throwable) was called, ignore JobConf CNFEs since mapreduce libs are not included as dependencies
        assertTrue(throwableArgumentCaptor.getAllValues().stream()
                // check that there are no throwables that are not of JobConf CNFE exceptions
                .allMatch(throwable -> throwable instanceof ClassNotFoundException && throwable.getMessage().contains("JobConf")));
        verify(mockLogger, never()).error(anyString(), any(Object[].class));
        verify(mockLogger, never()).error(anyString(), any(Object[].class));

        // assert that no files were listed
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    @Test
    void testListingNonExistingDir() {
        final String nonExistingPath = "/test/nonExistingDir";
        runner.setProperty(ListHDFS.DIRECTORY, nonExistingPath);

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "emptyDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "1.out", false);
        addFileStatus("/test/testDir/anotherDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);
        addFileStatus("/test/testDir/someDir", "1.out", false);

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        assertEquals(ProcessException.class, assertionError.getCause().getClass());

        // assert that no files were listed
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    @Test
    void testRecordWriter() throws InitializationException {
        runner.setProperty(ListHDFS.DIRECTORY, "/test");

        final MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(ListHDFS.RECORD_WRITER, "record-writer");

        addFileStatus("/test", "testFile.out", false);
        addFileStatus("/test", "testFile.txt", false);
        addFileStatus("/test", "testDir", true);
        addFileStatus("/test/testDir", "1.txt", false);
        addFileStatus("/test/testDir", "anotherDir", true);
        addFileStatus("/test/testDir/anotherDir", "1.out", false);
        addFileStatus("/test/testDir/anotherDir", "1.txt", false);
        addFileStatus("/test/testDir/anotherDir", "2.out", false);
        addFileStatus("/test/testDir/anotherDir", "2.txt", false);
        addFileStatus("/test/testDir", "someDir", true);
        addFileStatus("/test/testDir/someDir", "1.out", false);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        final List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile flowFile = flowFilesForRelationship.get(0);
        flowFile.assertAttributeEquals("record.count", "8");
    }

    private FsPermission create777() {
        return new FsPermission((short) 0x777);
    }


    private static class ListHDFSWithMockedFileSystem extends ListHDFS {
        private final MockFileSystem fileSystem = new MockFileSystem();

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem;
        }

        @Override
        protected FileSystem getFileSystem(final Configuration config) {
            return fileSystem;
        }
    }

    private static class MockFileSystem extends FileSystem {
        private final Map<Path, Set<FileStatus>> fileStatuses = new HashMap<>();

        public void addFileStatus(final Path parent, final FileStatus child) {
            final Set<FileStatus> children = fileStatuses.computeIfAbsent(parent, k -> new HashSet<>());

            children.add(child);

            // if the child is directory and a key for it does not exist, create it with an empty set of children
            if (child.isDirectory() && !fileStatuses.containsKey(child.getPath())) {
                fileStatuses.put(child.getPath(), new HashSet<>());
            }
        }

        @Override
        @SuppressWarnings("deprecation")
        public long getDefaultBlockSize() {
            return 1024L;
        }

        @Override
        @SuppressWarnings("deprecation")
        public short getDefaultReplication() {
            return 1;
        }

        @Override
        public URI getUri() {
            return null;
        }

        @Override
        public FSDataInputStream open(final Path f, final int bufferSize) {
            return null;
        }

        @Override
        public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication,
                                         final long blockSize, final Progressable progress) {
            return null;
        }

        @Override
        public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) {
            return null;
        }

        @Override
        public boolean rename(final Path src, final Path dst) {
            return false;
        }

        @Override
        public boolean delete(final Path f, final boolean recursive) {
            return false;
        }

        @Override
        public FileStatus[] listStatus(final Path f) throws IOException {
            return fileStatuses.keySet().stream()
                    // find the key in fileStatuses that matches the given Path f
                    .filter(pathKey -> f.isAbsoluteAndSchemeAuthorityNull()
                            // f is an absolute path with no scheme and no authority, compare with the keys of fileStatuses without their scheme and authority
                            ? Path.getPathWithoutSchemeAndAuthority(pathKey).equals(Path.getPathWithoutSchemeAndAuthority(f)) :
                            // f is absolute, but contains a scheme or authority, compare directly to the keys of fileStatuses
                            // if f is not absolute, false will be returned;
                            f.isAbsolute() && pathKey.equals(f))
                    // get the set of FileStatus objects for the filtered paths in the stream
                    .map(fileStatuses::get)
                    // return the first set of FileStatus objects in the stream; there should only be one, since fileStatuses is a Map
                    .findFirst()

                    // if no set of FileStatus objects was found, throw a FNFE
                    .orElseThrow(() -> new FileNotFoundException(String.format("%s instance does not contain an key for %s", this.getClass().getSimpleName(), f))).toArray(new FileStatus[0]);
        }

        @Override
        public void setWorkingDirectory(final Path new_dir) {

        }

        @Override
        public Path getWorkingDirectory() {
            return new Path(new File(".").getAbsolutePath());
        }

        @Override
        public boolean mkdirs(final Path f, final FsPermission permission) {
            return false;
        }

        @Override
        public FileStatus getFileStatus(final Path path) {
            final Optional<FileStatus> fileStatus = fileStatuses.values().stream()
                    .flatMap(Set::stream)
                    .filter(fs -> fs.getPath().equals(path))
                    .findFirst();
            if (fileStatus.isEmpty()) {
                throw new IllegalArgumentException("Could not find FileStatus");
            }
            return fileStatus.get();
        }
    }

    private void addFileStatus(final String path, final String filename, final boolean isDirectory, final long modificationTime, final long accessTime) {
        final Path fullPath = new Path("hdfs", "hdfscluster:8020", path);
        final Path filePath = new Path(fullPath, filename);
        final FileStatus fileStatus = new FileStatus(1L, isDirectory, 1, 1L, modificationTime, accessTime, create777(), "owner", "group", filePath);
        proc.fileSystem.addFileStatus(fullPath, fileStatus);
    }

    private void addFileStatus(final String path, final String filename, final boolean isDirectory) {
        addFileStatus(path, filename, isDirectory, 0L, 0L);
    }
}
