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
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.nifi.processors.hadoop.ListHDFS.LATEST_TIMESTAMP_KEY;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestListHDFS {

    private TestRunner runner;
    private ListHDFSWithMockedFileSystem proc;
    private MockComponentLog mockLogger;

    @BeforeEach
    public void setup() throws InitializationException {
        final NiFiProperties mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        final KerberosProperties kerberosProperties = new KerberosProperties(null);

        proc = new ListHDFSWithMockedFileSystem(kerberosProperties);
        mockLogger = spy(new MockComponentLog(UUID.randomUUID().toString(), proc));
        runner = TestRunners.newTestRunner(proc, mockLogger);

        runner.setProperty(ListHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
        runner.setProperty(ListHDFS.DIRECTORY, "/test");
    }

    @Test
    void testListingWithValidELFunction() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }

    @Test
    void testListingWithFilter() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

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
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ListHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        assertEquals(IllegalArgumentException.class, assertionError.getCause().getClass());

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
    }

    @Test
    void testListingHasCorrectAttributes() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    void testRecursiveWithDefaultFilterAndFilterMode() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/.testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

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

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/txtDir")));
        proc.fileSystem.addFileStatus(new Path("/test/txtDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/txtDir/3.out")));
        proc.fileSystem.addFileStatus(new Path("/test/txtDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/txtDir/3.txt")));

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

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/.partfile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/.txt")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.txt")));

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

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

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

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

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
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    void testNoListUntilUpdateFromRemoteOnPrimaryNodeChange() throws IOException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1999L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");

        runner.clearTransferState();

        // add new file to pull
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 2000L, 0L, create777(), "owner", "group", new Path("/test/testFile2.txt")));

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
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 8L, 0L, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        // this is a directory, so it won't be counted toward the entries
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 8L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 100L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        // The first iteration should pick up 3 files with the smaller timestamps.
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 100L, 0L, create777(), "owner", "group", new Path("/test/testDir/2.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 4);
    }

    @Test
    void testMinAgeMaxAge() throws IOException {

        long now = new Date().getTime();
        long oneHourAgo = now - 3600000;
        long twoHoursAgo = now - 2 * 3600000;
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now - 5, now - 5, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, oneHourAgo, oneHourAgo, create777(), "owner", "group", new Path("/test/testFile1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, twoHoursAgo, twoHoursAgo, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        // all files
        runner.run();
        runner.assertValid();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // invalid min_age > max_age
        runner.setProperty(ListHDFS.MIN_AGE, "30 sec");
        runner.setProperty(ListHDFS.MAX_AGE, "1 sec");
        runner.assertNotValid();

        // only one file (one hour ago)
        runner.setProperty(ListHDFS.MIN_AGE, "30 sec");
        runner.setProperty(ListHDFS.MAX_AGE, "90 min");
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0).assertAttributeEquals("filename", "testFile1.txt");
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // two files (one hour ago and two hours ago)
        runner.setProperty(ListHDFS.MIN_AGE, "30 sec");
        runner.removeProperty(ListHDFS.MAX_AGE);
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // two files (now and one hour ago)
        runner.setProperty(ListHDFS.MIN_AGE, "0 sec");
        runner.setProperty(ListHDFS.MAX_AGE, "90 min");
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);
    }

    @Test
    void testListAfterDirectoryChange() {
        proc.fileSystem.addFileStatus(new Path("/test1"), new FileStatus(1L, false, 1, 1L, 100L, 0L, create777(), "owner", "group", new Path("/test1/testFile-1_1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test1"), new FileStatus(1L, false, 1, 1L, 200L, 0L, create777(), "owner", "group", new Path("/test1/testFile-1_2.txt")));
        proc.fileSystem.addFileStatus(new Path("/test2"), new FileStatus(1L, false, 1, 1L, 150L, 0L, create777(), "owner", "group", new Path("/test2/testFile-2_1.txt")));

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

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/emptyDir")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
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
        verify(mockLogger, never()).error(anyString(), any(Object[].class), any(Throwable.class));

        // assert that no files were listed
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    @Test
    void testListingNonExistingDir() {
        String nonExistingPath = "/test/nonExistingDir";
        runner.setProperty(ListHDFS.DIRECTORY, nonExistingPath);

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/emptyDir")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        assertEquals(ProcessException.class, assertionError.getCause().getClass());

        // assert that no files were listed
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    private FsPermission create777() {
        return new FsPermission((short) 0x777);
    }


    private static class ListHDFSWithMockedFileSystem extends ListHDFS {
        private final MockFileSystem fileSystem = new MockFileSystem();
        private final KerberosProperties testKerberosProps;

        public ListHDFSWithMockedFileSystem(KerberosProperties kerberosProperties) {
            this.testKerberosProps = kerberosProperties;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProps;
        }

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
            Set<FileStatus> children = fileStatuses.computeIfAbsent(parent, k -> new HashSet<>());

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
        public FileStatus getFileStatus(final Path f) {
            return null;
        }
    }
}
