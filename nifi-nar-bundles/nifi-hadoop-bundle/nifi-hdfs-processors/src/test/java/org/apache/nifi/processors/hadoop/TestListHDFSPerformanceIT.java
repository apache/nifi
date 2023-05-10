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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processors.hadoop.util.FilterMode;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.spy;

/**
 * In order to test different ListHDFS implementations change the ListHDFSWithMockedFileSystem ancestor class to the one in question.
 * Provide the HADOOP_RESOURCE_CONFIG, the ROOT_DIR and set the depth of the HDFS tree structure (k-ary complete tree) with the number of files.
 * First create the structure by running createHdfsNaryCompleteTree() test case. Then run the testListHdfsTimeElapsed() test case with
 * the implementation to test.
 */
@Disabled("This is a performance test and should be run manually")
class TestListHDFSPerformanceIT {

    private static final long BYTE_TO_MB = 1024 * 1024;
    private static final String HADOOP_RESOURCE_CONFIG = "???";
    private static final FileSystem FILE_SYSTEM = getFileSystem();
    private static final String ROOT_DIR = "???";
    private static final int NUM_CHILDREN = 3;
    private static final int NUM_OF_FILES = 1000;


    private TestRunner runner;
    private MockComponentLog mockLogger;
    private ListHDFSWithMockedFileSystem proc;


    @BeforeEach
    public void setup() throws InitializationException {
        final KerberosProperties kerberosProperties = new KerberosProperties(null);

        proc = new ListHDFSWithMockedFileSystem(kerberosProperties);
        mockLogger = spy(new MockComponentLog(UUID.randomUUID().toString(), proc));
        runner = TestRunners.newTestRunner(proc, mockLogger);

        runner.setProperty(ListHDFS.HADOOP_CONFIGURATION_RESOURCES, HADOOP_RESOURCE_CONFIG);
        runner.setProperty(ListHDFS.DIRECTORY, ROOT_DIR);
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FilterMode.FILTER_DIRECTORIES_AND_FILES.getValue());
        runner.setProperty(ListHDFS.FILE_FILTER, "[^\\.].*\\.txt");
    }

    @Test
    @Disabled("Enable this test to create an HDFS file tree")
    void createHdfsNaryCompleteTree() throws IOException {
        createTree(FILE_SYSTEM, new Path(ROOT_DIR), 0);
    }

    /**
     * This only measures an estimate memory usage.
     */
    @Test
    void testListHdfsTimeElapsed() {
        final Runtime runtime = Runtime.getRuntime();
        long usedMemoryBefore = getCurrentlyUsedMemory(runtime);
        Instant start = Instant.now();

        runner.run();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        System.out.println("TIME ELAPSED: " + timeElapsed);

        long usedMemoryAfter = getCurrentlyUsedMemory(runtime);
        System.out.println("Memory increased (MB):" + (usedMemoryAfter - usedMemoryBefore));
    }


    private long getCurrentlyUsedMemory(final Runtime runtime) {
        return (runtime.totalMemory() - runtime.freeMemory()) / BYTE_TO_MB;
    }

    private void createTree(FileSystem fileSystem, Path currentPath, int depth) throws IOException {
        if (depth >= NUM_CHILDREN) {
            for (int j = 0; j < NUM_OF_FILES; j++) {
                fileSystem.createNewFile(new Path(currentPath + "/file_" + j));
            }
            return;
        }

        for (int i = 0; i < NUM_CHILDREN; i++) {
            String childPath = currentPath.toString() + "/dir_" + i;
            Path newPath = new Path(childPath);
            fileSystem.mkdirs(newPath);
            for (int j = 0; j < NUM_OF_FILES; j++) {
                fileSystem.createNewFile(new Path(currentPath + "/file_" + j));
                System.out.println(i + " | " + j + " | File: " + newPath);
            }
            System.out.println(i + " | Directory: " + newPath);
            createTree(fileSystem, newPath, depth + 1);
        }
    }


    private static FileSystem getFileSystem() {
        String[] locations = HADOOP_RESOURCE_CONFIG.split(",");
        Configuration config = new Configuration();
        for (String resource : locations) {
            config.addResource(new Path(resource.trim()));
        }
        try {
            return FileSystem.get(config);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to get FileSystem", e);
        }
    }

    private static class ListHDFSWithMockedFileSystem extends ListHDFS {
        private final KerberosProperties testKerberosProps;

        public ListHDFSWithMockedFileSystem(KerberosProperties kerberosProperties) {
            this.testKerberosProps = kerberosProperties;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProps;
        }
    }

}
