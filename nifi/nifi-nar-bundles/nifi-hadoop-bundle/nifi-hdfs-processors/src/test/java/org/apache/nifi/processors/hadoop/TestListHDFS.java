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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestListHDFS {

    private TestRunner runner;
    private ListHDFSWithMockedFileSystem proc;
    private MockCacheClient service;

    @Before
    public void setup() throws InitializationException {
        proc = new ListHDFSWithMockedFileSystem();
        runner = TestRunners.newTestRunner(proc);

        service = new MockCacheClient();
        runner.addControllerService("service", service);
        runner.enableControllerService(service);

        runner.setProperty(ListHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
        runner.setProperty(ListHDFS.DIRECTORY, "/test");
        runner.setProperty(ListHDFS.DISTRIBUTED_CACHE_SERVICE, "service");
    }

    @Test
    public void testListingHasCorrectAttributes() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    public void testRecursive() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");

        final MockFlowFile mff2 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(1);
        mff2.assertAttributeEquals("path", "/test/testDir");
        mff2.assertAttributeEquals("filename", "1.txt");
    }

    @Test
    public void testNotRecursive() {
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
    public void testNoListUntilUpdateFromRemoteOnPrimaryNodeChange() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");

        runner.clearTransferState();

        // add new file to pull
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        // trigger primary node change
        proc.onPrimaryNodeChange(PrimaryNodeState.ELECTED_PRIMARY_NODE);

        // cause calls to service to fail
        service.failOnCalls = true;

        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        final String key = proc.getKey("/test");

        // wait just to a bit to ensure that the timestamp changes when we update the service
        final Object curVal = service.values.get(key);
        try {
            Thread.sleep(10L);
        } catch (final InterruptedException ie) {
        }

        service.failOnCalls = false;
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        // ensure state saved both locally & remotely
        assertTrue(proc.localStateSaved);
        assertNotNull(service.values.get(key));
        assertNotSame(curVal, service.values.get(key));
    }


    private FsPermission create777() {
        return new FsPermission((short) 0777);
    }


    private class ListHDFSWithMockedFileSystem extends ListHDFS {
        private final MockFileSystem fileSystem = new MockFileSystem();
        private boolean localStateSaved = false;

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem;
        }

        @Override
        protected File getPersistenceFile() {
            return new File("target/conf/state-file");
        }

        @Override
        protected FileSystem getFileSystem(Configuration config) throws IOException {
            return fileSystem;
        }

        @Override
        protected void persistLocalState(String directory, String serializedState) throws IOException {
            super.persistLocalState(directory, serializedState);
            localStateSaved = true;
        }
    }


    private class MockFileSystem extends FileSystem {
        private final Map<Path, Set<FileStatus>> fileStatuses = new HashMap<>();

        public void addFileStatus(final Path parent, final FileStatus child) {
            Set<FileStatus> children = fileStatuses.get(parent);
            if ( children == null ) {
                children = new HashSet<>();
                fileStatuses.put(parent, children);
            }

            children.add(child);
        }


        @Override
        public long getDefaultBlockSize() {
            return 1024L;
        }

        @Override
        public short getDefaultReplication() {
            return 1;
        }

        @Override
        public URI getUri() {
            return null;
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
            return null;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            return false;
        }

        @Override
        public boolean delete(Path f, boolean recursive) throws IOException {
            return false;
        }

        @Override
        public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
            final Set<FileStatus> statuses = fileStatuses.get(f);
            if ( statuses == null ) {
                return new FileStatus[0];
            }

            return statuses.toArray(new FileStatus[statuses.size()]);
        }

        @Override
        public void setWorkingDirectory(Path new_dir) {

        }

        @Override
        public Path getWorkingDirectory() {
            return new Path(new File(".").getAbsolutePath());
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission) throws IOException {
            return false;
        }

        @Override
        public FileStatus getFileStatus(Path f) throws IOException {
            return null;
        }

    }


    private class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        private void verifyNotFail() throws IOException {
            if ( failOnCalls ) {
                throw new IOException("Could not call to remote service because Unit Test marked service unavailable");
            }
        }

        @Override
        public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            final Object retValue = values.putIfAbsent(key, value);
            return (retValue == null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.putIfAbsent(key, value);
        }

        @Override
        public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
            verifyNotFail();
            return values.containsKey(key);
        }

        @Override
        public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            values.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
            verifyNotFail();
            values.remove(key);
            return true;
        }
    }
}
