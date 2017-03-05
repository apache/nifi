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
package org.apache.nifi.processors.alluxio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;

public class AlluxioTest {

    public static final String TEST_DIR = "target/alluxio";
    private final FileSystem fsMock = new FileSystemMock();

    @Before
    public void before() {
        if (!new File(TEST_DIR).mkdirs()) {
            Assert.fail();
        }
    }

    @After
    public void after() {
        FileUtils.deleteQuietly(new File(TEST_DIR));
    }

    @Test
    public void testBadPort() {
        LocalListAlluxio listProc = new LocalListAlluxio();
        TestRunner runner = TestRunners.newTestRunner(listProc);
        runner.setProperty(LocalListAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalListAlluxio.MASTER_PORT, "abc");
        runner.setProperty(LocalListAlluxio.URI, "/");
        runner.assertNotValid();
    }

    @Test
    public void testPropertiesPutProcessor() {
        LocalPutAlluxio putProc = new LocalPutAlluxio();
        TestRunner runner = TestRunners.newTestRunner(putProc);
        runner.setProperty(LocalPutAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalPutAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalPutAlluxio.URI, "/");
        runner.setProperty(LocalPutAlluxio.BLOCK_SIZE, "1 MB");

        for (WriteType writeType : WriteType.values()) {
            runner.setProperty(LocalPutAlluxio.WRITE_TYPE, writeType.toString());
            runner.assertValid();
        }

        runner.setProperty(LocalPutAlluxio.WRITE_TYPE, "wrong_type");
        runner.assertNotValid();
    }

    @Test
    public void testPropertiesFetchProcessor() {
        LocalFetchAlluxio fetchProc = new LocalFetchAlluxio();
        TestRunner runner = TestRunners.newTestRunner(fetchProc);
        runner.setProperty(LocalFetchAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalFetchAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalFetchAlluxio.PATH, "/");

        for (ReadType readType : ReadType.values()) {
            runner.setProperty(LocalFetchAlluxio.READ_TYPE, readType.toString());
            runner.assertValid();
        }

        runner.setProperty(LocalFetchAlluxio.READ_TYPE, "wrong_type");
        runner.assertNotValid();
    }

    @Test
    public void testAlluxioProcessors() throws Exception {
        LocalListAlluxio listProc = new LocalListAlluxio();
        LocalFetchAlluxio fetchProc = new LocalFetchAlluxio();
        LocalPutAlluxio putProc = new LocalPutAlluxio();
        TestRunner runner = TestRunners.newTestRunner(listProc);

        runner.setProperty(LocalListAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalListAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalListAlluxio.URI, "/");

        // step 1 : list that returns empty list of files
        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(LocalListAlluxio.REL_SUCCESS).isEmpty());

        // step 2 : fetch not existing file
        runner = TestRunners.newTestRunner(fetchProc);
        runner.setNonLoopConnection(false);
        runner.setProperty(LocalFetchAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalFetchAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalFetchAlluxio.PATH, "/test.txt");

        runner.enqueue("Hello Joe".getBytes());
        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_NOT_FOUND).size() == 1);
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_FAILURE).isEmpty());

        // step 3 : put a file
        runner = TestRunners.newTestRunner(putProc);
        runner.setProperty(LocalPutAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalPutAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalPutAlluxio.URI, "/test.txt");
        runner.setProperty(LocalPutAlluxio.BLOCK_SIZE, "1 MB");

        runner.enqueue("Hello Joe".getBytes());
        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(LocalPutAlluxio.REL_SUCCESS).size() == 1);
        assertTrue(runner.getFlowFilesForRelationship(LocalPutAlluxio.REL_FAILURE).isEmpty());

        // step 4 : list files
        runner = TestRunners.newTestRunner(listProc);
        runner.setProperty(LocalListAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalListAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalListAlluxio.URI, "/");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(LocalListAlluxio.REL_SUCCESS).size() == 1);
        assertNotNull(runner.getFlowFilesForRelationship(LocalListAlluxio.REL_SUCCESS).get(0));
        assertEquals("test.txt", runner.getFlowFilesForRelationship(LocalListAlluxio.REL_SUCCESS).get(0).getAttribute("alluxio_name"));

        // step 5 : get file with direct URI
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(LocalListAlluxio.REL_SUCCESS).get(0);
        runner = TestRunners.newTestRunner(fetchProc);
        runner.setNonLoopConnection(false);
        runner.setProperty(LocalFetchAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalFetchAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalFetchAlluxio.PATH, "/${alluxio_name}");

        runner.enqueue(flowFile);
        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_SUCCESS).size() == 1);
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_FAILURE).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_NOT_FOUND).isEmpty());

        // step 7 : get file with incoming flow file
        runner = TestRunners.newTestRunner(fetchProc);
        runner.setNonLoopConnection(false);

        runner.setProperty(LocalFetchAlluxio.MASTER_HOSTNAME, "localhost");
        runner.setProperty(LocalFetchAlluxio.MASTER_PORT, "19998");
        runner.setProperty(LocalFetchAlluxio.PATH, "/${alluxio_name}");

        MockFlowFile ff = new MockFlowFile(1, flowFile);

        runner.enqueue(ff);
        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_SUCCESS).size() == 1);
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_FAILURE).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(LocalFetchAlluxio.REL_NOT_FOUND).isEmpty());
    }

    private class LocalFetchAlluxio extends FetchAlluxio {
        public LocalFetchAlluxio() {
            fileSystem.set(fsMock);
        }
        @Override
        public void onScheduled(ProcessContext context) {
            fileSystem.set(fsMock);
        }
    }

    private class LocalListAlluxio extends ListAlluxio {
        public LocalListAlluxio() {
            fileSystem.set(fsMock);
        }
        @Override
        public void onScheduled(ProcessContext context) {
            fileSystem.set(fsMock);
        }
    }

    private class LocalPutAlluxio extends PutAlluxio {
        public LocalPutAlluxio() {
            fileSystem.set(fsMock);
        }
        @Override
        public void onScheduled(ProcessContext context) {
            fileSystem.set(fsMock);
        }
    }

}
