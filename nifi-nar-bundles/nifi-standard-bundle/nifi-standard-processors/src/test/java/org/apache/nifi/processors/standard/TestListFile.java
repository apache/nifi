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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestListFile  {

    final String TESTDIR = "target/test/data/in";
    final File testDir = new File(TESTDIR);
    ListFile processor;
    TestRunner runner;
    ProcessContext context;
    ProcessorInitializationContext initContext;

    @Before
    public void setUp() throws Exception {
        processor = new ListFile();
        runner = TestRunners.newTestRunner(processor);
        context = runner.getProcessContext();
        deleteDirectory(testDir);
        assertTrue("Unable to create test data directory " + testDir.getAbsolutePath(), testDir.exists() || testDir.mkdirs());
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

    @After
    public void tearDown() throws Exception {
        deleteDirectory(testDir);
        File tempFile = processor.getPersistenceFile();
        if (tempFile.exists()) {
            assertTrue(tempFile.delete());
        }
    }

    @Test
    public void testGetSupportedPropertyDescriptors() throws Exception {
        List<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();
        assertEquals(1, properties.size());
        assertEquals(ListFile.PATH, properties.get(0));
    }

    @Test
    public void testGetRelationships() throws Exception {
        Set<Relationship> relationships = processor.getRelationships();
        assertEquals(1, relationships.size());
        assertEquals(AbstractListProcessor.REL_SUCCESS, relationships.toArray()[0]);
    }

    @Test
    public void testGetPath() {
        runner.setProperty(ListFile.PATH, "/dir/test1");
        assertEquals("/dir/test1", processor.getPath(context));
        runner.setProperty(ListFile.PATH, "${literal(\"/DIR/TEST2\"):toLower()}");
        assertEquals("/dir/test2", processor.getPath(context));
    }

    @Test
    public void testPerformListing() throws Exception {

        Long now = System.currentTimeMillis();

        // process first file and set new timestamp
        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(now - 10000));
        runner.setProperty(ListFile.PATH, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles.size());

        // process second file after timestamp
        final File file2 = new File(TESTDIR + "/file2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(now - 5000));
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());

        // process third file before timestamp
        final File file3 = new File(TESTDIR + "/file3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(now - 10000));
        System.out.printf("%d %d %d\n", file1.lastModified(), file2.lastModified(), file3.lastModified());
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(0, successFiles3.size());

        // force state to reset and process all files
        runner.clearTransferState();
        runner.removeProperty(ListFile.PATH);
        runner.setProperty(ListFile.PATH, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles4.size());
    }

    @Test
    public void testIsListingResetNecessary() throws Exception {
        assertEquals(true, processor.isListingResetNecessary(ListFile.PATH));
        assertEquals(false, processor.isListingResetNecessary(new PropertyDescriptor.Builder().name("x").build()));
    }
}
