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

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.*;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.*;

import static org.apache.nifi.processors.hadoop.ScanHDFS.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestScanHDFS {

    private TestRunner runner;
    private ScanHDFSWithMockedFileSystem proc;
    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @Before
    public void setup() throws InitializationException {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);
        proc = new ScanHDFSWithMockedFileSystem(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);

        runner.setProperty(ScanHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
        runner.setProperty(ScanHDFS.DIRECTORY, "/test");
        runner.setIncomingConnection(false);
        runner.setProperty(ScanHDFS.RECURSE_SUBDIRS,"false");
    }

    @Test
    public void testListingNoIncomingConnectionWithValidELFunction() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891234L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        runner.setProperty(ScanHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567891234");
        mff.assertAttributeEquals("hdfs.number.of.files", "1");
        assertEquals(1, evalJsonPath(mff,"$.num_files"));
        assertEquals( "/test/testFile.txt", evalJsonPath(mff, "$.files[0].path"));
    }

    @Test
    public void testListingNoIncomingConnectionWithFilter() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891234L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ScanHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");
        runner.setProperty(ScanHDFS.FILE_FILTER, "[^test].*");
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("hdfs.last.modified.ts", "-1");
        mff.assertAttributeEquals("hdfs.number.of.files", "0");
        assertEquals(0, evalJsonPath(mff,"$.num_files"));
    }

    @Test
    public void testListingWithInvalidELFunction() {
        runner.setProperty(ScanHDFS.DIRECTORY, "${literal('/test'):foo()}");
        runner.assertNotValid();
    }

    @Test
    public void testListingWithUnrecognizedELFunction() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891234L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ScanHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 0);
    }

    @Test
    public void testListingHasCorrectAttributes() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891234L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        runner.run();

        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567891234");
        mff.assertAttributeEquals("hdfs.number.of.files", "1");
    }


    @Test
    public void testDynamicListingForIncomingFlowfiles() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567892000L, 0L, create777(), "owner", "group", new Path("/test/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567892000L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 1234567891000L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 1234567891000L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 1234567891000L, 0L, create777(), "owner", "group", new Path("/test/testDir/2.txt")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 1234567893000L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/3.txt")));

        runner.setProperty(ScanHDFS.DIRECTORY,"${directory}");
        runner.setProperty(ScanHDFS.RECURSE_SUBDIRS,"${recursive}");
        runner.setProperty(ScanHDFS.FILE_FILTER,"${regex}");
        runner.setIncomingConnection(true);

        Map<String, String> attrMap = new HashMap<>();

        attrMap.put("directory", "/test");
        attrMap.put("recursive", "true");
        attrMap.put("regex", "^[0-9]\\.txt");
        runner.enqueue(new byte[]{}, attrMap);

        attrMap.clear();
        attrMap.put("directory", "/test/testDir");
        attrMap.put("recursive", "false");
        runner.enqueue(new byte[]{}, attrMap);

        runner.run(2);
        runner.assertTransferCount(ScanHDFS.REL_ORIGINAL, 2);
        runner.assertTransferCount(ScanHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS);

        MockFlowFile mff = flowFiles.get(0);
        Object obj = evalJsonPath(mff, "$.files[*].path");
        assertTrue(((LinkedList<String>) obj).contains("/test/testDir/anotherDir/3.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testDir/2.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/1.txt"));
        assertTrue( ! ((LinkedList<String>) obj).contains("/test/testFile.txt"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567893000");

        mff = flowFiles.get(1);
        assertEquals(1, evalJsonPath(mff, "$.num_files"));
        obj = evalJsonPath(mff, "$.files[*].path");
        assertTrue(((LinkedList<String>) obj).contains("/test/testDir/2.txt"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567891000");
    }

    @Test
    public void testHoldBackFilesInModification() throws InterruptedException {
        //original 2 files present on target root path
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891000L, 0L, create777(), "owner", "group", new Path("/test/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567892000L, 0L, create777(), "owner", "group", new Path("/test/2.txt")));

        //set wait time in between listings to 1 second
        runner.setProperty(ScanHDFS.LISTING_DELAY, "1000 millis");

        Thread thread = new AppendHdfs();
        runner.run();

        //inject 2 new files during wait time
        thread.start();
        thread.join();

        runner.assertTransferCount(ScanHDFS.REL_SUCCESS, 1);
        MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);

        assertEquals(2, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567892000");
        mff.assertAttributeEquals("hdfs.number.of.files", "2");
        Object obj = evalJsonPath(mff, "$.files[*].filename");
        assertTrue(((LinkedList<String>) obj).contains("1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("2.txt"));
        assertTrue(!((LinkedList<String>) obj).contains("3.txt"));
        assertTrue(!((LinkedList<String>) obj).contains("4.txt"));
    }

    @Test
    public void testNoHoldBackStaticFilesWithDefaultDelay() {
        //original 2 files present on target root path
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891000L, 0L, create777(), "owner", "group", new Path("/test/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567892000L, 0L, create777(), "owner", "group", new Path("/test/2.txt")));

        // Listing will run twice and find same last modification date, so both files qualify
        runner.run();
        runner.assertTransferCount(ScanHDFS.REL_SUCCESS, 1);
        MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);

        assertEquals(2, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567892000");
        mff.assertAttributeEquals("hdfs.number.of.files", "2");
        Object obj = evalJsonPath(mff, "$.files[*].filename");
        assertTrue(((LinkedList<String>) obj).contains("1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("2.txt"));
    }

    @Test
    public void testNoHoldBackFilesInModificationWithDelayZero() {
        //original 2 files present on target root path
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567891000L, 0L, create777(), "owner", "group", new Path("/test/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567892000L, 0L, create777(), "owner", "group", new Path("/test/2.txt")));
        runner.setProperty(ScanHDFS.LISTING_DELAY, "0 millis");
        runner.run();

        runner.assertTransferCount(ScanHDFS.REL_SUCCESS, 1);
        MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);

        assertEquals(2, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1234567892000");
        mff.assertAttributeEquals("hdfs.number.of.files", "2");
        Object obj = evalJsonPath(mff, "$.files[*].filename");
        assertTrue(((LinkedList<String>) obj).contains("1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("2.txt"));
    }

    class AppendHdfs extends Thread {

        public void run() {
            proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567893000L, 0L, create777(), "owner", "group", new Path("/test/3.txt")));
            proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1234567894000L, 0L, create777(), "owner", "group", new Path("/test/4.txt")));
        }
    }

    @Test
    public void testRecursiveWithCustomFilterFilesOnly() {
        // set custom regex filter
        runner.setProperty(ScanHDFS.FILE_FILTER, "[^\\.].*\\.txt");
        runner.setProperty(ScanHDFS.RECURSE_SUBDIRS,"true");

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 100L, 0L, create777(), "owner", "group", new Path("/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 200L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 300L, 0L, create777(), "owner", "group", new Path("/test/.partfile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 400L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 400L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, true, 1, 1L, 700L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 500L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/.txt")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 600L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 700L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.txt")));

        runner.run();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);

        MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);

        assertEquals(3, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "700");
        mff.assertAttributeEquals("hdfs.number.of.files", "3");
        Object obj = evalJsonPath(mff, "$.files[*].path");
        assertTrue(((LinkedList<String>) obj).contains("/test/testFile.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testDir/1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testDir/anotherDir/2.txt"));
    }

    @Test
    public void testNotRecursive() {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1000L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 2000L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        runner.run();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);

        assertEquals(1, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.last.modified.ts", "1000");
        Object obj = evalJsonPath(mff, "$.files[*].path");
        assertTrue(((LinkedList<String>) obj).contains("/test/testFile.txt"));
    }

    @Test
    public void testMinAgeMaxAge() {
        MockFlowFile mff = null;
        Object obj = null;
        long now = new Date().getTime();
        long oneHourAgo = now - 3600000;
        long twoHoursAgo = now - 2*3600000;
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now, now, create777(), "owner", "group", new Path("/test/File.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now-5, now-5, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, oneHourAgo, oneHourAgo, create777(), "owner", "group", new Path("/test/testFile1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, twoHoursAgo, twoHoursAgo, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        // all files
        runner.run();
        runner.assertValid();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        runner.clearTransferState();

        // invalid min_age > max_age
        runner.setProperty(ScanHDFS.MIN_AGE, "30 sec");
        runner.setProperty(ScanHDFS.MAX_AGE, "1 sec");
        runner.assertNotValid();

        // only one file (one hour ago)
        runner.setProperty(ScanHDFS.MIN_AGE, "30 sec");
        runner.setProperty(ScanHDFS.MAX_AGE, "90 min");
        runner.assertValid();
        runner.run(); // will ignore the file for this cycle
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0).assertAttributeEquals("hdfs.last.modified.ts", ((Long) oneHourAgo).toString());
        runner.clearTransferState();

        // two files (one hour ago and two hours ago)
        runner.setProperty(ScanHDFS.MIN_AGE, "30 sec");
        runner.removeProperty(ScanHDFS.MAX_AGE);
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        mff.assertAttributeEquals("hdfs.number.of.files", "2");
        assertEquals(2, evalJsonPath(mff, "$.num_files"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testFile1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testFile2.txt"));
        runner.clearTransferState();

        // three files (now, now-5 and one hour ago)
        runner.setProperty(ScanHDFS.MIN_AGE, "0 sec");
        runner.setProperty(ScanHDFS.MAX_AGE, "90 min");
        runner.assertValid();
        runner.run();
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        assertEquals(3, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "3");
        assertTrue(((LinkedList<String>) obj).contains("/test/File.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testFile.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/testFile1.txt"));
    }

    @Test
    public void testMinMaxModificationTsFilter() {
        MockFlowFile mff = null;
        Object obj = null;
        long now = new Date().getTime();
        long oneHourAgo = now - 3600000;
        long twoHoursAgo = now - 2*3600000;
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now, now, create777(), "owner", "group", new Path("/test/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now-5, now-5, create777(), "owner", "group", new Path("/test/2.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, oneHourAgo, oneHourAgo, create777(), "owner", "group", new Path("/test/3.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, twoHoursAgo, twoHoursAgo, create777(), "owner", "group", new Path("/test/4.txt")));

        // with the defaults MIN_MOD and MAX_MOD all files should be listed
        runner.assertValid();
        runner.run();
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        assertEquals(4, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "4");
        runner.clearTransferState();

        // return 1.txt, 2.txt & 3.txt
        runner.setProperty(ScanHDFS.MIN_MOD, Long.toString(oneHourAgo-1L));
        runner.setProperty(ScanHDFS.MAX_MOD, Long.toString(now+1L));
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        assertEquals(3, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "3");
        assertTrue(((LinkedList<String>) obj).contains("/test/1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/2.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/3.txt"));
        runner.clearTransferState();

        // return only 2.txt, MIN_MOD and MAX_MOD are exclusive
        runner.setProperty(ScanHDFS.MIN_MOD, Long.toString(oneHourAgo));
        runner.setProperty(ScanHDFS.MAX_MOD, Long.toString(now));
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        assertEquals(1, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "1");
        assertTrue(((LinkedList<String>) obj).contains("/test/2.txt"));
        runner.clearTransferState();

        // return nothing when MIN_MOD > MAX_MOD
        runner.setProperty(ScanHDFS.MAX_MOD, Long.toString(oneHourAgo));
        runner.setProperty(ScanHDFS.MIN_MOD, Long.toString(now));
        runner.run();
        runner.assertAllFlowFilesTransferred(ScanHDFS.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        assertEquals(0, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "0");
    }

    @Test
    public void testMinMaxModificationTsDynamicFilter() {
        MockFlowFile mff = null;
        Object obj = null;
        long now = new Date().getTime();
        long oneHourAgo = now - 3600000;
        long twoHoursAgo = now - 2*3600000;
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now, now, create777(), "owner", "group", new Path("/test/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now-5, now-5, create777(), "owner", "group", new Path("/test/2.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, oneHourAgo, oneHourAgo, create777(), "owner", "group", new Path("/test/3.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, twoHoursAgo, twoHoursAgo, create777(), "owner", "group", new Path("/test/4.txt")));

        runner.setProperty(ScanHDFS.DIRECTORY,"${directory}");
        runner.setProperty(ScanHDFS.MIN_MOD,"${min_mod}");
        runner.setIncomingConnection(true);

        Map<String, String> attrMap = new HashMap<>();

        attrMap.put("directory", "/test");
        attrMap.put("min_mod", Long.toString(oneHourAgo-1));
        runner.enqueue(new byte[]{}, attrMap);

        // Run the first enqueued ff only
        runner.run();
        runner.assertTransferCount(ScanHDFS.REL_ORIGINAL, 1);
        runner.assertTransferCount(ScanHDFS.REL_SUCCESS, 1);

        // at oneHourAgo-1, 4.txt does not list
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        assertEquals(3, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "3");
        mff.assertAttributeEquals("hdfs.last.modified.ts", Long.toString(now));
        assertTrue(((LinkedList<String>) obj).contains("/test/1.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/2.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/3.txt"));
        assertTrue( ! ((LinkedList<String>) obj).contains("/test/4.txt"));

        // Inject 2 brand new files with mod ts past now
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now+1000, now+1000, create777(), "owner", "group", new Path("/test/newA.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now+30000, now+30000, create777(), "owner", "group", new Path("/test/newB.txt")));

        attrMap.clear();
        attrMap.put("directory", "/test");
        attrMap.put("min_mod", Long.toString(now));
        runner.enqueue(new byte[]{}, attrMap);
        runner.clearTransferState();

        runner.run();
        mff = runner.getFlowFilesForRelationship(ScanHDFS.REL_SUCCESS).get(0);
        obj = evalJsonPath(mff, "$.files[*].path");
        assertEquals(2, evalJsonPath(mff, "$.num_files"));
        mff.assertAttributeEquals("hdfs.number.of.files", "2");
        mff.assertAttributeEquals("hdfs.last.modified.ts", Long.toString(now+30000));
        assertTrue(((LinkedList<String>) obj).contains("/test/newA.txt"));
        assertTrue(((LinkedList<String>) obj).contains("/test/newB.txt"));
    }

    private static Object evalJsonPath(MockFlowFile mff, String jsonPathAsString){
        final com.jayway.jsonpath.Configuration STRICT_PROVIDER_CONFIGURATION = com.jayway.jsonpath.Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();
        InputStream inputStream = new ByteArrayInputStream(mff.toByteArray());
        DocumentContext ctx = JsonPath.using(STRICT_PROVIDER_CONFIGURATION).parse(inputStream);

        return ctx.read(JsonPath.compile(jsonPathAsString));
    }

    private FsPermission create777() {
        return new FsPermission((short) 0777);
    }


    private class ScanHDFSWithMockedFileSystem extends ScanHDFS {
        private final MockFileSystem fileSystem = new MockFileSystem();
        private final KerberosProperties testKerberosProps;

        public ScanHDFSWithMockedFileSystem(KerberosProperties kerberosProperties) {
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
        protected FileSystem getFileSystem(final Configuration config) throws IOException {
            return fileSystem;
        }
    }

    private class MockFileSystem extends FileSystem {
        private final Map<Path, Set<FileStatus>> fileStatuses = new HashMap<>();

        public void addFileStatus(final Path parent, final FileStatus child) {
            Set<FileStatus> children = fileStatuses.get(parent);
            if (children == null) {
                children = new HashSet<>();
                fileStatuses.put(parent, children);
            }

            children.add(child);
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
        public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication,
                                         final long blockSize, final Progressable progress) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
            return null;
        }

        @Override
        public boolean rename(final Path src, final Path dst) throws IOException {
            return false;
        }

        @Override
        public boolean delete(final Path f, final boolean recursive) throws IOException {
            return false;
        }

        @Override
        public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
            final Set<FileStatus> statuses = fileStatuses.get(f);
            if (statuses == null) {
                return new FileStatus[0];
            }

            return statuses.toArray(new FileStatus[statuses.size()]);
        }

        @Override
        public void setWorkingDirectory(final Path new_dir) {

        }

        @Override
        public Path getWorkingDirectory() {
            return new Path(new File(".").getAbsolutePath());
        }

        @Override
        public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
            return false;
        }

        @Override
        public FileStatus getFileStatus(final Path f) throws IOException {
            return null;
        }

    }

}
