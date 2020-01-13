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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGetHDFSFileInfo {
    private static final Pattern SINGLE_JSON_PATTERN = Pattern.compile("^\\{[^\\}]*\\}$");

    private TestRunner runner;
    private GetHDFSFileInfoWithMockedFileSystem proc;
    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;

    @Before
    public void setup() throws InitializationException {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);

        proc = new GetHDFSFileInfoWithMockedFileSystem(kerberosProperties);
        runner = TestRunners.newTestRunner(proc);

        runner.setProperty(GetHDFSFileInfo.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
    }

    @Test
    public void testInvalidBatchSizeWhenDestinationAndGroupingDoesntAllowBatchSize() throws Exception {
        Arrays.asList("1", "2", "100").forEach(
            validBatchSize -> {
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_ALL, validBatchSize, false);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_PARENT_DIR, validBatchSize, false);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_NONE, validBatchSize, false);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_ALL, validBatchSize, false);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_PARENT_DIR, validBatchSize, false);
            }
        );
    }

    @Test
    public void testInvalidBatchSizeWhenValueIsInvalid() throws Exception {
        Arrays.asList("-1", "0", "someString").forEach(
            inValidBatchSize -> {
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, inValidBatchSize, false);
            }
        );
    }

    @Test
    public void testValidBatchSize() throws Exception {
        Arrays.asList("1", "2", "100").forEach(
            validBatchSize -> {
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, validBatchSize, true);
            }
        );

        Arrays.asList((String)null).forEach(
            nullBatchSize -> {
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_ALL, nullBatchSize, true);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_PARENT_DIR, nullBatchSize, true);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_NONE, nullBatchSize, true);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_ALL, nullBatchSize, true);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_PARENT_DIR, nullBatchSize, true);
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, nullBatchSize, true);
            }
        );
    }

    private void testValidateBatchSize(AllowableValue destination, AllowableValue grouping, String batchSize, boolean expectedValid) {
        runner.clearProperties();

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, destination);
        runner.setProperty(GetHDFSFileInfo.GROUPING, grouping);
        if (batchSize != null) {
            runner.setProperty(GetHDFSFileInfo.BATCH_SIZE, "" + batchSize);
        }

        if (expectedValid) {
            runner.assertValid();
        } else {
            runner.assertNotValid();
        }
    }

    @Test
    public void testNoRunOnIncomingConnectionExists() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(true);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "${literal('/some/home/mydir'):substring(0,15)}");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);
    }

    @Test
    public void testRunOnScheduleNoConnections() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "false");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);
    }

    @Test
    public void testValidELFunction() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(true);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "${literal('/some/home/mydir'):substring(0,16)}");
        runner.setProperty(GetHDFSFileInfo.DIR_FILTER, "${literal('_^(dir.*)$_'):substring(1,10)}");
        runner.setProperty(GetHDFSFileInfo.FILE_FILTER, "${literal('_^(.*)$_'):substring(1,7)}");
        runner.setProperty(GetHDFSFileInfo.FILE_EXCLUDE_FILTER, "${literal('_^(none.*)$_'):substring(1,11)}");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "false");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);
        runner.enqueue("foo", new HashMap<String,String>());

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_ORIGINAL).get(0);
        ProcessContext context = runner.getProcessContext();

        assertEquals(context.getProperty(GetHDFSFileInfo.FULL_PATH).evaluateAttributeExpressions(mff).getValue(), "/some/home/mydir");
        assertEquals(context.getProperty(GetHDFSFileInfo.DIR_FILTER).evaluateAttributeExpressions(mff).getValue(), "^(dir.*)$");
        assertEquals(context.getProperty(GetHDFSFileInfo.FILE_FILTER).evaluateAttributeExpressions(mff).getValue(), "^(.*)$");
        assertEquals(context.getProperty(GetHDFSFileInfo.FILE_EXCLUDE_FILTER).evaluateAttributeExpressions(mff).getValue(), "^(none.*)$");
    }

    @Test
    public void testRunWithConnections() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("input.dir", "/some/home/mydir");

        runner.setIncomingConnection(true);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "${input.dir}");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "false");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.enqueue("foo", attributes);
        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_ORIGINAL).get(0);
        ProcessContext context = runner.getProcessContext();

        assertEquals(context.getProperty(GetHDFSFileInfo.FULL_PATH).evaluateAttributeExpressions(mff).getValue(), "/some/home/mydir");
    }

    @Test
    public void testRunWithIOException() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);
        proc.fileSystem.addFileStatus(proc.fileSystem.newDir("/some/home/mydir"), proc.fileSystem.newFile("/some/home/mydir/exception_java.io.InterruptedIOException"));

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir/exception_java.io.InterruptedIOException");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "false");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 1);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_FAILURE).get(0);
        mff.assertAttributeEquals("hdfs.status", "Failed due to: java.io.InterruptedIOException");
    }

    @Test
    public void testRunWithPermissionsExceptionAttributes() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);
        proc.fileSystem.addFileStatus(proc.fileSystem.newDir("/some/home/mydir/dir1"), proc.fileSystem.newDir("/some/home/mydir/dir1/list_exception_java.io.InterruptedIOException"));

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_ATTRIBUTES);
        runner.setProperty(GetHDFSFileInfo.GROUPING, GetHDFSFileInfo.GROUP_PARENT_DIR);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 6);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);
    }

    @Test
    public void testRunWithPermissionsExceptionContent() throws Exception {

        setFileSystemBasicTree(proc.fileSystem);
        proc.fileSystem.addFileStatus(proc.fileSystem.newDir("/some/home/mydir/dir1"), proc.fileSystem.newDir("/some/home/mydir/dir1/list_exception_java.io.InterruptedIOException"));

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);
        runner.setProperty(GetHDFSFileInfo.GROUPING, GetHDFSFileInfo.GROUP_ALL);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_SUCCESS).get(0);
        mff.assertContentEquals(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRunWithPermissionsExceptionContent.json"));
    }

    @Test
    public void testObjectNotFound() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir/ObjectThatDoesNotExist");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 1);
    }

    @Test
    public void testRecursive() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);
    }

    @Test
    public void testRecursiveGroupAllToAttributes() throws Exception {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_ATTRIBUTES);
        runner.setProperty(GetHDFSFileInfo.GROUPING, GetHDFSFileInfo.GROUP_ALL);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("hdfs.objectName", "mydir");
        mff.assertAttributeEquals("hdfs.path", "/some/home");
        mff.assertAttributeEquals("hdfs.type", "directory");
        mff.assertAttributeEquals("hdfs.owner", "owner");
        mff.assertAttributeEquals("hdfs.group", "group");
        mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
        mff.assertAttributeEquals("hdfs.length", ""+900);
        mff.assertAttributeEquals("hdfs.count.files", ""+9);
        mff.assertAttributeEquals("hdfs.count.dirs", ""+10);
        mff.assertAttributeEquals("hdfs.replication", ""+3);
        mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
        mff.assertAttributeNotExists("hdfs.status");

        final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupAllToAttributes.json")));
        mff.assertAttributeEquals("hdfs.full.tree", expected);
    }

    @Test
    public void testRecursiveGroupNoneToAttributes() throws InterruptedException {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_ATTRIBUTES);
        runner.setProperty(GetHDFSFileInfo.GROUPING, GetHDFSFileInfo.GROUP_NONE);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 9);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);

        int matchCount = 0;
        for (final MockFlowFile mff : runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_SUCCESS)) {
            if (mff.getAttribute("hdfs.objectName").equals("mydir")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+900);
                mff.assertAttributeEquals("hdfs.count.files", ""+9);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+10);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("dir1")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+200);
                mff.assertAttributeEquals("hdfs.count.files", ""+2);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+3);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("dir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+200);
                mff.assertAttributeEquals("hdfs.count.files", ""+2);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+3);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("regDir")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir1");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+0);
                mff.assertAttributeEquals("hdfs.count.files", ""+0);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+1);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("regDir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir2");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+0);
                mff.assertAttributeEquals("hdfs.count.files", ""+0);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+1);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("regFile")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir1");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("regFile2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir2");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
            }else if (mff.getAttribute("hdfs.objectName").equals("regFile4")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
              }else if (mff.getAttribute("hdfs.objectName").equals("regFile5")) {
              matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
            }else {
               runner.assertNotValid();
            }
        }
        Assert.assertEquals(matchCount, 9);
    }

    @Test
    public void testRecursiveGroupDirToAttributes() throws Exception {

        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_ATTRIBUTES);
        runner.setProperty(GetHDFSFileInfo.GROUPING, GetHDFSFileInfo.GROUP_PARENT_DIR);

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 5);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);

        int matchCount = 0;
        for (final MockFlowFile mff : runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_SUCCESS)) {
            if (mff.getAttribute("hdfs.objectName").equals("mydir")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+900);
                mff.assertAttributeEquals("hdfs.count.files", ""+9);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+10);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-mydir.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            }else if (mff.getAttribute("hdfs.objectName").equals("dir1")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+200);
                mff.assertAttributeEquals("hdfs.count.files", ""+2);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+3);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-dir1.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            }else if (mff.getAttribute("hdfs.objectName").equals("dir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+200);
                mff.assertAttributeEquals("hdfs.count.files", ""+2);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+3);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-dir2.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            }else if (mff.getAttribute("hdfs.objectName").equals("regDir")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir1");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+0);
                mff.assertAttributeEquals("hdfs.count.files", ""+0);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+1);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-regDir.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            }else if (mff.getAttribute("hdfs.objectName").equals("regDir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir2");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", ""+1523456000000L);
                mff.assertAttributeEquals("hdfs.length", ""+0);
                mff.assertAttributeEquals("hdfs.count.files", ""+0);
                mff.assertAttributeEquals("hdfs.count.dirs", ""+1);
                mff.assertAttributeEquals("hdfs.replication", ""+3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-regDir2.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            }else {
               runner.assertNotValid();
            }
        }
        Assert.assertEquals(matchCount, 5);
    }

    @Test
    public void testBatchSizeWithDestAttributesGroupAllBatchSizeNull() throws Exception {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_ALL, 1);
    }

    @Test
    public void testBatchSizeWithDestAttributesGroupDirBatchSizeNull() throws Exception {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_PARENT_DIR, 5);
    }

    @Test
    public void testBatchSizeWithDestAttributesGroupNoneBatchSizeNull() throws Exception {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_NONE, 9);
    }

    @Test
    public void testBatchSizeWithDestContentGroupAllBatchSizeNull() throws Exception {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_ALL, 1);
    }

    @Test
    public void testBatchSizeWithDestContentGroupDirBatchSizeNull() throws Exception {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_PARENT_DIR, 5);
    }

    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSizeNull() throws Exception {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 9);

        checkContentSizes(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize1() throws Exception {
        testBatchSize("1", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 9);
        checkContentSizes(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize3() throws Exception {
        testBatchSize("3", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 3);
        checkContentSizes(Arrays.asList(3, 3, 3));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize4() throws Exception {
        testBatchSize("4", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 3);
        checkContentSizes(Arrays.asList(4, 4, 1));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize5() throws Exception {
        testBatchSize("5", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 2);
        checkContentSizes(Arrays.asList(5, 4));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize9() throws Exception {
        testBatchSize("9", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 1);
        checkContentSizes(Arrays.asList(9));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize100() throws Exception {
        testBatchSize("100", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 1);
        checkContentSizes(Arrays.asList(9));
    }

    private void testBatchSize(String batchSize, AllowableValue destination, AllowableValue grouping, int expectedNrTransferredToSuccess) {
        setFileSystemBasicTree(proc.fileSystem);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, destination);
        runner.setProperty(GetHDFSFileInfo.GROUPING, grouping);
        if (batchSize != null) {
            runner.setProperty(GetHDFSFileInfo.BATCH_SIZE, batchSize);
        }

        runner.run();

        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, expectedNrTransferredToSuccess);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);
    }

    private void checkContentSizes(List<Integer> expectedNumberOfRecords) {
        List<Integer> actualNumberOfRecords = runner.getFlowFilesForRelationship(GetHDFSFileInfo.REL_SUCCESS).stream()
            .map(MockFlowFile::toByteArray)
            .map(String::new)
            .map(
                content -> Arrays.stream(content.split("\n"))
                    .filter(line -> SINGLE_JSON_PATTERN.matcher(line).matches())
                    .count()
            )
            .map(Long::intValue)
            .collect(Collectors.toList());

        assertEquals(expectedNumberOfRecords, actualNumberOfRecords);
    }

    /*
     * For all basic tests, this provides a structure of files in dirs:
     * Total number of dirs:  9 (1 root, 4 dotted)
     * Total number of files: 8 (4 dotted)
     */
    protected void setFileSystemBasicTree(final MockFileSystem fs) {
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newFile("/some/home/mydir/regFile4"));
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newFile("/some/home/mydir/regFile5"));
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newFile("/some/home/mydir/.dotFile4"));
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newFile("/some/home/mydir/.dotFile5"));

        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newDir("/some/home/mydir/dir1"));
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newDir("/some/home/mydir/dir2"));
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newDir("/some/home/mydir/.dir3"));
        fs.addFileStatus(fs.newDir("/some/home/mydir"), fs.newDir("/some/home/mydir/.dir4"));

        fs.addFileStatus(fs.newDir("/some/home/mydir/dir1"), fs.newFile("/some/home/mydir/dir1/.dotFile"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/dir1"), fs.newDir("/some/home/mydir/dir1/.dotDir"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/dir1"), fs.newFile("/some/home/mydir/dir1/regFile"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/dir1"), fs.newDir("/some/home/mydir/dir1/regDir"));

        fs.addFileStatus(fs.newDir("/some/home/mydir/dir2"), fs.newFile("/some/home/mydir/dir2/.dotFile2"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/dir2"), fs.newDir("/some/home/mydir/dir2/.dotDir2"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/dir2"), fs.newFile("/some/home/mydir/dir2/regFile2"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/dir2"), fs.newDir("/some/home/mydir/dir2/regDir2"));

        fs.addFileStatus(fs.newDir("/some/home/mydir/.dir3"), fs.newFile("/some/home/mydir/.dir3/regFile3"));
        fs.addFileStatus(fs.newDir("/some/home/mydir/.dir3"), fs.newDir("/some/home/mydir/.dir3/regDir3"));
    }

    static FsPermission perms(short p) {
        return new FsPermission(p);
    }


    private class GetHDFSFileInfoWithMockedFileSystem extends GetHDFSFileInfo {
        private final MockFileSystem fileSystem = new MockFileSystem();
        private final KerberosProperties testKerberosProps;

        public GetHDFSFileInfoWithMockedFileSystem(KerberosProperties kerberosProperties) {
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
        private final Map<Path, FileStatus> pathToStatus = new HashMap<>();

        public void addFileStatus(final FileStatus parent, final FileStatus child) {
            Set<FileStatus> children = fileStatuses.get(parent.getPath());
            if (children == null) {
                children = new HashSet<>();
                fileStatuses.put(parent.getPath(), children);
            }
            if (child != null) {
                children.add(child);
                if (child.isDirectory() && !fileStatuses.containsKey(child.getPath())) {
                    fileStatuses.put(child.getPath(), new HashSet<FileStatus>());
                }
            }

            pathToStatus.put(parent.getPath(), parent);
            pathToStatus.put(child.getPath(), child);
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
            if (!fileStatuses.containsKey(f)) {
                throw new FileNotFoundException();
            }
            if (f.getName().startsWith("list_exception_")) {
                String clzName = f.getName().substring("list_exception_".length(), f.getName().length());
                IOException exception = null;
                try {
                     exception = (IOException)Class.forName(clzName).newInstance();
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
                throw exception;
            }
            final Set<FileStatus> statuses = fileStatuses.get(f);
            if (statuses == null) {
                return new FileStatus[0];
            }

            for (FileStatus s : statuses) {
                getFileStatus(s.getPath()); //support exception handling only.
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
            if (f!=null && f.getName().startsWith("exception_")) {
                String clzName = f.getName().substring("exception_".length(), f.getName().length());
                IOException exception = null;
                try {
                     exception = (IOException)Class.forName(clzName).newInstance();
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
                throw exception;
            }
            final FileStatus fileStatus = pathToStatus.get(f);
            if (fileStatus == null) throw new FileNotFoundException();
            return fileStatus;
        }

        public FileStatus newFile(String p) {
            return new FileStatus(100L, false, 3, 128*1024*1024, 1523456000000L, 1523457000000L, perms((short)0644), "owner", "group", new Path(p));
        }
        public FileStatus newDir(String p) {
            return new FileStatus(1L, true, 3, 128*1024*1024, 1523456000000L, 1523457000000L, perms((short)0755), "owner", "group", new Path(p));
        }
    }
}
