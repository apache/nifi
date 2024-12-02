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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.util.MockFileSystem;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGetHDFSFileInfo {
    private static final Pattern SINGLE_JSON_PATTERN = Pattern.compile("^\\{[^\\}]*\\}$");

    private TestRunner runner;
    private GetHDFSFileInfoWithMockedFileSystem proc;

    @BeforeEach
    public void setup() throws InitializationException {
        proc = new GetHDFSFileInfoWithMockedFileSystem();
        runner = TestRunners.newTestRunner(proc);

        runner.setProperty(GetHDFSFileInfo.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
    }

    @Test
    public void testInvalidBatchSizeWhenDestinationAndGroupingDoesntAllowBatchSize() {
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
    public void testInvalidBatchSizeWhenValueIsInvalid() {
        Arrays.asList("-1", "0", "someString").forEach(
            inValidBatchSize -> {
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, inValidBatchSize, false);
            }
        );
    }

    @Test
    public void testValidBatchSize() {
        Arrays.asList("1", "2", "100").forEach(
            validBatchSize -> {
                testValidateBatchSize(GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, validBatchSize, true);
            }
        );

        Arrays.asList((String) null).forEach(
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
        runner.enqueue("foo", new HashMap<>());

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
    public void testWithGSSException() {
        proc.fileSystem.setFailOnExists(true);

        runner.setIncomingConnection(false);
        runner.setProperty(GetHDFSFileInfo.FULL_PATH, "/some/home/mydir");
        runner.setProperty(GetHDFSFileInfo.RECURSE_SUBDIRS, "false");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_DIRS, "true");
        runner.setProperty(GetHDFSFileInfo.IGNORE_DOTTED_FILES, "true");
        runner.setProperty(GetHDFSFileInfo.DESTINATION, GetHDFSFileInfo.DESTINATION_CONTENT);

        runner.run();

        // Assert session rollback
        runner.assertTransferCount(GetHDFSFileInfo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_SUCCESS, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_FAILURE, 0);
        runner.assertTransferCount(GetHDFSFileInfo.REL_NOT_FOUND, 0);

        proc.fileSystem.setFailOnExists(false);
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
        mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
        mff.assertAttributeEquals("hdfs.length", "" + 900);
        mff.assertAttributeEquals("hdfs.count.files", "" + 9);
        mff.assertAttributeEquals("hdfs.count.dirs", "" + 10);
        mff.assertAttributeEquals("hdfs.replication", "" + 3);
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
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 900);
                mff.assertAttributeEquals("hdfs.count.files", "" + 9);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 10);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("dir1")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 200);
                mff.assertAttributeEquals("hdfs.count.files", "" + 2);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 3);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("dir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 200);
                mff.assertAttributeEquals("hdfs.count.files", "" + 2);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 3);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("regDir")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir1");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 0);
                mff.assertAttributeEquals("hdfs.count.files", "" + 0);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 1);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("regDir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir2");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 0);
                mff.assertAttributeEquals("hdfs.count.files", "" + 0);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 1);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("regFile")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir1");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("regFile2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir2");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
            } else if (mff.getAttribute("hdfs.objectName").equals("regFile4")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
              } else if (mff.getAttribute("hdfs.objectName").equals("regFile5")) {
              matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "file");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 100);
                mff.assertAttributeNotExists("hdfs.count.files");
                mff.assertAttributeNotExists("hdfs.count.dirs");
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rw-r--r--");
                mff.assertAttributeNotExists("hdfs.status");
            } else {
               runner.assertNotValid();
            }
        }
        assertEquals(matchCount, 9);
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
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 900);
                mff.assertAttributeEquals("hdfs.count.files", "" + 9);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 10);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-mydir.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            } else if (mff.getAttribute("hdfs.objectName").equals("dir1")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 200);
                mff.assertAttributeEquals("hdfs.count.files", "" + 2);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 3);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-dir1.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            } else if (mff.getAttribute("hdfs.objectName").equals("dir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 200);
                mff.assertAttributeEquals("hdfs.count.files", "" + 2);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 3);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-dir2.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            } else if (mff.getAttribute("hdfs.objectName").equals("regDir")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir1");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 0);
                mff.assertAttributeEquals("hdfs.count.files", "" + 0);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 1);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-regDir.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            } else if (mff.getAttribute("hdfs.objectName").equals("regDir2")) {
                matchCount++;
                mff.assertAttributeEquals("hdfs.path", "/some/home/mydir/dir2");
                mff.assertAttributeEquals("hdfs.type", "directory");
                mff.assertAttributeEquals("hdfs.owner", "owner");
                mff.assertAttributeEquals("hdfs.group", "group");
                mff.assertAttributeEquals("hdfs.lastModified", "" + 1523456000000L);
                mff.assertAttributeEquals("hdfs.length", "" + 0);
                mff.assertAttributeEquals("hdfs.count.files", "" + 0);
                mff.assertAttributeEquals("hdfs.count.dirs", "" + 1);
                mff.assertAttributeEquals("hdfs.replication", "" + 3);
                mff.assertAttributeEquals("hdfs.permissions", "rwxr-xr-x");
                mff.assertAttributeNotExists("hdfs.status");
                final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGetHDFSFileInfo/testRecursiveGroupDirToAttributes-regDir2.json")));
                mff.assertAttributeEquals("hdfs.full.tree", expected);
            } else {
               runner.assertNotValid();
            }
        }
        assertEquals(matchCount, 5);
    }

    @Test
    public void testBatchSizeWithDestAttributesGroupAllBatchSizeNull() {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_ALL, 1);
    }

    @Test
    public void testBatchSizeWithDestAttributesGroupDirBatchSizeNull() {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_PARENT_DIR, 5);
    }

    @Test
    public void testBatchSizeWithDestAttributesGroupNoneBatchSizeNull() {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_ATTRIBUTES, GetHDFSFileInfo.GROUP_NONE, 9);
    }

    @Test
    public void testBatchSizeWithDestContentGroupAllBatchSizeNull() {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_ALL, 1);
    }

    @Test
    public void testBatchSizeWithDestContentGroupDirBatchSizeNull() {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_PARENT_DIR, 5);
    }

    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSizeNull() {
        testBatchSize(null, GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 9);

        checkContentSizes(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize1() {
        testBatchSize("1", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 9);
        checkContentSizes(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize3() {
        testBatchSize("3", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 3);
        checkContentSizes(Arrays.asList(3, 3, 3));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize4() {
        testBatchSize("4", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 3);
        checkContentSizes(Arrays.asList(4, 4, 1));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize5() {
        testBatchSize("5", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 2);
        checkContentSizes(Arrays.asList(5, 4));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize9() {
        testBatchSize("9", GetHDFSFileInfo.DESTINATION_CONTENT, GetHDFSFileInfo.GROUP_NONE, 1);
        checkContentSizes(Arrays.asList(9));
    }
    @Test
    public void testBatchSizeWithDestContentGroupNoneBatchSize100() {
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

        public GetHDFSFileInfoWithMockedFileSystem() {
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
}
