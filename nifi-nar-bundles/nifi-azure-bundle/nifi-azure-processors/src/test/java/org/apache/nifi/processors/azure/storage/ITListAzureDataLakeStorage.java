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
package org.apache.nifi.processors.azure.storage;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILE_PATH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LAST_MODIFIED;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ITListAzureDataLakeStorage extends AbstractAzureDataLakeStorageIT {

    private Map<String, TestFile> testFiles;

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return ListAzureDataLakeStorage.class;
    }

    @BeforeEach
    public void setUp() {
        testFiles = new HashMap<>();

        TestFile testFile1 = new TestFile("", "file1");
        uploadFile(testFile1);
        testFiles.put(testFile1.getFilePath(), testFile1);

        TestFile testTempFile1 = new TestFile("_$azuretempdirectory$", "1234file1");
        uploadFile(testTempFile1);
        testFiles.put(testTempFile1.getFilePath(), testTempFile1);

        TestFile testFile2 = new TestFile("", "file2");
        uploadFile(testFile2);
        testFiles.put(testFile2.getFilePath(), testFile2);

        TestFile testFile11 = new TestFile("dir1", "file11");
        createDirectoryAndUploadFile(testFile11);
        testFiles.put(testFile11.getFilePath(), testFile11);

        TestFile testTempFile11 = new TestFile("dir1/_$azuretempdirectory$", "5678file11");
        uploadFile(testTempFile11);
        testFiles.put(testTempFile11.getFilePath(), testTempFile11);

        TestFile testFile12 = new TestFile("dir1", "file12");
        uploadFile(testFile12);
        testFiles.put(testFile12.getFilePath(), testFile12);

        TestFile testFile111 = new TestFile("dir1/dir11", "file111");
        createDirectoryAndUploadFile(testFile111);
        testFiles.put(testFile111.getFilePath(), testFile111);

        TestFile testTempFile111 = new TestFile("dir1/dir11/_$azuretempdirectory$", "9010file111");
        uploadFile(testTempFile111);
        testFiles.put(testTempFile111.getFilePath(), testTempFile111);

        TestFile testFile21 = new TestFile("dir 2", "file 21", "Test");
        createDirectoryAndUploadFile(testFile21);
        testFiles.put(testFile21.getFilePath(), testFile21);

        TestFile testTempFile21 = new TestFile("dir2/_$azuretempdirectory$", "1112file21", "Test");
        uploadFile(testTempFile21);
        testFiles.put(testTempFile21.getFilePath(), testTempFile21);

        createDirectory("dir3");
    }

    @Test
    public void testListRootRecursive() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "dir 2/file 21");
    }

    @Test
    public void testListRootRecursiveWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "dir 2/file 21",
                "_$azuretempdirectory$/1234file1", "dir1/_$azuretempdirectory$/5678file11",
                "dir1/dir11/_$azuretempdirectory$/9010file111", "dir2/_$azuretempdirectory$/1112file21");
    }

    @Test
    public void testListRootRecursiveUsingProxyConfigurationService() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        configureProxyService();

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "dir 2/file 21");
    }

    @Test
    public void testListRootNonRecursive() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.RECURSE_SUBDIRECTORIES, "false");

        runProcessor();

        assertSuccess("file1", "file2");
    }

    @Test
    public void testListRootNonRecursiveWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.RECURSE_SUBDIRECTORIES, "false");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("file1", "file2");
    }

    @Test
    public void testListSubdirectoryRecursive() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12", "dir1/dir11/file111");
    }

    @Test
    public void testListSubdirectoryRecursiveWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12", "dir1/dir11/file111", "dir1/_$azuretempdirectory$/5678file11",
                "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListSubdirectoryNonRecursive() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");
        runner.setProperty(ListAzureDataLakeStorage.RECURSE_SUBDIRECTORIES, "false");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12");
    }

    @Test
    public void testListSubdirectoryNonRecursiveWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");
        runner.setProperty(ListAzureDataLakeStorage.RECURSE_SUBDIRECTORIES, "false");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12");
    }

    @Test
    public void testListWithFileFilter() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.FILE_FILTER, ".*file1.*$");

        runProcessor();

        assertSuccess("file1", "dir1/file11", "dir1/file12", "dir1/dir11/file111");
    }

    @Test
    public void testListWithFileFilterWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.FILE_FILTER, ".*file1.*$");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("file1", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "_$azuretempdirectory$/1234file1",
                "dir1/_$azuretempdirectory$/5678file11", "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListWithFileFilterWithEL() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.FILE_FILTER, ".*file${suffix}$");
        runner.setVariable("suffix", "1.*");

        runProcessor();

        assertSuccess("file1", "dir1/file11", "dir1/file12", "dir1/dir11/file111");
    }

    @Test
    public void testListWithFileFilterWithELWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.FILE_FILTER, ".*file${suffix}$");
        runner.setVariable("suffix", "1.*");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("file1", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "_$azuretempdirectory$/1234file1",
                "dir1/_$azuretempdirectory$/5678file11", "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListRootWithPathFilter() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "^dir1.*$");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12", "dir1/dir11/file111");
    }

    @Test
    public void testListRootWithPathFilterWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "^dir1.*$");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12", "dir1/dir11/file111",
                "dir1/_$azuretempdirectory$/5678file11", "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListRootWithPathFilterWithEL() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "${prefix}${suffix}");
        runner.setVariable("prefix", "^dir");
        runner.setVariable("suffix", "1.*$");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12", "dir1/dir11/file111");
    }

    @Test
    public void testListRootWithPathFilterWithELWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "${prefix}${suffix}");
        runner.setVariable("prefix", "^dir");
        runner.setVariable("suffix", "1.*$");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/file12", "dir1/dir11/file111",
                "dir1/_$azuretempdirectory$/5678file11", "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListSubdirectoryWithPathFilter() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "dir1.*");

        runProcessor();

        assertSuccess("dir1/dir11/file111");
    }

    @Test
    public void testListSubdirectoryWithPathFilterWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "dir1.*");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir1/dir11/file111", "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListRootWithFileAndPathFilter() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.FILE_FILTER, ".*11");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "dir1.*");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/dir11/file111");
    }

    @Test
    public void testListRootWithFileAndPathFilterWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.FILE_FILTER, ".*11");
        runner.setProperty(ListAzureDataLakeStorage.PATH_FILTER, "dir1.*");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir1/file11", "dir1/dir11/file111", "dir1/_$azuretempdirectory$/5678file11",
                "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListEmptyDirectory() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir3");

        runProcessor();

        assertSuccess();
    }

    @Test
    public void testListNonExistingDirectory() {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dummy");

        runProcessor();

        assertFailure();
    }

    @Test
    public void testListWithNonExistingFileSystem() {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.FILESYSTEM, "dummy");
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");

        runProcessor();

        assertFailure();
    }

    @Test
    public void testListWithRecords() throws InitializationException {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");

        MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(ListAzureDataLakeStorage.RECORD_WRITER, "record-writer");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListAzureDataLakeStorage.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListAzureDataLakeStorage.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "3");
    }

    @Test
    public void testListWithRecordsWithTempFiles() throws InitializationException {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "dir1");

        MockRecordWriter recordWriter = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", recordWriter);
        runner.enableControllerService(recordWriter);
        runner.setProperty(ListAzureDataLakeStorage.RECORD_WRITER, "record-writer");

        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runner.run();

        runner.assertAllFlowFilesTransferred(ListAzureDataLakeStorage.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ListAzureDataLakeStorage.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "5");
    }

    @Test
    public void testListWithMinAge() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MIN_AGE, "1 hour");

        runProcessor();

        runner.assertTransferCount(ListAzureDataLakeStorage.REL_SUCCESS, 0);
    }

    @Test
    public void testListWithMinAgeWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MIN_AGE, "1 hour");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        runner.assertTransferCount(ListAzureDataLakeStorage.REL_SUCCESS, 0);
    }

    @Test
    public void testListWithMaxAge() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MAX_AGE, "1 hour");

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "dir 2/file 21");
    }

    @Test
    public void testListWithMaxAgeWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MAX_AGE, "1 hour");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111", "dir 2/file 21",
                "_$azuretempdirectory$/1234file1", "dir1/_$azuretempdirectory$/5678file11",
                "dir1/dir11/_$azuretempdirectory$/9010file111", "dir2/_$azuretempdirectory$/1112file21");
    }

    @Test
    public void testListWithMinSize() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MIN_SIZE, "5 B");

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111");
    }

    @Test
    public void testListWithMinSizeWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MIN_SIZE, "5 B");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("file1", "file2", "dir1/file11", "dir1/file12", "dir1/dir11/file111",
                "_$azuretempdirectory$/1234file1", "dir1/_$azuretempdirectory$/5678file11",
                "dir1/dir11/_$azuretempdirectory$/9010file111");
    }

    @Test
    public void testListWithMaxSize() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MAX_SIZE, "5 B");

        runProcessor();

        assertSuccess("dir 2/file 21");
    }

    @Test
    public void testListWithMaxSizeWithTempFiles() throws Exception {
        runner.setProperty(AbstractAzureDataLakeStorageProcessor.DIRECTORY, "");
        runner.setProperty(ListAzureDataLakeStorage.MAX_SIZE, "5 B");
        runner.setProperty(ListAzureDataLakeStorage.SHOW_TEMPORARY_FILES, "true");

        runProcessor();

        assertSuccess("dir 2/file 21", "dir2/_$azuretempdirectory$/1112file21");
    }

    private void runProcessor() {
        runner.assertValid();
        runner.run();
    }

    private void assertSuccess(String... testFilePaths) throws Exception {
        runner.assertTransferCount(ListAzureDataLakeStorage.REL_SUCCESS, testFilePaths.length);

        Map<String, TestFile> expectedFiles = new HashMap<>(testFiles);
        expectedFiles.keySet().retainAll(Arrays.asList(testFilePaths));

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListAzureDataLakeStorage.REL_SUCCESS);
        assertEquals(expectedFiles.size(), flowFiles.size());

        for (MockFlowFile flowFile : flowFiles) {
            String filePath = flowFile.getAttribute("azure.filePath");
            TestFile testFile = expectedFiles.remove(filePath);
            assertNotNull(testFile, "File path not found in the expected map");
            assertFlowFile(testFile, flowFile);
        }
    }

    private void assertFlowFile(TestFile testFile, MockFlowFile flowFile) {
        flowFile.assertAttributeEquals(ATTR_NAME_FILESYSTEM, fileSystemName);
        flowFile.assertAttributeEquals(ATTR_NAME_FILE_PATH, testFile.getFilePath());
        flowFile.assertAttributeEquals(ATTR_NAME_DIRECTORY, testFile.getDirectory());
        flowFile.assertAttributeEquals(ATTR_NAME_FILENAME, testFile.getFilename());
        flowFile.assertAttributeEquals(ATTR_NAME_LENGTH, String.valueOf(testFile.getFileContent().getBytes(StandardCharsets.UTF_8).length));

        flowFile.assertAttributeExists(ATTR_NAME_LAST_MODIFIED);
        flowFile.assertAttributeExists(ATTR_NAME_ETAG);

        flowFile.assertContentEquals("");
    }

    private void assertFailure() {
        assertFalse(runner.getLogger().getErrorMessages().isEmpty());
        runner.assertTransferCount(ListAzureDataLakeStorage.REL_SUCCESS, 0);
    }
}
