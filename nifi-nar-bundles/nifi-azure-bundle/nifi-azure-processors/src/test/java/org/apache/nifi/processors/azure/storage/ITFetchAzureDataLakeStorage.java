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

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ITFetchAzureDataLakeStorage extends AbstractAzureDataLakeStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return FetchAzureDataLakeStorage.class;
    }

    @Test
    public void testFetchFileFromDirectory() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchFileFromDirectoryUsingProxyConfigurationService() throws InitializationException {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        configureProxyService();

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchFileFromRoot() {
        // GIVEN
        String directory= "";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        uploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchFileFromDirectoryWithWhitespace() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchFileWithWhitespaceFromDirectory() {
        // GIVEN
        String directory= "TestDirectory";
        String filename = "A test file.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchFileCaseSensitiveFilename() {
        // GIVEN
        String directory = "TestDirectory";
        String filename1 = "testFile.txt";
        String filename2 = "testfile.txt";
        String fileContent1 = "ContentOfFile1";
        String fileContent2 = "ContentOfFile2";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename1, fileContent1);
        uploadFile(directory, filename2, fileContent2);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename1, inputFlowFileContent, fileContent1);
        runner.clearProvenanceEvents();
        runner.clearTransferState();
        testSuccessfulFetch(fileSystemName, directory, filename2, inputFlowFileContent, fileContent2);
    }

    @Test
    public void testFetchFileCaseSensitiveDirectoryName() {
        // GIVEN
        String directory1 = "TestDirectory";
        String directory2 = "Testdirectory";
        String filename1 = "testFile1.txt";
        String filename2 = "testFile2.txt";
        String fileContent1 = "ContentOfFile1";
        String fileContent2 = "ContentOfFile2";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory1, filename1, fileContent1);
        createDirectoryAndUploadFile(directory2, filename2, fileContent2);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory1, filename1, inputFlowFileContent, fileContent1);
        runner.clearProvenanceEvents();
        runner.clearTransferState();
        testSuccessfulFetch(fileSystemName, directory2, filename2, inputFlowFileContent, fileContent2);
    }

    @Test
    public void testFetchFileFromDeepDirectoryStructure() {
        // GIVEN
        String directory= "Directory01/Directory02/Directory03/Directory04/Directory05/Directory06/Directory07/"
                + "Directory08/Directory09/Directory10/Directory11/Directory12/Directory13/Directory14/Directory15/"
                + "Directory16/Directory17/Directory18/Directory19/Directory20/TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchDirectory() {
        // GIVEN
        String parentDirectory = "ParentDirectory";
        String childDirectory = "ChildDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(parentDirectory + "/" + childDirectory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testFailedFetchWithProcessException(fileSystemName, parentDirectory, childDirectory, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testFetchNonExistentFileSystem() {
        // GIVEN
        String fileSystem = "NonExistentFileSystem";
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        // WHEN
        // THEN
        testFailedFetch(fileSystem, directory, filename, inputFlowFileContent, inputFlowFileContent, 400);
    }

    @Test
    public void testFetchNonExistentDirectory() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        // WHEN
        // THEN
        testFailedFetch(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent, 404);
    }

    @Test
    public void testFetchNonExistentFile() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        fileSystemClient.createDirectory(directory);

        // WHEN
        // THEN
        testFailedFetch(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent, 404);
    }

    @Test
    public void testFetchLargeFile() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        Random random = new Random();
        byte[] fileContentBytes = new byte[120_000_000];
        random.nextBytes(fileContentBytes);
        String fileContent = new String(fileContentBytes);
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchInvalidDirectoryName() {
        // GIVEN
        String directory = "TestDirectory";
        String invalidDirectoryName = "Test/\\Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testFailedFetch(fileSystemName, invalidDirectoryName, filename, inputFlowFileContent, inputFlowFileContent, 404);
    }

    @Test
    public void testFetchInvalidFilename() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String invalidFilename = "test/\\File.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testFailedFetch(fileSystemName, directory, invalidFilename, inputFlowFileContent, inputFlowFileContent, 404);
    }

    @Test
    public void testFetchUsingExpressionLanguage() {
        // GIVEN
        String expLangFileSystem = "az.filesystem";
        String expLangDirectory = "az.directory";
        String expLangFilename = "az.filename";

        String directory = "TestDirectory";
        String filename = "testFile.txt";

        String inputFlowFileContent = "InputFlowFileContent";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(expLangFileSystem, fileSystemName);
        attributes.put(expLangDirectory, directory);
        attributes.put(expLangFilename, filename);

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch("${" + expLangFileSystem + "}",
                    "${" + expLangDirectory + "}",
                    "${" + expLangFilename + "}",
                            attributes,
                            inputFlowFileContent,
                            TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchUsingExpressionLanguageFileSystemIsNotSpecified() {
        // GIVEN
        String expLangFileSystem = "az.filesystem";
        String expLangDirectory = "az.directory";
        String expLangFilename = "az.filename";

        String directory = "TestDirectory";
        String filename = "testFile.txt";

        String inputFlowFileContent = "InputFlowFileContent";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(expLangDirectory, directory);
        attributes.put(expLangFilename, filename);

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testFailedFetchWithProcessException("${" + expLangFileSystem + "}",
                "${" + expLangDirectory + "}",
                "${" + expLangFilename + "}",
                attributes,
                inputFlowFileContent,
                inputFlowFileContent);
    }

    @Test
    public void testFetchUsingExpressionLanguageFilenameIsNotSpecified() {
        // GIVEN
        String expLangFileSystem = "az.filesystem";
        String expLangDirectory = "az.directory";
        String expLangFilename = "az.filename";

        String directory = "TestDirectory";
        String filename = "testFile.txt";

        String inputFlowFileContent = "InputFlowFileContent";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(expLangFileSystem, fileSystemName);
        attributes.put(expLangDirectory, directory);

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testFailedFetchWithProcessException("${" + expLangFileSystem + "}",
                "${" + expLangDirectory + "}",
                "${" + expLangFilename + "}",
                attributes,
                inputFlowFileContent,
                inputFlowFileContent);
    }

    @Test
    public void testFetchWithRangeZeroOne() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, "0B", "1B", inputFlowFileContent, TEST_FILE_CONTENT.substring(0, 1));
    }

    @Test
    public void testFetchWithRangeOneOne() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, "1B", "1B", inputFlowFileContent, TEST_FILE_CONTENT.substring(1, 2));
    }

    @Test
    public void testFetchWithRangeTwentyThreeTwentySix() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, "23B", "3B", inputFlowFileContent, TEST_FILE_CONTENT.substring(23, 26));
    }

    @Test
    public void testFetchWithRangeLengthGreater() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, "0B", "1KB", inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchWithRangeLengthUnset() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        // WHEN
        // THEN
        testSuccessfulFetch(fileSystemName, directory, filename, "0B", null, inputFlowFileContent, TEST_FILE_CONTENT);
    }

    @Test
    public void testFetchWithRangeStartOutOfRange() {
        // GIVEN
        String directory= "A Test Directory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, TEST_FILE_CONTENT);

        setRunnerProperties(fileSystemName, directory, filename, String.format("%sB", TEST_FILE_CONTENT.length() + 1), "1B");

        // WHEN
        startRunner(inputFlowFileContent, Collections.emptyMap());

        // THEN
        DataLakeStorageException e = (DataLakeStorageException)runner.getLogger().getErrorMessages().get(0).getThrowable();
        assertEquals(416, e.getStatusCode());
    }

    private void testSuccessfulFetch(String fileSystem, String directory, String filename, String inputFlowFileContent, String expectedFlowFileContent) {
        testSuccessfulFetch(fileSystem, directory, filename, Collections.emptyMap(), inputFlowFileContent, expectedFlowFileContent);
    }

    private void testSuccessfulFetch(String fileSystem, String directory, String filename, String rangeStart, String rangeLength, String inputFlowFileContent, String expectedFlowFileContent) {
        testSuccessfulFetch(fileSystem, directory, filename, rangeStart, rangeLength, Collections.emptyMap(), inputFlowFileContent, expectedFlowFileContent);
    }

    private void testSuccessfulFetch(String fileSystem, String directory, String filename, Map<String, String> attributes, String inputFlowFileContent, String expectedFlowFileContent) {
        testSuccessfulFetch(fileSystem, directory, filename, null, null, attributes, inputFlowFileContent, expectedFlowFileContent);
    }

    private void testSuccessfulFetch(String fileSystem, String directory, String filename, String rangeStart, String rangeLength,
                                 Map<String, String> attributes, String inputFlowFileContent, String expectedFlowFileContent) {
        // GIVEN
        Set<ProvenanceEventType> expectedEventTypes = new LinkedHashSet<>(Arrays.asList(ProvenanceEventType.CONTENT_MODIFIED, ProvenanceEventType.FETCH));

        setRunnerProperties(fileSystem, directory, filename, rangeStart, rangeLength);

        // WHEN
        startRunner(inputFlowFileContent, attributes);

        // THEN
        assertSuccess(expectedFlowFileContent, expectedEventTypes);
    }

    private void testFailedFetch(String fileSystem, String directory, String filename, String inputFlowFileContent, String expectedFlowFileContent, int expectedErrorCode) {
        testFailedFetch(fileSystem, directory, filename, Collections.emptyMap(), inputFlowFileContent, expectedFlowFileContent, expectedErrorCode);
    }

    private void testFailedFetch(String fileSystem, String directory, String filename, Map<String, String> attributes,
                                 String inputFlowFileContent, String expectedFlowFileContent, int expectedErrorCode) {
        // GIVEN
        setRunnerProperties(fileSystem, directory, filename);

        // WHEN
        startRunner(inputFlowFileContent, attributes);

        // THEN
        DataLakeStorageException e = (DataLakeStorageException)runner.getLogger().getErrorMessages().get(0).getThrowable();
        assertEquals(expectedErrorCode, e.getStatusCode());

        assertFailure(expectedFlowFileContent);
    }

    private void testFailedFetchWithProcessException(String fileSystem, String directory, String filename, String inputFlowFileContent, String expectedFlowFileContent) {
        testFailedFetchWithProcessException(fileSystem, directory, filename, Collections.emptyMap(), inputFlowFileContent, expectedFlowFileContent);
    }

    private void testFailedFetchWithProcessException(String fileSystem, String directory, String filename, Map<String, String> attributes,
                                                     String inputFlowFileContent, String expectedFlowFileContent) {
        // GIVEN
        setRunnerProperties(fileSystem, directory, filename);

        // WHEN
        startRunner(inputFlowFileContent, attributes);

        // THEN
        Throwable exception = runner.getLogger().getErrorMessages().get(0).getThrowable();
        assertEquals(ProcessException.class, exception.getClass());

        assertFailure(expectedFlowFileContent);
    }

    private void setRunnerProperties(String fileSystem, String directory, String filename) {
        setRunnerProperties(fileSystem, directory, filename, null, null);
    }

    private void setRunnerProperties(String fileSystem, String directory, String filename, String rangeStart, String rangeLength) {
        runner.setProperty(FetchAzureDataLakeStorage.FILESYSTEM, fileSystem);
        runner.setProperty(FetchAzureDataLakeStorage.DIRECTORY, directory);
        runner.setProperty(FetchAzureDataLakeStorage.FILE, filename);

        if (rangeStart != null) {
            runner.setProperty(FetchAzureDataLakeStorage.RANGE_START, rangeStart);
        }

        if (rangeLength != null) {
            runner.setProperty(FetchAzureDataLakeStorage.RANGE_LENGTH, rangeLength);
        }

        runner.assertValid();
    }

    private void startRunner(String inputFlowFileContent, Map<String, String> attributes) {
        runner.enqueue(inputFlowFileContent, attributes);
        runner.run();
    }

    private void assertSuccess(String expectedFlowFileContent, Set<ProvenanceEventType> expectedEventTypes) {
        runner.assertAllFlowFilesTransferred(FetchAzureDataLakeStorage.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchAzureDataLakeStorage.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(expectedFlowFileContent);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    private void assertFailure(String expectedFlowFileContent) {
        runner.assertAllFlowFilesTransferred(FetchAzureDataLakeStorage.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(FetchAzureDataLakeStorage.REL_FAILURE).get(0);
        flowFile.assertContentEquals(expectedFlowFileContent);
    }
}
