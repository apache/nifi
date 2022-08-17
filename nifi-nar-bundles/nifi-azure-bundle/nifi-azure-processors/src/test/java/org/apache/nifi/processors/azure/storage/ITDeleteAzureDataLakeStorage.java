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

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.DeleteAzureDataLakeStorage.FS_TYPE_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.DeleteAzureDataLakeStorage.FS_TYPE_FILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITDeleteAzureDataLakeStorage extends AbstractAzureDataLakeStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return DeleteAzureDataLakeStorage.class;
    }

    @Test
    public void testDeleteDirectoryWithFiles() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, null, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteDirectoryWithFilesUsingProxyConfigurationService() throws InitializationException {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        configureProxyService();

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, null, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteEmptyDirectoryWithFSTypeDirectory() {
        // GIVEN
        String directory = "TestDirectory";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectory(directory);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, null, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteSubdirectory() {
        // GIVEN
        String parentDirectory = "TestParentDirectory";
        String childDirectory = "TestParentDirectory/TestChildDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectory(parentDirectory);
        createDirectoryAndUploadFile(childDirectory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, childDirectory, null, inputFlowFileContent, inputFlowFileContent);
        assertTrue(directoryExists(parentDirectory));
    }

    @Test
    public void testDeleteParentDirectory() {
        // GIVEN
        String parentDirectory = "TestParentDirectory";
        String childDirectory = "TestParentDirectory/TestChildDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectory(parentDirectory);
        createDirectoryAndUploadFile(childDirectory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, parentDirectory, null, inputFlowFileContent, inputFlowFileContent);
        assertFalse(directoryExists(childDirectory));
    }

    @Test
    public void testDeleteFileFromRoot() {
        // GIVEN
        String directory = "";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        uploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteFileFromDirectory() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteFileFromDeepDirectory() {
        // GIVEN
        String directory = "Directory01/Directory02/Directory03/Directory04/Directory05/Directory06/Directory07/"
                + "Directory08/Directory09/Directory10/Directory11/Directory12/Directory13/Directory14/Directory15/"
                + "Directory16/Directory17/Directory18/Directory19/Directory20/TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteFileWithWhitespaceInFilename() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "A test file.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteFileWithWhitespaceInDirectoryName() {
        // GIVEN
        String directory = "A Test Directory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteEmptyDirectoryWithFSTypeFile() {
        // GIVEN
        String parentDirectory = "ParentDirectory";
        String childDirectory = "ChildDirectory";
        String inputFlowFileContent = "InputFlowFileContent";

        fileSystemClient.createDirectory(parentDirectory + "/" + childDirectory);

        // WHEN
        // THEN
        testSuccessfulDelete(fileSystemName, parentDirectory, childDirectory, inputFlowFileContent, inputFlowFileContent);
    }

    @Test
    public void testDeleteFileCaseSensitiveFilename() {
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
        testSuccessfulDelete(fileSystemName, directory, filename1, inputFlowFileContent, inputFlowFileContent);
        assertTrue(fileExists(directory, filename2));
    }

    @Test
    public void testDeleteUsingExpressionLanguage() {
        // GIVEN
        String expLangFileSystem = "az.filesystem";
        String expLangDirectory = "az.directory";
        String expLangFilename = "az.filename";

        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";

        String inputFlowFileContent = "InputFlowFileContent";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(expLangFileSystem, fileSystemName);
        attributes.put(expLangDirectory, directory);
        attributes.put(expLangFilename, filename);

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testSuccessfulDeleteWithExpressionLanguage("${" + expLangFileSystem + "}",
                "${" + expLangDirectory + "}",
                "${" + expLangFilename + "}",
                attributes,
                inputFlowFileContent,
                inputFlowFileContent,
                directory,
                filename);
    }

    @Test
    public void testDeleteUsingExpressionLanguageFileSystemIsNotSpecified() {
        // GIVEN
        String expLangFileSystem = "az.filesystem";
        String expLangDirectory = "az.directory";
        String expLangFilename = "az.filename";

        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";

        String inputFlowFileContent = "InputFlowFileContent";

        Map<String, String> attributes = new HashMap<>();
        attributes.put(expLangDirectory, directory);
        attributes.put(expLangFilename, filename);

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testFailedDeleteWithProcessException("${" + expLangFileSystem + "}",
                "${" + expLangDirectory + "}",
                "${" + expLangFilename + "}",
                attributes,
                inputFlowFileContent,
                inputFlowFileContent);
        assertTrue(fileExists(directory, filename));
    }

    @Test
    public void testDeleteUsingExpressionLanguageFilenameIsNotSpecified() {
        // GIVEN
        String expLangFileSystem = "az.filesystem";
        String expLangDirectory = "az.directory";
        String expLangFilename = "az.filename";

        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";

        String inputFlowFileContent = "InputFlowFileContent";
        Map<String, String> attributes = new HashMap<>();
        attributes.put(expLangFileSystem, fileSystemName);
        attributes.put(expLangDirectory, directory);

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testFailedDeleteWithProcessException("${" + expLangFileSystem + "}",
                "${" + expLangDirectory + "}",
                "${" + expLangFilename + "}",
                attributes,
                inputFlowFileContent,
                inputFlowFileContent);
        assertTrue(fileExists(directory, filename));
    }

    @Test
    public void testDeleteNonExistentFile() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        fileSystemClient.createDirectory(directory);

        // WHEN
        // THEN
        testFailedDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent, 404);
        assertTrue(fileExists("", directory));
    }

    @Test
    public void testDeleteFileFromNonExistentDirectory() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        // WHEN
        // THEN
        testFailedDelete(fileSystemName, directory, filename, inputFlowFileContent, inputFlowFileContent, 404);
    }

    @Test
    public void testDeleteFileFromNonExistentFileSystem() {
        // GIVEN
        String fileSystem = "NonExistentFileSystem";
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String inputFlowFileContent = "InputFlowFileContent";

        // WHEN
        // THEN
        testFailedDelete(fileSystem, directory, filename, inputFlowFileContent, inputFlowFileContent, 400);
    }

    @Test
    public void testDeleteNonEmptyDirectoryWithFSTypeFile() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        testFailedDelete(fileSystemName, "", directory, inputFlowFileContent, inputFlowFileContent, 409);
        assertTrue(fileExists(directory, filename));
    }

    @Test
    public void testDeleteFileWithInvalidFilename() {
        // GIVEN
        String directory = "TestDirectory";
        String filename = "testFile.txt";
        String invalidFilename = "test/\\File.txt";
        String fileContent = "AzureFileContent";
        String inputFlowFileContent = "InputFlowFileContent";

        createDirectoryAndUploadFile(directory, filename, fileContent);

        // WHEN
        // THEN
        testFailedDelete(fileSystemName, directory, invalidFilename, inputFlowFileContent, inputFlowFileContent, 400);
        assertTrue(fileExists(directory, filename));
    }

    private void testSuccessfulDelete(String fileSystem, String directory, String filename, String inputFlowFileContent, String expectedFlowFileContent) {
        testSuccessfulDeleteWithExpressionLanguage(fileSystem, directory, filename, Collections.emptyMap(), inputFlowFileContent, expectedFlowFileContent,
                directory, filename);
    }

    private void testSuccessfulDeleteWithExpressionLanguage(String expLangFileSystem, String expLangDirectory, String expLangFilename, Map<String, String> attributes,
                                                            String inputFlowFileContent, String expectedFlowFileContent, String directory, String filename) {
        // GIVEN
        int expectedNumberOfProvenanceEvents = 1;
        ProvenanceEventType expectedEventType = ProvenanceEventType.REMOTE_INVOCATION;

        setRunnerProperties(expLangFileSystem, expLangDirectory, expLangFilename);

        // WHEN
        startRunner(inputFlowFileContent, attributes);

        // THEN
        assertSuccess(directory, filename, expectedFlowFileContent, expectedNumberOfProvenanceEvents, expectedEventType);
    }

    private void testFailedDelete(String fileSystem, String directory, String filename, String inputFlowFileContent, String expectedFlowFileContent, int expectedErrorCode) {
        // GIVEN
        setRunnerProperties(fileSystem, directory, filename);

        // WHEN
        startRunner(inputFlowFileContent, Collections.emptyMap());

        // THEN
        DataLakeStorageException e = (DataLakeStorageException) runner.getLogger().getErrorMessages().get(0).getThrowable();
        assertEquals(expectedErrorCode, e.getStatusCode());

        assertFailure(expectedFlowFileContent);
    }

    private void testFailedDeleteWithProcessException(String fileSystem, String directory, String filename, Map<String, String> attributes,
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

    private boolean fileExists(String directory, String filename) {
        DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
        DataLakeFileClient fileClient = directoryClient.getFileClient(filename);

        return fileClient.exists();
    }

    private boolean directoryExists(String directory) {
        DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);

        return directoryClient.exists();
    }

    private void setRunnerProperties(String fileSystem, String directory, String filename) {
        runner.setProperty(DeleteAzureDataLakeStorage.FILESYSTEM_OBJECT_TYPE, filename != null ? FS_TYPE_FILE.getValue() : FS_TYPE_DIRECTORY.getValue());
        runner.setProperty(DeleteAzureDataLakeStorage.FILESYSTEM, fileSystem);
        runner.setProperty(DeleteAzureDataLakeStorage.DIRECTORY, directory);
        if (filename != null) {
            runner.setProperty(DeleteAzureDataLakeStorage.FILE, filename);
        }
        runner.assertValid();
    }

    private void startRunner(String inputFlowFileContent, Map<String, String> attributes) {
        runner.enqueue(inputFlowFileContent, attributes);
        runner.run();
    }

    private void assertSuccess(String directory, String filename, String expectedFlowFileContent, int expectedNumberOfProvenanceEvents, ProvenanceEventType expectedEventType) {
        runner.assertAllFlowFilesTransferred(DeleteAzureDataLakeStorage.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteAzureDataLakeStorage.REL_SUCCESS).get(0);
        flowFile.assertContentEquals(expectedFlowFileContent);

        int actualNumberOfProvenanceEvents = runner.getProvenanceEvents().size();
        assertEquals(expectedNumberOfProvenanceEvents, actualNumberOfProvenanceEvents);

        ProvenanceEventType actualEventType = runner.getProvenanceEvents().get(0).getEventType();
        assertEquals(expectedEventType, actualEventType);

        if (filename != null) {
            assertFalse(fileExists(directory, filename));
        } else {
            assertFalse(directoryExists(directory));
        }
    }

    private void assertFailure(String expectedFlowFileContent) {
        runner.assertAllFlowFilesTransferred(DeleteAzureDataLakeStorage.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(DeleteAzureDataLakeStorage.REL_FAILURE).get(0);
        flowFile.assertContentEquals(expectedFlowFileContent);
    }

}