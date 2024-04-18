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
package org.apache.nifi.services.azure.storage;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeFileOpenInputStreamResult;
import com.azure.storage.file.datalake.models.PathProperties;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AzureDataLakeStorageFileResourceServiceTest {
    private static final String CREDENTIALS_CONTROLLER_SERVICE = "ADLSCredentialsService";
    private static final String FILE_SYSTEM = "filesystem-name";
    private static final String DIRECTORY = "test-directory";
    private static final String FILE = "test-file";
    private static final long CONTENT_LENGTH = 10L;
    public static final String MSG_EMPTY_FILE_NAME = "'File Name' property evaluated to blank string. 'File Name' must be specified as a non-blank string.";
    public static final String MSG_EMPTY_FILE_SYSTEM_NAME = "'Filesystem Name' property evaluated to blank string. 'Filesystem Name' must be specified as a non-blank string.";

    @Mock
    private DataLakeServiceClient client;

    @Mock
    private DataLakeFileSystemClient fileSystemClient;

    @Mock
    private DataLakeDirectoryClient directoryClient;

    @Mock
    private DataLakeFileClient fileClient;

    @Mock
    private PathProperties properties;

    @Mock
    private InputStream inputStream;

    @InjectMocks
    private TestAzureDataLakeStorageFileResourceService service;

    private TestRunner runner;

    @BeforeEach
    void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("AzureDataLakeStorageFileResourceService", service);
    }

    @Test
    void testHappyPath() throws InitializationException {
        setupService();
        setupMocking();

        FileResource fileResource = service.getFileResource(Map.of());

        assertFileResource(fileResource);
        verifyMockInvocations();
    }

    @Test
    void testHappyPathWithValidEL() throws InitializationException {
        String fileSystemKey = "filesystem.name";
        String directoryKey = "directory";
        String fileNameKey = "filename";
        setupService("${" + fileSystemKey + "}", "${" + directoryKey + "}", "${" + fileNameKey + "}");
        setupMocking();

        FileResource fileResource = service.getFileResource(Map.of(
                fileSystemKey, FILE_SYSTEM,
                directoryKey, DIRECTORY,
                fileNameKey, FILE));

        assertFileResource(fileResource);
        verifyMockInvocations();
    }

    @Test
    void testFileIsDirectory() throws InitializationException {
        setupService();
        when(client.getFileSystemClient(FILE_SYSTEM)).thenReturn(fileSystemClient);
        when(fileSystemClient.getDirectoryClient(DIRECTORY)).thenReturn(directoryClient);
        when(directoryClient.getFileClient(FILE)).thenReturn(fileClient);
        when(fileClient.getProperties()).thenReturn(properties);
        when(properties.isDirectory()).thenReturn(true);

        executeAndAssertProcessException(Map.of(), "File Name (" + FILE + ") points to a directory. Full path: " + fileClient.getFilePath());
    }

    @Test
    void testNonExistentFile() throws InitializationException {
        setupService();
        when(client.getFileSystemClient(FILE_SYSTEM)).thenReturn(fileSystemClient);
        when(fileSystemClient.getDirectoryClient(DIRECTORY)).thenReturn(directoryClient);
        when(directoryClient.getFileClient(FILE)).thenReturn(fileClient);
        when(fileClient.getProperties()).thenReturn(properties);
        when(properties.isDirectory()).thenReturn(false);
        when(fileClient.exists()).thenReturn(false);

        executeAndAssertProcessException(Map.of(), "File " + DIRECTORY + "/" + FILE + " not found in file system: " + FILE_SYSTEM);
    }

    @Test
    void testInvalidDirectoryValueWithLeadingSlash() throws InitializationException {
        String directoryKey = "directory.name";
        String directoryValue = "/invalid-directory";
        setupService(FILE_SYSTEM, "${" + directoryKey + "}", FILE);

        executeAndAssertProcessException(Map.of(directoryKey, directoryValue), "'Directory Name' starts with '/'. 'Directory Name' cannot contain a leading '/'.");
    }

    @Test
    void testValidELWithMissingFileValue() throws InitializationException {
        setupService(FILE_SYSTEM, DIRECTORY, "${file.name}");

        executeAndAssertProcessException(Map.of(), MSG_EMPTY_FILE_NAME);
    }

    @Test
    void testInvalidFileSystem() throws InitializationException {
        String fileSystemKey = "fileSystem";
        String fileSystemValue = "  ";
        setupService("${" + fileSystemKey + "}", DIRECTORY, FILE);

        executeAndAssertProcessException(Map.of(fileSystemKey, fileSystemValue), MSG_EMPTY_FILE_SYSTEM_NAME);
    }

    @Test
    void testInvalidFileName() throws InitializationException {
        String fileKey = "fileSystem";
        String fileValue = "  ";
        setupService(FILE_SYSTEM, DIRECTORY, "${" + fileKey + "}");

        executeAndAssertProcessException(Map.of(fileKey, fileValue),
                MSG_EMPTY_FILE_NAME);
    }

    @Test
    void testInvalidDirectoryValueWithWhiteSpaceOnly() throws InitializationException {
        String directoryKey = "directory.name";
        String directoryValue = "   ";
        setupService(FILE_SYSTEM, "${" + directoryKey + "}", FILE);

        executeAndAssertProcessException(Map.of(directoryKey, directoryValue), "'Directory Name' contains whitespace characters only.");
    }

    private void setupService() throws InitializationException {
        setupService(FILE_SYSTEM, DIRECTORY, FILE);
    }

    private void setupService(String fileSystem, String directory, String fileName) throws InitializationException {
        final ADLSCredentialsService credentialsService = mock(ADLSCredentialsService.class);
        when(credentialsService.getIdentifier()).thenReturn(CREDENTIALS_CONTROLLER_SERVICE);
        runner.addControllerService(CREDENTIALS_CONTROLLER_SERVICE, credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(service, ADLS_CREDENTIALS_SERVICE, CREDENTIALS_CONTROLLER_SERVICE);
        runner.setProperty(service, AzureStorageUtils.FILESYSTEM, fileSystem);
        runner.setProperty(service, AzureStorageUtils.DIRECTORY, directory);
        runner.setProperty(service, AzureStorageUtils.FILE, fileName);

        runner.enableControllerService(service);
    }

    private void setupMocking() {
        when(client.getFileSystemClient(FILE_SYSTEM)).thenReturn(fileSystemClient);
        when(fileSystemClient.getDirectoryClient(DIRECTORY)).thenReturn(directoryClient);
        when(directoryClient.getFileClient(FILE)).thenReturn(fileClient);
        when(fileClient.getProperties()).thenReturn(properties);
        when(properties.isDirectory()).thenReturn(false);
        when(fileClient.exists()).thenReturn(true);
        when(properties.getFileSize()).thenReturn(CONTENT_LENGTH);
        DataLakeFileOpenInputStreamResult result = mock(DataLakeFileOpenInputStreamResult.class);
        when(fileClient.openInputStream()).thenReturn(result);
        when(result.getInputStream()).thenReturn(inputStream);
    }

    private void executeAndAssertProcessException(Map<String, String> arguments, String expectedMessage) {
        ProcessException exception = assertThrows(ProcessException.class,
                () -> service.getFileResource(arguments));
        assertEquals(expectedMessage, exception.getMessage());
    }

    private void assertFileResource(FileResource fileResource) {
        assertNotNull(fileResource);
        assertEquals(fileResource.getInputStream(), inputStream);
        assertEquals(fileResource.getSize(), CONTENT_LENGTH);
    }

    private void verifyMockInvocations() {
        verify(client).getFileSystemClient(FILE_SYSTEM);
        verify(fileSystemClient).getDirectoryClient(DIRECTORY);
        verify(directoryClient).getFileClient(FILE);
        verify(properties).isDirectory();
        verify(fileClient).exists();
        verify(fileClient).openInputStream();
        verify(properties).getFileSize();
    }

    private static class TestAzureDataLakeStorageFileResourceService extends AzureDataLakeStorageFileResourceService {
        private final DataLakeServiceClient client;

        private TestAzureDataLakeStorageFileResourceService(DataLakeServiceClient client) {
            this.client = client;
        }

        @Override
        protected DataLakeServiceClient getStorageClient(Map<String, String> attributes) {
            return client;
        }
    }
}
