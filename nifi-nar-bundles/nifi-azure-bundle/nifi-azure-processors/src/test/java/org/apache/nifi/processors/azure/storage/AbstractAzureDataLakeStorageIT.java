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

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.ADLSCredentialsControllerService;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.util.UUID;

public abstract class AbstractAzureDataLakeStorageIT extends AbstractAzureStorageIT {

    private static final String FILESYSTEM_NAME_PREFIX = "nifi-test-filesystem";

    protected static final String TEST_FILE_CONTENT = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    protected String fileSystemName;
    protected DataLakeFileSystemClient fileSystemClient;

    @Override
    protected void setUpCredentials() throws Exception {
        ADLSCredentialsService service = new ADLSCredentialsControllerService();
        runner.addControllerService("ADLSCredentials", service);
        runner.setProperty(service, ADLSCredentialsControllerService.ACCOUNT_NAME, getAccountName());
        runner.setProperty(service, AzureStorageUtils.ACCOUNT_KEY, getAccountKey());
        runner.enableControllerService(service);

        runner.setProperty(AbstractAzureDataLakeStorageProcessor.ADLS_CREDENTIALS_SERVICE, "ADLSCredentials");
    }

    @Before
    public void setUpAzureDataLakeStorageIT() {
        fileSystemName = String.format("%s-%s", FILESYSTEM_NAME_PREFIX, UUID.randomUUID());

        runner.setProperty(AbstractAzureDataLakeStorageProcessor.FILESYSTEM, fileSystemName);

        DataLakeServiceClient storageClient = createStorageClient();
        fileSystemClient = storageClient.createFileSystem(fileSystemName);
    }

    @After
    public void tearDownAzureDataLakeStorageIT() {
        fileSystemClient.delete();
    }

    private DataLakeServiceClient createStorageClient() {
        return new DataLakeServiceClientBuilder()
                .endpoint("https://" + getAccountName() + ".dfs.core.windows.net")
                .credential(new StorageSharedKeyCredential(getAccountName(), getAccountKey()))
                .buildClient();
    }

    protected void createDirectory(String directory) {
        fileSystemClient.createDirectory(directory);
    }

    protected void uploadFile(String directory, String filename, String fileContent) {
        byte[] fileContentBytes = fileContent.getBytes();

        DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
        DataLakeFileClient fileClient = directoryClient.createFile(filename);

        PutAzureDataLakeStorage.uploadContent(fileClient, new ByteArrayInputStream(fileContentBytes), fileContentBytes.length);
    }

    protected void uploadFile(TestFile testFile) {
        uploadFile(testFile.getDirectory(), testFile.getFilename(), testFile.getFileContent());
    }

    protected void createDirectoryAndUploadFile(String directory, String filename, String fileContent) {
        createDirectory(directory);

        uploadFile(directory, filename, fileContent);
    }

    protected void createDirectoryAndUploadFile(TestFile testFile) {
        createDirectoryAndUploadFile(testFile.getDirectory(), testFile.getFilename(), testFile.getFileContent());
    }

    protected static class TestFile {
        private final String directory;
        private final String filename;
        private final String fileContent;

        public TestFile(String directory, String filename, String fileContent) {
            this.directory = directory;
            this.filename = filename;
            this.fileContent = fileContent;
        }

        public TestFile(String directory, String filename) {
            this(directory, filename, TEST_FILE_CONTENT);
        }

        public String getDirectory() {
            return directory;
        }

        public String getFilename() {
            return filename;
        }

        public String getFileContent() {
            return fileContent;
        }

        public String getFilePath() {
            return StringUtils.isNotBlank(directory) ? String.format("%s/%s", directory, filename) : filename;
        }
    }
}
