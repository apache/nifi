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
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.util.UUID;

public abstract class AbstractAzureDataLakeStorageIT extends AbstractAzureStorageIT {

    private static final String FILESYSTEM_NAME_PREFIX = "nifi-test-filesystem";

    protected String fileSystemName;
    protected DataLakeFileSystemClient fileSystemClient;

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

    protected void uploadFile(String directory, String filename, String fileContent) {
        byte[] fileContentBytes = fileContent.getBytes();

        DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
        DataLakeFileClient fileClient = directoryClient.createFile(filename);

        fileClient.append(new ByteArrayInputStream(fileContentBytes), 0, fileContentBytes.length);
        fileClient.flush(fileContentBytes.length);
    }

    protected void createDirectoryAndUploadFile(String directory, String filename, String fileContent) {
        fileSystemClient.createDirectory(directory);

        uploadFile(directory, filename, fileContent);
    }
}
