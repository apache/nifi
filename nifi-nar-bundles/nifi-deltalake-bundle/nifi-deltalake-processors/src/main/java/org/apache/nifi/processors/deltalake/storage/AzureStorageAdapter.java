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
package org.apache.nifi.processors.deltalake.storage;

import io.delta.standalone.DeltaLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.nifi.processor.ProcessContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.AZURE_ACCOUNT_KEY;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.AZURE_PATH;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.AZURE_STORAGE_ACCOUNT;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.AZURE_STORAGE_NAME;

public class AzureStorageAdapter implements StorageAdapter {

    private static final String FS_AZURE_ACCOUNT_KEY_PREFIX = "fs.azure.account.key.";
    private static final String FS_AZURE_ACCOUNT_KEY_SUBFIX = ".blob.core.windows.net";
    private static final String AZURE_URI_PREFIX = "wasbs://";
    private static final String AZURE_URI_SUBFIX = ".blob.core.windows.net";

    private final FileSystem fileSystem;
    private final DeltaLog deltaLog;
    private final String dataPath;
    private final String engineInfo;
    private final Configuration configuration;

    public AzureStorageAdapter(ProcessContext processorContext, String engineInfo, Configuration configuration) {
        this.engineInfo = engineInfo;

        String accountKey = processorContext.getProperty(AZURE_ACCOUNT_KEY).getValue();
        String storageName = processorContext.getProperty(AZURE_STORAGE_NAME).getValue();
        String storageAccount = processorContext.getProperty(AZURE_STORAGE_ACCOUNT).getValue();
        String azurePath = processorContext.getProperty(AZURE_PATH).getValue();

        URI azureUri = URI.create(AZURE_URI_PREFIX + storageName + "@" + storageAccount + AZURE_URI_SUBFIX);
        dataPath = azureUri + "/" + azurePath;

        if (accountKey != null) {
            configuration.set(FS_AZURE_ACCOUNT_KEY_PREFIX + storageAccount + FS_AZURE_ACCOUNT_KEY_SUBFIX, accountKey);
        }

        fileSystem = new NativeAzureFileSystem();
        try {
            fileSystem.initialize(azureUri, configuration);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Azure Filesystem URI [%s] initialization failed", azureUri), e);
        }

        this.configuration = configuration;
        deltaLog = DeltaLog.forTable(configuration, dataPath);
    }

    @Override
    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    @Override
    public String getDataPath() {
        return dataPath;
    }

    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public String getEngineInfo() {
        return engineInfo;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }
}
