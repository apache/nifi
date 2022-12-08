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

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import io.delta.standalone.DeltaLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nifi.processor.ProcessContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.GCP_ACCOUNT_JSON_KEYFILE_PATH;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.GCP_BUCKET;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.GCP_PATH;

public class GcpStorageAdapter implements StorageAdapter {

    private FileSystem fileSystem;
    private DeltaLog deltaLog;
    private String dataPath;
    private String engineInfo;

    public GcpStorageAdapter(ProcessContext processorContext, String engineInfo) {
        this.engineInfo = engineInfo;

        String accountJsonKeyPath = processorContext.getProperty(GCP_ACCOUNT_JSON_KEYFILE_PATH).getValue();
        URI gcpBucketUri = URI.create(processorContext.getProperty(GCP_BUCKET).getValue());
        String gcpPath = processorContext.getProperty(GCP_PATH).getValue();

        Configuration configuration = createConfiguration(accountJsonKeyPath);
        fileSystem = new GoogleHadoopFileSystem();
        try {
            fileSystem.initialize(gcpBucketUri, configuration);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("GCP Filesystem Bucket URI [%s] initialization failed", gcpBucketUri), e);
        }

        dataPath = gcpBucketUri + "/" + gcpPath;
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

    private Configuration createConfiguration(String accountJsonKeyPath) {
        Configuration configuration = new Configuration();
        configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        configuration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        configuration.set("google.cloud.auth.service.account.enable", "true");
        configuration.set("google.cloud.auth.service.account.json.keyfile", accountJsonKeyPath);
        return configuration;
    }
}
