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
import org.apache.nifi.processor.ProcessContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.S3_ACCESS_KEY;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.S3_BUCKET;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.S3_PATH;
import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.S3_SECRET_KEY;

public class S3StorageAdapter implements StorageAdapter {

    public static final String S3_ACCESS_CONFIGURATION_KEY = "fs.s3a.access.key";
    public static final String S3_SECRET_CONFIGURATION_KEY = "fs.s3a.secret.key";

    private final FileSystem fileSystem;
    private final DeltaLog deltaLog;
    private final String dataPath;
    private final String engineInfo;

    public S3StorageAdapter(ProcessContext processorContext, String engineInfo) {
        this.engineInfo = engineInfo;

        String accessKey = processorContext.getProperty(S3_ACCESS_KEY).getValue();
        String secretKey = processorContext.getProperty(S3_SECRET_KEY).getValue();
        String bucket = processorContext.getProperty(S3_BUCKET).getValue();
        String path = processorContext.getProperty(S3_PATH).getValue();

        Configuration configuration = new Configuration();
        configuration.set(S3_ACCESS_CONFIGURATION_KEY, accessKey);
        configuration.set(S3_SECRET_CONFIGURATION_KEY, secretKey);

        URI s3uri = URI.create(bucket);
        try {
            fileSystem = FileSystem.get(s3uri, configuration);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("S3 Filesystem Bucket URI [%s] initialization failed", s3uri), e);
        }

        dataPath = bucket + "/" + path;
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
}
