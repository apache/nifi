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
import org.apache.hadoop.fs.Path;
import org.apache.nifi.processor.ProcessContext;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.nifi.processors.deltalake.UpdateDeltaLakeTable.LOCAL_PATH;

public class LocalStorageAdapter implements StorageAdapter {

    private FileSystem fileSystem;
    private DeltaLog deltaLog;
    private String dataPath;
    private String engineInfo;

    public LocalStorageAdapter(ProcessContext processorContext, String engineInfo) {
        this.engineInfo = engineInfo;

        dataPath = processorContext.getProperty(LOCAL_PATH).getValue();
        Path source = new Path(dataPath);

        Configuration configuration = new Configuration();
        try {
            fileSystem = source.getFileSystem(configuration);
        } catch (IOException e) {
            throw new UncheckedIOException(String.format("Local Storage Filesystem [%s] configuration failed", dataPath), e);
        }

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
