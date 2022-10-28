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
package org.apache.nifi.processors.deltalake.service;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.processors.deltalake.storage.StorageAdapter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StorageService {

    private static final String PARQUET_FILE_EXTENSION = ".parquet";

    private StorageAdapter storageAdapter;

    public StorageService(StorageAdapter storageAdapter) {
        this.storageAdapter = storageAdapter;
    }

    public FileStatus[] getFileStatuses() {
        try {
            return storageAdapter.getFileSystem().listStatus(new Path(storageAdapter.getDataPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<FileStatus> getParquetFilesFromDataStore() {
        return Arrays.stream(getFileStatuses())
                .filter(f -> f.isFile() && f.getPath().getName().endsWith(PARQUET_FILE_EXTENSION))
                .collect(Collectors.toList());
    }

    public Set<String> getParquetFileNamesFromDataStore() {
        return getParquetFilesFromDataStore().stream()
                .map(fileStatus -> fileStatus.getPath().getName())
                .collect(Collectors.toSet());
    }

}
