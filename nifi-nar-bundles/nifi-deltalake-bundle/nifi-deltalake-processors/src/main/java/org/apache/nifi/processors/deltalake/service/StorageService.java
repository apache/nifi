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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.nifi.processors.deltalake.storage.StorageAdapter;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StorageService {

    private static final String PARQUET_FILE_EXTENSION = ".parquet";
    private final List<String> partitionColumns;

    private StorageAdapter storageAdapter;

    public StorageService(StorageAdapter storageAdapter, List<String> columns) {
        this.storageAdapter = storageAdapter;
        this.partitionColumns = columns;
    }

    public FileStatus[] getFileStatuses() {
        try {
            return storageAdapter.getFileSystem().listStatus(new Path(storageAdapter.getDataPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<FileStatus> getParquetFilesFromDataStore() {
        try {
            Path path = new Path(storageAdapter.getDataPath());
            RemoteIterator<LocatedFileStatus> statuses = storageAdapter.getFileSystem().listFiles(path, true);

            List<FileStatus> fileStatuses = new ArrayList<>();
            while (statuses.hasNext()) {
                LocatedFileStatus fileStatus = statuses.next();
                if (fileStatus.isFile() && isDataParquetFile(fileStatus)) {
                    fileStatuses.add(fileStatus);
                }
            }
            return fileStatuses;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getParquetFileNamesFromDataStore() {
        return getParquetFilesFromDataStore().stream()
                .map(FileStatus::getPath)
                .map(this::getFileNameWithPartitionColumns)
                .collect(Collectors.toSet());
    }

    private String getFileNameWithPartitionColumns(Path fileName) {
        String separator = FileSystems.getDefault().getSeparator();
        List<String> pathElements = Arrays.stream(fileName.toString().split(separator))
                .collect(Collectors.toList());
        List<String> fileWithPartitionColumns = pathElements
                .subList(pathElements.size() - partitionColumns.size() - 1, pathElements.size());
        return StringUtils.join(fileWithPartitionColumns, separator);
    }

    private boolean isDataParquetFile(LocatedFileStatus fileStatus) {
        String fileName = fileStatus.getPath().getName();
        return fileName.endsWith(PARQUET_FILE_EXTENSION) && notInternalParquetFile(fileName);
    }

    private boolean notInternalParquetFile(String fileName) {
        return !fileName.endsWith("checkpoint" + PARQUET_FILE_EXTENSION);
    }

}
