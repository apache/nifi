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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StorageService {

    private final List<String> partitionColumns;

    private StorageAdapter storageAdapter;

    public StorageService(StorageAdapter storageAdapter, List<String> columns) {
        this.storageAdapter = storageAdapter;
        this.partitionColumns = columns;
    }

    public List<FileStatus> getParquetFilesFromDataStore() {
        try {
            Path path = new Path(storageAdapter.getDataPath());
            RemoteIterator<LocatedFileStatus> statuses = storageAdapter.getFileSystem().listFiles(path, true);

            List<FileStatus> fileStatuses = new ArrayList<>();
            while (statuses.hasNext()) {
                LocatedFileStatus fileStatus = statuses.next();
                if (fileStatus.isFile() && isInternalParquetFile(fileStatus)) {
                    fileStatuses.add(fileStatus);
                }
            }
            return fileStatuses;
        } catch (IOException e) {
            throw new RuntimeException("Error reading files from data store", e);
        }
    }

    public Set<String> getParquetFileNamesFromDataStore() {
        return getParquetFilesFromDataStore().stream()
                .map(FileStatus::getPath)
                .map(fileName -> Util.getFileNameWithPartitionColumns(fileName.toString(), partitionColumns))
                .collect(Collectors.toSet());
    }

    public void moveToStorage(String inputParquetPath) {
        String destinationFileName = Util.getFileNameWithPartitionColumns(inputParquetPath, partitionColumns);
        if (!getParquetFileNamesFromDataStore().contains(inputParquetPath)) {
            try {
                Path destinationPath = new Path(storageAdapter.getDataPath()
                        + FileSystems.getDefault().getSeparator()
                        + destinationFileName);
                Path inputPath = new Path("file://" + new File(inputParquetPath).getAbsolutePath());
                storageAdapter.getFileSystem().moveFromLocalFile(inputPath, destinationPath);
            } catch (IOException e) {
                throw new RuntimeException("Error moving file to data storage", e);
            }
        }
    }

    private boolean isInternalParquetFile(LocatedFileStatus fileStatus) {
        return !fileStatus.getPath().toString().contains("_delta_log");
    }

}
