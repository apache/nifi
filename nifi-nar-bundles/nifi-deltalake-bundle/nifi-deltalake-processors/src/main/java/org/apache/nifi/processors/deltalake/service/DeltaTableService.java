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

import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.FileAction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.nifi.processors.deltalake.storage.StorageAdapter;

import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DeltaTableService {

    private final static String DELTA_TABLE_NAME = "delta-table";
    private final StructType structType;
    private final StorageAdapter storageAdapter;
    private final List<String> partitionColumns;

    private OptimisticTransaction transaction;
    private List<FileAction> fileUpdates;
    private Metadata deltaTableMetadata;

    public DeltaTableService(StorageAdapter storageAdapter, StructType structType, List<String> partitionColumns) {
        this.storageAdapter = storageAdapter;
        this.structType = structType;
        this.partitionColumns = partitionColumns;
    }

    public void startTransaction() {
        fileUpdates = new ArrayList<>();
        deltaTableMetadata = createMetadata();
        transaction = storageAdapter.getDeltaLog().startTransaction();
    }

    public void finishTransaction() {
        if (!fileUpdates.isEmpty()) {
            transaction.updateMetadata(deltaTableMetadata);
            transaction.commit(fileUpdates, new Operation(Operation.Name.UPDATE), storageAdapter.getEngineInfo());
            fileUpdates = Collections.emptyList();
        }
    }

    public boolean deltaTableExists() {
        return storageAdapter.getDeltaLog().tableExists();
    }

    public void addFilesToDeltaTable(List<AddFile> newFiles) {
        if (!newFiles.isEmpty()) {
            fileUpdates.addAll(newFiles);
        }
    }

    public void removeFilesFromDeltaTable(List<RemoveFile> removeFileList) {
        if (!removeFileList.isEmpty()) {
            fileUpdates.addAll(removeFileList);
        }
    }

    public void createNewDeltaTable(List<AddFile> newFiles) {
        if (!newFiles.isEmpty()) {
            OptimisticTransaction transaction = storageAdapter.getDeltaLog().startTransaction();
            transaction.updateMetadata(createMetadata());
            transaction.commit(newFiles, new Operation(Operation.Name.CREATE_TABLE), storageAdapter.getEngineInfo());
        }
    }

    public List<AddFile> getAllFiles() {
        return storageAdapter.getDeltaLog().snapshot().getAllFiles();
    }

    public Set<String> getDataFileNamesInDeltaTable() {
        CloseableIterator<AddFile> dataFiles = storageAdapter.getDeltaLog().snapshot().scan().getFiles();
        Set<String> dataFileNamesInTable = new HashSet<>();
        dataFiles.forEachRemaining(file -> dataFileNamesInTable.add(file.getPath()));
        return dataFileNamesInTable;
    }

    public AddFile createAddFile(FileStatus file) {
        return new AddFile(
                file.getPath().toString(),
                calculatePartitionColumns(file.getPath().toString()),
                file.getLen(),
                file.getModificationTime(),
                true,
                null,
                null);
    }

    public RemoveFile createRemoveFile(AddFile file) {
        return new RemoveFile(
                file.getPath(),
                Optional.of(System.currentTimeMillis()),
                true,
                false,
                file.getPartitionValues(),
                Optional.of(file.getSize()),
                null);
    }

    public boolean fileAlreadyAdded(String newFile) {
        if (deltaTableExists()) {
            return storageAdapter.getDeltaLog().snapshot().getAllFiles().stream()
                    .anyMatch(file -> file.getPath().equals(Util.getFileNameWithPartitionColumns(newFile, partitionColumns)));
        }
        return false;
    }

    private Map<String, String> calculatePartitionColumns(String filePath) {
        Map<String, String> partitionColumns = new HashMap<>();
        String separator = FileSystems.getDefault().getSeparator();
        List<String> directories = Arrays.asList(filePath.split(separator));
        List<String> partitionDirectories = directories.subList(directories.size() - (this.partitionColumns.size() + 1), directories.size()-1);

        for (int i = 0; i < this.partitionColumns.size(); i++) {
            partitionColumns.put(this.partitionColumns.get(i), partitionDirectories.get(this.partitionColumns.size() - i - 1));
        }

        return partitionColumns;
    }

    private Metadata createMetadata() {
        return Metadata.builder()
                .id(DELTA_TABLE_NAME)
                .partitionColumns(partitionColumns)
                .schema(structType)
                .build();
    }

}
