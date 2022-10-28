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

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.deltalake.storage.StorageAdapter;
import org.apache.nifi.processors.deltalake.storage.StorageAdapterFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.STRUCTURE_JSON;

public class DeltaLakeService {

    private StorageService storageService;
    private DeltaTableService deltaTableService;

    public void initialize(ProcessContext processorContext) {
        StorageAdapterFactory adapterFactory = new StorageAdapterFactory();
        StorageAdapter storageAdapter = adapterFactory.initializeAdapter(processorContext);

        String parquetStructure = processorContext.getProperty(STRUCTURE_JSON).getValue();
        StructType structType = (StructType) StructType.fromJson(parquetStructure);

        storageService = new StorageService(storageAdapter);
        deltaTableService = new DeltaTableService(storageAdapter, structType);
    }

    public boolean deltaTableExists() {
        return deltaTableService.deltaTableExists();
    }

    public int addNewFilesToDeltaTable() {
        Set<String> dataFileNames = storageService.getParquetFileNamesFromDataStore();
        dataFileNames.removeAll(deltaTableService.getDataFileNamesInDeltaTable());

        List<FileStatus> dataFileStatuses = storageService.getParquetFilesFromDataStore();
        List<AddFile> newFiles = dataFileStatuses.stream()
                .filter(fileStatus -> dataFileNames.contains(fileStatus.getPath().getName()))
                .map(deltaTableService::createAddFile)
                .collect(Collectors.toList());

        deltaTableService.addFilesToDeltaTable(newFiles);
        return newFiles.size();
    }

    public int removeMissingFilesFromDeltaTable() {
        Set<String> dataFileNames = new HashSet<>(storageService.getParquetFileNamesFromDataStore());

        List<AddFile> filesInDeltaTable = deltaTableService.getAllFiles();
        List<RemoveFile> removeFileList = filesInDeltaTable.stream()
                .filter(fileInDeltaTable -> !dataFileNames.contains(fileInDeltaTable.getPath()))
                .map(deltaTableService::createRemoveFile)
                .collect(Collectors.toList());

        deltaTableService.removeFilesFromDeltaTable(removeFileList);
        return removeFileList.size();
    }

    public int createNewDeltaLakeTable() {
        List<FileStatus> dataFileStatuses = storageService.getParquetFilesFromDataStore();
        List<AddFile> newFiles = dataFileStatuses.stream()
                .map(deltaTableService::createAddFile)
                .collect(Collectors.toList());

        deltaTableService.createNewDeltaTable(newFiles);
        return newFiles.size();
    }

    public void startTransaction() {
        deltaTableService.startTransaction();
    }
    public void finishTransaction() {
        deltaTableService.finishTransaction();
    }
}
