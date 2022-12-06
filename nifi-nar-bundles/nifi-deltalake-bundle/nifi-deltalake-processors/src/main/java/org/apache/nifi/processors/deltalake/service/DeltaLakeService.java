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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.deltalake.storage.StorageAdapter;
import org.apache.nifi.processors.deltalake.storage.StorageAdapterFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.INPUT_FILE_NAME_ATTRIBUTE;
import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.INPUT_FILE_PATH_ATTRIBUTE;
import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.PARQUET_SCHEMA_INPUT_TEXT;
import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.PARQUET_SCHEMA_SELECTOR;
import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.PARTITION_COLUMNS;
import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.SCHEMA_FILE_JSON;
import static org.apache.nifi.processors.deltalake.DeltaLakeMetadataWriter.SCHEMA_TEXT_JSON;

public class DeltaLakeService {

    private StorageService storageService;
    private DeltaTableService deltaTableService;

    public void initialize(ProcessContext processorContext) {
        StorageAdapterFactory adapterFactory = new StorageAdapterFactory();
        StorageAdapter storageAdapter = adapterFactory.initializeAdapter(processorContext);

        StructType parquetSchema = getParquetSchema(processorContext);

        String partitionColumns = processorContext.getProperty(PARTITION_COLUMNS).getValue();
        List<String> columns = partitionColumns == null ? Collections.emptyList() : Arrays.asList(partitionColumns.split(","));

        storageService = new StorageService(storageAdapter, columns);
        deltaTableService = new DeltaTableService(storageAdapter, parquetSchema, columns);
    }

    public boolean deltaTableExists() {
        return deltaTableService.deltaTableExists();
    }

    public int addNewFilesToDeltaTable() {
        Set<String> fileNamesInDeltaTable = deltaTableService.getDataFileNamesInDeltaTable();

        List<FileStatus> dataFileStatuses = storageService.getParquetFilesFromDataStore();
        List<AddFile> newFiles = dataFileStatuses.stream()
                .filter(fileStatus -> !fileNamesInDeltaTable.stream()
                        .anyMatch(s -> fileStatus.getPath().toString().endsWith(s)))
                .map(deltaTableService::createAddFile)
                .collect(Collectors.toList());

        deltaTableService.addFilesToDeltaTable(newFiles);
        return newFiles.size();
    }

    public int removeMissingFilesFromDeltaTable() {
        Set<String> dataFileNames = new HashSet<>(storageService.getParquetFileNamesFromDataStore());

        List<AddFile> filesInDeltaTable = deltaTableService.getAllFiles();
        List<RemoveFile> removeFileList = filesInDeltaTable.stream()
                .filter(fileInDeltaTable -> !dataFileNames.stream()
                        .anyMatch(dataFileName -> fileInDeltaTable.getPath().equals(dataFileName)))
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

    public void handleInputParquet(ProcessContext processContext, FlowFile flowFile) {
        String inputParquetPath = null;
        String pathAttributeName = processContext.getProperty(INPUT_FILE_PATH_ATTRIBUTE).getValue();
        String filenameAttributeName = processContext.getProperty(INPUT_FILE_NAME_ATTRIBUTE).getValue();
        if (pathAttributeName != null && filenameAttributeName != null) {
            String pathAttributeValue = flowFile.getAttribute(pathAttributeName);
            String filenameAttributeValue = flowFile.getAttribute(filenameAttributeName);
            if (pathAttributeValue != null && filenameAttributeValue != null) {
                inputParquetPath = pathAttributeValue
                        + FileSystems.getDefault().getSeparator()
                        + filenameAttributeValue;
            }
        }
        if (inputParquetPath != null) {
            if (!deltaTableService.fileAlreadyAdded(inputParquetPath)) {
                storageService.moveToStorage(inputParquetPath);
            }
        }
    }

    private StructType getParquetSchema(ProcessContext processorContext) {
        String parquetSchemaType = processorContext.getProperty(PARQUET_SCHEMA_SELECTOR).getValue();

        String parquetSchema;
        if (parquetSchemaType.equals(PARQUET_SCHEMA_INPUT_TEXT.getValue())) {
            parquetSchema = processorContext.getProperty(SCHEMA_TEXT_JSON).getValue();
        } else {
            String parquetSchemaFilePath = processorContext.getProperty(SCHEMA_FILE_JSON).getValue();
            try {
                parquetSchema = new String(Files.readAllBytes(Paths.get(parquetSchemaFilePath)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return (StructType) StructType.fromJson(parquetSchema);
    }

}
