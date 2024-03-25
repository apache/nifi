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
package org.apache.nifi.processors.azure.storage;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.fileresource.service.StandardFileResourceService;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.WritingStrategy;
import org.apache.nifi.processors.transfer.ResourceTransferProperties;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ITPutAzureDataLakeStorage extends AbstractAzureDataLakeStorageIT {

    private static final String DIRECTORY = "dir1";
    private static final String FILE_NAME = "file1";
    private static final byte[] FILE_DATA = "0123456789".getBytes(StandardCharsets.UTF_8);

    private static final String EL_FILESYSTEM = "az.filesystem";
    private static final String EL_DIRECTORY = "az.directory";
    private static final String EL_FILE_NAME = "az.filename";

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutAzureDataLakeStorage.class;
    }

    @BeforeEach
    public void setUp() {
        runner.setProperty(AzureStorageUtils.DIRECTORY, DIRECTORY);
        runner.setProperty(AzureStorageUtils.FILE, FILE_NAME);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToExistingDirectory(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        fileSystemClient.createDirectory(DIRECTORY);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToExistingDirectoryWithReplaceResolution(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        fileSystemClient.createDirectory(DIRECTORY);

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.REPLACE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToExistingDirectoryWithIgnoreResolution(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        fileSystemClient.createDirectory(DIRECTORY);

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.IGNORE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToNonExistingDirectory(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToDeepDirectory(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        String baseDirectory = "dir1/dir2";
        String fullDirectory = baseDirectory + "/dir3/dir4";
        fileSystemClient.createDirectory(baseDirectory);
        runner.setProperty(AzureStorageUtils.DIRECTORY, fullDirectory);

        runProcessor(FILE_DATA);

        assertSuccess(fullDirectory, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToRootDirectory(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        String rootDirectory = "";
        runner.setProperty(AzureStorageUtils.DIRECTORY, rootDirectory);

        runProcessor(FILE_DATA);

        assertSuccess(rootDirectory, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutEmptyFile(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        byte[] fileData = new byte[0];

        runProcessor(fileData);

        assertSuccess(DIRECTORY, FILE_NAME, fileData);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutBigFile(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        Random random = new Random();
        byte[] fileData = new byte[120_000_000];
        random.nextBytes(fileData);

        runProcessor(fileData);

        assertSuccess(DIRECTORY, FILE_NAME, fileData);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileWithNonExistingFileSystem(WritingStrategy writingStrategy) {
        setWritingStrategy(writingStrategy);

        runner.setProperty(AzureStorageUtils.FILESYSTEM, "dummy");

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileWithInvalidFileName(WritingStrategy writingStrategy) {
        setWritingStrategy(writingStrategy);

        runner.setProperty(AzureStorageUtils.FILE, "/file1");

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileWithSpacesInDirectoryAndFileName(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        String directory = "dir 1";
        String fileName = "file 1";
        runner.setProperty(AzureStorageUtils.DIRECTORY, directory);
        runner.setProperty(AzureStorageUtils.FILE, fileName);

        runProcessor(FILE_DATA);

        assertSuccess(directory, fileName, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToExistingFileWithFailResolution(WritingStrategy writingStrategy) {
        setWritingStrategy(writingStrategy);

        fileSystemClient.createFile(String.format("%s/%s", DIRECTORY, FILE_NAME));

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToExistingFileWithReplaceResolution(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        fileSystemClient.createFile(String.format("%s/%s", DIRECTORY, FILE_NAME));

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.REPLACE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileToExistingFileWithIgnoreResolution(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        String azureFileContent = "AzureFileContent";
        createDirectoryAndUploadFile(DIRECTORY, FILE_NAME, azureFileContent);

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.IGNORE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccessWithIgnoreResolution(DIRECTORY, FILE_NAME, FILE_DATA, azureFileContent.getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileWithEL(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        Map<String, String> attributes = createAttributesMap();
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileWithELButFilesystemIsNotSpecified(WritingStrategy writingStrategy) {
        setWritingStrategy(writingStrategy);

        Map<String, String> attributes = createAttributesMap();
        attributes.remove(EL_FILESYSTEM);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertFailure();
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileWithELButFileNameIsNotSpecified(WritingStrategy writingStrategy) {
        setWritingStrategy(writingStrategy);

        Map<String, String> attributes = createAttributesMap();
        attributes.remove(EL_FILE_NAME);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertFailure();
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileFromLocalFile(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        String attributeName = "file.path";

        String serviceId = FileResourceService.class.getSimpleName();
        FileResourceService service = new StandardFileResourceService();
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, String.format("${%s}", attributeName));
        runner.enableControllerService(service);

        runner.setProperty(ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE, ResourceTransferSource.FILE_RESOURCE_SERVICE);
        runner.setProperty(ResourceTransferProperties.FILE_RESOURCE_SERVICE, serviceId);

        Path tempFilePath = Files.createTempFile("ITPutAzureDataLakeStorage_testPutFileFromLocalFile_", "");
        Files.write(tempFilePath, FILE_DATA);

        Map<String, String> attributes = new HashMap<>();
        attributes.put(attributeName, tempFilePath.toString());

        runProcessor(EMPTY_CONTENT, attributes);

        MockFlowFile flowFile = assertFlowFile(EMPTY_CONTENT);
        assertFlowFileAttributes(flowFile, DIRECTORY, FILE_NAME, FILE_DATA.length);
        assertAzureFile(DIRECTORY, FILE_NAME, FILE_DATA);
        assertProvenanceEvents();
    }

    @ParameterizedTest
    @EnumSource(WritingStrategy.class)
    public void testPutFileUsingProxy(WritingStrategy writingStrategy) throws Exception {
        setWritingStrategy(writingStrategy);

        fileSystemClient.createDirectory(DIRECTORY);
        configureProxyService();

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    private Map<String, String> createAttributesMap() {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(EL_FILESYSTEM, fileSystemName);
        attributes.put(EL_DIRECTORY, DIRECTORY);
        attributes.put(EL_FILE_NAME, FILE_NAME);

        return attributes;
    }

    private void setWritingStrategy(WritingStrategy writingStrategy) {
        runner.setProperty(PutAzureDataLakeStorage.WRITING_STRATEGY, writingStrategy);
    }

    private void setELProperties() {
        runner.setProperty(AzureStorageUtils.FILESYSTEM, String.format("${%s}", EL_FILESYSTEM));
        runner.setProperty(AzureStorageUtils.DIRECTORY, String.format("${%s}", EL_DIRECTORY));
        runner.setProperty(AzureStorageUtils.FILE, String.format("${%s}", EL_FILE_NAME));
    }

    private void runProcessor(byte[] fileData) {
        runProcessor(fileData, Collections.emptyMap());
    }

    private void runProcessor(byte[] testData, Map<String, String> attributes) {
        runner.assertValid();
        runner.enqueue(testData, attributes);
        runner.run();
    }

    private void assertSuccess(String directory, String fileName, byte[] fileData) throws Exception {
        assertFlowFile(fileData, fileName, directory);
        assertAzureFile(directory, fileName, fileData);
        assertProvenanceEvents();
    }

    private void assertSuccessWithIgnoreResolution(String directory, String fileName, byte[] fileData, byte[] azureFileData) throws Exception {
        assertFlowFile(fileData);
        assertAzureFile(directory, fileName, azureFileData);
    }

    private void assertFlowFile(byte[] fileData, String fileName, String directory) throws Exception {
        MockFlowFile flowFile = assertFlowFile(fileData);

        assertFlowFileAttributes(flowFile, directory, fileName, fileData.length);
    }

    private MockFlowFile assertFlowFile(byte[] fileData) throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureDataLakeStorage.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutAzureDataLakeStorage.REL_SUCCESS).getFirst();

        flowFile.assertContentEquals(fileData);

        return flowFile;
    }

    private void assertFlowFileAttributes(MockFlowFile flowFile, String directory, String fileName, int fileLength) {
        flowFile.assertAttributeEquals(ATTR_NAME_FILESYSTEM, fileSystemName);
        flowFile.assertAttributeEquals(ATTR_NAME_DIRECTORY, directory);
        flowFile.assertAttributeEquals(ATTR_NAME_FILENAME, fileName);

        flowFile.assertAttributeEquals(ATTR_NAME_LENGTH, Integer.toString(fileLength));
    }

    private void assertAzureFile(String directory, String fileName, byte[] fileData) {
        DataLakeFileClient fileClient;
        if (StringUtils.isNotEmpty(directory)) {
            DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
            assertTrue(directoryClient.exists());

            fileClient = directoryClient.getFileClient(fileName);
        } else {
            fileClient = fileSystemClient.getFileClient(fileName);
        }

        assertTrue(fileClient.exists());
        assertEquals(fileData.length, fileClient.getProperties().getFileSize());
    }

    private void assertProvenanceEvents() {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(ProvenanceEventType.SEND);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    private void assertFailure() {
        runner.assertAllFlowFilesTransferred(PutAzureDataLakeStorage.REL_FAILURE, 1);
    }
}
