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
import com.google.common.collect.Sets;
import com.google.common.net.UrlEscapers;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

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
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_PRIMARY_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ITMoveAzureDataLakeStorage extends AbstractAzureDataLakeStorageIT {

    private static final String SOURCE_DIRECTORY = "sourceDir1";
    private static final String DESTINATION_DIRECTORY = "destDir1";
    private static final String FILE_NAME = "file1";
    private static final byte[] FILE_DATA = "0123456789".getBytes();

    private static final String EL_FILESYSTEM = "az.filesystem";
    private static final String EL_DIRECTORY = "az.directory";
    private static final String EL_FILE_NAME = "az.filename";

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return MoveAzureDataLakeStorage.class;
    }

    @Before
    public void setUp() throws InterruptedException {
        runner.setProperty(MoveAzureDataLakeStorage.SOURCE_FILESYSTEM, fileSystemName);
        runner.setProperty(MoveAzureDataLakeStorage.SOURCE_DIRECTORY, SOURCE_DIRECTORY);
        runner.setProperty(MoveAzureDataLakeStorage.FILESYSTEM, fileSystemName);
        runner.setProperty(MoveAzureDataLakeStorage.DIRECTORY, DESTINATION_DIRECTORY);
        runner.setProperty(MoveAzureDataLakeStorage.FILE, FILE_NAME);
    }

    @Test
    public void testMoveFileToExistingDirectory() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        createDirectory(DESTINATION_DIRECTORY);

        runProcessor(FILE_DATA);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileToExistingDirectoryWithReplaceResolution() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        createDirectory(DESTINATION_DIRECTORY);

        runner.setProperty(MoveAzureDataLakeStorage.CONFLICT_RESOLUTION, MoveAzureDataLakeStorage.REPLACE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileToExistingDirectoryWithIgnoreResolution() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        createDirectory(DESTINATION_DIRECTORY);

        runner.setProperty(MoveAzureDataLakeStorage.CONFLICT_RESOLUTION, MoveAzureDataLakeStorage.IGNORE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileToNonExistingDirectory() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);

        runProcessor(FILE_DATA);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileToDeepDirectory() throws Exception {
        String sourceDirectory = "dir1/dir2";
        String destDirectory = sourceDirectory + "/dir3/dir4";
        createDirectoryAndUploadFileData(sourceDirectory, FILE_NAME, FILE_DATA);

        runner.setProperty(MoveAzureDataLakeStorage.SOURCE_DIRECTORY, sourceDirectory);
        runner.setProperty(MoveAzureDataLakeStorage.DIRECTORY, destDirectory);

        runProcessor(FILE_DATA);

        assertSuccess(destDirectory, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileToRootDirectory() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);

        String rootDirectory = "";
        runner.setProperty(MoveAzureDataLakeStorage.DIRECTORY, rootDirectory);

        runProcessor(FILE_DATA);

        assertSuccess(rootDirectory, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveEmptyFile() throws Exception {
        byte[] fileData = new byte[0];
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, fileData);

        runProcessor(fileData);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, fileData);
    }

    @Test
    public void testMoveBigFile() throws Exception {
        Random random = new Random();
        byte[] fileData = new byte[120_000_000];
        random.nextBytes(fileData);

        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, fileData);

        runProcessor(fileData);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, fileData);
    }

    @Test
    public void testMoveFileWithNonExistingFileSystem() {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);

        runner.setProperty(MoveAzureDataLakeStorage.FILESYSTEM, "dummy");

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @Test
    public void testMoveFileWithInvalidFileName() {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);

        runner.setProperty(MoveAzureDataLakeStorage.FILE, "/file1");

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @Test
    public void testMoveFileWithSpacesInDirectoryAndFileName() throws Exception {
        String directory = "dir 1";
        String destDirectory = "dest dir1";
        String fileName = "file 1";
        createDirectoryAndUploadFileData(directory, fileName, FILE_DATA);

        runner.setProperty(MoveAzureDataLakeStorage.SOURCE_DIRECTORY, directory);
        runner.setProperty(MoveAzureDataLakeStorage.DIRECTORY, destDirectory);
        runner.setProperty(MoveAzureDataLakeStorage.FILE, fileName);

        runProcessor(FILE_DATA);

        assertSuccess(destDirectory, fileName, FILE_DATA);
    }

    @Test
    public void testMoveFileToExistingFileWithFailResolution() {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        fileSystemClient.createFile(String.format("%s/%s", DESTINATION_DIRECTORY, FILE_NAME));

        runner.setProperty(MoveAzureDataLakeStorage.CONFLICT_RESOLUTION, MoveAzureDataLakeStorage.FAIL_RESOLUTION);

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @Test
    public void testMoveFileToExistingFileWithReplaceResolution() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        fileSystemClient.createFile(String.format("%s/%s", DESTINATION_DIRECTORY, FILE_NAME));

        runner.setProperty(MoveAzureDataLakeStorage.CONFLICT_RESOLUTION, MoveAzureDataLakeStorage.REPLACE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileToExistingFileWithIgnoreResolution() throws Exception {
        String fileContent = "destination";
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        createDirectoryAndUploadFile(DESTINATION_DIRECTORY, FILE_NAME, fileContent);

        runner.setProperty(MoveAzureDataLakeStorage.CONFLICT_RESOLUTION, MoveAzureDataLakeStorage.IGNORE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccessWithIgnoreResolution(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA, fileContent.getBytes());
    }

    @Test
    public void testMoveFileWithEL() throws Exception {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        Map<String, String> attributes = createAttributesMap(FILE_DATA);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertSuccess(DESTINATION_DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testMoveFileWithELButFilesystemIsNotSpecified() {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        Map<String, String> attributes = createAttributesMap(FILE_DATA);
        attributes.remove(EL_FILESYSTEM);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertFailure();
    }

    @Test
    public void testMoveFileWithELButFileNameIsNotSpecified() {
        createDirectoryAndUploadFileData(SOURCE_DIRECTORY, FILE_NAME, FILE_DATA);
        Map<String, String> attributes = createAttributesMap(FILE_DATA);
        attributes.remove(EL_FILE_NAME);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertFailure();
    }

    private Map<String, String> createAttributesMap(byte[] fileData) {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(EL_FILESYSTEM, fileSystemName);
        attributes.put(EL_DIRECTORY, SOURCE_DIRECTORY);
        attributes.put(EL_FILE_NAME, FILE_NAME);
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(fileData.length));

        return attributes;
    }

    private void setELProperties() {
        runner.setProperty(MoveAzureDataLakeStorage.SOURCE_FILESYSTEM, String.format("${%s}", EL_FILESYSTEM));
        runner.setProperty(MoveAzureDataLakeStorage.SOURCE_DIRECTORY, String.format("${%s}", EL_DIRECTORY));
        runner.setProperty(MoveAzureDataLakeStorage.FILE, String.format("${%s}", EL_FILE_NAME));
    }

    private void runProcessor(byte[] fileData) {
        runProcessor(fileData, Collections.singletonMap(ATTR_NAME_LENGTH, String.valueOf(fileData.length)));
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

        flowFile.assertAttributeEquals(ATTR_NAME_FILESYSTEM, fileSystemName);
        flowFile.assertAttributeEquals(ATTR_NAME_DIRECTORY, directory);
        flowFile.assertAttributeEquals(ATTR_NAME_FILENAME, fileName);

        String urlEscapedDirectory = UrlEscapers.urlPathSegmentEscaper().escape(directory);
        String urlEscapedFileName = UrlEscapers.urlPathSegmentEscaper().escape(fileName);
        String urlEscapedPathSeparator = UrlEscapers.urlPathSegmentEscaper().escape("/");
        String primaryUri = StringUtils.isNotEmpty(directory)
                ? String.format("https://%s.dfs.core.windows.net/%s/%s%s%s", getAccountName(), fileSystemName, urlEscapedDirectory, urlEscapedPathSeparator, urlEscapedFileName)
                : String.format("https://%s.dfs.core.windows.net/%s/%s", getAccountName(), fileSystemName, urlEscapedFileName);
        flowFile.assertAttributeEquals(ATTR_NAME_PRIMARY_URI, primaryUri);

        flowFile.assertAttributeEquals(ATTR_NAME_LENGTH, Integer.toString(fileData.length));
    }

    private MockFlowFile assertFlowFile(byte[] fileData) throws Exception {
        runner.assertAllFlowFilesTransferred(MoveAzureDataLakeStorage.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(MoveAzureDataLakeStorage.REL_SUCCESS).get(0);

        flowFile.assertContentEquals(fileData);

        return flowFile;
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
        Set<ProvenanceEventType> expectedEventTypes = Sets.newHashSet(ProvenanceEventType.SEND);

        Set<ProvenanceEventType> actualEventTypes = runner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(Collectors.toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    private void assertFailure() {
        runner.assertAllFlowFilesTransferred(MoveAzureDataLakeStorage.REL_FAILURE, 1);
    }
}
