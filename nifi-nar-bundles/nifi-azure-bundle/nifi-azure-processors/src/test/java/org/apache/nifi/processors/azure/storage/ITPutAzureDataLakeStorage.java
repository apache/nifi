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

import java.io.InputStream;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ITPutAzureDataLakeStorage extends AbstractAzureDataLakeStorageIT {

    private static final String DIRECTORY = "dir1";
    private static final String FILE_NAME = "file1";
    private static final byte[] FILE_DATA = "0123456789".getBytes();

    private static final String EL_FILESYSTEM = "az.filesystem";
    private static final String EL_DIRECTORY = "az.directory";
    private static final String EL_FILE_NAME = "az.filename";

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutAzureDataLakeStorage.class;
    }

    @Before
    public void setUp() {
        runner.setProperty(PutAzureDataLakeStorage.DIRECTORY, DIRECTORY);
        runner.setProperty(PutAzureDataLakeStorage.FILE, FILE_NAME);
    }

    @Test
    public void testPutFileToExistingDirectory() throws Exception {
        fileSystemClient.createDirectory(DIRECTORY);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileToExistingDirectoryWithReplaceResolution() throws Exception {
        fileSystemClient.createDirectory(DIRECTORY);

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.REPLACE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileToExistingDirectoryWithIgnoreResolution() throws Exception {
        fileSystemClient.createDirectory(DIRECTORY);

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.IGNORE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileToNonExistingDirectory() throws Exception {
        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileToDeepDirectory() throws Exception {
        String baseDirectory = "dir1/dir2";
        String fullDirectory = baseDirectory + "/dir3/dir4";
        fileSystemClient.createDirectory(baseDirectory);
        runner.setProperty(PutAzureDataLakeStorage.DIRECTORY, fullDirectory);

        runProcessor(FILE_DATA);

        assertSuccess(fullDirectory, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileToRootDirectory() throws Exception {
        String rootDirectory = "";
        runner.setProperty(PutAzureDataLakeStorage.DIRECTORY, rootDirectory);

        runProcessor(FILE_DATA);

        assertSuccess(rootDirectory, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutEmptyFile() throws Exception {
        byte[] fileData = new byte[0];

        runProcessor(fileData);

        assertSuccess(DIRECTORY, FILE_NAME, fileData);
    }

    @Test
    public void testPutBigFile() throws Exception {
        Random random = new Random();
        byte[] fileData = new byte[120_000_000];
        random.nextBytes(fileData);

        runProcessor(fileData);

        assertSuccess(DIRECTORY, FILE_NAME, fileData);
    }

    @Test
    public void testPutFileWithNonExistingFileSystem() {
        runner.setProperty(PutAzureDataLakeStorage.FILESYSTEM, "dummy");

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @Test
    public void testPutFileWithInvalidFileName() {
        runner.setProperty(PutAzureDataLakeStorage.FILE, "/file1");

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @Test
    public void testPutFileWithSpacesInDirectoryAndFileName() throws Exception {
        String directory = "dir 1";
        String fileName = "file 1";
        runner.setProperty(PutAzureDataLakeStorage.DIRECTORY, directory);
        runner.setProperty(PutAzureDataLakeStorage.FILE, fileName);

        runProcessor(FILE_DATA);

        assertSuccess(directory, fileName, FILE_DATA);
    }

    @Test
    public void testPutFileToExistingFileWithFailResolution() {
        fileSystemClient.createFile(String.format("%s/%s", DIRECTORY, FILE_NAME));

        runProcessor(FILE_DATA);

        assertFailure();
    }

    @Test
    public void testPutFileToExistingFileWithReplaceResolution() throws Exception {
        fileSystemClient.createFile(String.format("%s/%s", DIRECTORY, FILE_NAME));

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.REPLACE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileToExistingFileWithIgnoreResolution() throws Exception {
        String azureFileContent = "AzureFileContent";
        createDirectoryAndUploadFile(DIRECTORY, FILE_NAME, azureFileContent);

        runner.setProperty(PutAzureDataLakeStorage.CONFLICT_RESOLUTION, PutAzureDataLakeStorage.IGNORE_RESOLUTION);

        runProcessor(FILE_DATA);

        assertSuccessWithIgnoreResolution(DIRECTORY, FILE_NAME, FILE_DATA, azureFileContent.getBytes());
    }

    @Test
    public void testPutFileWithEL() throws Exception {
        Map<String, String> attributes = createAttributesMap();
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertSuccess(DIRECTORY, FILE_NAME, FILE_DATA);
    }

    @Test
    public void testPutFileWithELButFilesystemIsNotSpecified() {
        Map<String, String> attributes = createAttributesMap();
        attributes.remove(EL_FILESYSTEM);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertFailure();
    }

    @Test
    public void testPutFileWithELButFileNameIsNotSpecified() {
        Map<String, String> attributes = createAttributesMap();
        attributes.remove(EL_FILE_NAME);
        setELProperties();

        runProcessor(FILE_DATA, attributes);

        assertFailure();
    }

    @Test(expected = NullPointerException.class)
    public void testPutFileButFailedToAppend() {
        DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
        InputStream stream = mock(InputStream.class);
        doThrow(NullPointerException.class).when(fileClient).append(any(InputStream.class), anyLong(), anyLong());

        PutAzureDataLakeStorage.uploadContent(fileClient, stream, FILE_DATA.length);

        verify(fileClient).delete();
    }

    private Map<String, String> createAttributesMap() {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(EL_FILESYSTEM, fileSystemName);
        attributes.put(EL_DIRECTORY, DIRECTORY);
        attributes.put(EL_FILE_NAME, FILE_NAME);

        return attributes;
    }

    private void setELProperties() {
        runner.setProperty(PutAzureDataLakeStorage.FILESYSTEM, String.format("${%s}", EL_FILESYSTEM));
        runner.setProperty(PutAzureDataLakeStorage.DIRECTORY, String.format("${%s}", EL_DIRECTORY));
        runner.setProperty(PutAzureDataLakeStorage.FILE, String.format("${%s}", EL_FILE_NAME));
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

        flowFile.assertAttributeEquals(ATTR_NAME_FILESYSTEM, fileSystemName);
        flowFile.assertAttributeEquals(ATTR_NAME_DIRECTORY, directory);
        flowFile.assertAttributeEquals(ATTR_NAME_FILENAME, fileName);

        String urlEscapedDirectory = UrlEscapers.urlPathSegmentEscaper().escape(directory);
        String urlEscapedFileName = UrlEscapers.urlPathSegmentEscaper().escape(fileName);
        String primaryUri = StringUtils.isNotEmpty(directory)
                ? String.format("https://%s.dfs.core.windows.net/%s/%s/%s", getAccountName(), fileSystemName, urlEscapedDirectory, urlEscapedFileName)
                : String.format("https://%s.dfs.core.windows.net/%s/%s", getAccountName(), fileSystemName, urlEscapedFileName);
        flowFile.assertAttributeEquals(ATTR_NAME_PRIMARY_URI, primaryUri);

        flowFile.assertAttributeEquals(ATTR_NAME_LENGTH, Integer.toString(fileData.length));
    }

    private MockFlowFile assertFlowFile(byte[] fileData) throws Exception {
        runner.assertAllFlowFilesTransferred(PutAzureDataLakeStorage.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutAzureDataLakeStorage.REL_SUCCESS).get(0);

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
        runner.assertAllFlowFilesTransferred(PutAzureDataLakeStorage.REL_FAILURE, 1);
    }
}
