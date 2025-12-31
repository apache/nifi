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
package org.apache.nifi.processors.box;

import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.managers.folders.FoldersManager;
import com.box.sdkgen.managers.folders.GetFolderByIdQueryParams;
import com.box.sdkgen.managers.folders.GetFolderItemsQueryParams;
import com.box.sdkgen.managers.uploads.UploadsManager;
import com.box.sdkgen.schemas.file.FilePathCollectionField;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.files.Files;
import com.box.sdkgen.schemas.folder.FolderPathCollectionField;
import com.box.sdkgen.schemas.folderfull.FolderFull;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import com.box.sdkgen.schemas.items.Items;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PutBoxFileTest extends AbstractBoxFileTest implements FileListingTestTrait {

    public static final String SUBFOLDER1_ID = "aaaa";
    public static final String SUBFOLDER2_ID = "bbbb";

    @Mock
    protected FoldersManager mockFoldersManager;

    @Mock
    protected UploadsManager mockUploadsManager;

    @Mock
    protected FolderFull mockFolderFull;

    @Mock
    protected FolderFull mockSubfolder1Full;

    @Mock
    protected FolderFull mockSubfolder2Full;

    @Mock
    protected FolderMini mockSubfolder1Info;

    @Mock
    protected FolderMini mockSubfolder2Info;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final PutBoxFile testSubject = new PutBoxFile();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testUploadFilenameFromFlowFileAttribute() {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.FILE_NAME, "${filename}");

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        inputFlowFile.putAttributes(attributes);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        // Mock folder operations
        setupMockFolderInfo(TEST_FOLDER_ID, TEST_FOLDER_NAME, mockFolderFull);
        doReturn(mockFolderFull).when(mockFoldersManager).getFolderById(anyString(), any(GetFolderByIdQueryParams.class));
        when(mockBoxClient.getFolders()).thenReturn(mockFoldersManager);

        // Mock upload operation
        FileFull uploadedFile = createMockFileFull(TEST_FILE_ID, TEST_FILENAME, TEST_SIZE, MODIFIED_TIME);
        Files files = mock(Files.class);
        when(files.getEntries()).thenReturn(List.of(uploadedFile));
        when(mockUploadsManager.uploadFile(any())).thenReturn(files);
        when(mockBoxClient.getUploads()).thenReturn(mockUploadsManager);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testUploadFileExistingSubfolders() {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.SUBFOLDER_NAME, "sub1/sub2");
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutBoxFile.CREATE_SUBFOLDER, "true");

        // Setup folder info mocks
        setupMockFolderInfo(TEST_FOLDER_ID, TEST_FOLDER_NAME, mockFolderFull);
        setupMockFolderInfo(SUBFOLDER1_ID, "sub1", mockSubfolder1Full);
        setupMockFolderInfo(SUBFOLDER2_ID, "sub2", mockSubfolder2Full);

        // Mock subfolder1Info and subfolder2Info
        lenient().when(mockSubfolder1Info.getName()).thenReturn("sub1");
        lenient().when(mockSubfolder1Info.getId()).thenReturn(SUBFOLDER1_ID);
        lenient().when(mockSubfolder2Info.getName()).thenReturn("sub2");
        lenient().when(mockSubfolder2Info.getId()).thenReturn(SUBFOLDER2_ID);

        // Mock folder items query - root folder contains sub1
        Items rootItems = mock(Items.class);
        List rootEntries = new ArrayList<>();
        rootEntries.add(mockSubfolder1Info);
        doReturn(rootEntries).when(rootItems).getEntries();

        // Mock folder items query - sub1 contains sub2
        Items sub1Items = mock(Items.class);
        List sub1Entries = new ArrayList<>();
        sub1Entries.add(mockSubfolder2Info);
        doReturn(sub1Entries).when(sub1Items).getEntries();

        doAnswer(invocation -> {
            String folderId = invocation.getArgument(0);
            if (TEST_FOLDER_ID.equals(folderId)) {
                return mockFolderFull;
            } else if (SUBFOLDER1_ID.equals(folderId)) {
                return mockSubfolder1Full;
            } else if (SUBFOLDER2_ID.equals(folderId)) {
                return mockSubfolder2Full;
            }
            return null;
        }).when(mockFoldersManager).getFolderById(anyString(), any(GetFolderByIdQueryParams.class));

        doAnswer(invocation -> {
            String folderId = invocation.getArgument(0);
            if (TEST_FOLDER_ID.equals(folderId)) {
                return rootItems;
            } else if (SUBFOLDER1_ID.equals(folderId)) {
                return sub1Items;
            }
            return mock(Items.class);
        }).when(mockFoldersManager).getFolderItems(anyString(), any(GetFolderItemsQueryParams.class));

        when(mockBoxClient.getFolders()).thenReturn(mockFoldersManager);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        // Mock upload operation
        FileFull uploadedFile = createMockFileFull(TEST_FILE_ID, TEST_FILENAME, TEST_SIZE, MODIFIED_TIME);
        Files files = mock(Files.class);
        when(files.getEntries()).thenReturn(List.of(uploadedFile));
        when(mockUploadsManager.uploadFile(any())).thenReturn(files);
        when(mockBoxClient.getUploads()).thenReturn(mockUploadsManager);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_SUCCESS, 1);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testUploadError() {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        setupMockFolderInfo(TEST_FOLDER_ID, TEST_FOLDER_NAME, mockFolderFull);
        doReturn(mockFolderFull).when(mockFoldersManager).getFolderById(anyString(), any(GetFolderByIdQueryParams.class));
        when(mockBoxClient.getFolders()).thenReturn(mockFoldersManager);
        when(mockUploadsManager.uploadFile(any())).thenThrow(new RuntimeException("Upload error"));
        when(mockBoxClient.getUploads()).thenReturn(mockUploadsManager);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Upload error");
        assertNoProvenanceEvent();
    }

    @Test
    void testMigration() {
        final Map<String, String> expected = Map.ofEntries(
                Map.entry(AbstractBoxProcessor.OLD_BOX_CLIENT_SERVICE_PROPERTY_NAME, AbstractBoxProcessor.BOX_CLIENT_SERVICE.getName()),
                Map.entry("box-folder-id", PutBoxFile.FOLDER_ID.getName()),
                Map.entry("file-name", PutBoxFile.FILE_NAME.getName()),
                Map.entry("subfolder-name", PutBoxFile.SUBFOLDER_NAME.getName()),
                Map.entry("create-folder", PutBoxFile.CREATE_SUBFOLDER.getName()),
                Map.entry("conflict-resolution-strategy", PutBoxFile.CONFLICT_RESOLUTION.getName()),
                Map.entry("chunked-upload-threshold", PutBoxFile.CHUNKED_UPLOAD_THRESHOLD.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    @SuppressWarnings("unchecked")
    private void setupMockFolderInfo(String folderId, String folderName, FolderFull folderFull) {
        FolderMini pathEntry = mock(FolderMini.class);
        lenient().when(pathEntry.getName()).thenReturn(folderName);
        lenient().when(pathEntry.getId()).thenReturn(folderId);

        FolderPathCollectionField pathCollection = mock(FolderPathCollectionField.class);
        List entries = new ArrayList<>();
        entries.add(pathEntry);
        lenient().doReturn(entries).when(pathCollection).getEntries();

        lenient().when(folderFull.getId()).thenReturn(folderId);
        lenient().when(folderFull.getName()).thenReturn(folderName);
        lenient().when(folderFull.getPathCollection()).thenReturn(pathCollection);
    }

    private FileFull createMockFileFull(String fileId, String fileName, Long size, Long modifiedTime) {
        FileFull file = mock(FileFull.class);

        FolderMini folderMini = mock(FolderMini.class);
        when(folderMini.getName()).thenReturn(TEST_FOLDER_NAME);
        when(folderMini.getId()).thenReturn("not0");

        FilePathCollectionField pathCollection = mock(FilePathCollectionField.class);
        when(pathCollection.getEntries()).thenReturn(List.of(folderMini));

        when(file.getId()).thenReturn(fileId);
        when(file.getName()).thenReturn(fileName);
        when(file.getSize()).thenReturn(size);
        when(file.getPathCollection()).thenReturn(pathCollection);
        when(file.getModifiedAt()).thenReturn(java.time.OffsetDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(modifiedTime), java.time.ZoneOffset.UTC));

        return file;
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
