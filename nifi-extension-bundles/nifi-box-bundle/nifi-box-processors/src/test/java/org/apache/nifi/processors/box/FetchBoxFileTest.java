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

import com.box.sdkgen.managers.downloads.DownloadsManager;
import com.box.sdkgen.managers.files.FilesManager;
import com.box.sdkgen.managers.files.GetFileByIdQueryParams;
import com.box.sdkgen.schemas.file.FilePathCollectionField;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FetchBoxFileTest extends AbstractBoxFileTest {
    @Mock
    FilesManager mockFilesManager;

    @Mock
    DownloadsManager mockDownloadsManager;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFile testSubject = new FetchBoxFile();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        lenient().when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);
        lenient().when(mockBoxClient.getDownloads()).thenReturn(mockDownloadsManager);
    }

    @Test
    void testBoxIdFromFlowFileAttribute() {
        testRunner.setProperty(FetchBoxFile.FILE_ID, "${box.id}");
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(BoxFileAttributes.ID, TEST_FILE_ID);
        inputFlowFile.putAttributes(attributes);

        final FileFull fetchedFileInfo = createMockFileFull();
        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenReturn(fetchedFileInfo);
        doAnswer(invocation -> new ByteArrayInputStream(CONTENT.getBytes())).when(mockDownloadsManager).downloadFile(anyString());

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(BoxFileAttributes.ID, TEST_FILE_ID);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testBoxIdFromProperty() {
        testRunner.setProperty(FetchBoxFile.FILE_ID, TEST_FILE_ID);

        final FileFull fetchedFileInfo = createMockFileFull();
        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenReturn(fetchedFileInfo);
        doAnswer(invocation -> new ByteArrayInputStream(CONTENT.getBytes())).when(mockDownloadsManager).downloadFile(anyString());

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(BoxFileAttributes.ID, TEST_FILE_ID);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFileDownloadFailure() {
        testRunner.setProperty(FetchBoxFile.FILE_ID, TEST_FILE_ID);

        doThrow(new RuntimeException("Download failed")).when(mockDownloadsManager).downloadFile(anyString());

        MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFile.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFile.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Failed to download file from Box");
        assertNoProvenanceEvent();
    }

    @Test
    void testMigration() {
        final Map<String, String> expected = Map.ofEntries(
                Map.entry(AbstractBoxProcessor.OLD_BOX_CLIENT_SERVICE_PROPERTY_NAME, AbstractBoxProcessor.BOX_CLIENT_SERVICE.getName()),
                Map.entry("box-file-id", FetchBoxFile.FILE_ID.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }

    private FileFull createMockFileFull() {
        FolderMini folderInfo = org.mockito.Mockito.mock(FolderMini.class);
        when(folderInfo.getName()).thenReturn(TEST_FOLDER_NAME);
        when(folderInfo.getId()).thenReturn("not0");

        FilePathCollectionField pathCollection = org.mockito.Mockito.mock(FilePathCollectionField.class);
        when(pathCollection.getEntries()).thenReturn(List.of(folderInfo));

        FileFull fileInfo = org.mockito.Mockito.mock(FileFull.class);
        when(fileInfo.getId()).thenReturn(TEST_FILE_ID);
        when(fileInfo.getName()).thenReturn(TEST_FILENAME);
        when(fileInfo.getPathCollection()).thenReturn(pathCollection);
        when(fileInfo.getSize()).thenReturn(TEST_SIZE);
        when(fileInfo.getModifiedAt()).thenReturn(OffsetDateTime.ofInstant(Instant.ofEpochMilli(MODIFIED_TIME), ZoneOffset.UTC));

        return fileInfo;
    }
}
