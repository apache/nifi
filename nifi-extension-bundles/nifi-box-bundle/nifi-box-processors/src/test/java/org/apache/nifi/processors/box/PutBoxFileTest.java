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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PutBoxFileTest extends AbstractBoxFileTest {

    public static final String SUBFOLDER1_ID = "aaaa";
    public static final String SUBFOLDER2_ID = "bbbb";
    @Mock
    protected BoxFolder.Info mockSubfolder1Info;

    @Mock
    protected BoxFolder.Info mockSubfolder2Info;

    @Mock
    protected BoxFolder mockSubfolder1;

    @Mock
    protected BoxFolder mockSubfolder2;

    private final Map<String, BoxFolder> mockBoxFolders = new HashMap<>();


    @BeforeEach
    void setUp() throws Exception {
        initMockBoxFolderMap();
        final PutBoxFile testSubject = new PutBoxFile() {
            @Override
            BoxFolder getFolder(String folderId) {
               return mockBoxFolders.get(folderId);
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    private void initMockBoxFolderMap() {
        mockBoxFolders.put(TEST_FOLDER_ID, mockBoxFolder);
        mockBoxFolders.put(SUBFOLDER1_ID, mockSubfolder1);
        mockBoxFolders.put(SUBFOLDER2_ID, mockSubfolder2);
    }

    @Test
    void testUploadFilenameFromFlowFileAttribute()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.FILE_NAME, "${filename}");

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        inputFlowFile.putAttributes(attributes);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        final BoxFile.Info mockUploadedFileInfo = createFileInfo(TEST_FOLDER_NAME, MODIFIED_TIME);
        when(mockBoxFolder.uploadFile(any(InputStream.class), eq(TEST_FILENAME))).thenReturn(mockUploadedFileInfo);
        when(mockBoxFolder.getInfo()).thenReturn(mockBoxFolderInfo);
        when(mockBoxFolderInfo.getID()).thenReturn(TEST_FOLDER_ID);
        when(mockBoxFolderInfo.getName()).thenReturn(TEST_FOLDER_NAME);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testUploadFileExistingSubfolders()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.SUBFOLDER_NAME, "sub1/sub2");
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutBoxFile.CREATE_SUBFOLDER, "true");

        when(mockBoxFolder.getChildren("name")).thenReturn(singletonList(mockSubfolder1Info));
        when(mockSubfolder1.getChildren("name")).thenReturn(singletonList(mockSubfolder2Info));

        when(mockSubfolder1Info.getName()).thenReturn("sub1");
        when(mockSubfolder2Info.getName()).thenReturn("sub2");
        when(mockSubfolder1Info.getID()).thenReturn(SUBFOLDER1_ID);
        when(mockSubfolder2Info.getID()).thenReturn(SUBFOLDER2_ID);
        when(mockSubfolder1Info.getResource()).thenReturn(mockSubfolder1);
        when(mockSubfolder2Info.getResource()).thenReturn(mockSubfolder2);
        when(mockSubfolder2.getInfo()).thenReturn(mockSubfolder2Info);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        final BoxFile.Info mockUploadedFileInfo = createFileInfo(TEST_FOLDER_NAME, MODIFIED_TIME, asList(mockBoxFolderInfo, mockSubfolder1Info, mockSubfolder2Info));
        when(mockSubfolder2.uploadFile(any(InputStream.class), eq(TEST_FILENAME))).thenReturn(mockUploadedFileInfo);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0, format("/%s/%s/%s", TEST_FOLDER_NAME, "sub1", "sub2"));
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testUploadFileCreateSubfolders()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.SUBFOLDER_NAME, "new1/new2");
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutBoxFile.CREATE_SUBFOLDER, "true");

        when(mockBoxFolder.getChildren("name")).thenReturn(emptyList());
        when(mockSubfolder1.getChildren("name")).thenReturn(emptyList());

        when(mockBoxFolder.createFolder("new1")).thenReturn(mockSubfolder1Info);
        when(mockSubfolder1.createFolder("new2")).thenReturn(mockSubfolder2Info);

        when(mockSubfolder1Info.getResource()).thenReturn(mockSubfolder1);
        when(mockSubfolder2Info.getResource()).thenReturn(mockSubfolder2);

        when(mockSubfolder1Info.getName()).thenReturn("new1");
        when(mockSubfolder1Info.getID()).thenReturn(SUBFOLDER1_ID);
        when(mockSubfolder2Info.getName()).thenReturn("new2");
        when(mockSubfolder2Info.getID()).thenReturn(SUBFOLDER2_ID);
        when(mockSubfolder1.getID()).thenReturn(SUBFOLDER1_ID);

        when(mockSubfolder2.getInfo()).thenReturn(mockSubfolder2Info);

        when(mockBoxFolder.getID()).thenReturn(TEST_FOLDER_ID);
        when(mockBoxFolderInfo.getID()).thenReturn(TEST_FOLDER_ID);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        final BoxFile.Info mockUploadedFileInfo = createFileInfo(TEST_FOLDER_NAME, MODIFIED_TIME, asList(mockBoxFolderInfo, mockSubfolder1Info, mockSubfolder2Info));
        when(mockSubfolder2.uploadFile(any(InputStream.class), eq(TEST_FILENAME))).thenReturn(mockUploadedFileInfo);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0, format("/%s/%s/%s", TEST_FOLDER_NAME, "new1", "new2"));
        assertProvenanceEvent(ProvenanceEventType.SEND);
        verify(mockBoxFolder).createFolder("new1");
        verify(mockSubfolder1).createFolder("new2");
    }

    @Test
    void testUploadError()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        when(mockBoxFolderInfo.getName()).thenReturn(TEST_FOLDER_NAME);
        when(mockBoxFolder.getInfo()).thenReturn(mockBoxFolderInfo);
        when(mockBoxFolder.uploadFile(any(InputStream.class), eq(TEST_FILENAME))).thenThrow(new RuntimeException("Upload error"));

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Upload error");
        assertNoProvenanceEvent();
    }
}
