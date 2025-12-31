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

import com.box.sdkgen.box.errors.BoxAPIError;
import com.box.sdkgen.box.errors.ResponseInfo;
import com.box.sdkgen.managers.files.FilesManager;
import com.box.sdkgen.managers.files.GetFileByIdQueryParams;
import com.box.sdkgen.schemas.file.FileItemStatusField;
import com.box.sdkgen.schemas.file.FilePathCollectionField;
import com.box.sdkgen.schemas.file.FileSharedLinkField;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import com.box.sdkgen.schemas.usermini.UserMini;
import com.box.sdkgen.serialization.json.EnumWrapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FetchBoxFileInfoTest extends AbstractBoxFileTest {
    private static final String TEST_DESCRIPTION = "Test file description";
    private static final String TEST_ETAG = "0";
    private static final String TEST_SHA1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
    private static final String TEST_ITEM_STATUS = "active";
    private static final String TEST_SEQUENCE_ID = "1";
    private static final String TEST_OWNER_NAME = "Test User";
    private static final String TEST_OWNER_ID = "123456";
    private static final String TEST_OWNER_LOGIN = "Test.User@mail.org";
    private static final String TEST_SHARED_LINK_URL = "https://app.box.com/s/abcdef123456";
    private static final OffsetDateTime TEST_CREATED_AT = OffsetDateTime.of(1970, 1, 1, 3, 25, 45, 678000000, ZoneOffset.UTC);
    private static final OffsetDateTime TEST_CONTENT_CREATED_AT = OffsetDateTime.of(1970, 1, 1, 3, 25, 45, 600000000, ZoneOffset.UTC);
    private static final OffsetDateTime TEST_CONTENT_MODIFIED_AT = OffsetDateTime.of(1970, 1, 1, 3, 25, 45, 700000000, ZoneOffset.UTC);

    @Mock
    FilesManager mockFilesManager;

    @Mock
    FileFull mockFileFull;

    @Mock
    UserMini mockBoxUser;

    @Mock
    FileSharedLinkField mockSharedLink;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFileInfo testSubject = new FetchBoxFileInfo();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testFetchMetadataFromFlowFileAttribute() {
        testRunner.setProperty(FetchBoxFileInfo.FILE_ID, "${box.id}");
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(BoxFileAttributes.ID, TEST_FILE_ID);
        inputFlowFile.putAttributes(attributes);

        setupMockFileInfoWithExtendedAttributes();

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileInfo.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileInfo.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        assertOutFlowFileAttributes(flowFilesFirst);
        verifyExtendedAttributes(flowFilesFirst);
    }

    @Test
    void testFetchMetadataFromProperty() {
        testRunner.setProperty(FetchBoxFileInfo.FILE_ID, TEST_FILE_ID);

        setupMockFileInfoWithExtendedAttributes();

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileInfo.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileInfo.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        assertOutFlowFileAttributes(flowFilesFirst);
        verifyExtendedAttributes(flowFilesFirst);
    }

    @Test
    void testApiErrorHandling() {
        testRunner.setProperty(FetchBoxFileInfo.FILE_ID, TEST_FILE_ID);

        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("API Error [404]");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenThrow(mockException);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileInfo.REL_NOT_FOUND, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileInfo.REL_NOT_FOUND);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testBoxApiExceptionHandling() {
        testRunner.setProperty(FetchBoxFileInfo.FILE_ID, TEST_FILE_ID);

        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(500);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("General API Error:\nUnexpected Error");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenThrow(mockException);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileInfo.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileInfo.REL_FAILURE);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
    }

    private void setupMockFileInfoWithExtendedAttributes() {
        // Set up path collection
        FolderMini folderMini = mock(FolderMini.class);
        when(folderMini.getName()).thenReturn(TEST_FOLDER_NAME);
        when(folderMini.getId()).thenReturn("not0");

        FilePathCollectionField pathCollection = mock(FilePathCollectionField.class);
        when(pathCollection.getEntries()).thenReturn(List.of(folderMini));

        // Set up basic file info
        when(mockFileFull.getId()).thenReturn(TEST_FILE_ID);
        when(mockFileFull.getName()).thenReturn(TEST_FILENAME);
        when(mockFileFull.getSize()).thenReturn(TEST_SIZE);
        when(mockFileFull.getPathCollection()).thenReturn(pathCollection);
        when(mockFileFull.getModifiedAt()).thenReturn(OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(MODIFIED_TIME), ZoneOffset.UTC));

        // Set up additional metadata attributes
        when(mockFileFull.getDescription()).thenReturn(TEST_DESCRIPTION);
        when(mockFileFull.getEtag()).thenReturn(TEST_ETAG);
        when(mockFileFull.getSha1()).thenReturn(TEST_SHA1);
        when(mockFileFull.getItemStatus()).thenReturn(new EnumWrapper<>(FileItemStatusField.ACTIVE));
        when(mockFileFull.getSequenceId()).thenReturn(TEST_SEQUENCE_ID);
        when(mockFileFull.getCreatedAt()).thenReturn(TEST_CREATED_AT);
        when(mockFileFull.getContentCreatedAt()).thenReturn(TEST_CONTENT_CREATED_AT);
        when(mockFileFull.getContentModifiedAt()).thenReturn(TEST_CONTENT_MODIFIED_AT);
        when(mockFileFull.getTrashedAt()).thenReturn(null);
        when(mockFileFull.getPurgedAt()).thenReturn(null);

        when(mockBoxUser.getName()).thenReturn(TEST_OWNER_NAME);
        when(mockBoxUser.getId()).thenReturn(TEST_OWNER_ID);
        when(mockBoxUser.getLogin()).thenReturn(TEST_OWNER_LOGIN);
        when(mockFileFull.getOwnedBy()).thenReturn(mockBoxUser);

        when(mockSharedLink.getUrl()).thenReturn(TEST_SHARED_LINK_URL);
        when(mockFileFull.getSharedLink()).thenReturn(mockSharedLink);

        // Return the file info when requested
        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenReturn(mockFileFull);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);
    }

    private void verifyExtendedAttributes(MockFlowFile flowFile) {
        flowFile.assertAttributeEquals("box.description", TEST_DESCRIPTION);
        flowFile.assertAttributeEquals("box.etag", TEST_ETAG);
        flowFile.assertAttributeEquals("box.sha1", TEST_SHA1);
        flowFile.assertAttributeEquals("box.item.status", TEST_ITEM_STATUS);
        flowFile.assertAttributeEquals("box.sequence.id", TEST_SEQUENCE_ID);
        flowFile.assertAttributeEquals("box.created.at", TEST_CREATED_AT.toString());
        flowFile.assertAttributeEquals("box.content.created.at", TEST_CONTENT_CREATED_AT.toString());
        flowFile.assertAttributeEquals("box.content.modified.at", TEST_CONTENT_MODIFIED_AT.toString());
        flowFile.assertAttributeEquals("box.owner", TEST_OWNER_NAME);
        flowFile.assertAttributeEquals("box.owner.id", TEST_OWNER_ID);
        flowFile.assertAttributeEquals("box.owner.login", TEST_OWNER_LOGIN);
        flowFile.assertAttributeEquals("box.shared.link", TEST_SHARED_LINK_URL);
        flowFile.assertAttributeEquals("box.path.folder.ids", "not0");
        flowFile.assertAttributeEquals("path", "/" + TEST_FOLDER_NAME);
    }
}
