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

import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxSharedLink;
import com.box.sdk.BoxUser;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
    private static final Date TEST_CREATED_AT = new Date(12345678L);
    private static final Date TEST_CONTENT_CREATED_AT = new Date(12345600L);
    private static final Date TEST_CONTENT_MODIFIED_AT = new Date(12345700L);
    private static final Date TEST_TRASHED_AT = null;
    private static final Date TEST_PURGED_AT = null;

    @Mock
    BoxFile mockBoxFile;

    @Mock
    BoxUser.Info mockBoxUser;

    @Mock
    BoxSharedLink mockSharedLink;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFileInfo testSubject = new FetchBoxFileInfo() {
            @Override
            protected BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

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

        BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        doThrow(mockException).when(mockBoxFile).getInfo(anyString(), anyString(), anyString(), anyString(), anyString(),
                anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
                anyString(), anyString(), anyString(), anyString(), anyString());

        MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileInfo.REL_NOT_FOUND, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileInfo.REL_NOT_FOUND);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testBoxApiExceptionHandling() {
        testRunner.setProperty(FetchBoxFileInfo.FILE_ID, TEST_FILE_ID);

        BoxAPIException mockException = new BoxAPIException("General API Error:", 500, "Unexpected Error");
        doThrow(mockException).when(mockBoxFile).getInfo(anyString(), anyString(), anyString(), anyString(), anyString(),
                anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
                anyString(), anyString(), anyString(), anyString(), anyString());

        MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileInfo.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileInfo.REL_FAILURE);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error:\nUnexpected Error");
    }

    private void setupMockFileInfoWithExtendedAttributes() {
        final BoxFile.Info fetchedFileInfo = createFileInfo(TEST_FOLDER_NAME, MODIFIED_TIME);

        // Set up additional metadata attributes
        when(mockFileInfo.getDescription()).thenReturn(TEST_DESCRIPTION);
        when(mockFileInfo.getEtag()).thenReturn(TEST_ETAG);
        when(mockFileInfo.getSha1()).thenReturn(TEST_SHA1);
        when(mockFileInfo.getItemStatus()).thenReturn(TEST_ITEM_STATUS);
        when(mockFileInfo.getSequenceID()).thenReturn(TEST_SEQUENCE_ID);
        when(mockFileInfo.getCreatedAt()).thenReturn(TEST_CREATED_AT);
        when(mockFileInfo.getContentCreatedAt()).thenReturn(TEST_CONTENT_CREATED_AT);
        when(mockFileInfo.getContentModifiedAt()).thenReturn(TEST_CONTENT_MODIFIED_AT);
        when(mockFileInfo.getTrashedAt()).thenReturn(TEST_TRASHED_AT);
        when(mockFileInfo.getPurgedAt()).thenReturn(TEST_PURGED_AT);

        when(mockBoxUser.getName()).thenReturn(TEST_OWNER_NAME);
        when(mockBoxUser.getID()).thenReturn(TEST_OWNER_ID);
        when(mockBoxUser.getLogin()).thenReturn(TEST_OWNER_LOGIN);
        when(mockFileInfo.getOwnedBy()).thenReturn(mockBoxUser);

        when(mockSharedLink.getURL()).thenReturn(TEST_SHARED_LINK_URL);
        when(mockFileInfo.getSharedLink()).thenReturn(mockSharedLink);

        // Return the file info when requested
        doReturn(fetchedFileInfo).when(mockBoxFile).getInfo("name", "description", "size", "created_at", "modified_at",
                "owned_by", "parent", "etag", "sha1", "item_status", "sequence_id", "path_collection",
                "content_created_at", "content_modified_at", "trashed_at", "purged_at", "shared_link");
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
        flowFile.assertAttributeEquals("box.path.folder.ids", mockBoxFolderInfo.getID());
        flowFile.assertAttributeEquals("path", "/" + mockBoxFolderInfo.getName());
    }
}
