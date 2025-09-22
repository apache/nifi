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

import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFolder;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class ListBoxFileInfoTest extends AbstractBoxFileTest implements FileListingTestTrait {

    private static final String RECORD_WRITER_ID = "record-writer";

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final ListBoxFileInfo testSubject = new ListBoxFileInfo() {
            @Override
            BoxFolder getFolder(final String folderId) {
                return mockBoxFolder;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);

        final MockRecordWriter writerService = new MockRecordWriter("id,filename,path,size,timestamp", false);
        testRunner.addControllerService(RECORD_WRITER_ID, writerService);
        testRunner.enableControllerService(writerService);
        testRunner.setProperty(ListBoxFileInfo.RECORD_WRITER, RECORD_WRITER_ID);

        super.setUp();
    }

    @Test
    void testFetchMetadataFromFolderWithFolderIdProperty() {
        testRunner.setProperty(ListBoxFileInfo.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(ListBoxFileInfo.RECURSIVE_SEARCH, "false");

        final List<String> pathParts = Arrays.asList("path", "to", "file");
        mockFetchedFileList(TEST_FILE_ID, TEST_FILENAME, pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME);

        testRunner.enqueue("test file");
        testRunner.run();

        testRunner.assertTransferCount(ListBoxFileInfo.REL_SUCCESS, 1);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_FAILURE, 0);

        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ListBoxFileInfo.REL_SUCCESS);
        final MockFlowFile outputFlowFile = successFiles.getFirst();

        outputFlowFile.assertAttributeExists("record.count");
        assertEquals("1", outputFlowFile.getAttribute("record.count"));
        outputFlowFile.assertAttributeEquals("box.folder.id", TEST_FOLDER_ID);

        final String content = new String(outputFlowFile.toByteArray());
        assertTrue(content.contains(TEST_FILE_ID));
        assertTrue(content.contains(TEST_FILENAME));
        assertTrue(content.contains("/path"));
        assertTrue(content.contains(String.valueOf(TEST_SIZE)));
    }

    @Test
    void testFetchMetadataFromFolderWithFolderIdAttributeExpression() {
        testRunner.setProperty(ListBoxFileInfo.FOLDER_ID, "${box.folder.id}");
        testRunner.setProperty(ListBoxFileInfo.RECURSIVE_SEARCH, "true");

        final List<String> pathParts = Arrays.asList("path", "to", "file");
        mockFetchedFileList(TEST_FILE_ID, TEST_FILENAME, pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME);

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("box.folder.id", TEST_FOLDER_ID);

        testRunner.enqueue("test file", attributeMap);
        testRunner.run();

        testRunner.assertTransferCount(ListBoxFileInfo.REL_SUCCESS, 1);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_FAILURE, 0);

        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ListBoxFileInfo.REL_SUCCESS);
        final MockFlowFile outputFlowFile = successFiles.getFirst();

        outputFlowFile.assertAttributeExists("record.count");
        assertEquals("1", outputFlowFile.getAttribute("record.count"));
        outputFlowFile.assertAttributeEquals("box.folder.id", TEST_FOLDER_ID);

        final String content = new String(outputFlowFile.toByteArray());
        assertTrue(content.contains(TEST_FILE_ID));
        assertTrue(content.contains(TEST_FILENAME));
        assertTrue(content.contains("/path"));
        assertTrue(content.contains(String.valueOf(TEST_SIZE)));
    }

    @Test
    void testProcessingMultipleFiles() {
        testRunner.setProperty(ListBoxFileInfo.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(ListBoxFileInfo.RECURSIVE_SEARCH, "false");
        mockMultipleFilesResponse();

        testRunner.enqueue("test file");
        testRunner.run();

        testRunner.assertTransferCount(ListBoxFileInfo.REL_SUCCESS, 1);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_FAILURE, 0);

        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ListBoxFileInfo.REL_SUCCESS);
        final MockFlowFile outputFlowFile = successFiles.getFirst();
        outputFlowFile.assertAttributeEquals("record.count", "3");
        outputFlowFile.assertAttributeEquals("box.folder.id", TEST_FOLDER_ID);
        final String content = new String(outputFlowFile.toByteArray());
        assertTrue(content.contains(TEST_FILE_ID + "1"));
        assertTrue(content.contains(TEST_FILE_ID + "2"));
        assertTrue(content.contains(TEST_FILE_ID + "3"));
    }

    @Test
    void testBoxAPIResponseException() {
        testRunner.setProperty(ListBoxFileInfo.FOLDER_ID, TEST_FOLDER_ID);

        final BoxAPIResponseException apiException = new BoxAPIResponseException("API Error", 500, "Internal Server Error", null);
        doThrow(apiException).when(mockBoxFolder).getChildren(
                "id",
                "name",
                "item_status",
                "size",
                "created_at",
                "modified_at",
                "content_created_at",
                "content_modified_at",
                "path_collection");

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();
        testRunner.assertTransferCount(ListBoxFileInfo.REL_FAILURE, 1);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_NOT_FOUND, 0);

        final List<MockFlowFile> failureFiles = testRunner.getFlowFilesForRelationship(ListBoxFileInfo.REL_FAILURE);
        final MockFlowFile failureFlowFile = failureFiles.getFirst();
        failureFlowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
        failureFlowFile.assertAttributeExists(ERROR_MESSAGE);
    }

    @Test
    void testBoxAPIResponseExceptionNotFound() {
        testRunner.setProperty(ListBoxFileInfo.FOLDER_ID, TEST_FOLDER_ID);

        final BoxAPIResponseException apiException = new BoxAPIResponseException("API Error", 404, "Not Found", null);
        doThrow(apiException).when(mockBoxFolder).getChildren(
                "id",
                "name",
                "item_status",
                "size",
                "created_at",
                "modified_at",
                "content_created_at",
                "content_modified_at",
                "path_collection");

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertTransferCount(ListBoxFileInfo.REL_FAILURE, 0);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_SUCCESS, 0);
        testRunner.assertTransferCount(ListBoxFileInfo.REL_NOT_FOUND, 1);

        final List<MockFlowFile> notFoundFiles = testRunner.getFlowFilesForRelationship(ListBoxFileInfo.REL_NOT_FOUND);
        final MockFlowFile notFoundFlowFile = notFoundFiles.getFirst();
        notFoundFlowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        notFoundFlowFile.assertAttributeExists(ERROR_MESSAGE);
    }

    private void mockMultipleFilesResponse() {
        List<String> pathParts = Arrays.asList("path", "to", "file");

        doReturn(Arrays.asList(
                createFileInfo(TEST_FILE_ID + "1", TEST_FILENAME + "1", pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME),
                createFileInfo(TEST_FILE_ID + "2", TEST_FILENAME + "2", pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME),
                createFileInfo(TEST_FILE_ID + "3", TEST_FILENAME + "3", pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME)
        )).when(mockBoxFolder).getChildren(
                "id",
                "name",
                "item_status",
                "size",
                "created_at",
                "modified_at",
                "content_created_at",
                "content_modified_at",
                "path_collection");
    }

    @Override
    public BoxFolder getMockBoxFolder() {
        return mockBoxFolder;
    }
}
