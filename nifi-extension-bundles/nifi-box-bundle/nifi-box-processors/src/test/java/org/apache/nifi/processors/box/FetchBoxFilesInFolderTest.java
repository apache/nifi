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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class FetchBoxFilesInFolderTest extends AbstractBoxFileTest implements FileListingTestTrait {

    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFilesInFolder testSubject = new FetchBoxFilesInFolder() {
            @Override
            BoxFolder getFolder(final String folderId) {
                return mockBoxFolder;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testFetchMetadataFromFolderWithFolderIdProperty() {
        testRunner.setProperty(FetchBoxFilesInFolder.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(FetchBoxFilesInFolder.RECURSIVE_SEARCH, "false");

        final List<String> pathParts = Arrays.asList("path", "to", "file");
        mockFetchedFileList(TEST_FILE_ID, TEST_FILENAME, pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME);

        testRunner.enqueue("test file");
        testRunner.run();

        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_SUCCESS, 1);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_ORIGINAL, 1);

        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(FetchBoxFilesInFolder.REL_SUCCESS);
        final MockFlowFile outputFlowFile = successFiles.getFirst();
        outputFlowFile.assertAttributeEquals(ID, TEST_FILE_ID);
        outputFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        outputFlowFile.assertAttributeEquals(CoreAttributes.PATH.key(), "/path/to/file");
        outputFlowFile.assertAttributeEquals(SIZE, valueOf(TEST_SIZE));
        outputFlowFile.assertAttributeEquals(TIMESTAMP, valueOf(new Date(MODIFIED_TIME)));
    }

    @Test
    void testFetchMetadataFromFolderWithFolderIdAttributeExpression() {
        testRunner.setProperty(FetchBoxFilesInFolder.FOLDER_ID, "${box.folder.id}");
        testRunner.setProperty(FetchBoxFilesInFolder.RECURSIVE_SEARCH, "true");

        final List<String> pathParts = Arrays.asList("path", "to", "file");
        mockFetchedFileList(TEST_FILE_ID, TEST_FILENAME, pathParts, TEST_SIZE, CREATED_TIME, MODIFIED_TIME);

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("box.folder.id", TEST_FOLDER_ID);

        testRunner.enqueue("test file", attributeMap);
        testRunner.run();

        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_SUCCESS, 1);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_FAILURE, 0);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_ORIGINAL, 1);

        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(FetchBoxFilesInFolder.REL_SUCCESS);
        final MockFlowFile outputFlowFile = successFiles.get(0);
        outputFlowFile.assertAttributeEquals(ID, TEST_FILE_ID);
        outputFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        outputFlowFile.assertAttributeEquals(CoreAttributes.PATH.key(), "/path/to/file");
        outputFlowFile.assertAttributeEquals(SIZE, valueOf(TEST_SIZE));
        outputFlowFile.assertAttributeEquals(TIMESTAMP, valueOf(new Date(MODIFIED_TIME)));
    }

    @Test
    void testBoxAPIResponseException() {
        testRunner.setProperty(FetchBoxFilesInFolder.FOLDER_ID, TEST_FOLDER_ID);

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

        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_FAILURE, 1);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_ORIGINAL, 0);

        final List<MockFlowFile> failureFiles = testRunner.getFlowFilesForRelationship(FetchBoxFilesInFolder.REL_FAILURE);
        final MockFlowFile failureFlowFile = failureFiles.get(0);
        failureFlowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        failureFlowFile.assertAttributeExists(ERROR_MESSAGE);
    }

    @Test
    void testGenericException() {
        testRunner.setProperty(FetchBoxFilesInFolder.FOLDER_ID, TEST_FOLDER_ID);

        final RuntimeException exception = new RuntimeException("Generic error");
        doThrow(exception).when(mockBoxFolder).getChildren(
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

        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_FAILURE, 1);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_SUCCESS, 0);
        testRunner.assertTransferCount(FetchBoxFilesInFolder.REL_ORIGINAL, 0);

        final List<MockFlowFile> failureFiles = testRunner.getFlowFilesForRelationship(FetchBoxFilesInFolder.REL_FAILURE);
        final MockFlowFile failureFlowFile = failureFiles.get(0);
        failureFlowFile.assertAttributeEquals(ERROR_MESSAGE, "Generic error");
    }

    @Override
    public BoxFolder getMockBoxFolder() {
        return mockBoxFolder;
    }
}