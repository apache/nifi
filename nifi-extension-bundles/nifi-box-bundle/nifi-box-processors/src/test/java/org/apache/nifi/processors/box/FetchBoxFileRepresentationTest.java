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
import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.managers.files.FilesManager;
import com.box.sdkgen.managers.files.GetFileByIdQueryParams;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.filefull.FileFullRepresentationsField;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchBoxFileRepresentationTest extends AbstractBoxFileTest implements FileListingTestTrait {
    private static final String TEST_FILE_ID = "238490238429";
    private static final String TEST_REPRESENTATION_TYPE = "pdf";
    private static final String TEST_FILE_NAME = "testfile.txt";
    private static final long TEST_FILE_SIZE = 1024L;
    private static final OffsetDateTime TEST_CREATED_TIME = OffsetDateTime.of(2022, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    private static final OffsetDateTime TEST_MODIFIED_TIME = OffsetDateTime.of(2022, 2, 2, 0, 0, 0, 0, ZoneOffset.UTC);

    @Mock
    private FilesManager mockFilesManager;

    @Mock
    private FileFull mockFileFull;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFileRepresentation testProcessor = new FetchBoxFileRepresentation();

        testRunner = TestRunners.newTestRunner(testProcessor);
        testRunner.setProperty(FetchBoxFileRepresentation.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileRepresentation.REPRESENTATION_TYPE, TEST_REPRESENTATION_TYPE);
        super.setUp();
    }

    @Test
    void testFileNotFound() {
        // Create 404 exception using mock
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError notFoundException = mock(BoxAPIError.class);
        when(notFoundException.getMessage()).thenReturn("File not found");
        when(notFoundException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenThrow(notFoundException);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_FILE_NOT_FOUND, 1);

        final MockFlowFile resultFile = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_FILE_NOT_FOUND).getFirst();
        resultFile.assertAttributeEquals("box.error.message", "File not found");
        resultFile.assertAttributeEquals("box.error.code", "404");
    }

    @Test
    void testRepresentationNotFound() {
        // Set up file without matching representation
        lenient().when(mockFileFull.getId()).thenReturn(TEST_FILE_ID);
        lenient().when(mockFileFull.getName()).thenReturn(TEST_FILE_NAME);
        lenient().when(mockFileFull.getSize()).thenReturn(TEST_FILE_SIZE);
        lenient().when(mockFileFull.getCreatedAt()).thenReturn(TEST_CREATED_TIME);
        lenient().when(mockFileFull.getModifiedAt()).thenReturn(TEST_MODIFIED_TIME);
        when(mockFileFull.getRepresentations()).thenReturn(null);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenReturn(mockFileFull);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_REPRESENTATION_NOT_FOUND, 1);

        final MockFlowFile resultFile = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_REPRESENTATION_NOT_FOUND).getFirst();
        resultFile.assertAttributeEquals("box.error.message", "No matching representation found");
    }

    @Test
    void testGeneralApiError() {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(500);
        BoxAPIError generalException = mock(BoxAPIError.class);
        when(generalException.getMessage()).thenReturn("API error occurred");
        when(generalException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenThrow(generalException);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_FAILURE, 1);

        final MockFlowFile resultFile = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_FAILURE).getFirst();
        resultFile.assertAttributeEquals("box.error.message", "API error occurred");
        resultFile.assertAttributeEquals("box.error.code", "500");
    }

    @Test
    void testFileIdFromFlowFileAttributes() {
        // Set up file without matching representation to test attribute flow
        lenient().when(mockFileFull.getId()).thenReturn(TEST_FILE_ID);
        lenient().when(mockFileFull.getName()).thenReturn(TEST_FILE_NAME);
        lenient().when(mockFileFull.getSize()).thenReturn(TEST_FILE_SIZE);
        lenient().when(mockFileFull.getCreatedAt()).thenReturn(TEST_CREATED_TIME);
        lenient().when(mockFileFull.getModifiedAt()).thenReturn(TEST_MODIFIED_TIME);
        when(mockFileFull.getRepresentations()).thenReturn(null);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenReturn(mockFileFull);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        testRunner.setProperty(FetchBoxFileRepresentation.FILE_ID, "${box.id}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("box.id", TEST_FILE_ID);
        testRunner.enqueue("", attributes);
        testRunner.run();

        // Will be routed to representation not found since we don't have representations set up
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_REPRESENTATION_NOT_FOUND, 1);
    }

    @Test
    void testEmptyRepresentationEntries() {
        // Set up file with empty representation entries
        FileFullRepresentationsField representations = mock(FileFullRepresentationsField.class);
        when(representations.getEntries()).thenReturn(List.of());

        lenient().when(mockFileFull.getId()).thenReturn(TEST_FILE_ID);
        lenient().when(mockFileFull.getName()).thenReturn(TEST_FILE_NAME);
        lenient().when(mockFileFull.getSize()).thenReturn(TEST_FILE_SIZE);
        lenient().when(mockFileFull.getCreatedAt()).thenReturn(TEST_CREATED_TIME);
        lenient().when(mockFileFull.getModifiedAt()).thenReturn(TEST_MODIFIED_TIME);
        when(mockFileFull.getRepresentations()).thenReturn(representations);

        when(mockFilesManager.getFileById(anyString(), any(GetFileByIdQueryParams.class))).thenReturn(mockFileFull);
        when(mockBoxClient.getFiles()).thenReturn(mockFilesManager);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_REPRESENTATION_NOT_FOUND, 1);
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
