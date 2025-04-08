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
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchBoxFileRepresentationTest extends AbstractBoxFileTest {
    private static final String TEST_FILE_ID = "238490238429";
    private static final String TEST_REPRESENTATION_TYPE = "pdf";
    private static final String TEST_FILE_NAME = "testfile.txt";
    private static final long TEST_FILE_SIZE = 1024L;
    private static final Date TEST_CREATED_TIME = new Date(1643673600000L); // 2022-02-01
    private static final Date TEST_MODIFIED_TIME = new Date(1643760000000L); // 2022-02-02
    private static final String TEST_FILE_TYPE = "file";
    private static final byte[] TEST_CONTENT = "test content".getBytes();

    @Mock
    private BoxFile mockBoxFile;

    @Mock
    private BoxFile.Info mockFileInfo;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        when(mockBoxFile.getInfo()).thenReturn(mockFileInfo);

        final FetchBoxFileRepresentation testProcessor = new FetchBoxFileRepresentation() {
            @Override
            protected BoxFile getBoxFile(final String fileId) {
                return mockBoxFile;
            }
        };

        testRunner = TestRunners.newTestRunner(testProcessor);
        testRunner.setProperty(FetchBoxFileRepresentation.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileRepresentation.REPRESENTATION_TYPE, TEST_REPRESENTATION_TYPE);
        super.setUp();
    }

    @Test
    void testSuccessfulFetch() throws Exception {
        when(mockFileInfo.getName()).thenReturn(TEST_FILE_NAME);
        when(mockFileInfo.getSize()).thenReturn(TEST_FILE_SIZE);
        when(mockFileInfo.getCreatedAt()).thenReturn(TEST_CREATED_TIME);
        when(mockFileInfo.getModifiedAt()).thenReturn(TEST_MODIFIED_TIME);
        when(mockFileInfo.getType()).thenReturn(TEST_FILE_TYPE);

        doAnswer(invocation -> {
            final OutputStream outputStream = invocation.getArgument(2);
            outputStream.write(TEST_CONTENT);
            return null;
        }).when(mockBoxFile).getRepresentationContent(eq("[" + TEST_REPRESENTATION_TYPE + "]"), anyString(), any(OutputStream.class), anyInt());


        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_SUCCESS, 1);

        final List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_SUCCESS);
        final MockFlowFile resultFile = successFiles.getFirst();
        resultFile.assertContentEquals(TEST_CONTENT);

        resultFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        resultFile.assertAttributeEquals("box.file.name", TEST_FILE_NAME);
        resultFile.assertAttributeEquals("box.file.size", String.valueOf(TEST_FILE_SIZE));
        resultFile.assertAttributeEquals("box.file.created.time", TEST_CREATED_TIME.toString());
        resultFile.assertAttributeEquals("box.file.modified.time", TEST_MODIFIED_TIME.toString());
        resultFile.assertAttributeEquals("box.file.mime.type", TEST_FILE_TYPE);
        resultFile.assertAttributeEquals("box.file.representation.type", TEST_REPRESENTATION_TYPE);

        final List<ProvenanceEventRecord> provenanceEvents = testRunner.getProvenanceEvents();
        assertEquals(1, provenanceEvents.size());
        assertEquals(ProvenanceEventType.FETCH, provenanceEvents.getFirst().getEventType());
    }

    @Test
    void testFileNotFound() {
        // Create 404 exception
        final BoxAPIResponseException notFoundException = mock(BoxAPIResponseException.class);
        when(notFoundException.getResponseCode()).thenReturn(404);
        when(notFoundException.getMessage()).thenReturn("File not found");

        when(mockBoxFile.getInfo()).thenThrow(notFoundException);

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_FILE_NOT_FOUND, 1);

        final MockFlowFile resultFile = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_FILE_NOT_FOUND).getFirst();
        resultFile.assertAttributeEquals("box.error.message", "File not found");
        resultFile.assertAttributeEquals("box.error.code", "404");
    }

    @Test
    void testRepresentationNotFound() {
        // Have getRepresentationContent throw a BoxAPIException with representation not found error
        final BoxAPIException repNotFoundException = mock(BoxAPIException.class);
        when(repNotFoundException.getMessage()).thenReturn("No matching representations found for requested hint");
        when(repNotFoundException.getResponseCode()).thenReturn(400);

        doThrow(repNotFoundException).when(mockBoxFile).getRepresentationContent(eq("[" + TEST_REPRESENTATION_TYPE + "]"), anyString(), any(OutputStream.class), anyInt());

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_REPRESENTATION_NOT_FOUND, 1);

        final MockFlowFile resultFile = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_REPRESENTATION_NOT_FOUND).getFirst();
        resultFile.assertAttributeEquals("box.error.message", "No matching representations found for requested hint");
        resultFile.assertAttributeEquals("box.error.code", "400");
    }

    @Test
    void testGeneralApiError() {
        // Have getRepresentationContent throw a BoxAPIException with a general error
        final BoxAPIException generalException = mock(BoxAPIException.class);
        when(generalException.getMessage()).thenReturn("API error occurred");
        when(generalException.getResponseCode()).thenReturn(500);

        doThrow(generalException).when(mockBoxFile).getRepresentationContent(eq("[" + TEST_REPRESENTATION_TYPE + "]"), anyString(), any(OutputStream.class), anyInt());

        testRunner.enqueue("");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_FAILURE, 1);

        final MockFlowFile resultFile = testRunner.getFlowFilesForRelationship(FetchBoxFileRepresentation.REL_FAILURE).getFirst();
        resultFile.assertAttributeEquals("box.error.message", "API error occurred");
        resultFile.assertAttributeEquals("box.error.code", "500");
    }

    @Test
    void testFileIdFromFlowFileAttributes() {
        when(mockFileInfo.getName()).thenReturn(TEST_FILE_NAME);
        when(mockFileInfo.getSize()).thenReturn(TEST_FILE_SIZE);
        when(mockFileInfo.getCreatedAt()).thenReturn(TEST_CREATED_TIME);
        when(mockFileInfo.getModifiedAt()).thenReturn(TEST_MODIFIED_TIME);
        when(mockFileInfo.getType()).thenReturn(TEST_FILE_TYPE);
        testRunner.setProperty(FetchBoxFileRepresentation.FILE_ID, "${box.id}");

        doAnswer(invocation -> {
            final OutputStream outputStream = invocation.getArgument(2);
            outputStream.write(TEST_CONTENT);
            return null;
        }).when(mockBoxFile).getRepresentationContent(eq("[" + TEST_REPRESENTATION_TYPE + "]"), anyString(), any(OutputStream.class), anyInt());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("box.id", TEST_FILE_ID);
        testRunner.enqueue("", attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileRepresentation.REL_SUCCESS, 1);
    }
}
