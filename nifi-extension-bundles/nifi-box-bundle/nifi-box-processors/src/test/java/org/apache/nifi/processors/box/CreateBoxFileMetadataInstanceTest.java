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
import com.box.sdkgen.managers.filemetadata.CreateFileMetadataByIdScope;
import com.box.sdkgen.managers.filemetadata.FileMetadataManager;
import com.box.sdkgen.schemas.metadatafull.MetadataFull;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateBoxFileMetadataInstanceTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_NAME = "fileProperties";

    @Mock
    private FileMetadataManager mockFileMetadataManager;

    @Mock
    private MetadataFull mockMetadata;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final CreateBoxFileMetadataInstance testSubject = new CreateBoxFileMetadataInstance();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        configureJsonRecordReader(testRunner);

        testRunner.setProperty(CreateBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(CreateBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_NAME);
        testRunner.setProperty(CreateBoxFileMetadataInstance.RECORD_READER, "json-reader");

        lenient().when(mockFileMetadataManager.createFileMetadataById(anyString(), any(CreateFileMetadataByIdScope.class), anyString(), anyMap())).thenReturn(mockMetadata);
        lenient().when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();

        runner.addControllerService("json-reader", readerService);
        runner.setProperty(readerService, "Date Format", "yyyy-MM-dd");
        runner.setProperty(readerService, "Timestamp Format", "yyyy-MM-dd HH:mm:ss");

        runner.enableControllerService(readerService);
    }

    @Test
    void testSuccessfulMetadataCreation() {
        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans",
                  "competitiveDocument": "no",
                  "status": "active",
                  "author": "Jones",
                  "int": 1,
                  "double": 1.234,
                  "almostTenToThePowerOfThirty": 1000000000000000000000000000123,
                  "array": [ "one", "two", "three" ],
                  "intArray": [ 1, 2, 3 ],
                  "date": "2025-01-01"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, Object>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(mockFileMetadataManager).createFileMetadataById(eq(TEST_FILE_ID), eq(CreateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), metadataCaptor.capture());

        final Map<String, Object> capturedMetadata = metadataCaptor.getValue();
        assertEquals("internal", capturedMetadata.get("audience"));
        assertEquals("Q1 plans", capturedMetadata.get("documentType"));
        assertEquals("no", capturedMetadata.get("competitiveDocument"));
        assertEquals("active", capturedMetadata.get("status"));
        assertEquals("Jones", capturedMetadata.get("author"));
        assertEquals(1.0, capturedMetadata.get("int"));
        assertEquals(1.234, capturedMetadata.get("double"));
        assertTrue(capturedMetadata.get("array") instanceof List);
        assertEquals(List.of("one", "two", "three"), capturedMetadata.get("array"));
        assertEquals(List.of("1", "2", "3"), capturedMetadata.get("intArray"));

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.template.key", TEMPLATE_NAME);
    }

    @Test
    void testEmptyInput() {
        final String inputJson = "{}";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_FAILURE).getFirst();
        flowFile.assertAttributeExists("error.message");
    }

    @Test
    void testFileNotFound() throws Exception {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("API Error [404]");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFileMetadataManager.createFileMetadataById(anyString(), any(CreateFileMetadataByIdScope.class), anyString(), anyMap())).thenThrow(mockException);
        when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testTemplateNotFound() throws Exception {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("Specified Metadata Template not found");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFileMetadataManager.createFileMetadataById(anyString(), any(CreateFileMetadataByIdScope.class), anyString(), anyMap())).thenThrow(mockException);
        when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(CreateBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(CreateBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }
}
