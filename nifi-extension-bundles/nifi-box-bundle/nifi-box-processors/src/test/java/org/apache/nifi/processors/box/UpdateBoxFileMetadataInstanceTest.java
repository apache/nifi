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
import com.box.sdkgen.managers.filemetadata.FileMetadataManager;
import com.box.sdkgen.managers.filemetadata.UpdateFileMetadataByIdScope;
import com.box.sdkgen.schemas.metadatafull.MetadataFull;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateBoxFileMetadataInstanceTest extends AbstractBoxFileTest implements FileListingTestTrait {

    private static final String TEMPLATE_NAME = "fileProperties";
    private static final String TEMPLATE_SCOPE = "enterprise";

    @Mock
    private FileMetadataManager mockFileMetadataManager;

    @Mock
    private MetadataFull mockMetadata;

    private UpdateBoxFileMetadataInstance createTestSubject() {
        return new UpdateBoxFileMetadataInstance() {
            @Override
            MetadataFull getMetadata(final String fileId, final String templateKey) {
                return mockMetadata;
            }
        };
    }

    @Override
    @BeforeEach
    void setUp() throws Exception {
        testRunner = TestRunners.newTestRunner(createTestSubject());
        super.setUp();

        configureJsonRecordReader(testRunner);

        testRunner.setProperty(UpdateBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_NAME);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.RECORD_READER, "json-reader");

        // Setup mock metadata
        Map<String, Object> extraData = new HashMap<>();
        extraData.put("temp1", "value1");
        extraData.put("test", "test");
        lenient().when(mockMetadata.getExtraData()).thenReturn(extraData);

        lenient().when(mockFileMetadataManager.updateFileMetadataById(anyString(), any(UpdateFileMetadataByIdScope.class), anyString(), anyList())).thenReturn(mockMetadata);
        lenient().when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();
        runner.addControllerService("json-reader", readerService);
        runner.setProperty(readerService, "Date Format", "yyyy-MM-dd");
        runner.enableControllerService(readerService);
    }

    @Test
    void testSuccessfulMetadataUpdate() {
        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans",
                  "competitiveDocument": "no",
                  "status": "active",
                  "author": "Jones"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UpdateBoxFileMetadataInstance.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.template.key", TEMPLATE_NAME);
    }

    @Test
    void testEmptyInput() {
        final String inputJson = "{}";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UpdateBoxFileMetadataInstance.REL_FAILURE).getFirst();
        flowFile.assertAttributeExists("error.message");
    }

    @Test
    void testFileNotFound() throws Exception {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError exception = mock(BoxAPIError.class);
        when(exception.getMessage()).thenReturn("API Error [404]");
        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);

        testRunner = TestRunners.newTestRunner(new UpdateBoxFileMetadataInstance() {
            @Override
            MetadataFull getMetadata(final String fileId, final String templateKey) {
                throw exception;
            }
        });
        super.setUp();
        configureJsonRecordReader(testRunner);

        testRunner.setProperty(UpdateBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_NAME);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.RECORD_READER, "json-reader");

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UpdateBoxFileMetadataInstance.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testTemplateNotFound() throws Exception {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError exception = mock(BoxAPIError.class);
        when(exception.getMessage()).thenReturn("Specified Metadata Template not found");
        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);

        testRunner = TestRunners.newTestRunner(new UpdateBoxFileMetadataInstance() {
            @Override
            MetadataFull getMetadata(final String fileId, final String templateKey) {
                throw exception;
            }
        });
        super.setUp();
        configureJsonRecordReader(testRunner);

        testRunner.setProperty(UpdateBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_NAME);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.RECORD_READER, "json-reader");

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UpdateBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testNullValues() {
        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans",
                  "status": "active"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        // Verify the mockBoxFile.updateMetadata was called
        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testExpressionLanguage() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("file.id", TEST_FILE_ID);
        attributes.put("template.key", TEMPLATE_NAME);

        testRunner.setProperty(UpdateBoxFileMetadataInstance.FILE_ID, "${file.id}");
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_KEY, "${template.key}");

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson, attributes);
        testRunner.run();

        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());

        assertEquals(TEST_FILE_ID,
                testRunner.getProcessContext().getProperty(UpdateBoxFileMetadataInstance.FILE_ID).evaluateAttributeExpressions(attributes).getValue());
        assertEquals(TEMPLATE_NAME,
                testRunner.getProcessContext().getProperty(UpdateBoxFileMetadataInstance.TEMPLATE_KEY).evaluateAttributeExpressions(attributes).getValue());

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testMetadataPatchChanges() {
        // This tests the core functionality where we replace the entire state
        // Original metadata has "temp1":"value1" and "test":"test"
        // New metadata will have "temp2":"value2" and "test":"updated"
        // We expect: temp1 to be removed, temp2 to be added, test to be replaced
        final String inputJson = """
                {
                  "temp2": "value2",
                  "test": "updated"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testAddingDifferentDataTypes() {
        // Clear the extra data to simulate no existing metadata
        lenient().when(mockMetadata.getExtraData()).thenReturn(new HashMap<>());

        final String inputJson = """
                {
                  "stringField": "text value",
                  "numberField": 42,
                  "doubleField": 42.5,
                  "booleanField": true,
                  "listField": ["item1", "item2", "item3"],
                  "emptyListField": [],
                  "date": "2025-01-01"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testUpdateExistingFieldsWithDifferentTypes() {
        Map<String, Object> extraData = new HashMap<>();
        extraData.put("stringField", "old value");
        extraData.put("numberField", 10.0);
        extraData.put("listField", List.of("old1", "old2"));
        lenient().when(mockMetadata.getExtraData()).thenReturn(extraData);

        final String inputJson = """
                {
                  "stringField": "new value",
                  "numberField": 20,
                  "listField": ["new1", "new2", "new3"]
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testNoUpdateWhenValuesUnchanged() {
        // Set up existing fields with same values
        Map<String, Object> extraData = new HashMap<>();
        extraData.put("unchangedField", "same value");
        extraData.put("unchangedNumber", 42.0);
        lenient().when(mockMetadata.getExtraData()).thenReturn(extraData);

        final String inputJson = """
                {
                  "unchangedField": "same value",
                  "unchangedNumber": 42
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testMixedListHandling() {
        lenient().when(mockMetadata.getExtraData()).thenReturn(new HashMap<>());

        final String inputJson = """
                {
                  "mixedList": ["string", 42, true, 3.14]
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        verify(mockFileMetadataManager).updateFileMetadataById(eq(TEST_FILE_ID), eq(UpdateFileMetadataByIdScope.ENTERPRISE), eq(TEMPLATE_NAME), anyList());
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
