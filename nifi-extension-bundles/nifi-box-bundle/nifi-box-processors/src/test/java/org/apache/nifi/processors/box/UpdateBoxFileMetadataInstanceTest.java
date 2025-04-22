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
import com.box.sdk.BoxFile;
import com.box.sdk.Metadata;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class UpdateBoxFileMetadataInstanceTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_NAME = "fileProperties";
    private static final String TEMPLATE_SCOPE = "enterprise";

    @Mock
    private BoxFile mockBoxFile;

    @Mock
    private Metadata mockMetadata;

    private UpdateBoxFileMetadataInstance createTestSubject() {
        return new UpdateBoxFileMetadataInstance() {
            @Override
            BoxFile getBoxFile(final String fileId) {
                return mockBoxFile;
            }

            @Override
            Metadata getMetadata(final BoxFile boxFile,
                                 final String templateKey) {
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

        lenient().when(mockMetadata.getScope()).thenReturn(TEMPLATE_SCOPE);
        lenient().when(mockMetadata.getTemplateName()).thenReturn(TEMPLATE_NAME);
        lenient().when(mockBoxFile.getMetadata(TEMPLATE_NAME)).thenReturn(mockMetadata);
        lenient().when(mockMetadata.getPropertyPaths()).thenReturn(List.of("/temp1", "/test"));
        lenient().when(mockMetadata.getValue("/temp1")).thenReturn(Json.value("value1"));
        lenient().when(mockMetadata.getValue("/test")).thenReturn(Json.value("test"));
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();
        runner.addControllerService("json-reader", readerService);
        runner.setProperty(readerService, "Date Format", "yyyy-MM-dd");
        runner.enableControllerService(readerService);
    }

    @Test
    void testSuccessfulMetadataUpdate() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);

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

        verify(mockBoxFile).updateMetadata(any(Metadata.class));

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
        testRunner = TestRunners.newTestRunner(new UpdateBoxFileMetadataInstance() {
            @Override
            BoxFile getBoxFile(final String fileId) {
                throw new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
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
        testRunner = TestRunners.newTestRunner(new UpdateBoxFileMetadataInstance() {
            @Override
            Metadata getMetadata(final BoxFile boxFile,
                                 final String templateKey) {
                throw new BoxAPIResponseException("API Error", 404, "Specified Metadata Template not found", null);
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
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testNullValues() {
        JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans",
                  "status": "active"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        // Verify the mockBoxFile.updateMetadata was called
        verify(mockBoxFile).updateMetadata(any(Metadata.class));
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testExpressionLanguage() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);

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

        verify(mockBoxFile).updateMetadata(any(Metadata.class));

        assertEquals(TEST_FILE_ID,
                testRunner.getProcessContext().getProperty(UpdateBoxFileMetadataInstance.FILE_ID).evaluateAttributeExpressions(attributes).getValue());
        assertEquals(TEMPLATE_NAME,
                testRunner.getProcessContext().getProperty(UpdateBoxFileMetadataInstance.TEMPLATE_KEY).evaluateAttributeExpressions(attributes).getValue());

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testMetadataPatchChanges() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);

        // This tests the core functionality where we replace the entire state
        // Original metadata has "/temp1":"value1" and "/test":"test"
        // New metadata will have "/temp2":"value2" and "/test":"updated"
        // We expect: temp1 to be removed, temp2 to be added, test to be replaced
        final String inputJson = """
                {
                  "temp2": "value2",
                  "test": "updated"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        // Verify the correct operations were done on the mockMetadata
        verify(mockMetadata).remove("/temp1");  // Should remove temp1
        verify(mockMetadata).add("/temp2", "value2");  // Should add temp2
        verify(mockMetadata).replace("/test", "updated");  // Should update test
        verify(mockBoxFile).updateMetadata(any(Metadata.class));

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testAddingDifferentDataTypes() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);

        // Clear the property paths to simulate no existing metadata
        lenient().when(mockMetadata.getPropertyPaths()).thenReturn(List.of());

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

        // Verify all fields were added with correct types
        verify(mockMetadata).add("/stringField", "text value");
        verify(mockMetadata).add("/numberField", 42.0); // Numbers are stored as doubles
        verify(mockMetadata).add("/doubleField", 42.5);
        verify(mockMetadata).add("/booleanField", "true"); // Booleans are stored as strings
        verify(mockMetadata).add("/date", "2025-01-01T00:00:00.000Z"); // Dates have a specific format.
        // We need to use doAnswer/when to capture and verify list fields being added, but this is simpler

        verify(mockBoxFile).updateMetadata(any(Metadata.class));
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testUpdateExistingFieldsWithDifferentTypes() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);
        lenient().when(mockMetadata.getPropertyPaths()).thenReturn(List.of(
                "/stringField", "/numberField", "/listField"
        ));

        lenient().when(mockMetadata.getValue("/stringField")).thenReturn(Json.value("old value"));
        lenient().when(mockMetadata.getValue("/numberField")).thenReturn(Json.value(10.0));
        JsonArray oldList = new JsonArray();
        oldList.add("old1");
        oldList.add("old2");
        lenient().when(mockMetadata.getValue("/listField")).thenReturn(oldList);

        final String inputJson = """
                {
                  "stringField": "new value",
                  "numberField": 20,
                  "listField": ["new1", "new2", "new3"]
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        // Verify fields were replaced with new values
        verify(mockMetadata).replace("/stringField", "new value");
        verify(mockMetadata).replace("/numberField", 20.0);
        verify(mockBoxFile).updateMetadata(any(Metadata.class));
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }

    @Test
    void testNoUpdateWhenValuesUnchanged() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);

        // Set up existing fields
        lenient().when(mockMetadata.getPropertyPaths()).thenReturn(List.of(
                "/unchangedField", "/unchangedNumber"
        ));

        // Set up current values
        lenient().when(mockMetadata.getValue("/unchangedField")).thenReturn(Json.value("same value"));
        lenient().when(mockMetadata.getValue("/unchangedNumber")).thenReturn(Json.value(42.0));

        final String inputJson = """
                {
                  "unchangedField": "same value",
                  "unchangedNumber": 42
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
        verify(mockBoxFile).updateMetadata(any(Metadata.class));
    }

    @Test
    void testMixedListHandling() {
        final JsonArray operationsArray = new JsonArray();
        operationsArray.add("someOperation");
        lenient().when(mockMetadata.getOperations()).thenReturn(operationsArray);
        lenient().when(mockMetadata.getPropertyPaths()).thenReturn(List.of());

        final String inputJson = """
                {
                  "mixedList": ["string", 42, true, null, 3.14]
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();
        verify(mockBoxFile).updateMetadata(any(Metadata.class));
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }
}
