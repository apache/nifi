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

import com.box.sdk.BoxAIExtractStructuredResponse;
import com.box.sdk.BoxAPIResponseException;
import com.eclipsesource.json.JsonObject;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.box.ExtractStructuredBoxFileMetadata.ExtractionMethod;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ExtractStructuredBoxFileMetadataTest extends AbstractBoxFileTest {

    // Simple mock RecordReaderFactory for testing
    private static class MockJsonRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {
        @Override
        public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long l, ComponentLog componentLog) {
            return mock(RecordReader.class);
        }

        @Override
        public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog componentLog) {
            return mock(RecordReader.class);
        }
    }

    private static final String TEMPLATE_KEY = "testTemplate";
    private static final String FIELDS_JSON = """
            [
              {
                "key": "name",
                "description": "The name of the person.",
                "displayName": "Name",
                "prompt": "The name is the first and last name from the email address.",
                "type": "string",
                "options": [
                  { "key": "First Name" },
                  { "key": "Last Name" }
                ]
              }
            ]
            """;
    private static final String COMPLETION_REASON = "success";
    private static final Date CREATED_AT = new Date();

    @Mock
    private BoxAIExtractStructuredResponse mockAIResponse;

    // Suppliers to simulate responses from the Box API calls.
    private BiFunction<String, String, BoxAIExtractStructuredResponse> templateResponseSupplier;
    private Function<InputStream, BoxAIExtractStructuredResponse> fieldsInputStreamResponseSupplier;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        // Default suppliers simply return the mock response.
        templateResponseSupplier = (templateKey, fileId) -> mockAIResponse;
        fieldsInputStreamResponseSupplier = (inputStream) -> mockAIResponse;

        // Override the processor methods to use our suppliers.
        final ExtractStructuredBoxFileMetadata testSubject = new ExtractStructuredBoxFileMetadata() {
            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponseWithTemplate(final String templateKey,
                                                                                         final String fileId) {
                return templateResponseSupplier.apply(templateKey, fileId);
            }

            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponseWithFields(final RecordReader recordReader,
                                                                                       final String fileId) {
                // For testing, simply use the supplier.
                return fieldsInputStreamResponseSupplier.apply(null);
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.TEMPLATE.getValue());
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, TEMPLATE_KEY);

        // Add and enable a mock RecordReader service for FIELDS extraction.
        final MockJsonRecordReaderFactory mockReaderFactory = new MockJsonRecordReaderFactory();
        testRunner.addControllerService("mockReader", mockReaderFactory);
        testRunner.enableControllerService(mockReaderFactory);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.RECORD_READER, "mockReader");

        lenient().when(mockAIResponse.getCompletionReason()).thenReturn(COMPLETION_REASON);
        lenient().when(mockAIResponse.getCreatedAt()).thenReturn(CREATED_AT);
        // Prepare a sample JSON answer.
        JsonObject jsonAnswer = new JsonObject();
        jsonAnswer.add("title", "Sample Document");
        jsonAnswer.add("author", "John Doe");
        lenient().when(mockAIResponse.getAnswer()).thenReturn(jsonAnswer);
    }

    @Test
    void testSuccessfulMetadataExtractionWithTemplate() {
        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).get(0);

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.template.key", TEMPLATE_KEY);
        flowFile.assertAttributeEquals("box.ai.extraction.method", ExtractionMethod.TEMPLATE.name());
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);

        assertProvenanceEvent(ProvenanceEventType.ATTRIBUTES_MODIFIED);
    }

    @Test
    void testSuccessfulMetadataExtractionWithFields() {
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS.getValue());
        // Remove the template key property when using FIELDS.
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);

        testRunner.enqueue(FIELDS_JSON);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).get(0);

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.extraction.method", ExtractionMethod.FIELDS.name());
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
        flowFile.assertAttributeNotExists("box.ai.template.key");

        assertProvenanceEvent(ProvenanceEventType.ATTRIBUTES_MODIFIED);
    }

    @Test
    void testFileNotFoundWithTemplate() {
        // Simulate a 404 error when processing a template.
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new BoxAPIResponseException("Not Found", 404, "Not Found", null);
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Not Found [404]");
    }

    @Test
    void testFileNotFoundWithFields() {
        // Simulate a 404 error when processing fields.
        fieldsInputStreamResponseSupplier = (inputStream) -> {
            throw new BoxAPIResponseException("Not Found", 404, "Not Found", null);
        };

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS.getValue());
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);

        testRunner.enqueue(FIELDS_JSON);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Not Found [404]");
    }

    @Test
    void testTemplateNotFound() {
        // Simulate a 404 error that indicates the template was not found.
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new BoxAPIResponseException("API Error", 404, "Specified Metadata Template not found", null);
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_TEMPLATE_NOT_FOUND).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testOtherAPIError() {
        // Simulate a non-404 error.
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new BoxAPIResponseException("Server Error", 500, "Server Error", null);
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Server Error [500]");
    }

    @Test
    void testGenericException() {
        // Simulate a generic runtime exception.
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new RuntimeException("Something went wrong");
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Something went wrong");
    }

    @Test
    void testExpressionLanguageWithTemplate() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("file.id", TEST_FILE_ID);
        attributes.put("template.key", TEMPLATE_KEY);

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, "${file.id}");
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, "${template.key}");
        testRunner.enqueue("test data", attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.template.key", TEMPLATE_KEY);
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
    }

    @Test
    void testExpressionLanguageWithFields() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("file.id", TEST_FILE_ID);

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS.getValue());
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, "${file.id}");
        testRunner.enqueue(FIELDS_JSON, attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.extraction.method", ExtractionMethod.FIELDS.name());
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
    }

    @Test
    void testMalformedJsonFields() {
        fieldsInputStreamResponseSupplier = (inputStream) -> {
            throw new RuntimeException("Error parsing JSON fields");
        };

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS.getValue());
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);

        testRunner.enqueue("{bad json}");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).get(0);
        flowFile.assertAttributeExists(BoxFileAttributes.ERROR_MESSAGE);
    }
}
