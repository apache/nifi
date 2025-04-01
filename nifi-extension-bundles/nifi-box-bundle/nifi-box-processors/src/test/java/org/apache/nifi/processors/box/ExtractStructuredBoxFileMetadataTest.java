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
import org.apache.nifi.processors.box.ExtractStructuredBoxFileMetadata.ExtractionMethod;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class ExtractStructuredBoxFileMetadataTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_KEY = "testTemplate";
    private static final String FIELDS_STRING = "field1, field2, field3";
    private static final String COMPLETION_REASON = "success";
    private static final Date CREATED_AT = new Date();

    @Mock
    private BoxAIExtractStructuredResponse mockAIResponse;

    private BiFunction<String, String, BoxAIExtractStructuredResponse> templateResponseSupplier;

    private BiFunction<String, String, BoxAIExtractStructuredResponse> fieldsResponseSupplier;

    @BeforeEach
    void setUp() throws Exception {
        // Set up default suppliers to return mockAIResponse
        templateResponseSupplier = (templateKey, fileId) -> mockAIResponse;
        fieldsResponseSupplier = (fieldsString, fileId) -> mockAIResponse;

        final ExtractStructuredBoxFileMetadata testSubject = new ExtractStructuredBoxFileMetadata() {
            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponseWithTemplate(final String templateKey,
                                                                                         final String fileId) {
                return templateResponseSupplier.apply(templateKey, fileId);
            }

            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponseWithFields(final String fieldsString,
                                                                                       final String fileId) {
                return fieldsResponseSupplier.apply(fieldsString, fileId);
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.TEMPLATE);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, TEMPLATE_KEY);
        lenient().when(mockAIResponse.getCompletionReason()).thenReturn(COMPLETION_REASON);
        lenient().when(mockAIResponse.getCreatedAt()).thenReturn(CREATED_AT);
        // Add a mock answer
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
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.template.key", TEMPLATE_KEY);
        flowFile.assertAttributeEquals("box.ai.extraction.method", ExtractionMethod.TEMPLATE);
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);

        assertProvenanceEvent(ProvenanceEventType.ATTRIBUTES_MODIFIED);
    }

    @Test
    void testSuccessfulMetadataExtractionWithFields() {
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS);
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FIELDS, FIELDS_STRING);

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.extraction.method", ExtractionMethod.FIELDS);
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
        flowFile.assertAttributeNotExists("box.ai.template.key");

        assertProvenanceEvent(ProvenanceEventType.ATTRIBUTES_MODIFIED);
    }

    @Test
    void testFileNotFoundWithTemplate() {
        // Configure template response supplier to throw a 404 exception
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new BoxAPIResponseException("Not Found", 404, "Not Found", null);
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Not Found [404]");
    }

    @Test
    void testFileNotFoundWithFields() {
        // Configure fields response supplier to throw a 404 exception
        fieldsResponseSupplier = (fieldsString, fileId) -> {
            throw new BoxAPIResponseException("Not Found", 404, "Not Found", null);
        };

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS);
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FIELDS, FIELDS_STRING);

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Not Found [404]");
    }

    @Test
    void testTemplateNotFound() {
        // Configure template response supplier to throw a 404 exception with template not found message
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new BoxAPIResponseException("API Error", 404, "Specified Metadata Template not found", null);
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_TEMPLATE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testOtherAPIError() {
        // Configure template response supplier to throw a 500 exception
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new BoxAPIResponseException("Server Error", 500, "Server Error", null);
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
    }

    @Test
    void testGenericException() {
        // Configure template response supplier to throw a runtime exception
        templateResponseSupplier = (templateKey, fileId) -> {
            throw new RuntimeException("Something went wrong");
        };

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Something went wrong");
    }

    @Test
    void testExpressionLanguageWithTemplate() {
        // Test with expression language in property values
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("file.id", TEST_FILE_ID);
        attributes.put("template.key", TEMPLATE_KEY);

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, "${file.id}");
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, "${template.key}");
        testRunner.enqueue("test data", attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.template.key", TEMPLATE_KEY);
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
    }

    @Test
    void testExpressionLanguageWithFields() {
        // Test with expression language in property values
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("file.id", TEST_FILE_ID);
        attributes.put("fields", FIELDS_STRING);

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.EXTRACTION_METHOD, ExtractionMethod.FIELDS);
        testRunner.removeProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, "${file.id}");
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FIELDS, "${fields}");
        testRunner.enqueue("test data", attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.extraction.method", ExtractionMethod.FIELDS);
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
    }
}
