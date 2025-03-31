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

import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class ExtractStructuredBoxFileMetadataTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_KEY = "testTemplate";
    private static final String COMPLETION_REASON = "success";
    private static final Date CREATED_AT = new Date();

    @Mock
    private BoxAIExtractStructuredResponse mockAIResponse;

    @BeforeEach
    void setUp() throws Exception {
        final ExtractStructuredBoxFileMetadata testSubject = new ExtractStructuredBoxFileMetadata() {
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponse(final String templateKey,
                                                                             final String fileId) {
                return mockAIResponse;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, TEMPLATE_KEY);
        lenient().when(mockAIResponse.getCompletionReason()).thenReturn(COMPLETION_REASON);
        lenient().when(mockAIResponse.getCreatedAt()).thenReturn(CREATED_AT);
    }

    @Test
    void testSuccessfulMetadataExtraction() {
        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);

        assertProvenanceEvent(ProvenanceEventType.ATTRIBUTES_MODIFIED);
    }

    @Test
    void testFileNotFound() throws Exception {
        final ExtractStructuredBoxFileMetadata testSubject = new ExtractStructuredBoxFileMetadata() {
            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponse(final String templateKey,
                                                                             final String fileId) {
                throw new BoxAPIResponseException("Not Found", 404, "Not Found", null);
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, TEMPLATE_KEY);

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testOtherAPIError() throws Exception {
        final ExtractStructuredBoxFileMetadata testSubject = new ExtractStructuredBoxFileMetadata() {
            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponse(final String templateKey,
                                                                             final String fileId) {
                throw new BoxAPIResponseException("Server Error", 500, "Server Error", null);
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, TEMPLATE_KEY);

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
    }

    @Test
    void testGenericException() throws Exception {
        final ExtractStructuredBoxFileMetadata testSubject = new ExtractStructuredBoxFileMetadata() {
            @Override
            BoxAIExtractStructuredResponse getBoxAIExtractStructuredResponse(final String templateKey,
                                                                             final String fileId) {
                throw new RuntimeException("Something went wrong");
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        testRunner.setProperty(ExtractStructuredBoxFileMetadata.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ExtractStructuredBoxFileMetadata.TEMPLATE_KEY, TEMPLATE_KEY);

        testRunner.enqueue("test data");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ExtractStructuredBoxFileMetadata.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ExtractStructuredBoxFileMetadata.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Something went wrong");
    }

    @Test
    void testExpressionLanguage() {
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
        flowFile.assertAttributeEquals("box.ai.completion.reason", COMPLETION_REASON);
        ;
    }
}
