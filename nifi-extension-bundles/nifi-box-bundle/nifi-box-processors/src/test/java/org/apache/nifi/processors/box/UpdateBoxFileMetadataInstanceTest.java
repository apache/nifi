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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateBoxFileMetadataInstanceTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_NAME = "fileProperties";
    private static final String TEMPLATE_SCOPE = "enterprise";

    @Mock
    private BoxFile mockBoxFile;

    @Mock
    private Metadata mockMetadata;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final UpdateBoxFileMetadataInstance testSubject = new UpdateBoxFileMetadataInstance() {
            @Override
            BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        configureJsonRecordReader(testRunner);

        testRunner.setProperty(UpdateBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_NAME, TEMPLATE_NAME);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_SCOPE, TEMPLATE_SCOPE);
        testRunner.setProperty(UpdateBoxFileMetadataInstance.RECORD_READER, "json-reader");

        // Set up mocks
        lenient().when(mockMetadata.getScope()).thenReturn(TEMPLATE_SCOPE);
        lenient().when(mockMetadata.getTemplateName()).thenReturn(TEMPLATE_NAME);
        lenient().when(mockBoxFile.getMetadata(TEMPLATE_NAME, TEMPLATE_SCOPE)).thenReturn(mockMetadata);
    }

    private void configureJsonRecordReader(TestRunner runner) throws InitializationException {
        final JsonTreeReader readerService = new JsonTreeReader();
        runner.addControllerService("json-reader", readerService);
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

        ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockBoxFile).updateMetadata(metadataCaptor.capture());

        Metadata capturedMetadata = metadataCaptor.getValue();
        assertEquals(TEMPLATE_SCOPE, capturedMetadata.getScope());
        assertEquals(TEMPLATE_NAME, capturedMetadata.getTemplateName());

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UpdateBoxFileMetadataInstance.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.template.name", TEMPLATE_NAME);
        flowFile.assertAttributeEquals("box.template.scope", TEMPLATE_SCOPE);
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
    void testFileNotFound() {
        // Simulate 404 Not Found response when getMetadata is called
        BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        when(mockBoxFile.updateMetadata(any())).thenThrow(mockException);

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(UpdateBoxFileMetadataInstance.REL_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testNullValues() {
        // Test with null values for keys/values
        final String inputJson = """
                {
                  "audience": null,
                  "documentType": "Q1 plans",
                  "status": "active"
                }""";

        testRunner.enqueue(inputJson);
        testRunner.run();

        ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockBoxFile).updateMetadata(metadataCaptor.capture());
        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);

    }

    @Test
    void testExpressionLanguage() {
        // Test with expression language in property values
        Map<String, String> attributes = new HashMap<>();
        attributes.put("file.id", TEST_FILE_ID);
        attributes.put("template.key", TEMPLATE_NAME);
        attributes.put("template.scope", TEMPLATE_SCOPE);

        testRunner.setProperty(UpdateBoxFileMetadataInstance.FILE_ID, "${file.id}");
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_NAME, "${template.key}");
        testRunner.setProperty(UpdateBoxFileMetadataInstance.TEMPLATE_SCOPE, "${template.scope}");

        final String inputJson = """
                {
                  "audience": "internal",
                  "documentType": "Q1 plans"
                }""";

        testRunner.enqueue(inputJson, attributes);
        testRunner.run();

        ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
        verify(mockBoxFile).updateMetadata(metadataCaptor.capture());

        Metadata capturedMetadata = metadataCaptor.getValue();
        assertEquals(TEMPLATE_SCOPE, capturedMetadata.getScope());
        assertEquals(TEMPLATE_NAME, capturedMetadata.getTemplateName());

        testRunner.assertAllFlowFilesTransferred(UpdateBoxFileMetadataInstance.REL_SUCCESS, 1);
    }
}
