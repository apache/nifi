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
import com.box.sdk.Metadata;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FetchBoxFileMetadataInstanceTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_KEY = "fileMetadata";
    private static final String TEMPLATE_SCOPE = "enterprise_123";
    private static final String TEMPLATE_ID = "12345";

    @Mock
    private BoxFile mockBoxFile;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFileMetadataInstance testSubject = new FetchBoxFileMetadataInstance() {
            @Override
            BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testSuccessfulMetadataRetrieval() {
        final JsonObject metadataJson = Json.object()
                .add("$id", TEMPLATE_ID)
                .add("$type", "fileMetadata-123")
                .add("$parent", "file_" + TEST_FILE_ID)
                .add("$template", TEMPLATE_KEY)
                .add("$scope", TEMPLATE_SCOPE)
                .add("fileName", "document.pdf")
                .add("fileExtension", "pdf");
        final Metadata metadata = new Metadata(metadataJson);

        when(mockBoxFile.getMetadata(TEMPLATE_KEY, TEMPLATE_SCOPE)).thenReturn(metadata);

        testRunner.setProperty(FetchBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_SCOPE, TEMPLATE_SCOPE);
        testRunner.enqueue(new byte[0]);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(FetchBoxFileMetadataInstance.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchBoxFileMetadataInstance.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("box.metadata.template.key", TEMPLATE_KEY);
        flowFile.assertAttributeEquals("box.metadata.template.scope", TEMPLATE_SCOPE);

        final String content = new String(flowFile.toByteArray());
        assertTrue(content.contains("\"$id\":\"" + TEMPLATE_ID + "\""));
        assertTrue(content.contains("\"$template\":\"" + TEMPLATE_KEY + "\""));
        assertTrue(content.contains("\"$scope\":\"" + TEMPLATE_SCOPE + "\""));
        assertTrue(content.contains("\"$parent\":\"file_" + TEST_FILE_ID + "\""));
        assertTrue(content.contains("\"fileName\":\"document.pdf\""));
        assertTrue(content.contains("\"fileExtension\":\"pdf\""));
    }

    @Test
    void testMetadataNotFound() {
        when(mockBoxFile.getMetadata(anyString(), anyString())).thenThrow(
                new BoxAPIResponseException("instance_not_found - Template not found", 404, "instance_not_found", null));

        testRunner.setProperty(FetchBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_SCOPE, TEMPLATE_SCOPE);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND).getFirst();
        flowFile.assertAttributeExists(BoxFileAttributes.ERROR_MESSAGE);
    }

    @Test
    void testFileNotFound() {
        final BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        doThrow(mockException).when(mockBoxFile).getMetadata(anyString(), anyString());

        testRunner.setProperty(FetchBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_SCOPE, TEMPLATE_SCOPE);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileMetadataInstance.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchBoxFileMetadataInstance.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testBoxApiException() {
        final BoxAPIException mockException = new BoxAPIException("General API Error", 500, "Unexpected Error");
        doThrow(mockException).when(mockBoxFile).getMetadata(anyString(), anyString());

        testRunner.setProperty(FetchBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_SCOPE, TEMPLATE_SCOPE);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileMetadataInstance.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchBoxFileMetadataInstance.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error\nUnexpected Error");
    }
}
