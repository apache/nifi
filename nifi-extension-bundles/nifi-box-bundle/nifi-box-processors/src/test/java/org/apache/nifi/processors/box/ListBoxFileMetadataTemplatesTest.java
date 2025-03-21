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
import com.eclipsesource.json.JsonValue;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListBoxFileMetadataTemplatesTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_1_ID = "12345";
    private static final String TEMPLATE_1_NAME = "fileMetadata";
    private static final String TEMPLATE_1_SCOPE = "enterprise_123";
    private static final String TEMPLATE_2_ID = "67890";
    private static final String TEMPLATE_2_NAME = "properties";
    private static final String TEMPLATE_2_SCOPE = "global";

    @Mock
    private BoxFile mockBoxFile;

    @Mock
    private Metadata mockMetadata1;

    @Mock
    private Metadata mockMetadata2;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final ListBoxFileMetadataTemplates testSubject = new ListBoxFileMetadataTemplates() {
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
        final List<Metadata> metadataList = new ArrayList<>();
        metadataList.add(mockMetadata1);
        metadataList.add(mockMetadata2);
        JsonValue mockJsonValue1 = mock(JsonValue.class);
        JsonValue mockJsonValue2 = mock(JsonValue.class);
        JsonValue mockJsonValue3 = mock(JsonValue.class);
        JsonValue mockJsonValue4 = mock(JsonValue.class);

        when(mockJsonValue1.asString()).thenReturn("document.pdf");
        when(mockJsonValue2.asString()).thenReturn("pdf");
        when(mockJsonValue3.asString()).thenReturn("Test Document");
        when(mockJsonValue4.asString()).thenReturn("John Doe");

        // Template 1 setup (fileMetadata)
        when(mockMetadata1.getID()).thenReturn(TEMPLATE_1_ID);
        when(mockMetadata1.getTemplateName()).thenReturn(TEMPLATE_1_NAME);
        when(mockMetadata1.getScope()).thenReturn(TEMPLATE_1_SCOPE);
        List<String> template1Fields = List.of("fileName", "fileExtension");
        when(mockMetadata1.getPropertyPaths()).thenReturn(template1Fields);
        when(mockMetadata1.getValue("fileName")).thenReturn(mockJsonValue1);
        when(mockMetadata1.getValue("fileExtension")).thenReturn(mockJsonValue2);

        // Template 2 setup (properties)
        when(mockMetadata2.getID()).thenReturn(TEMPLATE_2_ID);
        when(mockMetadata2.getTemplateName()).thenReturn(TEMPLATE_2_NAME);
        when(mockMetadata2.getScope()).thenReturn(TEMPLATE_2_SCOPE);

        List<String> template2Fields = List.of("Test Number", "Title", "Author", "Date");
        when(mockMetadata2.getPropertyPaths()).thenReturn(template2Fields);
        when(mockMetadata2.getValue("Test Number")).thenReturn(null); // Test null handling
        when(mockMetadata2.getValue("Title")).thenReturn(mockJsonValue3);
        when(mockMetadata2.getValue("Author")).thenReturn(mockJsonValue4);
        doReturn(metadataList).when(mockBoxFile).getAllMetadata();

        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("box.file.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("record.count", "2");
        flowFile.assertAttributeEquals("box.metadata.templates.names", "fileMetadata,properties");
        flowFile.assertAttributeEquals("box.metadata.templates.count", "2");

        String content = new String(flowFile.toByteArray());
        testRunner.getLogger().info("FlowFile content: {}", content);

        // Check that content contains key elements
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("\"$id\""));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("\"$template\""));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("\"$scope\""));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("["));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("]"));
    }

    @Test
    void testNoMetadata() {
        when(mockBoxFile.getAllMetadata()).thenReturn(new ArrayList<>());
        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_SUCCESS).getFirst();
        flowFile.assertAttributeEquals("box.file.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.metadata.templates.count", "0");
    }

    @Test
    void testFileNotFound() {
        BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        doThrow(mockException).when(mockBoxFile).getAllMetadata();

        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
    }

    @Test
    void testBoxApiException() {
        BoxAPIException mockException = new BoxAPIException("General API Error", 500, "Unexpected Error");
        doThrow(mockException).when(mockBoxFile).getAllMetadata();

        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error\nUnexpected Error");
    }

}
