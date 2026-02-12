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
import com.box.sdkgen.schemas.metadata.Metadata;
import com.box.sdkgen.schemas.metadatas.Metadatas;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListBoxFileMetadataTemplatesTest extends AbstractBoxFileTest implements FileListingTestTrait {

    private static final String TEMPLATE_1_NAME = "fileMetadata";
    private static final String TEMPLATE_1_SCOPE = "enterprise_123";
    private static final String TEMPLATE_2_NAME = "properties";
    private static final String TEMPLATE_2_SCOPE = "global";

    @Mock
    private FileMetadataManager mockFileMetadataManager;

    @Mock
    private Metadata mockMetadata1;

    @Mock
    private Metadata mockMetadata2;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final ListBoxFileMetadataTemplates testSubject = new ListBoxFileMetadataTemplates();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        lenient().when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);
    }

    @Test
    void testSuccessfulMetadataRetrieval() {
        final List<Metadata> metadataList = new ArrayList<>();
        metadataList.add(mockMetadata1);
        metadataList.add(mockMetadata2);

        // Template 1 setup (fileMetadata)
        when(mockMetadata1.getTemplate()).thenReturn(TEMPLATE_1_NAME);
        when(mockMetadata1.getScope()).thenReturn(TEMPLATE_1_SCOPE);
        when(mockMetadata1.getParent()).thenReturn("file_" + TEST_FILE_ID);
        when(mockMetadata1.getVersion()).thenReturn(1L);

        // Template 2 setup (properties)
        when(mockMetadata2.getTemplate()).thenReturn(TEMPLATE_2_NAME);
        when(mockMetadata2.getScope()).thenReturn(TEMPLATE_2_SCOPE);
        when(mockMetadata2.getParent()).thenReturn("file_" + TEST_FILE_ID);
        when(mockMetadata2.getVersion()).thenReturn(1L);

        Metadatas metadatas = mock(Metadatas.class);
        when(metadatas.getEntries()).thenReturn(metadataList);
        doReturn(metadatas).when(mockFileMetadataManager).getFileMetadata(anyString());

        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(ListBoxFileMetadataTemplates.FETCH_FULL_METADATA, "false");
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
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("\"$template\""));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("\"$scope\""));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("["));
        org.junit.jupiter.api.Assertions.assertTrue(content.contains("]"));
    }

    @Test
    void testNoMetadata() {
        Metadatas metadatas = mock(Metadatas.class);
        when(metadatas.getEntries()).thenReturn(new ArrayList<>());
        when(mockFileMetadataManager.getFileMetadata(anyString())).thenReturn(metadatas);

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
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("API Error [404]");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        doThrow(mockException).when(mockFileMetadataManager).getFileMetadata(anyString());

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
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(500);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("General API Error\nUnexpected Error");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        doThrow(mockException).when(mockFileMetadataManager).getFileMetadata(anyString());

        testRunner.setProperty(ListBoxFileMetadataTemplates.FILE_ID, TEST_FILE_ID);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ListBoxFileMetadataTemplates.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListBoxFileMetadataTemplates.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error\nUnexpected Error");
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
