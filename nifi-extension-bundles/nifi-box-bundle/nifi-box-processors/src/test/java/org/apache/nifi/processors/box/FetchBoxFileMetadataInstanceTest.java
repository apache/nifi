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
import com.box.sdkgen.managers.filemetadata.GetFileMetadataByIdScope;
import com.box.sdkgen.schemas.metadatafull.MetadataFull;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FetchBoxFileMetadataInstanceTest extends AbstractBoxFileTest implements FileListingTestTrait {

    private static final String TEMPLATE_KEY = "fileMetadata";
    private static final String TEMPLATE_SCOPE = "enterprise_123";
    private static final String TEMPLATE_ID = "12345";

    @Mock
    private FileMetadataManager mockFileMetadataManager;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final FetchBoxFileMetadataInstance testSubject = new FetchBoxFileMetadataInstance();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        lenient().when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);
    }

    @Test
    void testSuccessfulMetadataRetrieval() {
        Map<String, Object> extraData = new HashMap<>();
        extraData.put("fileName", "document.pdf");
        extraData.put("fileExtension", "pdf");

        MetadataFull metadata = mock(MetadataFull.class);
        when(metadata.getId()).thenReturn(TEMPLATE_ID);
        when(metadata.getType()).thenReturn("fileMetadata-123");
        when(metadata.getTypeVersion()).thenReturn(1L);
        when(metadata.getCanEdit()).thenReturn(true);
        when(metadata.getExtraData()).thenReturn(extraData);

        when(mockFileMetadataManager.getFileMetadataById(eq(TEST_FILE_ID), any(GetFileMetadataByIdScope.class), eq(TEMPLATE_KEY)))
                .thenReturn(metadata);

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
        assertTrue(content.contains("\"$id\":\"" + TEMPLATE_ID + "\"") || content.contains("\"$id\" : \"" + TEMPLATE_ID + "\""));
        assertTrue(content.contains("\"$template\":\"" + TEMPLATE_KEY + "\"") || content.contains("\"$template\" : \"" + TEMPLATE_KEY + "\""));
        assertTrue(content.contains("\"fileName\":\"document.pdf\"") || content.contains("\"fileName\" : \"document.pdf\""));
        assertTrue(content.contains("\"fileExtension\":\"pdf\"") || content.contains("\"fileExtension\" : \"pdf\""));
    }

    @Test
    void testMetadataNotFound() {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError exception = mock(BoxAPIError.class);
        when(exception.getMessage()).thenReturn("instance_not_found - Template not found");
        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockFileMetadataManager.getFileMetadataById(anyString(), any(GetFileMetadataByIdScope.class), anyString()))
                .thenThrow(exception);

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
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError exception = mock(BoxAPIError.class);
        when(exception.getMessage()).thenReturn("API Error [404]");
        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);

        doThrow(exception).when(mockFileMetadataManager)
                .getFileMetadataById(anyString(), any(GetFileMetadataByIdScope.class), anyString());

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
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(500);
        BoxAPIError exception = mock(BoxAPIError.class);
        when(exception.getMessage()).thenReturn("General API Error\nUnexpected Error");
        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);

        doThrow(exception).when(mockFileMetadataManager)
                .getFileMetadataById(anyString(), any(GetFileMetadataByIdScope.class), anyString());

        testRunner.setProperty(FetchBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_KEY);
        testRunner.setProperty(FetchBoxFileMetadataInstance.TEMPLATE_SCOPE, TEMPLATE_SCOPE);
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchBoxFileMetadataInstance.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchBoxFileMetadataInstance.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error\nUnexpected Error");
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
