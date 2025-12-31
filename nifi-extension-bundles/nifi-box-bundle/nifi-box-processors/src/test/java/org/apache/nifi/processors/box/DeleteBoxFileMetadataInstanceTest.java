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
import com.box.sdkgen.managers.filemetadata.FileMetadataManager;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteBoxFileMetadataInstanceTest extends AbstractBoxFileTest {

    private static final String TEMPLATE_KEY = "fileProperties";

    @Mock
    private FileMetadataManager mockFileMetadataManager;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final DeleteBoxFileMetadataInstance testSubject = new DeleteBoxFileMetadataInstance();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();

        when(mockBoxClient.getFileMetadata()).thenReturn(mockFileMetadataManager);

        testRunner.setProperty(DeleteBoxFileMetadataInstance.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(DeleteBoxFileMetadataInstance.TEMPLATE_KEY, TEMPLATE_KEY);
    }

    @Test
    void testSuccessfulMetadataDeletion() {
        doNothing().when(mockFileMetadataManager).deleteFileMetadataById(anyString(), any(), anyString());

        testRunner.enqueue("test content");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(DeleteBoxFileMetadataInstance.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(DeleteBoxFileMetadataInstance.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals("box.id", TEST_FILE_ID);
        flowFile.assertAttributeEquals("box.template.key", TEMPLATE_KEY);
    }

    @Test
    void testFileNotFound() {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("Box File Not Found");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);
        doThrow(mockException).when(mockFileMetadataManager).deleteFileMetadataById(anyString(), any(), anyString());

        testRunner.enqueue("test content");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(DeleteBoxFileMetadataInstance.REL_FILE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(DeleteBoxFileMetadataInstance.REL_FILE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testMetadataNotFound() {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("Specified metadata template not found");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);
        doThrow(mockException).when(mockFileMetadataManager).deleteFileMetadataById(anyString(), any(), anyString());

        testRunner.enqueue("test content");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(DeleteBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(DeleteBoxFileMetadataInstance.REL_TEMPLATE_NOT_FOUND).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testGeneralError() {
        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(500);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("Internal Server Error");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);
        doThrow(mockException).when(mockFileMetadataManager).deleteFileMetadataById(anyString(), any(), anyString());

        testRunner.enqueue("test content");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(DeleteBoxFileMetadataInstance.REL_FAILURE, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(DeleteBoxFileMetadataInstance.REL_FAILURE).getFirst();
        flowFile.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
    }
}
