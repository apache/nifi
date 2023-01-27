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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PutBoxFileTest extends AbstractBoxFileTest {

    @BeforeEach
    void setUp() throws Exception {
        final PutBoxFile testSubject = new PutBoxFile() {

            @Override
            BoxFolder getFolder(String folderId) {
                return mockBoxFolder;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testUploadFilenameFromFlowFileAttribute()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.FILE_NAME, "${filename}");

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        inputFlowFile.putAttributes(attributes);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        final BoxFile.Info mockUploadedFileInfo = createFileInfo(TEST_FOLDER_NAME, MODIFIED_TIME);
        when(mockBoxFolder.uploadFile(any(InputStream.class), eq(TEST_FILENAME))).thenReturn(mockUploadedFileInfo);
        when(mockBoxFolder.getInfo()).thenReturn(mockFolderInfo);
        when(mockFolderInfo.getID()).thenReturn(TEST_FOLDER_ID);
        when(mockFolderInfo.getName()).thenReturn(TEST_FOLDER_NAME);

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.get(0);
        assertOutFlowFileAttributes(ff0);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testUploadError()  {
        testRunner.setProperty(PutBoxFile.FOLDER_ID, TEST_FOLDER_ID);
        testRunner.setProperty(PutBoxFile.FILE_NAME, TEST_FILENAME);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));

        when(mockFolderInfo.getName()).thenReturn(TEST_FOLDER_NAME);
        when(mockBoxFolder.getInfo()).thenReturn(mockFolderInfo);
        when(mockBoxFolder.uploadFile(any(InputStream.class), eq(TEST_FILENAME))).thenThrow(new RuntimeException("Upload error"));

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutBoxFile.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutBoxFile.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Upload error");
        assertNoProvenanceEvent();
    }
}
