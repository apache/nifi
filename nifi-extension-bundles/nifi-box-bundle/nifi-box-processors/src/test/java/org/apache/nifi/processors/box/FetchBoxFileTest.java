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

import com.box.sdk.BoxFile;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class FetchBoxFileTest extends AbstractBoxFileTest {
    @Mock
    BoxFile mockBoxFile;

    @Override
    @BeforeEach
    void setUp() throws Exception {

        final FetchBoxFile testSubject = new FetchBoxFile() {
            @Override
            protected BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testBoxIdFromFlowFileAttribute()  {
        testRunner.setProperty(FetchBoxFile.FILE_ID, "${box.id}");
        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(BoxFileAttributes.ID, TEST_FILE_ID);
        inputFlowFile.putAttributes(attributes);

        final BoxFile.Info fetchedFileInfo = createFileInfo(TEST_FOLDER_NAME,  MODIFIED_TIME);
        doReturn(fetchedFileInfo).when(mockBoxFile).getInfo();


        testRunner.enqueue(inputFlowFile);
        testRunner.run();


        testRunner.assertAllFlowFilesTransferred(FetchBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0);
        verify(mockBoxFile).download(any(OutputStream.class));
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testBoxIdFromProperty()  {
        testRunner.setProperty(FetchBoxFile.FILE_ID, TEST_FILE_ID);

        final BoxFile.Info fetchedFileInfo = createFileInfo(TEST_FOLDER_NAME, MODIFIED_TIME);
        doReturn(fetchedFileInfo).when(mockBoxFile).getInfo();


        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();


        testRunner.assertAllFlowFilesTransferred(FetchBoxFile.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFile.REL_SUCCESS);
        final MockFlowFile ff0 = flowFiles.getFirst();
        assertOutFlowFileAttributes(ff0);
        verify(mockBoxFile).download(any(OutputStream.class));
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFileDownloadFailure() {
        testRunner.setProperty(FetchBoxFile.FILE_ID, TEST_FILE_ID);

        doThrow(new RuntimeException("Download failed")).when(mockBoxFile).download(any(OutputStream.class));


        MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();


        testRunner.assertAllFlowFilesTransferred(FetchBoxFile.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchBoxFile.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "Download failed");
        assertNoProvenanceEvent();
    }

    @Test
    void testMigration() {
        final Map<String, String> expected = Map.ofEntries(
                Map.entry(AbstractBoxProcessor.OLD_BOX_CLIENT_SERVICE_PROPERTY_NAME, AbstractBoxProcessor.BOX_CLIENT_SERVICE.getName()),
                Map.entry("box-file-id", FetchBoxFile.FILE_ID.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expected, propertyMigrationResult.getPropertiesRenamed());
    }
}
