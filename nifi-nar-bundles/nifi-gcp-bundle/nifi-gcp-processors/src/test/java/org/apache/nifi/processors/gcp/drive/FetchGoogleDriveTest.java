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
package org.apache.nifi.processors.gcp.drive;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpTransport;
import com.google.api.services.drive.Drive;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FetchGoogleDriveTest extends AbstractGoogleDriveTest {

    @BeforeEach
    protected void setUp() throws Exception {
        final FetchGoogleDrive testSubject = new FetchGoogleDrive() {
            @Override
            public Drive createDriveService(ProcessContext context, HttpTransport httpTransport, String... scopes) {
                return mockDriverService;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testFileFetchFileNameFromProperty() throws IOException {
        testRunner.setProperty(FetchGoogleDrive.FILE_ID, TEST_FILE_ID);

        mockFileDownload(TEST_FILE_ID);
        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(FetchGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFetchFileNameFromFlowFileAttribute() throws Exception {
        final MockFlowFile mockFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(GoogleDriveAttributes.ID, TEST_FILE_ID);
        mockFlowFile.putAttributes(attributes);

        mockFileDownload(TEST_FILE_ID);

        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(FetchGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFileFetchError() throws Exception {
        testRunner.setProperty(FetchGoogleDrive.FILE_ID, TEST_FILE_ID);

        mockFileDownloadError(TEST_FILE_ID, new RuntimeException("Error during download"));

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(GoogleDriveAttributes.ERROR_MESSAGE, "Error during download");
        assertNoProvenanceEvent();
    }

    private void mockFileDownload(String fileId) throws IOException {
        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .executeMediaAsInputStream()).thenReturn(new ByteArrayInputStream(CONTENT.getBytes(UTF_8)));

        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .setFields("id, name, createdTime, mimeType, size")
                .execute()).thenReturn(createFile());
    }

    private void mockFileDownloadError(String fileId, Exception exception) throws IOException {
        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .executeMediaAsInputStream())
                .thenThrow(exception);
    }

    private void runWithFlowFile() {
        testRunner.enqueue(new MockFlowFile(0));
        testRunner.run();
    }
}
