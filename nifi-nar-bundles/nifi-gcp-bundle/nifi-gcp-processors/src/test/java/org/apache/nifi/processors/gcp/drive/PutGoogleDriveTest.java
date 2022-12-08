/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.drive;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveTrait.DRIVE_FOLDER_MIME_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.gson.JsonParseException;
import java.io.FileNotFoundException;
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
public class PutGoogleDriveTest extends AbstractGoogleDriveTest{

    @BeforeEach
    protected void setUp() throws Exception {
        final PutGoogleDrive testSubject = new PutGoogleDrive() {
            @Override
            public Drive createDriveService(ProcessContext context, HttpTransport httpTransport, String... scopes) {
                return mockDriverService;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testFileUploadFileNameFromProperty() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, FILENAME);
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, FOLDER_ID);
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, BASE_FOLDER_ID);

        mockFileUpload(createFile());
        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(PutGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadFileNameFromFlowFileAttribute() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, FOLDER_ID);
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, BASE_FOLDER_ID);

        mockFileUpload(createFile());

        final MockFlowFile mockFlowFile = getMockFlowFile();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", FILENAME);
        mockFlowFile.putAttributes(attributes);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(PutGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadFileToFolderSpecifiedByNameFolderExists() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FOLDER_NAME, FOLDER_NAME);
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, BASE_FOLDER_ID);
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, FILENAME);

        when(mockDriverService.files()
                .list()
                .setQ(format("mimeType='%s' and ('%s' in parents)", DRIVE_FOLDER_MIME_TYPE, BASE_FOLDER_ID))
                .setFields("files(name, id)")
                .execute())
                .thenReturn(new FileList().setFiles(singletonList(createFile(FOLDER_ID, FOLDER_NAME, BASE_FOLDER_ID, DRIVE_FOLDER_MIME_TYPE))));

        mockFileUpload(createFile());

        runWithFlowFile();
        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(PutGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadError() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, FOLDER_ID);
        testRunner.setProperty(PutGoogleDrive.BASE_FOLDER_ID, BASE_FOLDER_ID);
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, FILENAME);

        final JsonParseException exception = new JsonParseException("Google Drive error", new FileNotFoundException());
        mockFileUploadError(exception);

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeExists(ERROR_MESSAGE);
        assertNoProvenanceEvent();
    }

    private MockFlowFile getMockFlowFile() {
        MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(CONTENT.getBytes(UTF_8));
        return inputFlowFile;
    }

    private void runWithFlowFile() {
        MockFlowFile mockFlowFile = getMockFlowFile();
        testRunner.enqueue(mockFlowFile);
        testRunner.run();
    }

    private void mockFileUpload(File uploadedFile) throws IOException {
        when(mockDriverService.files().create(any(File.class), any(InputStreamContent.class))
                .setFields("id, name, createdTime, mimeType, size")
                .execute())
                .thenReturn(uploadedFile);
    }

    private void mockFileUploadError(Exception exception) throws IOException {
        when(mockDriverService.files()
                .create(any(File.class), any(InputStreamContent.class)))
                .thenThrow(exception);
    }
}