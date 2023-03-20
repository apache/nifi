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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, SHARED_FOLDER_ID);
    }

    @Test
    void testUploadChunkSizeValidity() {
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "");
        testRunner.assertNotValid();
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "40 MB");
        testRunner.assertValid();
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "1024");
        testRunner.assertNotValid();
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "510 KB");
        testRunner.assertNotValid();
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "2 GB");
        testRunner.assertNotValid();

        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_THRESHOLD, "100 MB");
        testRunner.setProperty(PutGoogleDrive.CHUNKED_UPLOAD_SIZE, "110 MB");
        testRunner.assertNotValid();
    }

    @Test
    void testFileUploadFileNameFromProperty() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);

        mockFileExists(emptyList());
        mockFileUpload(createFile());
        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(PutGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadFileNameFromFlowFileAttribute() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FOLDER_ID, SHARED_FOLDER_ID);

        mockFileExists(emptyList());
        mockFileUpload(createFile());

        final MockFlowFile mockFlowFile = getMockFlowFile();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), TEST_FILENAME);
        mockFlowFile.putAttributes(attributes);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(PutGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadError() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);

        final JsonParseException exception = new JsonParseException("Google Drive error", new FileNotFoundException());
        mockFileExists(emptyList());
        mockFileUploadError(exception);

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeExists(ERROR_MESSAGE);
        assertNoProvenanceEvent();
    }

    @Test
    void testFileAlreadyExistsFailResolution() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);

        mockFileExists(singletonList(createFile()));

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_FAILURE, 1);
        assertNoProvenanceEvent();
    }

    @Test
    void testFileAlreadyExistsIgnoreResolution() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CONFLICT_RESOLUTION, IGNORE.getValue());

        mockFileExists(singletonList(createFile()));

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.ID, TEST_FILE_ID);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.FILENAME, TEST_FILENAME);
        assertNoProvenanceEvent();
    }

    @Test
    void testFileAlreadyExistsReplaceResolution() throws Exception {
        testRunner.setProperty(PutGoogleDrive.FILE_NAME, TEST_FILENAME);
        testRunner.setProperty(PutGoogleDrive.CONFLICT_RESOLUTION, REPLACE.getValue());

        mockFileExists(singletonList(createFile()));

        mockFileUpdate(createFile());

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(PutGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.SEND);
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
        when(mockDriverService.files()
                .create(any(File.class), any(InputStreamContent.class))
                .setSupportsAllDrives(true)
                .setFields("id, name, createdTime, mimeType, size")
                .execute())
                .thenReturn(uploadedFile);
    }

    private void mockFileUpdate(File uploadedFile) throws IOException {
        when(mockDriverService.files()
                .update(eq(uploadedFile.getId()), any(File.class), any(InputStreamContent.class))
                .setSupportsAllDrives(true)
                .setFields("id, name, createdTime, mimeType, size")
                .execute())
                .thenReturn(uploadedFile);
    }

    private void mockFileUploadError(Exception exception) throws IOException {
        when(mockDriverService.files()
                .create(any(File.class), any(InputStreamContent.class))
                .setSupportsAllDrives(true))
                .thenThrow(exception);
    }

    private void mockFileExists(List<File> fileList) throws IOException {
        when(mockDriverService.files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ(format("name='%s' and ('%s' in parents)", TEST_FILENAME, SHARED_FOLDER_ID))
                .setFields("files(name, id)")
                .execute())
                .thenReturn(new FileList().setFiles(fileList));
    }
}