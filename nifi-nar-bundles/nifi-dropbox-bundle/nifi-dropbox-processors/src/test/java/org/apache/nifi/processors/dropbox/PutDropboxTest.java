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

package org.apache.nifi.processors.dropbox;

import static com.dropbox.core.v2.files.UploadError.path;
import static com.dropbox.core.v2.files.WriteConflictError.FILE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ERROR_MESSAGE;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dropbox.core.DbxException;
import com.dropbox.core.LocalizedText;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.UploadErrorException;
import com.dropbox.core.v2.files.UploadSessionAppendV2Uploader;
import com.dropbox.core.v2.files.UploadSessionCursor;
import com.dropbox.core.v2.files.UploadSessionFinishUploader;
import com.dropbox.core.v2.files.UploadSessionStartResult;
import com.dropbox.core.v2.files.UploadSessionStartUploader;
import com.dropbox.core.v2.files.UploadUploader;
import com.dropbox.core.v2.files.UploadWriteFailed;
import com.dropbox.core.v2.files.WriteError;
import com.dropbox.core.v2.files.WriteMode;
import java.io.InputStream;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PutDropboxTest extends AbstractDropboxTest {

    public static final long CHUNKED_UPLOAD_SIZE_IN_BYTES = 8;
    public static final long CHUNKED_UPLOAD_THRESHOLD_IN_BYTES = 15;
    public static final String CONTENT = "1234567890";
    public static final String LARGE_CONTENT_30B = "123456789012345678901234567890";
    public static final String SESSION_ID = "sessionId";


    @Mock(answer = RETURNS_DEEP_STUBS)
    private DbxUserFilesRequests mockDbxUserFilesRequest;

    @Mock
    private UploadUploader mockUploadUploader;

    @Mock
    private UploadSessionStartUploader mockUploadSessionStartUploader;

    @Mock
    private UploadSessionStartResult mockUploadSessionStartResult;

    @Mock
    private UploadSessionAppendV2Uploader mockUploadSessionAppendV2Uploader;

    @Mock
    private UploadSessionFinishUploader mockUploadSessionFinishUploader;

    @BeforeEach
    protected void setUp() throws Exception {
        final PutDropbox testSubject = new PutDropbox() {
            @Override
            public DbxClientV2 getDropboxApiClient(ProcessContext context, String id) {
                return mockDropboxClient;
            }
        };
        testRunner = TestRunners.newTestRunner(testSubject);
        testRunner.setProperty(PutDropbox.FOLDER, TEST_FOLDER);
        super.setUp();
    }

    @Test
    void testFolderValidity() {
        testRunner.setProperty(PutDropbox.FOLDER, "/");
        testRunner.assertValid();
        testRunner.setProperty(PutDropbox.FOLDER, "/tempFolder");
        testRunner.assertValid();
    }

    @Test
    void testUploadChunkSizeValidity() {
        testRunner.setProperty(PutDropbox.CHUNKED_UPLOAD_SIZE, "");
        testRunner.assertNotValid();
        testRunner.setProperty(PutDropbox.CHUNKED_UPLOAD_SIZE, "40 MB");
        testRunner.assertValid();
        testRunner.setProperty(PutDropbox.CHUNKED_UPLOAD_SIZE, "152 MB");
        testRunner.assertNotValid();
        testRunner.setProperty(PutDropbox.CHUNKED_UPLOAD_SIZE, "1024");
        testRunner.assertNotValid();
    }

    @Test
    void testFileUploadFileNameFromProperty() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        mockFileUpload(TEST_FOLDER, FILENAME_1);
        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        assertOutFlowFileAttributes(ff0);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadFileNameFromFlowFileAttribute() throws Exception {
        mockFileUpload(TEST_FOLDER, FILENAME_2);

        final MockFlowFile mockFlowFile = getMockFlowFile(CONTENT);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", FILENAME_2);
        mockFlowFile.putAttributes(attributes);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadFileToRoot() throws Exception {
        testRunner.setProperty(PutDropbox.FOLDER, "/");
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);

        mockFileUpload("/", FILENAME_1);

        runWithFlowFile();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
        assertProvenanceEvent(ProvenanceEventType.SEND);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        assertOutFlowFileAttributes(ff0, "/");
    }

    @Test
    void testFileUploadWithReplaceConflictResolutionStrategy() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, REPLACE.getValue());

        mockFileUpload(TEST_FOLDER, FILENAME_1, WriteMode.OVERWRITE);

        runWithFlowFile();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    @Test
    void testFileUploadError() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);

        mockFileUploadError(new DbxException("Dropbox error"));

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutDropbox.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals(ERROR_MESSAGE, "Dropbox error");
        assertNoProvenanceEvent();
    }

    @Test
    void testFileUploadOtherExceptionIsNotIgnored() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, IGNORE.getValue());

        mockFileUploadError(getException(WriteError.INSUFFICIENT_SPACE));

        runWithFlowFile();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_FAILURE, 1);
        assertNoProvenanceEvent();
    }

    @Test
    void testFileUploadConflictIgnoredWithIgnoreResolutionStrategy() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        testRunner.setProperty(PutDropbox.CONFLICT_RESOLUTION, IGNORE.getValue());

        mockFileUploadError(getException(WriteError.conflict(FILE)));

        runWithFlowFile();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
        assertNoProvenanceEvent();
    }

    @Test
    void testFileUploadConflictNotIgnoredWithDefaultFailStrategy() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);

        mockFileUploadError(getException(WriteError.conflict(FILE)));

        runWithFlowFile();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_FAILURE, 1);
        assertNoProvenanceEvent();
    }

    @Test
    void testFileUploadLargeFile() throws Exception {
        MockFlowFile mockFlowFile = getMockFlowFile(LARGE_CONTENT_30B);

        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        testRunner.setProperty(PutDropbox.CHUNKED_UPLOAD_SIZE, CHUNKED_UPLOAD_SIZE_IN_BYTES + " B");
        testRunner.setProperty(PutDropbox.CHUNKED_UPLOAD_THRESHOLD, CHUNKED_UPLOAD_THRESHOLD_IN_BYTES + " B");

        when(mockDropboxClient.files())
                .thenReturn(mockDbxUserFilesRequest);

        //start session: 8 bytes uploaded
        when(mockDbxUserFilesRequest
                .uploadSessionStart())
                .thenReturn(mockUploadSessionStartUploader);

        when(mockUploadSessionStartUploader
                .uploadAndFinish(any(InputStream.class), eq(CHUNKED_UPLOAD_SIZE_IN_BYTES)))
                .thenReturn(mockUploadSessionStartResult);

        when(mockUploadSessionStartResult
                .getSessionId())
                .thenReturn(SESSION_ID);

        //append session: invoked twice, 2 * 8 bytes uploaded
        when(mockDbxUserFilesRequest
                .uploadSessionAppendV2(any(UploadSessionCursor.class)))
                .thenReturn(mockUploadSessionAppendV2Uploader);

        //finish session: 30 - 8 - 2 * 8 = 6 bytes uploaded
        when(mockDbxUserFilesRequest
                .uploadSessionFinish(any(UploadSessionCursor.class), any(CommitInfo.class)))
                .thenReturn(mockUploadSessionFinishUploader);

        when(mockUploadSessionFinishUploader
                .uploadAndFinish(any(InputStream.class), anyLong()))
                .thenReturn(createFileMetadata(FILE_ID_1, FILENAME_1, TEST_FOLDER, CREATED_TIME));

        testRunner.enqueue(mockFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);

        verify(mockUploadSessionAppendV2Uploader, times(2))
                .uploadAndFinish(any(InputStream.class), eq(CHUNKED_UPLOAD_SIZE_IN_BYTES));
        assertProvenanceEvent(ProvenanceEventType.SEND);
    }

    private void mockFileUpload(String folder, String filename) throws Exception {
        mockFileUpload(folder, filename, WriteMode.ADD);
    }

    private void mockFileUpload(String folder, String filename, WriteMode writeMode) throws Exception {
        when(mockDropboxClient.files())
                .thenReturn(mockDbxUserFilesRequest);

        when(mockDbxUserFilesRequest
                .uploadBuilder(getPath(folder, filename))
                .withMode(writeMode)
                .withStrictConflict(true)
                .start())
                .thenReturn(mockUploadUploader);

        when(mockUploadUploader
                .uploadAndFinish(any(InputStream.class)))
                .thenReturn(createFileMetadata(FILE_ID_1, filename, folder, CREATED_TIME));
    }

    private void mockFileUploadError(DbxException exception) throws Exception {
        when(mockDropboxClient.files())
                .thenReturn(mockDbxUserFilesRequest);

        when(mockDbxUserFilesRequest
                .uploadBuilder(getPath(TEST_FOLDER, FILENAME_1))
                .withMode(WriteMode.ADD)
                .withStrictConflict(true)
                .start())
                .thenReturn(mockUploadUploader);

        when(mockUploadUploader
                .uploadAndFinish(any(InputStream.class)))
                .thenThrow(exception);
    }

    private UploadErrorException getException(WriteError writeErrorReason) {
        return new UploadErrorException("route", "requestId", new LocalizedText("upload error", "en-us"),
                path(new UploadWriteFailed(writeErrorReason, "uploadSessionId")));
    }

    private MockFlowFile getMockFlowFile(String content) {
        MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(content.getBytes(UTF_8));
        return inputFlowFile;
    }

    private void runWithFlowFile() {
        MockFlowFile mockFlowFile = getMockFlowFile(CONTENT);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();
    }
}