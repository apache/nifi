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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.UploadSessionAppendV2Uploader;
import com.dropbox.core.v2.files.UploadSessionCursor;
import com.dropbox.core.v2.files.UploadSessionFinishUploader;
import com.dropbox.core.v2.files.UploadSessionStartResult;
import com.dropbox.core.v2.files.UploadSessionStartUploader;
import com.dropbox.core.v2.files.UploadUploader;
import com.dropbox.core.v2.files.WriteMode;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PutDropboxTest {

    public static final String TEST_FOLDER = "/testFolder";
    public static final String FILENAME_1 = "file_name_1";
    public static final String FILENAME_2 = "file_name_2";
    private static final String CONTENT = "1234567890";
    private static final String LARGE_CONTENT_30B = "123456789012345678901234567890";
    private static final String SESSION_ID = "sessionId";
    public static final long CHUNK_SIZE_IN_BYTES = 8;
    public static final long MAX_FILE_SIZE_IN_BYTES = 15;
    private TestRunner testRunner;

    @Mock
    private DbxClientV2 mockDropboxClient;

    @Mock
    private DropboxCredentialService credentialService;

    @Mock
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

    @Mock
    private FileMetadata mockFileMetadata;

    @BeforeEach
    void setUp() throws Exception {
        PutDropbox testSubject = new PutDropbox() {
            @Override
            public DbxClientV2 getDropboxApiClient(ProcessContext context, ProxyConfiguration proxyConfiguration, String clientId) {
                return mockDropboxClient;
            }

            @Override
            protected long getUploadFileSizeLimit() {
                return MAX_FILE_SIZE_IN_BYTES;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);

        mockStandardDropboxCredentialService();

        testRunner.setProperty(PutDropbox.FOLDER, TEST_FOLDER);
    }

    @Test
    void testFolderValidity() {
        testRunner.setProperty(PutDropbox.FOLDER, "id:odTlUvbpIEAAAAAAAAABmw");
        testRunner.assertValid();
        testRunner.setProperty(PutDropbox.FOLDER, "/");
        testRunner.assertValid();
        testRunner.setProperty(PutDropbox.FOLDER, "/tempFolder");
        testRunner.assertValid();
    }

    @Test
    void testUploadChunkSizeValidity() {
        testRunner.setProperty(PutDropbox.UPLOAD_CHUNK_SIZE, "");
        testRunner.assertNotValid();
        testRunner.setProperty(PutDropbox.UPLOAD_CHUNK_SIZE, "40 MB");
        testRunner.assertValid();
        testRunner.setProperty(PutDropbox.UPLOAD_CHUNK_SIZE, "152 MB");
        testRunner.assertNotValid();
        testRunner.setProperty(PutDropbox.UPLOAD_CHUNK_SIZE, "1024");
        testRunner.assertNotValid();
    }

    @Test
    void testFileUploadFileNameFromProperty() throws Exception {
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        mockFileUpload(TEST_FOLDER + "/" + FILENAME_1);

        MockFlowFile mockFlowFile = getMockFlowFile(CONTENT);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
    }

    @Test
    void testFileUploadFileNameFromFlowFileAttribute() throws Exception {
        mockFileUpload(TEST_FOLDER + "/" + FILENAME_2);

        MockFlowFile mockFlowFile = getMockFlowFile(CONTENT);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", FILENAME_2);
        mockFlowFile.putAttributes(attributes);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
    }

    @Test
    void testFileUploadFileToRoot() throws Exception {
        testRunner.setProperty(PutDropbox.FOLDER, "/");
        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);

        mockFileUpload("/" + FILENAME_1);

        MockFlowFile mockFlowFile = getMockFlowFile(CONTENT);
        testRunner.enqueue(mockFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);
    }

    @Test
    void testFileUploadLargeFile() throws Exception {
        MockFlowFile mockFlowFile = getMockFlowFile(LARGE_CONTENT_30B);

        testRunner.setProperty(PutDropbox.FILE_NAME, FILENAME_1);
        testRunner.setProperty(PutDropbox.UPLOAD_CHUNK_SIZE, CHUNK_SIZE_IN_BYTES + " B");

        when(mockDropboxClient.files())
                .thenReturn(mockDbxUserFilesRequest);

        //start session: 8 bytes uploaded
        when(mockDbxUserFilesRequest
                .uploadSessionStart())
                .thenReturn(mockUploadSessionStartUploader);

        when(mockUploadSessionStartUploader
                .uploadAndFinish(any(InputStream.class), eq(CHUNK_SIZE_IN_BYTES)))
                .thenReturn(mockUploadSessionStartResult);

        when(mockUploadSessionStartResult
                .getSessionId())
                .thenReturn(SESSION_ID);

        //append session: invoked twice, 2 * 8 bytes uploaded
        when(mockDbxUserFilesRequest
                .uploadSessionAppendV2(any(UploadSessionCursor.class)))
                .thenReturn(mockUploadSessionAppendV2Uploader);

        //finish session: 30 - 8 - 2 * 8 = 6 bytes uploaded
        CommitInfo commitInfo = CommitInfo.newBuilder(TEST_FOLDER + "/" + FILENAME_1)
                .withMode(WriteMode.ADD)
                .withClientModified(new Date(mockFlowFile.getEntryDate()))
                .build();

        when(mockDbxUserFilesRequest
                .uploadSessionFinish(any(UploadSessionCursor.class), eq(commitInfo)))
                .thenReturn(mockUploadSessionFinishUploader);

        when(mockUploadSessionFinishUploader
                .uploadAndFinish(any(InputStream.class), eq(6L)))
                .thenReturn(mockFileMetadata);

        testRunner.enqueue(mockFlowFile);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutDropbox.REL_SUCCESS, 1);

        verify(mockUploadSessionAppendV2Uploader, times(2))
                .uploadAndFinish(any(InputStream.class), eq(CHUNK_SIZE_IN_BYTES));
    }

    private void mockStandardDropboxCredentialService() throws Exception {
        String credentialServiceId = "dropbox_credentials";
        when(credentialService.getIdentifier()).thenReturn(credentialServiceId);
        testRunner.addControllerService(credentialServiceId, credentialService);
        testRunner.enableControllerService(credentialService);
        testRunner.setProperty(PutDropbox.CREDENTIAL_SERVICE, credentialServiceId);
    }

    private void mockFileUpload(String path) throws DbxException, IOException {
        when(mockDropboxClient.files())
                .thenReturn(mockDbxUserFilesRequest);

        when(mockDbxUserFilesRequest
                .upload(path))
                .thenReturn(mockUploadUploader);

        when(mockUploadUploader
                .uploadAndFinish(any(InputStream.class)))
                .thenReturn(mockFileMetadata);
    }

    private MockFlowFile getMockFlowFile(String content) {
        MockFlowFile inputFlowFile = new MockFlowFile(0);
        inputFlowFile.setData(content.getBytes(UTF_8));
        return inputFlowFile;
    }
}