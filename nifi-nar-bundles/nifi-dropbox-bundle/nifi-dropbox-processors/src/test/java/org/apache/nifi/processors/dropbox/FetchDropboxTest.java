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
import static org.mockito.Mockito.when;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.FileMetadata;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FetchDropboxTest {

    public static final String FILE_ID_1 = "id:odTlUvbpIEAAAAAAAAAGGQ";
    public static final String FILE_ID_2 = "id:odTlUvbpIEBBBBBBBBBGGQ";
    public static final String FILENAME = "file_name";
    public static final String FOLDER = "/testFolder";
    public static final String SIZE = "125";
    public static final String CREATED_TIME = "1659707000";
    public static final String REVISION = "5e4ddb1320676a5c29261";

    private TestRunner testRunner;

    @Mock
    private DbxClientV2 mockDropboxClient;

    @Mock
    private DropboxCredentialService credentialService;

    @Mock
    private DbxUserFilesRequests mockDbxUserFilesRequest;


    @Mock
    private DbxDownloader<FileMetadata> mockDbxDownloader;

    @BeforeEach
    void setUp() throws Exception {
        FetchDropbox testSubject = new FetchDropbox() {
            @Override
            public DbxClientV2 getDropboxApiClient(ProcessContext context, ProxyConfiguration proxyConfiguration, String clientId) {
                return mockDropboxClient;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);

        when(mockDropboxClient.files()).thenReturn(mockDbxUserFilesRequest);

        mockStandardDropboxCredentialService();
    }

    @Test
    void testFileIsDownloadedById() throws Exception {

        testRunner.setProperty(FetchDropbox.FILE, "${dropbox.id}");

        when(mockDbxUserFilesRequest.download(FILE_ID_1)).thenReturn(mockDbxDownloader);
        when(mockDbxDownloader.getInputStream()).thenReturn(new ByteArrayInputStream("content".getBytes(UTF_8)));

        MockFlowFile inputFlowFile = getMockFlowFile(FILE_ID_1);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertContentEquals("content");
        assertOutFlowFileAttributes(ff0, FILE_ID_1);
    }

    @Test
    void testFileIsDownloadedByPath() throws Exception {
        testRunner.setProperty(FetchDropbox.FILE, "${path}/${filename}");

        when(mockDbxUserFilesRequest.download(FOLDER + "/" + FILENAME)).thenReturn(mockDbxDownloader);
        when(mockDbxDownloader.getInputStream()).thenReturn(new ByteArrayInputStream("contentByPath".getBytes(UTF_8)));

        MockFlowFile inputFlowFile = getMockFlowFile(FILE_ID_1);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertContentEquals("contentByPath");
        assertOutFlowFileAttributes(ff0, FILE_ID_1);
    }

    @Test
    void testFetchFails() throws Exception {
        testRunner.setProperty(FetchDropbox.FILE, "${dropbox.id}");

        when(mockDbxUserFilesRequest.download(FILE_ID_2)).thenThrow(new DbxException("Error in Dropbox"));

        MockFlowFile inputFlowFile = getMockFlowFile(FILE_ID_2);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchDropbox.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.get(0);
        ff0.assertAttributeEquals("error.message", "Error in Dropbox");
        assertOutFlowFileAttributes(ff0, FILE_ID_2);
    }

    private void mockStandardDropboxCredentialService() throws InitializationException {
        String credentialServiceId = "dropbox_credentials";
        when(credentialService.getIdentifier()).thenReturn(credentialServiceId);
        testRunner.addControllerService(credentialServiceId, credentialService);
        testRunner.enableControllerService(credentialService);
        testRunner.setProperty(FetchDropbox.CREDENTIAL_SERVICE, credentialServiceId);
    }

    private MockFlowFile getMockFlowFile(String fileId) {
        MockFlowFile inputFlowFile = new MockFlowFile(0);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(DropboxFileInfo.ID, fileId);
        attributes.put(DropboxFileInfo.REVISION, REVISION);
        attributes.put(DropboxFileInfo.FILENAME, FILENAME);
        attributes.put(DropboxFileInfo.PATH, FOLDER);
        attributes.put(DropboxFileInfo.SIZE, SIZE);
        attributes.put(DropboxFileInfo.TIMESTAMP, CREATED_TIME);
        inputFlowFile.putAttributes(attributes);
        return inputFlowFile;
    }

    private void assertOutFlowFileAttributes(MockFlowFile flowFile, String fileId) {
        flowFile.assertAttributeEquals(DropboxFileInfo.ID, fileId);
        flowFile.assertAttributeEquals(DropboxFileInfo.REVISION, REVISION);
        flowFile.assertAttributeEquals(DropboxFileInfo.PATH, FOLDER);
        flowFile.assertAttributeEquals(DropboxFileInfo.SIZE, SIZE);
        flowFile.assertAttributeEquals(DropboxFileInfo.TIMESTAMP, CREATED_TIME);
        flowFile.assertAttributeEquals(DropboxFileInfo.FILENAME, FILENAME);
    }
}
