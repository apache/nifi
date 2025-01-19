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

import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ERROR_MESSAGE;
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
public class FetchDropboxTest extends AbstractDropboxTest {

    @Mock
    private DbxUserFilesRequests mockDbxUserFilesRequest;

    @Mock
    private DbxDownloader<FileMetadata> mockDbxDownloader;

    @BeforeEach
    public void setUp() throws Exception {
        FetchDropbox testSubject = new FetchDropbox() {
            @Override
            public DbxClientV2 getDropboxApiClient(ProcessContext context, String id) {
                return mockDropboxClient;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);

        when(mockDropboxClient.files()).thenReturn(mockDbxUserFilesRequest);
        super.setUp();
    }

    @Test
    void testFileIsDownloadedById() throws Exception {
        testRunner.setProperty(FetchDropbox.FILE, "${dropbox.id}");

        when(mockDbxUserFilesRequest.download(FILE_ID_1)).thenReturn(mockDbxDownloader);
        when(mockDbxDownloader.getInputStream()).thenReturn(new ByteArrayInputStream("content".getBytes(UTF_8)));
        when(mockDbxDownloader.getResult()).thenReturn(createFileMetadata());

        MockFlowFile inputFlowFile = getMockFlowFile();
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertContentEquals("content");
        assertOutFlowFileAttributes(ff0);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFileIsDownloadedByPath() throws Exception {
        testRunner.setProperty(FetchDropbox.FILE, "${path}/${filename}");

        when(mockDbxUserFilesRequest.download(getPath(TEST_FOLDER, FILENAME_1))).thenReturn(mockDbxDownloader);
        when(mockDbxDownloader.getInputStream()).thenReturn(new ByteArrayInputStream("contentByPath".getBytes(UTF_8)));
        when(mockDbxDownloader.getResult()).thenReturn(createFileMetadata());

        MockFlowFile inputFlowFile = getMockFlowFile();
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchDropbox.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertContentEquals("contentByPath");
        assertOutFlowFileAttributes(ff0);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFetchFails() throws Exception {
        testRunner.setProperty(FetchDropbox.FILE, "${dropbox.id}");

        when(mockDbxUserFilesRequest.download(FILE_ID_1)).thenThrow(new DbxException("Error in Dropbox"));

        MockFlowFile inputFlowFile = getMockFlowFile();
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(FetchDropbox.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FetchDropbox.REL_FAILURE);
        MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(ERROR_MESSAGE, "Error in Dropbox");
        assertOutFlowFileAttributes(ff0);
        assertNoProvenanceEvent();
    }

    private MockFlowFile getMockFlowFile() {
        MockFlowFile inputFlowFile = new MockFlowFile(0);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(DropboxAttributes.ID, FILE_ID_1);
        attributes.put(DropboxAttributes.REVISION, REVISION);
        attributes.put(DropboxAttributes.FILENAME, FILENAME_1);
        attributes.put(DropboxAttributes.PATH, TEST_FOLDER);
        attributes.put(DropboxAttributes.SIZE, valueOf(SIZE));
        attributes.put(DropboxAttributes.TIMESTAMP, valueOf(CREATED_TIME));
        inputFlowFile.putAttributes(attributes);
        return inputFlowFile;
    }
}
