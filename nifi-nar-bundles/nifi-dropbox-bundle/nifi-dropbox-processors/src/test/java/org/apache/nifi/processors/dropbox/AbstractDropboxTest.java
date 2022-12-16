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

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

public class AbstractDropboxTest {
    public static final String TEST_FOLDER = "/testFolder";
    public static final String FILENAME_1 = "file_name_1";
    public static final String FILENAME_2 = "file_name_2";
    public static final String FILE_ID_1 = "id:odTlUvbpIEAAAAAAAAAGGQ";
    public static final String FILE_ID_2 = "id:bdCQUvbpIEAABBAAAAAGKK";
    public static final long CREATED_TIME = 1659707000;
    public static final long SIZE = 125;
    public static final String REVISION = "5e4ddb1320676a5c29261";

    protected TestRunner testRunner;

    @Mock
    protected DbxClientV2 mockDropboxClient;

    @Mock
    private DropboxCredentialService mockCredentialService;

    @BeforeEach
    protected void setUp() throws Exception {
        mockStandardDropboxCredentialService();
    }

    protected void assertProvenanceEvent(ProvenanceEventType eventType) {
        Set<ProvenanceEventType> expectedEventTypes = Collections.singleton(eventType);
        Set<ProvenanceEventType> actualEventTypes = testRunner.getProvenanceEvents().stream()
                .map(ProvenanceEventRecord::getEventType)
                .collect(toSet());
        assertEquals(expectedEventTypes, actualEventTypes);
    }

    protected void assertNoProvenanceEvent() {
        assertTrue(testRunner.getProvenanceEvents().isEmpty());
    }

    protected void mockStandardDropboxCredentialService() throws InitializationException {
        String credentialServiceId = "dropbox_credentials";
        when(mockCredentialService.getIdentifier()).thenReturn(credentialServiceId);
        testRunner.addControllerService(credentialServiceId, mockCredentialService);
        testRunner.enableControllerService(mockCredentialService);
        testRunner.setProperty(FetchDropbox.CREDENTIAL_SERVICE, credentialServiceId);
    }

    protected FileMetadata createFileMetadata() {
        return FileMetadata.newBuilder(FILENAME_1, FILE_ID_1,
                        new Date(CREATED_TIME),
                        new Date(CREATED_TIME),
                        REVISION, SIZE)
                .withPathDisplay(getPath(TEST_FOLDER, FILENAME_1))
                .withIsDownloadable(true)
                .build();
    }

    protected FileMetadata createFileMetadata(
            String id, String filename,
            String parent,
            long createdTime,
            boolean isDownloadable) {
        return FileMetadata.newBuilder(filename, id,
                        new Date(createdTime),
                        new Date(createdTime),
                        REVISION, SIZE)
                .withPathDisplay(getPath(parent, filename))
                .withIsDownloadable(isDownloadable)
                .build();
    }

    protected FileMetadata createFileMetadata(String id,
            String filename,
            String parent,
            long createdTime) {
        return createFileMetadata(id, filename, parent, createdTime, true);
    }

    protected void assertOutFlowFileAttributes(MockFlowFile flowFile) {
        assertOutFlowFileAttributes(flowFile, TEST_FOLDER);
    }

    protected void assertOutFlowFileAttributes(MockFlowFile flowFile, String folderName) {
        flowFile.assertAttributeEquals(DropboxAttributes.ID, FILE_ID_1);
        flowFile.assertAttributeEquals(DropboxAttributes.FILENAME, FILENAME_1);
        flowFile.assertAttributeEquals(DropboxAttributes.PATH, folderName);
        flowFile.assertAttributeEquals(DropboxAttributes.TIMESTAMP, Long.toString(CREATED_TIME));
        flowFile.assertAttributeEquals(DropboxAttributes.SIZE, Long.toString(SIZE));
        flowFile.assertAttributeEquals(DropboxAttributes.REVISION, REVISION);
    }

    protected String getPath(String folder, String filename) {
        return "/".equals(folder) ? folder + filename : folder + "/" + filename;
    }
}
