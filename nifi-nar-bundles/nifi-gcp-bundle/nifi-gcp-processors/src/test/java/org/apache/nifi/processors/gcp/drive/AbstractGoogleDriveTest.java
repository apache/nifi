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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import java.util.Collections;
import java.util.Set;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.Mockito;

public class AbstractGoogleDriveTest {
    public static final String CONTENT = "1234567890";
    public static final String TEST_FILENAME = "testFile";
    public static final String TEST_FILE_ID = "fileId";
    public static final String SUBFOLDER_NAME = "subFolderName";
    public static final String SHARED_FOLDER_ID = "sharedFolderId";
    public static final String SUBFOLDER_ID = "subFolderId";
    public static final long TEST_SIZE = 42;
    public static final long CREATED_TIME = 1659707000;
    public static final String TEXT_TYPE = "text/plain";

    protected TestRunner testRunner;

    @Mock(answer = RETURNS_DEEP_STUBS)
    protected Drive mockDriverService;


    @BeforeEach
    protected void setUp() throws Exception {
        String gcpCredentialsControllerServiceId = "gcp_credentials_provider_service";

        final GCPCredentialsControllerService gcpCredentialsControllerService = mock(GCPCredentialsControllerService.class, Mockito.RETURNS_DEEP_STUBS);
        when(gcpCredentialsControllerService.getIdentifier()).thenReturn(gcpCredentialsControllerServiceId);

        testRunner.addControllerService(gcpCredentialsControllerServiceId, gcpCredentialsControllerService);
        testRunner.enableControllerService(gcpCredentialsControllerService);
        testRunner.setProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE, gcpCredentialsControllerServiceId);
    }

    protected void assertFlowFileAttributes(Relationship relationship) {
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(relationship).get(0);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.ID, TEST_FILE_ID);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.FILENAME, TEST_FILENAME);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.TIMESTAMP, String.valueOf(new DateTime(CREATED_TIME)));
        flowFile.assertAttributeEquals(GoogleDriveAttributes.SIZE, Long.toString(TEST_SIZE));
        flowFile.assertAttributeEquals(GoogleDriveAttributes.MIME_TYPE, TEXT_TYPE);
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

    protected File createFile() {
        return createFile(TEST_FILE_ID, TEST_FILENAME, SUBFOLDER_ID, TEXT_TYPE);
    }

    protected File createFile(String id, String name, String parentId, String mimeType) {
        File file = new File();
        file.setId(id);
        file.setName(name);
        file.setParents(singletonList(parentId));
        file.setCreatedTime(new DateTime(CREATED_TIME));
        file.setSize(TEST_SIZE);
        file.setMimeType(mimeType);
        return file;
    }
}
