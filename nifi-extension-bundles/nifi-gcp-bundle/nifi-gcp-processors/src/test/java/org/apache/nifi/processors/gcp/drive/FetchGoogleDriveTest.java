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

import com.google.api.client.http.HttpTransport;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.User;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FetchGoogleDriveTest extends AbstractGoogleDriveTest {

    private static final String TEST_OWNER = "user1";
    private static final String TEST_LAST_MODIFYING_USER = "user2";
    private static final String TEST_WEB_VIEW_LINK = "http://web.view";
    private static final String TEST_WEB_CONTENT_LINK = "http://web.content";
    private static final String TEST_PARENT_FOLDER_ID = "folder-id";
    private static final String TEST_PARENT_FOLDER_NAME = "folder-name";
    private static final String TEST_SHARED_DRIVE_ID = "drive-id";
    private static final String TEST_SHARED_DRIVE_NAME = "drive-name";

    @Override
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
    void testFileFetchFileIdFromProperty() throws IOException {
        testRunner.setProperty(FetchGoogleDrive.FILE_ID, TEST_FILE_ID);

        mockGetFileMetaDataExtended(TEST_FILE_ID);
        mockFileDownloadSuccess(TEST_FILE_ID);

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(FetchGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @Test
    void testFetchFileIdFromFlowFileAttribute() throws Exception {
        final MockFlowFile mockFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(GoogleDriveAttributes.ID, TEST_FILE_ID);
        mockFlowFile.putAttributes(attributes);

        mockGetFileMetaDataExtended(TEST_FILE_ID);
        mockFileDownloadSuccess(TEST_FILE_ID);

        runWithFlowFile(mockFlowFile);

        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_SUCCESS, 1);
        assertFlowFileAttributes(FetchGoogleDrive.REL_SUCCESS);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {TEST_SHARED_DRIVE_ID})
    void testFileFetchByListResult(String driveId) throws IOException {
        final MockFlowFile mockFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(GoogleDriveAttributes.ID, TEST_FILE_ID);
        attributes.put(GoogleDriveAttributes.LISTED_FOLDER_ID, TEST_PARENT_FOLDER_ID);
        attributes.put(GoogleDriveAttributes.OWNER, TEST_OWNER);
        attributes.put(GoogleDriveAttributes.LAST_MODIFYING_USER, TEST_LAST_MODIFYING_USER);
        attributes.put(GoogleDriveAttributes.WEB_VIEW_LINK, TEST_WEB_VIEW_LINK);
        attributes.put(GoogleDriveAttributes.WEB_CONTENT_LINK, TEST_WEB_CONTENT_LINK);
        attributes.put(GoogleDriveAttributes.PARENT_FOLDER_ID, TEST_PARENT_FOLDER_ID);
        attributes.put(GoogleDriveAttributes.PARENT_FOLDER_NAME, TEST_PARENT_FOLDER_NAME);
        attributes.put(GoogleDriveAttributes.SHARED_DRIVE_ID, driveId);
        attributes.put(GoogleDriveAttributes.SHARED_DRIVE_NAME, driveId != null ? TEST_SHARED_DRIVE_NAME : null);
        mockFlowFile.putAttributes(attributes);

        mockGetFileMetaDataBasic(TEST_FILE_ID);
        mockFileDownloadSuccess(TEST_FILE_ID);

        runWithFlowFile(mockFlowFile);

        assertFlowFile(driveId);
        assertProvenanceEvent(ProvenanceEventType.FETCH);

        verify(mockDriverService, never()).drives();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {TEST_SHARED_DRIVE_ID})
    void testFileFetchByIdOnly(String driveId) throws IOException {
        testRunner.setProperty(FetchGoogleDrive.FILE_ID, TEST_FILE_ID);

        mockGetFileMetaDataExtended(TEST_FILE_ID, driveId);
        mockFileDownloadSuccess(TEST_FILE_ID);

        runWithFlowFile();

        assertFlowFile(driveId);
        assertProvenanceEvent(ProvenanceEventType.FETCH);
    }

    private void assertFlowFile(String driveId) {
        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_SUCCESS, 1);

        assertFlowFileAttributes(FetchGoogleDrive.REL_SUCCESS);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchGoogleDrive.REL_SUCCESS).getFirst();

        flowFile.assertAttributeEquals(GoogleDriveAttributes.OWNER, TEST_OWNER);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.LAST_MODIFYING_USER, TEST_LAST_MODIFYING_USER);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.WEB_VIEW_LINK, TEST_WEB_VIEW_LINK);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.WEB_CONTENT_LINK, TEST_WEB_CONTENT_LINK);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.PARENT_FOLDER_ID, TEST_PARENT_FOLDER_ID);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.PARENT_FOLDER_NAME, TEST_PARENT_FOLDER_NAME);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.SHARED_DRIVE_ID, driveId);
        flowFile.assertAttributeEquals(GoogleDriveAttributes.SHARED_DRIVE_NAME, driveId != null ? TEST_SHARED_DRIVE_NAME : null);
    }

    @Test
    void testFileFetchError() throws Exception {
        testRunner.setProperty(FetchGoogleDrive.FILE_ID, TEST_FILE_ID);

        mockGetFileMetaDataExtended(TEST_FILE_ID);
        mockFileDownloadError(TEST_FILE_ID, new RuntimeException("Error during download"));

        runWithFlowFile();

        testRunner.assertAllFlowFilesTransferred(FetchGoogleDrive.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutGoogleDrive.REL_FAILURE);
        final MockFlowFile ff0 = flowFiles.getFirst();
        ff0.assertAttributeEquals(GoogleDriveAttributes.ERROR_MESSAGE, "Error during download");
        assertNoProvenanceEvent();
    }

    @Test
    void testMigrateProperties() {
        final TestRunner testRunner = TestRunners.newTestRunner(FetchGoogleDrive.class);
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("drive-file-id", FetchGoogleDrive.FILE_ID.getName()),
                Map.entry(GoogleDriveTrait.OLD_CONNECT_TIMEOUT_PROPERTY_NAME, GoogleDriveTrait.CONNECT_TIMEOUT.getName()),
                Map.entry(GoogleDriveTrait.OLD_READ_TIMEOUT_PROPERTY_NAME, GoogleDriveTrait.READ_TIMEOUT.getName()),
                Map.entry(GoogleUtils.OLD_GCP_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE.getName()),
                Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE)
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    private void mockFileDownloadSuccess(String fileId) throws IOException {
        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .executeMediaAsInputStream())
                .thenReturn(new ByteArrayInputStream(CONTENT.getBytes(UTF_8)));
    }

    private void mockFileDownloadError(String fileId, Exception exception) throws IOException {
        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .executeMediaAsInputStream())
                .thenThrow(exception);
    }

    private void mockGetFileMetaDataBasic(String fileId) throws IOException {
        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .setFields(FetchGoogleDrive.FILE_METADATA_FIELDS_BASIC)
                .execute())
                .thenReturn(createFile());
    }

    private void mockGetFileMetaDataExtended(String fileId) throws IOException {
        mockGetFileMetaDataExtended(fileId, null);
    }

    private void mockGetFileMetaDataExtended(String fileId, String driveId) throws IOException {
        when(mockDriverService.files()
                .get(fileId)
                .setSupportsAllDrives(true)
                .setFields(FetchGoogleDrive.FILE_METADATA_FIELDS_EXTENDED)
                .execute())
                .thenReturn(createFile()
                        .setOwners(List.of(new User().setDisplayName(TEST_OWNER)))
                        .setLastModifyingUser(new User().setDisplayName(TEST_LAST_MODIFYING_USER))
                        .setWebViewLink(TEST_WEB_VIEW_LINK)
                        .setWebContentLink(TEST_WEB_CONTENT_LINK)
                        .setParents(List.of(TEST_PARENT_FOLDER_ID)));

        when(mockDriverService.files()
                .get(TEST_PARENT_FOLDER_ID)
                .setSupportsAllDrives(true)
                .setFields("name, driveId")
                .execute()
        ).thenReturn(new File()
                .setName(TEST_PARENT_FOLDER_NAME)
                .setDriveId(driveId)
        );

        if (driveId != null) {
            when(mockDriverService.drives()
                    .get(driveId)
                    .setFields("name")
                    .execute()
                    .getName()
            ).thenReturn(TEST_SHARED_DRIVE_NAME);
        }
    }

    private void runWithFlowFile() {
        runWithFlowFile(new MockFlowFile(0));
    }

    private void runWithFlowFile(FlowFile flowFile) {
        testRunner.enqueue(flowFile);
        testRunner.run();
    }
}
