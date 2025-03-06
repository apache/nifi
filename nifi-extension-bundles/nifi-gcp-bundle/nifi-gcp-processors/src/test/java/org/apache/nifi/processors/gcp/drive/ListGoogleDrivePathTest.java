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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListGoogleDrivePathTest implements OutputChecker {
    private ListGoogleDrive testSubject;
    private TestRunner testRunner;

    private Drive mockDriverService;

    @BeforeEach
    void setUp() throws Exception {
        mockDriverService = mock(Drive.class, Mockito.RETURNS_DEEP_STUBS);

        testSubject = new ListGoogleDrive() {
            @Override
            protected List<GoogleDriveFileInfo> performListing(ProcessContext context, Long minTimestamp, ListingMode ignoredListingMode) throws IOException {
                return super.performListing(context, minTimestamp, ListingMode.EXECUTION);
            }

            @Override
            public Drive createDriveService(ProcessContext context, HttpTransport httpTransport, String... scopes) {
                return mockDriverService;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);

        String gcpCredentialsControllerServiceId = "gcp_credentials_provider_service";

        GCPCredentialsControllerService gcpCredentialsControllerService = mock(GCPCredentialsControllerService.class, RETURNS_DEEP_STUBS);
        when(gcpCredentialsControllerService.getIdentifier()).thenReturn(gcpCredentialsControllerServiceId);

        testRunner.addControllerService(gcpCredentialsControllerServiceId, gcpCredentialsControllerService);
        testRunner.enableControllerService(gcpCredentialsControllerService);
        testRunner.setProperty(GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE, gcpCredentialsControllerServiceId);
    }

    @Test
    void testPath() throws Exception {
        String folderId1 = "folder_id_1";
        String folderName1 = "Base Folder";

        String folderId2 = "folder_id_2";
        String folderName2 = "Folder/with/slashes";

        String fileId1 = "file_id_1";
        String fileName1 = "file_name_1";

        String fileId2 = "file_id_2";
        String fileName2 = "file_name_2";

        testRunner.setProperty(ListGoogleDrive.FOLDER_ID, folderId1);

        mockGetFolderName(folderId1, folderName1);

        mockFolderQuery(folderId1,
                createFile(folderId2, folderName2, GoogleDriveTrait.DRIVE_FOLDER_MIME_TYPE),
                createFile(fileId1, fileName1, "text/plain")
        );

        mockFolderQuery(folderId2,
                createFile(fileId2, fileName2, "text/plain")
        );

        Set<Map<String, String>> expectedAttributes = Set.of(
                createAttributeMap(fileId1, fileName1, urlEncode(folderName1)),
                createAttributeMap(fileId2, fileName2, urlEncode(folderName1) + "/" + urlEncode(folderName2))
        );

        testRunner.run();

        checkAttributes(ListGoogleDrive.REL_SUCCESS, expectedAttributes);
    }

    private void mockGetFolderName(String folderId, String folderName) throws  IOException {
        when(mockDriverService.files()
                .get(folderId)
                .setSupportsAllDrives(true)
                .setFields("name, driveId")
                .execute()
        ).thenReturn(new File()
                .setName(folderName)
        );
    }

    private void mockFolderQuery(String folderId, File... files) throws IOException {
        when(mockDriverService.files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ("('" + folderId + "' in parents) and (mimeType != 'application/vnd.google-apps.shortcut') and trashed = false")
                .setPageToken(null)
                .setFields("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType, owners, lastModifyingUser, webViewLink, webContentLink)")
                .execute()
                .getFiles()
        ).thenReturn(Arrays.asList(files));
    }

    private File createFile(
            String id,
            String name,
            String mimeType
    ) {
        return new File()
                .setId(id)
                .setName(name)
                .setMimeType(mimeType);
    }

    private Map<String, String> createAttributeMap(String fileId, String fileName, String path) {
        return Map.of(
                GoogleDriveAttributes.ID, fileId,
                GoogleDriveAttributes.FILENAME, fileName,
                GoogleDriveAttributes.PATH, path
        );
    }

    private String urlEncode(final String str) {
        return URLEncoder.encode(str, StandardCharsets.UTF_8);
    }

    @Override
    public TestRunner getTestRunner() {
        return testRunner;
    }

    @Override
    public Set<String> getCheckedAttributeNames() {
        return Set.of(ID, FILENAME, PATH);
    }
}
