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

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.User;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.CREATED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LAST_MODIFYING_USER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MODIFIED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.OWNER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_AVAILABLE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_CONTENT_LINK;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_VIEW_LINK;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListGoogleDriveTestRunnerTest implements OutputChecker {
    private ListGoogleDrive testSubject;
    private TestRunner testRunner;

    private Drive mockDriverService;

    private final String folderId = "folderId";
    private final String folderName = "folderName";

    private final String driveId = "drive_id";
    private final String driveName = "drive_name";

    @BeforeEach
    void setUp() throws Exception {
        mockDriverService = mock(Drive.class, Mockito.RETURNS_DEEP_STUBS);

        when(mockDriverService.files()
                .get(folderId)
                .setSupportsAllDrives(true)
                .setFields("name, driveId")
                .execute()
        ).thenReturn(new File()
                .setName(folderName)
                .setDriveId(driveId)
        );

        when(mockDriverService.drives()
                .get(driveId)
                .setFields("name")
                .execute()
                .getName()
        ).thenReturn(driveName);

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

        testRunner.setProperty(ListGoogleDrive.FOLDER_ID, folderId);
    }

    @Test
    void testOutputAsAttributesWhereTimestampIsCreatedTime() throws Exception {
        String id = "id_1";
        String filename = "file_name_1";
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = null;
        String mimeType = "mime_type_1";
        String owner = "user1";
        String lastModifyingUser = "user2";
        String webViewLink = "http://web.view";
        String webContentLink = "http://web.content";

        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink, createdTime);
    }

    @Test
    void testOutputAsAttributesWhereTimestampIsModifiedTime() throws Exception {
        String id = "id_1";
        String filename = "file_name_1";
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;
        String mimeType = "mime_type_1";
        String owner = "user1";
        String lastModifyingUser = "user2";
        String webViewLink = "http://web.view";
        String webContentLink = "http://web.content";

        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink, modifiedTime);
    }

    @Test
    void testOutputAsAttributesWhereSizeIsNotAvailable() throws Exception {
        String id = "id_1";
        String filename = "file_name_1";
        Long size = null;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;
        String mimeType = "mime_type_1";
        String owner = "user1";
        String lastModifyingUser = "user2";
        String webViewLink = "http://web.view";
        String webContentLink = "http://web.content";

        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink, modifiedTime);
    }

    @Test
    void testOutputAsAttributesWhereSharedDriveNameIsNotAvailable() throws Exception {
        when(mockDriverService.drives()
                .get(driveId)
                .setFields("name")
                .execute()
        ).thenThrow(new HttpResponseException.Builder(404, "Not Found", new HttpHeaders()).build());

        String id = "id_1";
        String filename = "file_name_1";
        Long size = null;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;
        String mimeType = "mime_type_1";
        String owner = "user1";
        String lastModifyingUser = "user2";
        String webViewLink = "http://web.view";
        String webContentLink = "http://web.content";

        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink, modifiedTime, folderId, folderName, driveId, null);
    }

    @Test
    void testOutputAsContent() throws Exception {
        String id = "id_1";
        String filename = "file_name_1";
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;
        String mimeType = "mime_type_1";
        String owner = "user1";
        String lastModifyingUser = "user2";
        String webViewLink = "http://web.view";
        String webContentLink = "http://web.content";

        addJsonRecordWriterFactory();

        mockFetchedGoogleDriveFileList(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink);

        List<String> expectedContents = singletonList(
                "[" +
                        "{" +
                        "\"drive.id\":\"" + id + "\"," +
                        "\"filename\":\"" + filename + "\"," +
                        "\"drive.size\":" + size + "," +
                        "\"drive.size.available\":" + (size != null) + "," +
                        "\"drive.timestamp\":" + modifiedTime + "," +
                        "\"drive.created.time\":\"" + Instant.ofEpochMilli(createdTime) + "\"," +
                        "\"drive.modified.time\":\"" + Instant.ofEpochMilli(modifiedTime) + "\"," +
                        "\"mime.type\":\"" + mimeType + "\"," +
                        "\"drive.path\":\"" + folderName + "\"," +
                        "\"drive.owner\":\"" + owner + "\"," +
                        "\"drive.last.modifying.user\":\"" + lastModifyingUser + "\"," +
                        "\"drive.web.view.link\":\"" + webViewLink + "\"," +
                        "\"drive.web.content.link\":\"" + webContentLink + "\"," +
                        "\"drive.parent.folder.id\":\"" + folderId + "\"," +
                        "\"drive.parent.folder.name\":\"" + folderName + "\"," +
                        "\"drive.listed.folder.id\":\"" + folderId + "\"," +
                        "\"drive.listed.folder.name\":\"" + folderName + "\"," +
                        "\"drive.shared.drive.id\":\"" + driveId + "\"," +
                        "\"drive.shared.drive.name\":\"" + driveName + "\"" +
                        "}" +
                        "]");

        testRunner.run();
        checkContent(ListGoogleDrive.REL_SUCCESS, expectedContents);
    }

    private void addJsonRecordWriterFactory() throws InitializationException {
        RecordSetWriterFactory recordSetWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record_writer", recordSetWriter);
        testRunner.enableControllerService(recordSetWriter);
        testRunner.setProperty(ListGoogleDrive.RECORD_WRITER, "record_writer");
    }

    private void mockFetchedGoogleDriveFileList(String id, String filename, Long size, Long createdTime, Long modifiedTime, String mimeType,
                                                String owner, String lastModifyingUser, String webViewLink, String webContentLink) throws IOException {
        when(mockDriverService.files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ("('" + folderId + "' in parents) and (mimeType != 'application/vnd.google-apps.shortcut') and trashed = false")
                .setPageToken(null)
                .setFields("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType, owners, lastModifyingUser, webViewLink, webContentLink)")
                .execute()
                .getFiles()
        ).thenReturn(singletonList(
                createFile(
                        id,
                        filename,
                        size,
                        Optional.ofNullable(createdTime).map(DateTime::new).orElse(null),
                        Optional.ofNullable(modifiedTime).map(DateTime::new).orElse(null),
                        mimeType,
                        owner,
                        lastModifyingUser,
                        webViewLink,
                        webContentLink
                )
        ));
    }

    private void testOutputAsAttributes(String id, String filename, Long size, Long createdTime, Long modifiedTime, String mimeType,
                                        String owner, String lastModifyingUser, String webViewLink, String webContentLink,
                                        Long expectedTimestamp) throws IOException {
        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink, expectedTimestamp,
                folderId, folderName, driveId, driveName);
    }

    private void testOutputAsAttributes(String id, String filename, Long size, Long createdTime, Long modifiedTime, String mimeType,
                                        String owner, String lastModifyingUser, String webViewLink, String webContentLink,
                                        Long expectedTimestamp, String folderId, String folderName, String driveId, String driveName) throws IOException {
        mockFetchedGoogleDriveFileList(id, filename, size, createdTime, modifiedTime, mimeType, owner, lastModifyingUser, webViewLink, webContentLink);

        Map<String, String> inputFlowFileAttributes = new HashMap<>() {
            @Override
            public String put(String key, String value) {
                if (value == null) {
                    // skip null values as a FlowFile attribute is not added in that case
                    return null;
                }
                return super.put(key, value);
            }
        };
        inputFlowFileAttributes.put(GoogleDriveAttributes.ID, id);
        inputFlowFileAttributes.put(GoogleDriveAttributes.FILENAME, filename);
        inputFlowFileAttributes.put(GoogleDriveAttributes.SIZE, valueOf(size != null ? size : 0L));
        inputFlowFileAttributes.put(GoogleDriveAttributes.SIZE_AVAILABLE, valueOf(size != null));
        inputFlowFileAttributes.put(GoogleDriveAttributes.TIMESTAMP, valueOf(expectedTimestamp));
        inputFlowFileAttributes.put(GoogleDriveAttributes.CREATED_TIME, Instant.ofEpochMilli(createdTime != null ? createdTime : 0L).toString());
        inputFlowFileAttributes.put(GoogleDriveAttributes.MODIFIED_TIME, Instant.ofEpochMilli(modifiedTime != null ? modifiedTime : 0L).toString());
        inputFlowFileAttributes.put(GoogleDriveAttributes.MIME_TYPE, mimeType);
        inputFlowFileAttributes.put(GoogleDriveAttributes.PATH, folderName);
        inputFlowFileAttributes.put(GoogleDriveAttributes.OWNER, owner);
        inputFlowFileAttributes.put(GoogleDriveAttributes.LAST_MODIFYING_USER, lastModifyingUser);
        inputFlowFileAttributes.put(GoogleDriveAttributes.WEB_VIEW_LINK, webViewLink);
        inputFlowFileAttributes.put(GoogleDriveAttributes.WEB_CONTENT_LINK, webContentLink);
        inputFlowFileAttributes.put(GoogleDriveAttributes.PARENT_FOLDER_ID, folderId);
        inputFlowFileAttributes.put(GoogleDriveAttributes.PARENT_FOLDER_NAME, folderName);
        inputFlowFileAttributes.put(GoogleDriveAttributes.LISTED_FOLDER_ID, folderId);
        inputFlowFileAttributes.put(GoogleDriveAttributes.LISTED_FOLDER_NAME, folderName);
        inputFlowFileAttributes.put(GoogleDriveAttributes.SHARED_DRIVE_ID, driveId);
        inputFlowFileAttributes.put(GoogleDriveAttributes.SHARED_DRIVE_NAME, driveName);
        Set<Map<String, String>> expectedAttributes = new HashSet<>(singletonList(inputFlowFileAttributes));

        testRunner.run();

        checkAttributes(ListGoogleDrive.REL_SUCCESS, expectedAttributes);
    }

    private File createFile(
            String id,
            String name,
            Long size,
            DateTime createdTime,
            DateTime modifiedTime,
            String mimeType,
            String owner,
            String lastModifyingUser,
            String webViewLink,
            String webContentLink
    ) {
        File file = new File();

        file
                .setId(id)
                .setName(name)
                .setMimeType(mimeType)
                .setCreatedTime(createdTime)
                .setModifiedTime(modifiedTime)
                .setSize(size)
                .setOwners(List.of(new User().setDisplayName(owner)))
                .setLastModifyingUser(new User().setDisplayName(lastModifyingUser))
                .setWebViewLink(webViewLink)
                .setWebContentLink(webContentLink);

        return file;
    }

    @Override
    public TestRunner getTestRunner() {
        return testRunner;
    }

    @Override
    public Set<String> getCheckedAttributeNames() {
        return Set.of(ID, FILENAME, SIZE, SIZE_AVAILABLE, TIMESTAMP, CREATED_TIME, MODIFIED_TIME, MIME_TYPE, PATH, OWNER, LAST_MODIFYING_USER, WEB_VIEW_LINK, WEB_CONTENT_LINK,
                PARENT_FOLDER_ID, PARENT_FOLDER_NAME, LISTED_FOLDER_ID, LISTED_FOLDER_NAME, SHARED_DRIVE_ID, SHARED_DRIVE_NAME);
    }
}
