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

import static java.lang.String.valueOf;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

public class ListGoogleDriveTestRunnerTest implements OutputChecker {
    private ListGoogleDrive testSubject;
    private TestRunner testRunner;

    private Drive mockDriverService;

    private final String folderId = "folderId";

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

        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, createdTime);
    }
    @Test
    void testOutputAsAttributesWhereTimestampIsModifiedTime() throws Exception {
        String id = "id_1";
        String filename = "file_name_1";
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;
        String mimeType = "mime_type_1";

        testOutputAsAttributes(id, filename, size, createdTime, modifiedTime, mimeType, modifiedTime);
    }

    @Test
    void testOutputAsContent() throws Exception {
        String id = "id_1";
        String filename = "file_name_1";
        Long size = 125L;
        Long createdTime = 123456L;
        Long modifiedTime = 123456L + 1L;
        String mimeType = "mime_type_1";

        addJsonRecordWriterFactory();

        mockFetchedGoogleDriveFileList(id, filename, size, createdTime, modifiedTime, mimeType);

        List<String> expectedContents = singletonList(
                "[" +
                        "{" +
                        "\"drive.id\":\"" + id + "\"," +
                        "\"filename\":\"" + filename + "\"," +
                        "\"drive.size\":" + size + "," +
                        "\"drive.timestamp\":" + modifiedTime + "," +
                        "\"mime.type\":\"" + mimeType + "\"" +
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

    private void mockFetchedGoogleDriveFileList(String id, String filename, Long size, Long createdTime, Long modifiedTime, String mimeType) throws IOException {
        when(mockDriverService.files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ("('" + folderId + "' in parents) and (mimeType != 'application/vnd.google-apps.folder') and (mimeType != 'application/vnd.google-apps.shortcut') and trashed = false")
                .setPageToken(null)
                .setFields("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType)")
                .execute()
                .getFiles()
        ).thenReturn(singletonList(
                createFile(
                        id,
                        filename,
                        size,
                        Optional.ofNullable(createdTime).map(DateTime::new).orElse(null),
                        Optional.ofNullable(modifiedTime).map(DateTime::new).orElse(null),
                        mimeType
                )
        ));
    }

    private void testOutputAsAttributes(String id, String filename, Long size, Long createdTime, Long modifiedTime, String mimeType, Long expectedTimestamp) throws IOException {
        mockFetchedGoogleDriveFileList(id, filename, size, createdTime, modifiedTime, mimeType);

        Map<String, String> inputFlowFileAttributes = new HashMap<>();
        inputFlowFileAttributes.put(GoogleDriveAttributes.ID, id);
        inputFlowFileAttributes.put(GoogleDriveAttributes.FILENAME, filename);
        inputFlowFileAttributes.put(GoogleDriveAttributes.SIZE, valueOf(size));
        inputFlowFileAttributes.put(GoogleDriveAttributes.TIMESTAMP, valueOf(expectedTimestamp));
        inputFlowFileAttributes.put(GoogleDriveAttributes.MIME_TYPE, mimeType);

        HashSet<Map<String, String>> expectedAttributes = new HashSet<>(singletonList(inputFlowFileAttributes));

        testRunner.run();

        checkAttributes(ListGoogleDrive.REL_SUCCESS, expectedAttributes);
    }

    private File createFile(
            String id,
            String name,
            Long size,
            DateTime createdTime,
            DateTime modifiedTime,
            String mimeType
    ) {
        File file = new File();

        file
                .setId(id)
                .setName(name)
                .setMimeType(mimeType)
                .setCreatedTime(createdTime)
                .setModifiedTime(modifiedTime)
                .setSize(size);

        return file;
    }

    @Override
    public TestRunner getTestRunner() {
        return testRunner;
    }
}
