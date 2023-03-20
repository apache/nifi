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
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.EqualsWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.nifi.util.EqualsWrapper.wrapList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ListGoogleDriveSimpleTest {
    private ListGoogleDrive testSubject;
    private ProcessContext mockProcessContext;
    private Drive mockDriverService;

    private String listingModeAsString = "EXECUTION";

    @BeforeEach
    void setUp() throws Exception {
        mockProcessContext = mock(ProcessContext.class, RETURNS_DEEP_STUBS);
        mockDriverService = mock(Drive.class, RETURNS_DEEP_STUBS);

        testSubject = new ListGoogleDrive() {
            @Override
            protected List<GoogleDriveFileInfo> performListing(ProcessContext context, Long minTimestamp, ListingMode ignoredListingMode) throws IOException {
                ListingMode acquiredListingMode = ListingMode.valueOf(listingModeAsString);

                return super.performListing(context, minTimestamp, acquiredListingMode);
            }

            @Override
            public Drive createDriveService(ProcessContext context, HttpTransport httpTransport, String... scopes) {
                return mockDriverService;
            }
        };

        testSubject.onScheduled(mockProcessContext);
    }

    @Test
    void testCreatedListableEntityContainsCorrectData() throws Exception {
        // GIVEN
        Long minTimestamp = 0L;
        listingModeAsString = "EXECUTION";

        String id = "id_1";
        String filename = "file_name_1";
        long size = 125L;
        long createdTime = 123456L;
        long modifiedTime = 234567L;
        String mimeType = "mime_type_1";

        when(mockDriverService.files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ("('null' in parents) and (mimeType != 'application/vnd.google-apps.folder') and (mimeType != 'application/vnd.google-apps.shortcut') and trashed = false")
                .setPageToken(null)
                .setFields("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType)")
                .execute()
                .getFiles()
        ).thenReturn(singletonList(
                createFile(
                        id,
                        filename,
                        size,
                        new DateTime(createdTime),
                        new DateTime(modifiedTime),
                        mimeType
                )
        ));

        List<GoogleDriveFileInfo> expected = singletonList(
                new GoogleDriveFileInfo.Builder()
                        .id(id)
                        .fileName(filename)
                        .size(size)
                        .createdTime(createdTime)
                        .modifiedTime(modifiedTime)
                        .mimeType(mimeType)
                        .build()
        );

        // WHEN
        List<GoogleDriveFileInfo> actual = testSubject.performListing(mockProcessContext, minTimestamp, null);

        // THEN
        List<Function<GoogleDriveFileInfo, Object>> propertyProviders = asList(
                GoogleDriveFileInfo::getId,
                GoogleDriveFileInfo::getIdentifier,
                GoogleDriveFileInfo::getName,
                GoogleDriveFileInfo::getSize,
                GoogleDriveFileInfo::getTimestamp,
                GoogleDriveFileInfo::getCreatedTime,
                GoogleDriveFileInfo::getModifiedTime,
                GoogleDriveFileInfo::getMimeType
        );

        List<EqualsWrapper<GoogleDriveFileInfo>> expectedWrapper = wrapList(expected, propertyProviders);
        List<EqualsWrapper<GoogleDriveFileInfo>> actualWrapper = wrapList(actual, propertyProviders);

        assertEquals(expectedWrapper, actualWrapper);
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
}
