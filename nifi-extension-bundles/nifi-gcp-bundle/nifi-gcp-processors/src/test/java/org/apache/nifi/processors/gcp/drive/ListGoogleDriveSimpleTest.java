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
import com.google.api.services.drive.model.User;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    void setUp() {
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
    }

    @Test
    void testMigrateProperties() {
        TestRunner testRunner = TestRunners.newTestRunner(ListGoogleDrive.class);
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListGoogleDrive.TRACKING_STATE_CACHE.getName()),
                Map.entry(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListGoogleDrive.TRACKING_TIME_WINDOW.getName()),
                Map.entry(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListGoogleDrive.INITIAL_LISTING_TARGET.getName()),
                Map.entry(GoogleDriveTrait.OLD_CONNECT_TIMEOUT_PROPERTY_NAME, GoogleDriveTrait.CONNECT_TIMEOUT.getName()),
                Map.entry(GoogleDriveTrait.OLD_READ_TIMEOUT_PROPERTY_NAME, GoogleDriveTrait.READ_TIMEOUT.getName()),
                Map.entry("folder-id", ListGoogleDrive.FOLDER_ID.getName()),
                Map.entry("recursive-search", ListGoogleDrive.RECURSIVE_SEARCH.getName()),
                Map.entry("min-age", ListGoogleDrive.MIN_AGE.getName()),
                Map.entry(GoogleUtils.OLD_GCP_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE.getName()),
                Map.entry(ProxyServiceMigration.OBSOLETE_PROXY_CONFIGURATION_SERVICE, ProxyServiceMigration.PROXY_CONFIGURATION_SERVICE),
                Map.entry("target-system-timestamp-precision", AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION.getName()),
                Map.entry("listing-strategy", AbstractListProcessor.LISTING_STRATEGY.getName()),
                Map.entry("record-writer", AbstractListProcessor.RECORD_WRITER.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());

        final Set<String> expectedRemoved = Set.of(
                "Distributed Cache Service"
        );

        assertEquals(expectedRemoved, propertyMigrationResult.getPropertiesRemoved());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"drive_id"})
    void testCreatedListableEntityContainsCorrectData(String driveId) throws Exception {
        // GIVEN
        Long minTimestamp = 0L;
        listingModeAsString = "EXECUTION";

        String folderId = "folder_id";
        String folderName = "folder_name";

        String driveName = driveId != null ? "drive_name" : null;

        String id = "id_1";
        String filename = "file_name_1";
        long size = 125L;
        long createdTime = 123456L;
        long modifiedTime = 234567L;
        String mimeType = "mime_type_1";
        String owner = "user1";
        String lastModifyingUser = "user2";
        String webViewLink = "http://web.view";
        String webContentLink = "http://web.content";

        when(mockProcessContext.getProperty(ListGoogleDrive.FOLDER_ID)
                .evaluateAttributeExpressions()
                .getValue()
        ).thenReturn(folderId);

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
                        new DateTime(createdTime),
                        new DateTime(modifiedTime),
                        mimeType,
                        owner,
                        lastModifyingUser,
                        webViewLink,
                        webContentLink
                )
        ));

        List<GoogleDriveFileInfo> expected = singletonList(
                new GoogleDriveFileInfo.Builder()
                        .id(id)
                        .fileName(filename)
                        .size(size)
                        .sizeAvailable(true)
                        .createdTime(createdTime)
                        .modifiedTime(modifiedTime)
                        .mimeType(mimeType)
                        .path(folderName)
                        .owner(owner)
                        .lastModifyingUser(lastModifyingUser)
                        .webViewLink(webViewLink)
                        .webContentLink(webContentLink)
                        .parentFolderId(folderId)
                        .parentFolderName(folderName)
                        .listedFolderId(folderId)
                        .listedFolderName(folderName)
                        .sharedDriveId(driveId)
                        .sharedDriveName(driveName)
                        .build()
        );

        testSubject.onScheduled(mockProcessContext);

        // WHEN
        List<GoogleDriveFileInfo> actual = testSubject.performListing(mockProcessContext, minTimestamp, null);

        // THEN
        List<Function<GoogleDriveFileInfo, Object>> propertyProviders = asList(
                GoogleDriveFileInfo::getId,
                GoogleDriveFileInfo::getIdentifier,
                GoogleDriveFileInfo::getName,
                GoogleDriveFileInfo::getSize,
                GoogleDriveFileInfo::isSizeAvailable,
                GoogleDriveFileInfo::getTimestamp,
                GoogleDriveFileInfo::getCreatedTime,
                GoogleDriveFileInfo::getModifiedTime,
                GoogleDriveFileInfo::getMimeType,
                GoogleDriveFileInfo::getPath,
                GoogleDriveFileInfo::getOwner,
                GoogleDriveFileInfo::getLastModifyingUser,
                GoogleDriveFileInfo::getWebViewLink,
                GoogleDriveFileInfo::getWebContentLink,
                GoogleDriveFileInfo::getParentFolderId,
                GoogleDriveFileInfo::getParentFolderName,
                GoogleDriveFileInfo::getListedFolderId,
                GoogleDriveFileInfo::getListedFolderName,
                GoogleDriveFileInfo::getSharedDriveId,
                GoogleDriveFileInfo::getSharedDriveName
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
            String mimeType,
            String owner,
            String lastModifyingUser,
            String webViewLink,
            String webContentLink) {
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
}
