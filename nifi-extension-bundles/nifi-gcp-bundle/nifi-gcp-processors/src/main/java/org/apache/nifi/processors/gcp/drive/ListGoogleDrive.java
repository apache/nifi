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
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.api.services.drive.model.User;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.CREATED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.CREATED_TIME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LAST_MODIFYING_USER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LAST_MODIFYING_USER_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LISTED_FOLDER_NAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MODIFIED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MODIFIED_TIME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.OWNER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.OWNER_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PARENT_FOLDER_NAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_NAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SHARED_DRIVE_NAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_AVAILABLE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_AVAILABLE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_CONTENT_LINK;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_CONTENT_LINK_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_VIEW_LINK;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.WEB_VIEW_LINK_DESC;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"google", "drive", "storage"})
@CapabilityDescription("Performs a listing of concrete files (shortcuts are ignored) in a Google Drive folder. " +
        "If the 'Record Writer' property is set, a single Output FlowFile is created, and each file in the listing is written as a single record to the output file. " +
        "Otherwise, for each file in the listing, an individual FlowFile is created, the metadata being written as FlowFile attributes. " +
        "This Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the " +
        "previous node left off without duplicating all of the data. " +
        "Please see Additional Details to set up access to Google Drive.")
@SeeAlso({FetchGoogleDrive.class, PutGoogleDrive.class})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "mime.type", description = MIME_TYPE_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = SIZE_AVAILABLE, description = SIZE_AVAILABLE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = CREATED_TIME, description = CREATED_TIME_DESC),
        @WritesAttribute(attribute = MODIFIED_TIME, description = MODIFIED_TIME_DESC),
        @WritesAttribute(attribute = PATH, description = PATH_DESC),
        @WritesAttribute(attribute = OWNER, description = OWNER_DESC),
        @WritesAttribute(attribute = LAST_MODIFYING_USER, description = LAST_MODIFYING_USER_DESC),
        @WritesAttribute(attribute = WEB_VIEW_LINK, description = WEB_VIEW_LINK_DESC),
        @WritesAttribute(attribute = WEB_CONTENT_LINK, description = WEB_CONTENT_LINK_DESC),
        @WritesAttribute(attribute = PARENT_FOLDER_ID, description = PARENT_FOLDER_ID_DESC),
        @WritesAttribute(attribute = PARENT_FOLDER_NAME, description = PARENT_FOLDER_NAME_DESC),
        @WritesAttribute(attribute = LISTED_FOLDER_ID, description = LISTED_FOLDER_ID_DESC),
        @WritesAttribute(attribute = LISTED_FOLDER_NAME, description = LISTED_FOLDER_NAME_DESC),
        @WritesAttribute(attribute = SHARED_DRIVE_ID, description = SHARED_DRIVE_ID_DESC),
        @WritesAttribute(attribute = SHARED_DRIVE_NAME, description = SHARED_DRIVE_NAME_DESC)})
@Stateful(scopes = {Scope.CLUSTER}, description = "The processor stores necessary data to be able to keep track what files have been listed already." +
        " What exactly needs to be stored depends on the 'Listing Strategy'." +
        " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up" +
        " where the previous node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListGoogleDrive extends AbstractListProcessor<GoogleDriveFileInfo> implements GoogleDriveTrait {
    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("Folder ID")
            .description("The ID of the folder from which to pull list of files." +
                    " Please see Additional Details to set up access to Google Drive and obtain Folder ID." +
                    " WARNING: Unauthorized access to the folder is treated as if the folder was empty." +
                    " This results in the processor not creating outgoing FlowFiles. No additional error message is provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("Search Recursively")
            .description("When 'true', will include list of files from concrete sub-folders (ignores shortcuts)." +
                    " Otherwise, will return only files that have the defined 'Folder ID' as their parent directly." +
                    " WARNING: The listing may fail if there are too many sub-folders (500+).")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age a file must be in order to be considered; any files younger than this will be ignored.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractListProcessor.LISTING_STRATEGY)
            .allowableValues(BY_TIMESTAMPS, BY_ENTITIES, BY_TIME_WINDOW, NO_TRACKING)
            .build();

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_STATE_CACHE)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_TIME_WINDOW)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.INITIAL_LISTING_TARGET)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE,
            FOLDER_ID,
            RECURSIVE_SEARCH,
            MIN_AGE,
            LISTING_STRATEGY,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET,
            RECORD_WRITER,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxyAwareTransportFactory.PROXY_SPECS),
            CONNECT_TIMEOUT,
            READ_TIMEOUT
    );

    private volatile Drive driveService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
    }

    @Override
    protected Map<String, String> createAttributes(
            final GoogleDriveFileInfo entity,
            final ProcessContext context
    ) {
        return entity.toAttributeMap();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        HttpTransport httpTransport = new ProxyAwareTransportFactory(proxyConfiguration).create();

        driveService = createDriveService(context, httpTransport, DriveScopes.DRIVE, DriveScopes.DRIVE_METADATA_READONLY);
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, INITIAL_LISTING_TARGET.getName());
        config.renameProperty(OLD_CONNECT_TIMEOUT_PROPERTY_NAME, CONNECT_TIMEOUT.getName());
        config.renameProperty(OLD_READ_TIMEOUT_PROPERTY_NAME, READ_TIMEOUT.getName());
        config.renameProperty("folder-id", FOLDER_ID.getName());
        config.renameProperty("recursive-search", RECURSIVE_SEARCH.getName());
        config.renameProperty("min-age", MIN_AGE.getName());
        config.renameProperty(GoogleUtils.OLD_GCP_CREDENTIALS_PROVIDER_SERVICE_PROPERTY_NAME, GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE.getName());
        ProxyServiceMigration.renameProxyConfigurationServiceProperty(config);
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Google Drive Folder [%s]", getPath(context));
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(FOLDER_ID).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return LISTING_STRATEGY.equals(property)
                || FOLDER_ID.equals(property)
                || RECURSIVE_SEARCH.equals(property);
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return GoogleDriveFileInfo.getRecordSchema();
    }

    @Override
    protected String getDefaultTimePrecision() {
        return PRECISION_SECONDS.getValue();
    }

    @Override
    protected List<GoogleDriveFileInfo> performListing(
            final ProcessContext context,
            final Long minTimestamp,
            final ListingMode listingMode
    ) throws IOException {
        final List<GoogleDriveFileInfo> listing = new ArrayList<>();

        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions().getValue();
        final Boolean recursive = context.getProperty(RECURSIVE_SEARCH).asBoolean();
        final Long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        StringBuilder queryTemplateBuilder = new StringBuilder();
        queryTemplateBuilder.append("('%s' in parents)");
        queryTemplateBuilder.append(" and (mimeType != '").append(DRIVE_SHORTCUT_MIME_TYPE).append("')");
        queryTemplateBuilder.append(" and trashed = false");
        if (minTimestamp != null && minTimestamp > 0) {
            String formattedMinTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(minTimestamp), ZoneOffset.UTC));

            queryTemplateBuilder.append(" and (mimeType = '").append(DRIVE_FOLDER_MIME_TYPE).append("'");
            queryTemplateBuilder.append(" or modifiedTime >= '").append(formattedMinTimestamp).append("'");
            queryTemplateBuilder.append(" or createdTime >= '").append(formattedMinTimestamp).append("'");
            queryTemplateBuilder.append(")");
        }
        if (minAge != null && minAge > 0) {
            long maxTimestamp = System.currentTimeMillis() - minAge;
            String formattedMaxTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(maxTimestamp), ZoneOffset.UTC));

            queryTemplateBuilder.append(" and (mimeType = '").append(DRIVE_FOLDER_MIME_TYPE).append("'");
            queryTemplateBuilder.append(" or (modifiedTime < '").append(formattedMaxTimestamp).append("'");
            queryTemplateBuilder.append(" and createdTime < '").append(formattedMaxTimestamp).append("')");
            queryTemplateBuilder.append(")");
        }

        final String queryTemplate = queryTemplateBuilder.toString();

        final FolderDetails folderDetails = getFolderDetails(driveService, folderId);
        final String folderPath = urlEncode(folderDetails.getFolderName());

        queryFolder(folderDetails.getFolderId(), folderDetails.getFolderName(), folderPath, queryTemplate, recursive, folderDetails, listing);

        return listing;
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, null, ListingMode.CONFIGURATION_VERIFICATION).size();
    }

    private void queryFolder(
            final String folderId,
            final String folderName,
            final String folderPath,
            final String queryTemplate,
            final boolean recursive,
            final FolderDetails listedFolderDetails,
            final List<GoogleDriveFileInfo> listing
    ) throws IOException {
        final List<File> subfolders = new ArrayList<>();

        String pageToken = null;
        do {
            final FileList result = driveService.files() //NOPMD
                    .list()
                    .setSupportsAllDrives(true)
                    .setIncludeItemsFromAllDrives(true)
                    .setQ(String.format(queryTemplate, folderId))
                    .setPageToken(pageToken)
                    .setFields("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType, owners, lastModifyingUser, webViewLink, webContentLink)")
                    .execute();

            for (final File file : result.getFiles()) {
                if (DRIVE_FOLDER_MIME_TYPE.equals(file.getMimeType())) {
                    if (recursive) {
                        subfolders.add(file);
                    }
                } else {
                    final GoogleDriveFileInfo.Builder builder = createGoogleDriveFileInfoBuilder(file)
                            .path(folderPath)
                            .owner(Optional.ofNullable(file.getOwners()).filter(owners -> !owners.isEmpty()).map(List::getFirst).map(User::getDisplayName).orElse(null))
                            .lastModifyingUser(Optional.ofNullable(file.getLastModifyingUser()).map(User::getDisplayName).orElse(null))
                            .webViewLink(file.getWebViewLink())
                            .webContentLink(file.getWebContentLink())
                            .parentFolderId(folderId)
                            .parentFolderName(folderName)
                            .listedFolderId(listedFolderDetails.getFolderId())
                            .listedFolderName(listedFolderDetails.getFolderName())
                            .sharedDriveId(listedFolderDetails.getSharedDriveId())
                            .sharedDriveName(listedFolderDetails.getSharedDriveName());

                    listing.add(builder.build());
                }
            }

            pageToken = result.getNextPageToken();
        } while (pageToken != null);

        for (final File subfolder : subfolders) {
            final String subfolderPath = folderPath + "/" + urlEncode(subfolder.getName());
            queryFolder(subfolder.getId(), subfolder.getName(), subfolderPath, queryTemplate, true, listedFolderDetails, listing);
        }
    }

    private String urlEncode(final String str) {
        return URLEncoder.encode(str, StandardCharsets.UTF_8);
    }
}
