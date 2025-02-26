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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.CREATED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.CREATED_TIME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.DRIVE_ATTR_PREFIX;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LAST_MODIFYING_USER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.LAST_MODIFYING_USER_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MODIFIED_TIME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MODIFIED_TIME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.OWNER;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.OWNER_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.PATH_DESC;
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
        @WritesAttribute(attribute = WEB_CONTENT_LINK, description = WEB_CONTENT_LINK_DESC)})
@Stateful(scopes = {Scope.CLUSTER}, description = "The processor stores necessary data to be able to keep track what files have been listed already." +
        " What exactly needs to be stored depends on the 'Listing Strategy'." +
        " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up" +
        " where the previous node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListGoogleDrive extends AbstractListProcessor<GoogleDriveFileInfo> implements GoogleDriveTrait {
    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("folder-id")
            .displayName("Folder ID")
            .description("The ID of the folder from which to pull list of files." +
                    " Please see Additional Details to set up access to Google Drive and obtain Folder ID." +
                    " WARNING: Unauthorized access to the folder is treated as if the folder was empty." +
                    " This results in the processor not creating outgoing FlowFiles. No additional error message is provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("recursive-search")
            .displayName("Search Recursively")
            .description("When 'true', will include list of files from concrete sub-folders (ignores shortcuts)." +
                    " Otherwise, will return only files that have the defined 'Folder ID' as their parent directly." +
                    " WARNING: The listing may fail if there are too many sub-folders (500+).")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Minimum File Age")
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

    public static final PropertyDescriptor ENRICH_FILE_METADATA = new PropertyDescriptor.Builder()
            .name("enrich-file-metadata")
            .displayName("Enrich File Metadata")
            .description(String.format("Specifies which FlowFile attributes to add in the listing result that hold additional Google Drive File Metadata information. " +
                            "Comma separated list of any of the following attribute names: %s, %s, %s, %s, %s (the 'drive.' prefix can be omitted). " +
                            "Example: 'path, owner'",
                    PATH, OWNER, LAST_MODIFYING_USER, WEB_VIEW_LINK, WEB_CONTENT_LINK))
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor FOLDER_SEPARATOR_IN_PATH = new PropertyDescriptor.Builder()
            .name("folder-separator-in-path")
            .displayName("Folder Separator in Path")
            .description(String.format("String of one or more characters that will be used to separate the folder names in '%s' attribute value. " +
                            "Google Drive allows special characters in folder names including '/' (slash) and '\\' (backslash). " +
                            "As a result, there is no universal separator that functions consistently across all environments. " +
                            "A separator should be chosen that does not appear in any of the folder names to be listed. " +
                            "The property is mandatory when '%s' attribute has been configured in '%s' property.",
                    PATH, PATH, ENRICH_FILE_METADATA.getDisplayName()))
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(ENRICH_FILE_METADATA)
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
            ENRICH_FILE_METADATA,
            FOLDER_SEPARATOR_IN_PATH,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxyAwareTransportFactory.PROXY_SPECS),
            CONNECT_TIMEOUT,
            READ_TIMEOUT
    );

    private volatile Drive driveService;

    private volatile RecordSchema recordSchema;
    private volatile List<EnrichAttribute> enrichAttributes;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        final List<String> enrichAttributeNames = getEnrichAttributeNames(validationContext);

        enrichAttributeNames.stream()
                .filter(n -> !GoogleDriveFlowFileAttribute.isValidName(n))
                .forEach(n -> results.add(new ValidationResult.Builder()
                        .subject(ENRICH_FILE_METADATA.getDisplayName())
                        .valid(false)
                        .explanation(String.format("'%s' is not a valid FlowFile attribute name.", n))
                        .build())
                );

        if (enrichAttributeNames.contains(GoogleDriveFlowFileAttribute.PATH.getName())) {
            final String folderSeparator = validationContext.getProperty(FOLDER_SEPARATOR_IN_PATH).getValue();
            if (StringUtils.isBlank(folderSeparator)) {
                results.add(new ValidationResult.Builder()
                        .subject(FOLDER_SEPARATOR_IN_PATH.getDisplayName())
                        .valid(false)
                        .explanation(String.format("it is mandatory when '%s' attribute has been configured in '%s' property.", PATH, ENRICH_FILE_METADATA.getDisplayName()))
                        .build());
            }
        }
    }

    @Override
    protected Map<String, String> createAttributes(
            final GoogleDriveFileInfo entity,
            final ProcessContext context
    ) {
        return entity.toStringMap();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        HttpTransport httpTransport = new ProxyAwareTransportFactory(proxyConfiguration).create();

        driveService = createDriveService(context, httpTransport, DriveScopes.DRIVE, DriveScopes.DRIVE_METADATA_READONLY);
    }

    @OnScheduled
    @Override
    // initListedEntityTracker() needs to be overridden because the super implementation calls getRecordSchema()
    // so recordSchema must be initialized before that
    public void initListedEntityTracker(ProcessContext context) {
        this.enrichAttributes = new ArrayList<>();

        final List<RecordField> recordFields = new ArrayList<>();

        Stream.of(
                GoogleDriveFlowFileAttribute.ID,
                GoogleDriveFlowFileAttribute.FILENAME,
                GoogleDriveFlowFileAttribute.SIZE,
                GoogleDriveFlowFileAttribute.SIZE_AVAILABLE,
                GoogleDriveFlowFileAttribute.TIMESTAMP,
                GoogleDriveFlowFileAttribute.CREATED_TIME,
                GoogleDriveFlowFileAttribute.MODIFIED_TIME,
                GoogleDriveFlowFileAttribute.MIME_TYPE
        ).forEach(a -> recordFields.add(a.getRecordField()));

        getEnrichAttributeNames(context).forEach(name -> {
            recordFields.add(GoogleDriveFlowFileAttribute.getByName(name).getRecordField());
            enrichAttributes.add(EnrichAttribute.getByName(name));
        });

        this.recordSchema = new SimpleRecordSchema(recordFields);

        super.initListedEntityTracker(context);
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
        return recordSchema;
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
        final String folderSeparator = context.getProperty(FOLDER_SEPARATOR_IN_PATH).getValue();

        StringBuilder queryTemplateBuilder = new StringBuilder();
        queryTemplateBuilder.append("('%s' in parents)");
        queryTemplateBuilder.append(" and (mimeType != '").append(DRIVE_SHORTCUT_MIME_TYPE).append("')");
        queryTemplateBuilder.append(" and trashed = false");
        if (minTimestamp != null && minTimestamp > 0) {
            String formattedMinTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(minTimestamp), ZoneOffset.UTC));

            queryTemplateBuilder.append(" and (mimeType = '").append(DRIVE_FOLDER_MIME_TYPE).append("'");
            queryTemplateBuilder.append(" or modifiedTime >= '").append(formattedMinTimestamp).append("'");
            queryTemplateBuilder.append(" or createdTime >= '").append(formattedMinTimestamp).append( "'");
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

        final String queryFields = getQueryFields();

        final String folderName = getFolderName(folderId);

        queryFolder(folderId, folderName, queryTemplate, queryFields, recursive, folderSeparator, listing);

        return listing;
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, null, ListingMode.CONFIGURATION_VERIFICATION).size();
    }

    private String getFolderName(final String folderId) throws IOException {
        final File folder = driveService
                .files()
                .get(folderId)
                .setSupportsAllDrives(true)
                .setFields("name, driveId")
                .execute();

        if (folder.getDriveId() == null) {
            return folder.getName();
        } else {
            return driveService
                    .drives()
                    .get(folderId)
                    .setFields("name")
                    .execute()
                    .getName();
        }
    }

    private void queryFolder(
            final String folderId,
            final String folderPath,
            final String queryTemplate,
            final String queryFields,
            final boolean recursive,
            final String folderSeparator,
            final List<GoogleDriveFileInfo> listing
    ) throws IOException {
        final List<File> subfolders = new ArrayList<>();

        String pageToken = null;
        do {
            final FileList result = driveService.files()
                    .list()
                    .setSupportsAllDrives(true)
                    .setIncludeItemsFromAllDrives(true)
                    .setQ(String.format(queryTemplate, folderId))
                    .setPageToken(pageToken)
                    .setFields(queryFields)
                    .execute();

            for (final File file : result.getFiles()) {
                if (DRIVE_FOLDER_MIME_TYPE.equals(file.getMimeType())) {
                    if (recursive) {
                        subfolders.add(file);
                    }
                } else {
                    final GoogleDriveFileInfo.Builder builder = createGoogleDriveFileInfoBuilder(file);

                    enrichAttributes.forEach(ea -> ea.map(builder, file, folderPath));

                    builder.recordSchema(recordSchema);

                    listing.add(builder.build());
                }
            }

            pageToken = result.getNextPageToken();
        } while (pageToken != null);

        for (final File subfolder : subfolders) {
            final String subfolderPath = folderPath + folderSeparator + subfolder.getName();
            queryFolder(subfolder.getId(), subfolderPath, queryTemplate, queryFields, true, folderSeparator, listing);
        }
    }

    private List<String> getEnrichAttributeNames(final PropertyContext context) {
        final String enrichFileMetadata = context.getProperty(ENRICH_FILE_METADATA).getValue();
        return StringUtils.isNotBlank(enrichFileMetadata)
                ? Arrays.stream(enrichFileMetadata.split(",\\s*")).map(a -> a.startsWith(DRIVE_ATTR_PREFIX) ? a : DRIVE_ATTR_PREFIX + a).toList()
                : Collections.emptyList();
    }

    private String getQueryFields() {
        final StringBuilder fieldsBuilder = new StringBuilder("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType");

        enrichAttributes.forEach(ea -> Optional.ofNullable(ea.getFieldName()).ifPresent((fn -> fieldsBuilder.append(", ").append(fn))));

        fieldsBuilder.append(')');

        return fieldsBuilder.toString();
    }

    enum EnrichAttribute {
        PATH(GoogleDriveAttributes.PATH, null,
                (builder, file, path) -> builder.path(path)),
        OWNER(GoogleDriveAttributes.OWNER, "owners",
                (builder, file, path) -> builder.owner(Optional.ofNullable(file.getOwners()).filter(owners -> !owners.isEmpty()).map(List::getFirst).map(User::getDisplayName).orElse(null))),
        LAST_MODIFYING_USER(GoogleDriveAttributes.LAST_MODIFYING_USER, "lastModifyingUser",
                (builder, file, path) -> builder.lastModifyingUser(Optional.ofNullable(file.getLastModifyingUser()).map(User::getDisplayName).orElse(null))),
        WEB_VIEW_LINK(GoogleDriveAttributes.WEB_VIEW_LINK, "webViewLink",
                (builder, file, path) -> builder.webViewLink(file.getWebViewLink())),
        WEB_CONTENT_LINK(GoogleDriveAttributes.WEB_CONTENT_LINK, "webContentLink",
                (builder, file, path) -> builder.webContentLink(file.getWebContentLink()));

        private final String name;
        private final String fieldName;
        private final EnrichAttributeMapper mapper;

        EnrichAttribute(final String name, final String fieldName, final EnrichAttributeMapper mapper) {
            this.name = name;
            this.fieldName = fieldName;
            this.mapper = mapper;
        }

        public String getName() {
            return name;
        }

        String getFieldName() {
            return fieldName;
        }

        void map(final GoogleDriveFileInfo.Builder builder, final File file, final String path) {
            mapper.map(builder, file, path);
        }

        static EnrichAttribute getByName(final String name) {
            for (EnrichAttribute item : values()) {
                if (item.getName().equals(name)) {
                    return item;
                }
            }
            throw new IllegalArgumentException("EnrichAttribute with name [" + name + "] does not exist");
        }
    }

    interface EnrichAttributeMapper {
        void map(final GoogleDriveFileInfo.Builder builder, final File file, final String path);
    }
}
