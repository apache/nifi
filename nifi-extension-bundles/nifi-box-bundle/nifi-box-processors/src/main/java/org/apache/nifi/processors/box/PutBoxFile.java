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

package org.apache.nifi.processors.box;

import com.box.sdkgen.box.errors.BoxAPIError;
import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.managers.folders.CreateFolderRequestBody;
import com.box.sdkgen.managers.folders.CreateFolderRequestBodyParentField;
import com.box.sdkgen.managers.folders.GetFolderByIdQueryParams;
import com.box.sdkgen.managers.folders.GetFolderItemsQueryParams;
import com.box.sdkgen.managers.uploads.UploadFileRequestBody;
import com.box.sdkgen.managers.uploads.UploadFileRequestBodyAttributesField;
import com.box.sdkgen.managers.uploads.UploadFileRequestBodyAttributesParentField;
import com.box.sdkgen.managers.uploads.UploadFileVersionRequestBody;
import com.box.sdkgen.managers.uploads.UploadFileVersionRequestBodyAttributesField;
import com.box.sdkgen.schemas.filefull.FileFull;
import com.box.sdkgen.schemas.filemini.FileMini;
import com.box.sdkgen.schemas.files.Files;
import com.box.sdkgen.schemas.folderfull.FolderFull;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import com.box.sdkgen.schemas.items.Items;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy;

import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.apache.nifi.processor.util.StandardValidators.createRegexMatchingValidator;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.PATH_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP_DESC;
import static org.apache.nifi.processors.box.BoxFileUtils.BOX_URL;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;

@SeeAlso({ListBoxFile.class, FetchBoxFile.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "put"})
@CapabilityDescription("Puts content to a Box folder.")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Box object.")
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "path", description = PATH_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)})
public class PutBoxFile extends AbstractBoxProcessor {
    public static final int CHUNKED_UPLOAD_LOWER_LIMIT_IN_BYTES = 20 * 1024 * 1024;
    public static final int CHUNKED_UPLOAD_UPPER_LIMIT_IN_BYTES = 50 * 1024 * 1024;

    public static final int NUMBER_OF_RETRIES = 10;
    public static final int WAIT_TIME_MS = 1000;

    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("Folder ID")
            .description("The ID of the folder where the file is uploaded." +
            " Please see Additional Details to obtain Folder ID.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .description("The name of the file to upload to the specified Box folder.")
            .required(true)
            .defaultValue("${filename}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBFOLDER_NAME = new PropertyDescriptor.Builder()
            .name("Subfolder Name")
            .description("The name (path) of the subfolder where files are uploaded. The subfolder name is relative to the folder specified by 'Folder ID'."
                    + " Example: subFolder, subFolder1/subfolder2")
            .addValidator(createRegexMatchingValidator(Pattern.compile("^(?!/).+(?<!/)$"), false,
                    "Subfolder Name should not contain leading or trailing slash ('/') character."))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor CREATE_SUBFOLDER = new PropertyDescriptor.Builder()
            .name("Create Subfolder")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(SUBFOLDER_NAME)
            .description("Specifies whether to check if the subfolder exists and to automatically create it if it does not. " +
                    "Permission to list folders is required. ")
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the specified Box folder.")
            .required(true)
            .defaultValue(ConflictResolutionStrategy.FAIL.getValue())
            .allowableValues(ConflictResolutionStrategy.class)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Chunked Upload Threshold")
            .description("The maximum size of the content which is uploaded at once. FlowFiles larger than this threshold are uploaded in chunks."
                    + " Chunked upload is allowed for files larger than 20 MB. It is recommended to use chunked upload for files exceeding 50 MB.")
            .defaultValue("20 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(CHUNKED_UPLOAD_LOWER_LIMIT_IN_BYTES, CHUNKED_UPLOAD_UPPER_LIMIT_IN_BYTES))
            .required(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            FOLDER_ID,
            SUBFOLDER_NAME,
            CREATE_SUBFOLDER,
            FILE_NAME,
            CONFLICT_RESOLUTION,
            CHUNKED_UPLOAD_THRESHOLD
    );

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("Files that have been successfully written to Box are transferred to this relationship.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("Files that could not be written to Box for some reason are transferred to this relationship.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final int CONFLICT_RESPONSE_CODE = 409;
    private static final int NOT_FOUND_RESPONSE_CODE = 404;

    private volatile BoxClient boxClient;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxClient = boxClientService.getBoxClient();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final long chunkUploadThreshold = context.getProperty(CHUNKED_UPLOAD_THRESHOLD)
                .asDataSize(DataUnit.B)
                .longValue();
        final ConflictResolutionStrategy conflictResolution = ConflictResolutionStrategy.forValue(context.getProperty(CONFLICT_RESOLUTION).getValue());

        final long startNanos = System.nanoTime();
        String fullPath = null;

        try {
            final long size = flowFile.getSize();
            final String parentFolderId = getOrCreateDirectParentFolder(context, flowFile);
            final FolderFull parentFolderInfo = getFolderInfo(parentFolderId);
            fullPath = BoxFileUtils.getFolderPath(parentFolderInfo);
            FileFull uploadedFileInfo = null;

            try (InputStream rawIn = session.read(flowFile)) {

                if (REPLACE.equals(conflictResolution)) {
                    uploadedFileInfo = replaceBoxFileIfExists(parentFolderId, filename, rawIn, size, chunkUploadThreshold);
                }

                if (uploadedFileInfo == null) {
                    uploadedFileInfo = createBoxFile(parentFolderId, filename, rawIn, size, chunkUploadThreshold);
                }
            } catch (BoxAPIError e) {
                if (e.getResponseInfo() != null && e.getResponseInfo().getStatusCode() == CONFLICT_RESPONSE_CODE) {
                    handleConflict(conflictResolution, filename, fullPath, e);
                } else {
                    throw e;
                }
            }

            if (uploadedFileInfo != null) {
                final Map<String, String> attributes = BoxFileUtils.createAttributeMap(uploadedFileInfo);
                final String url = BOX_URL + uploadedFileInfo.getId();
                flowFile = session.putAllAttributes(flowFile, attributes);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, url, transferMillis);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (BoxAPIError e) {
            int statusCode = e.getResponseInfo() != null ? e.getResponseInfo().getStatusCode() : 0;
            getLogger().error("Upload failed: File [{}] Folder [{}] Response Code [{}]", filename, fullPath, statusCode, e);
            handleExpectedError(session, flowFile, e);
        } catch (Exception e) {
            getLogger().error("Upload failed: File [{}], Folder [{}]", filename, fullPath, e);
            handleUnexpectedError(session, flowFile, e);
        }
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("box-folder-id", FOLDER_ID.getName());
        config.renameProperty("file-name", FILE_NAME.getName());
        config.renameProperty("subfolder-name", SUBFOLDER_NAME.getName());
        config.renameProperty("create-folder", CREATE_SUBFOLDER.getName());
        config.renameProperty("conflict-resolution-strategy", CONFLICT_RESOLUTION.getName());
        config.renameProperty("chunked-upload-threshold", CHUNKED_UPLOAD_THRESHOLD.getName());
    }

    private FolderFull getFolderInfo(String folderId) {
        final GetFolderByIdQueryParams queryParams = new GetFolderByIdQueryParams.Builder()
                .fields(List.of("id", "name", "path_collection"))
                .build();
        return boxClient.getFolders().getFolderById(folderId, queryParams);
    }

    private String getOrCreateDirectParentFolder(ProcessContext context, FlowFile flowFile) {
        final String subfolderPath = context.getProperty(SUBFOLDER_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        String parentFolderId = getFolderById(folderId);

        if (subfolderPath != null) {
            final boolean createFolder = context.getProperty(CREATE_SUBFOLDER).asBoolean();

            final Queue<String> subFolderNames = getSubFolderNames(subfolderPath);
            parentFolderId = getOrCreateSubfolders(subFolderNames, parentFolderId, createFolder);
        }

        return parentFolderId;
    }

    private FileFull replaceBoxFileIfExists(String parentFolderId, String filename, final InputStream inputStream, final long size, final long chunkUploadThreshold)
            throws Exception {
        final Optional<String> existingFileId = getFileIdByName(filename, parentFolderId);
        if (existingFileId.isPresent()) {
            final String fileId = existingFileId.get();

            // Upload new version
            final UploadFileVersionRequestBodyAttributesField attributes = new UploadFileVersionRequestBodyAttributesField(filename);
            final UploadFileVersionRequestBody requestBody = new UploadFileVersionRequestBody(attributes, inputStream);
            final Files files = boxClient.getUploads().uploadFileVersion(fileId, requestBody);
            if (files.getEntries() != null && !files.getEntries().isEmpty()) {
                return files.getEntries().get(0);
            }
        }
        return null;
    }

    private FileFull createBoxFile(String parentFolderId, String filename, InputStream inputStream, long size, final long chunkUploadThreshold)
            throws Exception {
        final UploadFileRequestBodyAttributesParentField parent = new UploadFileRequestBodyAttributesParentField(parentFolderId);
        final UploadFileRequestBodyAttributesField attributes = new UploadFileRequestBodyAttributesField(filename, parent);
        final UploadFileRequestBody requestBody = new UploadFileRequestBody(attributes, inputStream);
        final Files files = boxClient.getUploads().uploadFile(requestBody);
        if (files.getEntries() != null && !files.getEntries().isEmpty()) {
            return files.getEntries().get(0);
        }
        return null;
    }

    private Queue<String> getSubFolderNames(String subfolderPath) {
        final Queue<String> subfolderNames = new LinkedList<>();
        Collections.addAll(subfolderNames, subfolderPath.split("/"));
        return subfolderNames;
    }

    private String getOrCreateSubfolders(Queue<String> subFolderNames, String parentFolderId, boolean createFolder) {
        final String newParentFolderId = getOrCreateFolder(subFolderNames.poll(), parentFolderId, createFolder);

        if (!subFolderNames.isEmpty()) {
            return getOrCreateSubfolders(subFolderNames, newParentFolderId, createFolder);
        } else {
            return newParentFolderId;
        }
    }

    private String getOrCreateFolder(String folderName, String parentFolderId, boolean createFolder) {
        final Optional<String> existingFolderId = getFolderIdByName(folderName, parentFolderId);

        if (existingFolderId.isPresent()) {
            return existingFolderId.get();
        }

        if (!createFolder) {
            throw new ProcessException(format("The specified subfolder [%s] does not exist and [%s] is false.",
                    folderName, CREATE_SUBFOLDER.getDisplayName()));
        }

        return createFolder(folderName, parentFolderId);
    }

    private String createFolder(final String folderName, final String parentFolderId) {
        getLogger().info("Creating Folder [{}], Parent [{}]", folderName, parentFolderId);

        try {
            final CreateFolderRequestBodyParentField parent = new CreateFolderRequestBodyParentField(parentFolderId);
            final CreateFolderRequestBody requestBody = new CreateFolderRequestBody(folderName, parent);
            final FolderFull createdFolder = boxClient.getFolders().createFolder(requestBody);
            return createdFolder.getId();
        } catch (BoxAPIError e) {
            if (e.getResponseInfo() != null && e.getResponseInfo().getStatusCode() != CONFLICT_RESPONSE_CODE) {
                throw new ProcessException("Failed to create folder: " + e.getMessage(), e);
            } else {
                Optional<String> createdFolderId = waitForOngoingFolderCreationToFinish(folderName, parentFolderId);
                return createdFolderId.orElseThrow(() -> new ProcessException(format("Created subfolder [%s] can not be found under [%s]",
                        folderName, parentFolderId)));
            }
        }
    }

    private Optional<String> waitForOngoingFolderCreationToFinish(final String folderName, final String parentFolderId) {
        try {
            Optional<String> createdFolderId = getFolderIdByName(folderName, parentFolderId);

            for (int i = 0; i < NUMBER_OF_RETRIES && createdFolderId.isEmpty(); i++) {
                getLogger().debug("Subfolder [{}] under [{}] has not been created yet, waiting {} ms",
                        folderName, parentFolderId, WAIT_TIME_MS);
                Thread.sleep(WAIT_TIME_MS);
                createdFolderId = getFolderIdByName(folderName, parentFolderId);
            }
            return createdFolderId;
        } catch (InterruptedException ie) {
            throw new RuntimeException(format("Waiting for creation of subfolder [%s] under [%s] was interrupted",
                    folderName, parentFolderId), ie);
        }
    }

    private String getFolderById(final String folderId) {
        try {
            final GetFolderByIdQueryParams queryParams = new GetFolderByIdQueryParams.Builder()
                    .fields(List.of("id"))
                    .build();
            boxClient.getFolders().getFolderById(folderId, queryParams);
            return folderId;
        } catch (BoxAPIError e) {
            if (e.getResponseInfo() != null && e.getResponseInfo().getStatusCode() == NOT_FOUND_RESPONSE_CODE) {
                throw new ProcessException(format("The Folder [%s] specified by [%s] does not exist", folderId, FOLDER_ID.getDisplayName()));
            }
            throw new ProcessException("Failed to get folder: " + e.getMessage(), e);
        }
    }

    private Optional<String> getFolderIdByName(final String folderName, final String parentFolderId) {
        final GetFolderItemsQueryParams queryParams = new GetFolderItemsQueryParams.Builder()
                .fields(List.of("name", "type"))
                .build();

        final Items items = boxClient.getFolders().getFolderItems(parentFolderId, queryParams);

        if (items.getEntries() != null) {
            for (Object itemObj : items.getEntries()) {
                if (itemObj instanceof FolderMini folder) {
                    if (folderName.equals(folder.getName())) {
                        return Optional.of(folder.getId());
                    }
                }
            }
        }
        return Optional.empty();
    }

    private Optional<String> getFileIdByName(final String filename, final String parentFolderId) {
        final GetFolderItemsQueryParams queryParams = new GetFolderItemsQueryParams.Builder()
                .fields(List.of("name", "type"))
                .build();

        final Items items = boxClient.getFolders().getFolderItems(parentFolderId, queryParams);

        if (items.getEntries() != null) {
            for (Object itemObj : items.getEntries()) {
                if (itemObj instanceof FileMini file) {
                    if (filename.equals(file.getName())) {
                        return Optional.of(file.getId());
                    }
                }
            }
        }
        return Optional.empty();
    }

    private void handleConflict(final ConflictResolutionStrategy conflictResolution, final String filename, String path, final BoxAPIError e) {
        if (conflictResolution == IGNORE) {
            getLogger().info("File with the same name [{}] already exists in [{}]. Remote file is not modified due to [{}] being set to [{}]",
                    filename, path, CONFLICT_RESOLUTION.getDisplayName(), conflictResolution.getDisplayName());
        } else {
            throw new ProcessException(format("File with the same name [%s] already exists in [%s]", filename, path), e);
        }
    }

    private void handleUnexpectedError(final ProcessSession session, FlowFile flowFile, final Exception e) {
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleExpectedError(final ProcessSession session, FlowFile flowFile, final BoxAPIError e) {
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_MESSAGE, e.getMessage());
        if (e.getResponseInfo() != null) {
            flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_CODE, valueOf(e.getResponseInfo().getStatusCode()));
        }
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}
