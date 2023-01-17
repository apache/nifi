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

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
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

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
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
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


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
public class PutBoxFile extends AbstractProcessor {
    public static final int CHUNKED_UPLOAD_LOWER_LIMIT_IN_BYTES = 20 * 1024 * 1024;
    public static final int CHUNKED_UPLOAD_UPPER_LIMIT_IN_BYTES = 50 * 1024 * 1024;

    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String REPLACE_RESOLUTION = "replace";
    public static final String FAIL_RESOLUTION = "fail";

    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("box-folder-id")
            .displayName("Folder ID")
            .description("The ID of the folder where the file is uploaded.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor
            .Builder()
            .name("file-name")
            .displayName("Filename")
            .description("The name of the file to upload to the specified Box folder.")
            .required(true)
            .defaultValue("${filename}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBFOLDER_NAME = new PropertyDescriptor.Builder()
            .name("subfolder-name")
            .displayName("Subfolder Name")
            .description("The name (path) of the subfolder where files are uploaded. The subfolder name is relative to the folder specified by 'Folder ID'."
                    + " Example: subFolder, subFolder1/subfolder2")
            .addValidator(createRegexMatchingValidator(Pattern.compile("^(?!/).+(?<!/)$"), false,
                    "Subfolder Name should not contain leading or trailing slash ('/') character."))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor CREATE_SUBFOLDER = new PropertyDescriptor.Builder()
            .name("create-folder")
            .displayName("Create Subfolder")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to check if the subfolder exists and to automatically create it if it does not. " +
                    "Permission to list folders is required. ")
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the specified Box folder.")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(FAIL_RESOLUTION, IGNORE_RESOLUTION, REPLACE_RESOLUTION)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_THRESHOLD = new PropertyDescriptor.Builder()
            .name("chunked-upload-threshold")
            .displayName("Chunked Upload Threshold")
            .description("The maximum size of the content which is uploaded at once. FlowFiles larger than this threshold are uploaded in chunks."
                    + " Chunked upload is allowed for files larger than 20 MB. It is recommended to use chunked upload for files exceeding 50 MB.")
            .defaultValue("20 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(CHUNKED_UPLOAD_LOWER_LIMIT_IN_BYTES, CHUNKED_UPLOAD_UPPER_LIMIT_IN_BYTES))
            .required(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(asList(
            BoxClientService.BOX_CLIENT_SERVICE,
            FOLDER_ID,
            SUBFOLDER_NAME,
            CREATE_SUBFOLDER,
            FILE_NAME,
            CONFLICT_RESOLUTION,
            CHUNKED_UPLOAD_THRESHOLD
    ));

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

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    public static final int CONFLICT_RESPONSE_CODE = 409;

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String subfolderName = context.getProperty(SUBFOLDER_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final boolean createFolder = context.getProperty(CREATE_SUBFOLDER).asBoolean();
        BoxFile.Info uploadedFile = null;

        final long startNanos = System.nanoTime();

        String fullPath = null;

        try {
            final long size = flowFile.getSize();

            folderId = subfolderName != null ? getOrCreateParentSubfolder(subfolderName, folderId, createFolder).getID() : folderId;
            final BoxFolder parentFolder = getFolder(folderId);
            fullPath = BoxFileUtils.getPath(parentFolder.getInfo());

            final long chunkUploadThreshold = context.getProperty(CHUNKED_UPLOAD_THRESHOLD)
                    .asDataSize(DataUnit.B)
                    .longValue();

            final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();

            try (InputStream rawIn = session.read(flowFile)){

                boolean isNewVersionUpload = false;

                if (REPLACE_RESOLUTION.equals(conflictResolution)) {
                    final Optional<BoxFile.Info> alreadyUploadedFile = getFileByName(filename, parentFolder);

                    if (alreadyUploadedFile.isPresent()) {
                        BoxFile existingBoxFile = new BoxFile(boxAPIConnection, alreadyUploadedFile.get().getID());
                        existingBoxFile.uploadNewVersion(rawIn);
                        isNewVersionUpload = true;
                    }
                }

                if (!isNewVersionUpload) {
                    if (size > chunkUploadThreshold) {
                        uploadedFile = parentFolder.uploadLargeFile(rawIn, filename, size);
                    } else {
                        uploadedFile = parentFolder.uploadFile(rawIn, filename);
                    }
                }

            } catch (BoxAPIResponseException e) {
                handleUploadError(conflictResolution, filename, parentFolder, e);
            }

            if (uploadedFile != null) {
                final Map<String, String> attributes = BoxFileUtils.createAttributeMap(uploadedFile);
                final String url = BOX_URL + uploadedFile.getID();
                flowFile = session.putAllAttributes(flowFile, attributes);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, url, transferMillis);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (BoxAPIResponseException e) {
            getLogger().error("Exception occurred while uploading file '{}' to Box folder '{}'", filename, fullPath, e);
            handleExpectedError(session, flowFile, e);
        } catch (Exception e) {
            getLogger().error("Unexpected exception occurred while uploading file '{}' to Box folder '{}'", filename, fullPath, e);
            handleUnexpectedError(session, flowFile, e);
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);

        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    BoxFolder getFolder(String folderId) {
        return new BoxFolder(boxAPIConnection, folderId);
    }

    private BoxFolder.Info getOrCreateParentSubfolder(String folderName, String parentFolderId, boolean createFolder)  {
        final int indexOfPathSeparator = folderName.indexOf("/");

        if (isMultiLevelFolder(indexOfPathSeparator, folderName)) {
            final String mainFolderName = folderName.substring(0, indexOfPathSeparator);
            final String subFolders = folderName.substring(indexOfPathSeparator + 1);
            final BoxFolder.Info mainFolder = getOrCreateFolder(mainFolderName, parentFolderId, createFolder);
            return getOrCreateParentSubfolder(subFolders, mainFolder.getID(), createFolder);
        } else {
            return getOrCreateFolder(folderName, parentFolderId, createFolder);
        }
    }

    private BoxFolder.Info getOrCreateFolder(String folderName, String parentFolderId, boolean createFolder) {
        final Optional<BoxFolder.Info> existingFolder = checkFolderExistence(folderName, parentFolderId);

        if (existingFolder.isPresent()) {
            return existingFolder.get();
        }

        if (createFolder) {
            getLogger().debug("Create folder " + folderName + " parent id: " + parentFolderId);

            final BoxFolder parentFolder = getFolder(parentFolderId);
            return parentFolder.createFolder(folderName);
        } else {
            throw new ProcessException(format("The specified subfolder '%s' does not exist and '%s' is false.", folderName, CREATE_SUBFOLDER.getDisplayName()));
        }
    }

    private Optional<BoxFolder.Info> checkFolderExistence(final String folderName, final String parentFolderId) {
        final BoxFolder parentFolder = getFolder(parentFolderId);
        return StreamSupport.stream(parentFolder.getChildren("name").spliterator(), false)
                .filter(BoxFolder.Info.class::isInstance)
                .map(BoxFolder.Info.class::cast)
                .filter(info -> info.getName().equals(folderName))
                .findAny();
    }

    private Optional<BoxFile.Info> getFileByName(final String filename, final BoxFolder parentFolder) {
        return StreamSupport.stream(parentFolder.getChildren("name").spliterator(), false)
                .filter(BoxFile.Info.class::isInstance)
                .map(BoxFile.Info.class::cast)
                .filter(info -> info.getName().equals(filename))
                .findAny();
    }

    private boolean isMultiLevelFolder(int indexOfPathSeparator, String folderName) {
        return indexOfPathSeparator > 0 && indexOfPathSeparator < folderName.length() - 1;
    }

    private void handleUploadError(final String conflictResolution, final String filename, BoxFolder folder, final BoxAPIException e) {
        if (e.getResponseCode() == CONFLICT_RESPONSE_CODE) {
            handleConflict(conflictResolution, filename, folder, e);
        } else {
            throw new ProcessException(e);
        }
    }

    private void handleConflict(final String conflictResolution, final String filename, BoxFolder folder, final BoxAPIException e) {
        final String path = BoxFileUtils.getPath(folder.getInfo());

        if (IGNORE_RESOLUTION.equals(conflictResolution)) {
            getLogger().info("File with the same name '{}' already exists in '%s'. Remote file is not modified due to {} being set to '{}'.",
                    filename, path, CONFLICT_RESOLUTION.getDisplayName(), conflictResolution);
        } else if (FAIL_RESOLUTION.equals(conflictResolution)) {
            throw new ProcessException(format("File with the same name '%s' already exists in '%s'.", filename, path), e);
        }
    }

    private void handleUnexpectedError(final ProcessSession session, FlowFile flowFile, final Exception e) {
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleExpectedError(final ProcessSession session, FlowFile flowFile, final BoxAPIResponseException e) {
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_CODE, valueOf(e.getResponseCode()));
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}