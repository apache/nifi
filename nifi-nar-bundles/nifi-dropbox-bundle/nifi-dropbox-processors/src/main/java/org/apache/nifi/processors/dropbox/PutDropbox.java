/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxUploader;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.UploadSessionAppendV2Uploader;
import com.dropbox.core.v2.files.UploadSessionCursor;
import com.dropbox.core.v2.files.UploadSessionFinishUploader;
import com.dropbox.core.v2.files.UploadSessionStartUploader;
import com.dropbox.core.v2.files.UploadUploader;
import com.dropbox.core.v2.files.WriteMode;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
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
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

/**
 * This processor uploads objects to Dropbox.
 */
@SeeAlso({ListDropbox.class, FetchDropbox.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"dropbox", "storage", "put"})
@CapabilityDescription("Puts content to a Dropbox folder.")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Dropbox object.")
public class PutDropbox extends AbstractProcessor implements DropboxTrait {

    public static final int SINGLE_UPLOAD_LIMIT_IN_BYTES = 150 * 1024 * 1024;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to Dropbox are transferred to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to Dropbox for some reason are transferred to this relationship.")
            .build();

    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("folder")
            .displayName("Folder")
            .description("The path of the Dropbox folder to upload files to. "
                    + "The folder will be created if it does not exist yet.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("/.*")))
            .defaultValue("/")
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("Filename")
            .description("The full name of the file to upload.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${filename}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor UPLOAD_CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("upload-chunk-size")
            .displayName("Upload Chunk Size")
            .description("Defines the size of a chunk. Used when a FlowFile's size exceeds CHUNK_UPLOAD_LIMIT and content is uploaded in smaller chunks. "
                    + "It is recommended to specify chunk size as multiples of 4 MB.")
            .defaultValue("8 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, SINGLE_UPLOAD_LIMIT_IN_BYTES))
            .required(false)
            .build();

    public static final PropertyDescriptor CHUNK_UPLOAD_LIMIT = new PropertyDescriptor.Builder()
            .name("chunk-upload-limit")
            .displayName("Chunk Upload Limit")
            .description("The maximum size of the content which is uploaded at once. FlowFiles larger than this limit are uploaded in chunks. "
                    + "Maximum allowed value is 150 MB.")
            .defaultValue("150 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, SINGLE_UPLOAD_LIMIT_IN_BYTES))
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            CREDENTIAL_SERVICE,
            FOLDER,
            FILE_NAME,
            UPLOAD_CHUNK_SIZE,
            CHUNK_UPLOAD_LIMIT,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(false, ProxySpec.HTTP_AUTH)
    ));


    private static final Set<Relationship> RELATIONSHIPS;

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(rels);
    }

    private DbxClientV2 dropboxApiClient;

    private DbxUploader dbxUploader;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dropboxApiClient = getDropboxApiClient(context, getIdentifier());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String folder = context.getProperty(FOLDER).evaluateAttributeExpressions(flowFile).getValue();
        final String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final long uploadFileSizeLimit = context.getProperty(CHUNK_UPLOAD_LIMIT)
                .asDataSize(DataUnit.B)
                .longValue();

        FileMetadata uploadedFileMetadata = null;

        long size = flowFile.getSize();
        final String uploadPath = convertFolderName(folder) + "/" + filename;

        try (final InputStream rawIn = session.read(flowFile)) {
            if (size <= uploadFileSizeLimit) {
                uploadedFileMetadata = createUploadUploader(uploadPath).uploadAndFinish(rawIn);
            } else {
                long chunkSize = context.getProperty(UPLOAD_CHUNK_SIZE)
                        .asDataSize(DataUnit.B)
                        .longValue();

                uploadedFileMetadata = uploadLargeFileInChunks(rawIn, size, chunkSize, uploadPath);
            }
        } catch (Exception e) {
            getLogger().error("Exception occurred while uploading file '{}' to Dropbox folder '{}'", filename, folder, e);
        } finally {
            dbxUploader.close();
        }

        if (uploadedFileMetadata != null) {
            session.transfer(flowFile, REL_SUCCESS);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @OnUnscheduled
    public void shutdown() {
        if (dbxUploader != null) {
            dbxUploader.close();
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private FileMetadata uploadLargeFileInChunks(InputStream rawIn, long size, long uploadChunkSize, String path) throws Exception {
        final String sessionId = createUploadSessionStartUploader()
                .uploadAndFinish(rawIn, uploadChunkSize)
                .getSessionId();

        long uploadedBytes = uploadChunkSize;

        UploadSessionCursor cursor = new UploadSessionCursor(sessionId, uploadedBytes);

        while (size - uploadedBytes > uploadChunkSize) {
            createUploadSessionAppendV2Uploader(cursor).uploadAndFinish(rawIn, uploadChunkSize);
            uploadedBytes += uploadChunkSize;
            cursor = new UploadSessionCursor(sessionId, uploadedBytes);
        }

        final long remainingBytes = size - uploadedBytes;

        final CommitInfo commitInfo = CommitInfo.newBuilder(path)
                .withMode(WriteMode.ADD)
                .withClientModified(new Date(System.currentTimeMillis()))
                .build();

        return createUploadSessionFinishUploader(cursor, commitInfo).uploadAndFinish(rawIn, remainingBytes);
    }

    private UploadUploader createUploadUploader(String path) throws DbxException {
        final UploadUploader uploadUploader = dropboxApiClient
                .files()
                .uploadBuilder(path)
                .withMode(WriteMode.ADD)
                .start();
        dbxUploader = uploadUploader;
        return uploadUploader;
    }

    private UploadSessionStartUploader createUploadSessionStartUploader() throws DbxException {
        final UploadSessionStartUploader sessionStartUploader = dropboxApiClient
                .files()
                .uploadSessionStart();
        dbxUploader = sessionStartUploader;
        return sessionStartUploader;
    }

    private UploadSessionAppendV2Uploader createUploadSessionAppendV2Uploader(UploadSessionCursor cursor) throws DbxException {
        final UploadSessionAppendV2Uploader sessionAppendV2Uploader = dropboxApiClient
                .files()
                .uploadSessionAppendV2(cursor);
        dbxUploader = sessionAppendV2Uploader;
        return sessionAppendV2Uploader;
    }

    private UploadSessionFinishUploader createUploadSessionFinishUploader(UploadSessionCursor cursor, CommitInfo commitInfo) throws DbxException {
        final UploadSessionFinishUploader sessionFinishUploader = dropboxApiClient
                .files()
                .uploadSessionFinish(cursor, commitInfo);
        dbxUploader = sessionFinishUploader;
        return sessionFinishUploader;
    }
}
