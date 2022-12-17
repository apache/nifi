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
package org.apache.nifi.processors.azure.storage;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_PRIMARY_URI;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({DeleteAzureDataLakeStorage.class, FetchAzureDataLakeStorage.class, ListAzureDataLakeStorage.class})
@CapabilityDescription("Puts content into an Azure Data Lake Storage Gen 2")
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_FILESYSTEM, description = ATTR_DESCRIPTION_FILESYSTEM),
        @WritesAttribute(attribute = ATTR_NAME_DIRECTORY, description = ATTR_DESCRIPTION_DIRECTORY),
        @WritesAttribute(attribute = ATTR_NAME_FILENAME, description = ATTR_DESCRIPTION_FILENAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH)})
@InputRequirement(Requirement.INPUT_REQUIRED)
public class PutAzureDataLakeStorage extends AbstractAzureDataLakeStorageProcessor {

    public static final String FAIL_RESOLUTION = "fail";
    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";

    public static long MAX_CHUNK_SIZE = 100 * 1024 * 1024; // current chunk limit is 100 MiB on Azure

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(FAIL_RESOLUTION, REPLACE_RESOLUTION, IGNORE_RESOLUTION)
            .build();

    public static final PropertyDescriptor BASE_TEMPORARY_PATH = new PropertyDescriptor.Builder()
            .name("base-temporary-path")
            .displayName("Base Temporary Path")
            .description("The Path where the temporary directory will be created. The Path name cannot contain a leading '/'." +
                    " The root directory can be designated by the empty string value. Non-existing directories will be created." +
                    "The Temporary File Directory name is " + TEMP_FILE_DIRECTORY)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new DirectoryValidator("Base Temporary Path"))
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            FILE,
            BASE_TEMPORARY_PATH,
            CONFLICT_RESOLUTION,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        try {
            final String fileSystem = evaluateFileSystemProperty(context, flowFile);
            final String originalDirectory = evaluateDirectoryProperty(context, flowFile);
            final String tempPath = evaluateDirectoryProperty(context, flowFile, BASE_TEMPORARY_PATH);
            final String tempDirectory = createPath(tempPath, TEMP_FILE_DIRECTORY);
            final String fileName = evaluateFileNameProperty(context, flowFile);

            final DataLakeFileSystemClient fileSystemClient = getFileSystemClient(context, flowFile, fileSystem);
            final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(originalDirectory);

            final String tempFilePrefix = UUID.randomUUID().toString();
            final DataLakeDirectoryClient tempDirectoryClient = fileSystemClient.getDirectoryClient(tempDirectory);
            final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();

            final DataLakeFileClient tempFileClient = tempDirectoryClient.createFile(tempFilePrefix + fileName, true);
            appendContent(flowFile, tempFileClient, session);
            createDirectoryIfNotExists(directoryClient);

            final String fileUrl = renameFile(tempFileClient, directoryClient.getDirectoryPath(), fileName, conflictResolution);
            if (fileUrl != null) {
                final Map<String, String> attributes = createAttributeMap(flowFile, fileSystem, originalDirectory, fileName, fileUrl);
                flowFile = session.putAllAttributes(flowFile, attributes);

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, fileUrl, transferMillis);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to create file on Azure Data Lake Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private DataLakeFileSystemClient getFileSystemClient(ProcessContext context, FlowFile flowFile, String fileSystem) {
        final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);
        return storageClient.getFileSystemClient(fileSystem);
    }

    private Map<String, String> createAttributeMap(FlowFile flowFile, String fileSystem, String originalDirectory, String fileName, String fileUrl) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_NAME_FILESYSTEM, fileSystem);
        attributes.put(ATTR_NAME_DIRECTORY, originalDirectory);
        attributes.put(ATTR_NAME_FILENAME, fileName);
        attributes.put(ATTR_NAME_PRIMARY_URI, fileUrl);
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(flowFile.getSize()));
        return attributes;
    }

    private void createDirectoryIfNotExists(DataLakeDirectoryClient directoryClient) {
        if (!directoryClient.getDirectoryPath().isEmpty() && !directoryClient.exists()) {
            directoryClient.create();
        }
    }

    //Visible for testing
    void appendContent(FlowFile flowFile, DataLakeFileClient fileClient, ProcessSession session) throws IOException {
        final long length = flowFile.getSize();
        if (length > 0) {
            try (final InputStream rawIn = session.read(flowFile); final BufferedInputStream bufferedIn = new BufferedInputStream(rawIn)) {
                uploadContent(fileClient, bufferedIn, length);
            } catch (Exception e) {
                removeTempFile(fileClient);
                throw e;
            }
        }
    }

    //Visible for testing
    static void uploadContent(DataLakeFileClient fileClient, InputStream in, long length) {
        long chunkStart = 0;
        long chunkSize;

        while (chunkStart < length) {
            chunkSize = Math.min(length - chunkStart, MAX_CHUNK_SIZE);

            // com.azure.storage.common.Utility.convertStreamToByteBuffer() throws an exception
            // if there are more available bytes in the stream after reading the chunk
            BoundedInputStream boundedIn = new BoundedInputStream(in, chunkSize);

            fileClient.append(boundedIn, chunkStart, chunkSize);

            chunkStart += chunkSize;
        }

        // use overwrite mode due to https://github.com/Azure/azure-sdk-for-java/issues/31248
        fileClient.flush(length, true);
    }

    /**
     * This method serves as a "commit" for the upload process. Upon upload, a 0-byte file is created, then the payload is appended to it.
     * Because of that, a work-in-progress file is available for readers before the upload is complete. It is not an efficient approach in
     * case of conflicts because FlowFiles are uploaded unnecessarily, but it is a calculated risk because consistency is more important.
     *
     * Visible for testing
     *
     * @param sourceFileClient client of the temporary file
     * @param destinationDirectory final location of the uploaded file
     * @param destinationFileName final name of the uploaded file
     * @param conflictResolution conflict resolution strategy
     * @return URL of the uploaded file
     */
    String renameFile(final DataLakeFileClient sourceFileClient, final String destinationDirectory, final String destinationFileName, final String conflictResolution) {
        final String destinationPath = createPath(destinationDirectory, destinationFileName);

        try {
            final DataLakeRequestConditions destinationCondition = new DataLakeRequestConditions();
            if (!conflictResolution.equals(REPLACE_RESOLUTION)) {
                destinationCondition.setIfNoneMatch("*");
            }
            return sourceFileClient.renameWithResponse(null, destinationPath, null, destinationCondition, null, null).getValue().getFileUrl();
        } catch (DataLakeStorageException dataLakeStorageException) {
            removeTempFile(sourceFileClient);
            if (dataLakeStorageException.getStatusCode() == 409 && conflictResolution.equals(IGNORE_RESOLUTION)) {
                getLogger().info("File [{}] already exists. Remote file not modified due to {} being set to '{}'.",
                        destinationPath, CONFLICT_RESOLUTION.getDisplayName(), conflictResolution);
                return null;
            } else if (dataLakeStorageException.getStatusCode() == 409 && conflictResolution.equals(FAIL_RESOLUTION)) {
                throw new ProcessException(String.format("File [%s] already exists.", destinationPath), dataLakeStorageException);
            } else {
                throw new ProcessException(String.format("Renaming File [%s] failed", destinationPath), dataLakeStorageException);
            }
        }
    }

    private String createPath(final String baseDirectory, final String path) {
        return StringUtils.isNotBlank(baseDirectory)
                ? baseDirectory + "/" + path
                : path;
    }

    private void removeTempFile(final DataLakeFileClient fileClient) {
        try {
            fileClient.delete();
        } catch (Exception e) {
            getLogger().error("Renaming File [{}] failed", fileClient.getFileName(), e);
        }
    }
}
