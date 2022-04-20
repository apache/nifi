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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(FAIL_RESOLUTION, REPLACE_RESOLUTION, IGNORE_RESOLUTION)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            CONFLICT_RESOLUTION
    ));

    public static long MAX_CHUNK_SIZE = 100 * 1024 * 1024; // current chunk limit is 100 MiB on Azure

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

        fileClient.flush(length);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.of(super.getSupportedPropertyDescriptors(), PROPERTIES)
                .flatMap(Collection::stream)
                .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
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
            final String directory = evaluateDirectoryProperty(context, flowFile);
            final String fileName = evaluateFileNameProperty(context, flowFile);

            final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);
            final DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);
            final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
            final DataLakeFileClient fileClient;

            final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
            boolean overwrite = conflictResolution.equals(REPLACE_RESOLUTION);

            try {
                fileClient = directoryClient.createFile(fileName, overwrite);

                final long length = flowFile.getSize();
                if (length > 0) {
                    try (final InputStream rawIn = session.read(flowFile); final BufferedInputStream bufferedIn = new BufferedInputStream(rawIn)) {
                        uploadContent(fileClient, bufferedIn, length);
                    } catch (Exception e) {
                        removeTempFile(fileClient);
                        throw e;
                    }
                }

                final Map<String, String> attributes = new HashMap<>();
                attributes.put(ATTR_NAME_FILESYSTEM, fileSystem);
                attributes.put(ATTR_NAME_DIRECTORY, directory);
                attributes.put(ATTR_NAME_FILENAME, fileName);
                attributes.put(ATTR_NAME_PRIMARY_URI, fileClient.getFileUrl());
                attributes.put(ATTR_NAME_LENGTH, String.valueOf(length));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, fileClient.getFileUrl(), transferMillis);
            } catch (DataLakeStorageException dlsException) {
                if (dlsException.getStatusCode() == 409) {
                    if (conflictResolution.equals(IGNORE_RESOLUTION)) {
                        session.transfer(flowFile, REL_SUCCESS);
                        String warningMessage = String.format("File with the same name already exists. " +
                                "Remote file not modified. " +
                                "Transferring {} to success due to %s being set to '%s'.", CONFLICT_RESOLUTION.getDisplayName(), conflictResolution);
                        getLogger().warn(warningMessage, new Object[]{flowFile});
                    } else {
                        throw dlsException;
                    }
                } else {
                    throw dlsException;
                }
            }
        } catch (Exception e) {
            getLogger().error("Failed to create file on Azure Data Lake Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void removeTempFile(DataLakeFileClient fileClient) {
        try {
            fileClient.delete();
        } catch (Exception e) {
            getLogger().error("Error while removing temp file on Azure Data Lake Storage", e);
        }
    }
}
