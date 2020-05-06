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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({DeleteAzureDataLakeStorage.class, FetchAzureDataLakeStorage.class})
@CapabilityDescription("Puts content into an Azure Data Lake Storage Gen 2")
@WritesAttributes({@WritesAttribute(attribute = "azure.filesystem", description = "The name of the Azure File System"),
        @WritesAttribute(attribute = "azure.directory", description = "The name of the Azure Directory"),
        @WritesAttribute(attribute = "azure.filename", description = "The name of the Azure File Name"),
        @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for file content"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the file")})
@InputRequirement(Requirement.INPUT_REQUIRED)
public class PutAzureDataLakeStorage extends AbstractAzureDataLakeStorageProcessor {

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        try {
            final String fileSystem = context.getProperty(FILESYSTEM).evaluateAttributeExpressions(flowFile).getValue();
            final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
            final String fileName = context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue();

            if (StringUtils.isBlank(fileSystem)) {
                throw new ProcessException(FILESYSTEM.getDisplayName() + " property evaluated to empty string. " +
                        FILESYSTEM.getDisplayName() + " must be specified as a non-empty string.");
            }
            if (StringUtils.isBlank(fileName)) {
                throw new ProcessException(FILE.getDisplayName() + " property evaluated to empty string. " +
                        FILE.getDisplayName() + " must be specified as a non-empty string.");
            }

            final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);
            final DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);
            final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
            final DataLakeFileClient fileClient = directoryClient.createFile(fileName);

            final long length = flowFile.getSize();
            if (length > 0) {
                try (final InputStream rawIn = session.read(flowFile); final BufferedInputStream in = new BufferedInputStream(rawIn)) {
                    fileClient.append(in, 0, length);
                }
            }
            fileClient.flush(length);

            final Map<String, String> attributes = new HashMap<>();
            attributes.put("azure.filesystem", fileSystem);
            attributes.put("azure.directory", directory);
            attributes.put("azure.filename", fileName);
            attributes.put("azure.primaryUri", fileClient.getFileUrl());
            attributes.put("azure.length", String.valueOf(length));
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.transfer(flowFile, REL_SUCCESS);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, fileClient.getFileUrl(), transferMillis);
        } catch (Exception e) {
            getLogger().error("Failed to create file", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}