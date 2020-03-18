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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Locale;

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
import org.apache.commons.lang3.StringUtils;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import com.azure.storage.file.datalake.implementation.models.StorageErrorException;

@Tags({ "azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake" })
@CapabilityDescription("Puts content into an Azure Data Lake Storage Gen 2")
@SeeAlso({})
@WritesAttributes({ @WritesAttribute(attribute = "azure.filesystem", description = "The name of the Azure File System"),
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
        final String accountName = context.getProperty(ACCOUNT_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String accountKey = context.getProperty(ACCOUNT_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String sasToken = context.getProperty(PROP_SAS_TOKEN).evaluateAttributeExpressions(flowFile).getValue();
        final String endpoint = String.format(Locale.ROOT, "https://%s.dfs.core.windows.net", accountName);
        DataLakeServiceClient storageClient;
                if (StringUtils.isNotBlank(accountKey)) {
                    final StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName,
                        accountKey);
                    storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).credential(credential)
                        .buildClient();
                } else if (StringUtils.isNotBlank(sasToken)) {
                    storageClient = new DataLakeServiceClientBuilder().endpoint(endpoint).sasToken(sasToken)
                        .buildClient();
                } else {
                    throw new IllegalArgumentException(String.format("Either '%s' or '%s' must be defined.",
                        ACCOUNT_KEY.getDisplayName(), PROP_SAS_TOKEN.getDisplayName()));
                }
        final DataLakeFileSystemClient dataLakeFileSystemClient = storageClient.getFileSystemClient(fileSystem);
        final DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient(directory);

        final long length = flowFile.getSize();
        final Map<String, String> attributes = new HashMap<>();
        final DataLakeFileClient fileClient = directoryClient.createFile(fileName);
        if (length > 0) {
            try (final InputStream rawIn = session.read(flowFile); final BufferedInputStream in = new BufferedInputStream(rawIn)) {
                fileClient.append( in , 0, length);

            } catch (final IOException|StorageErrorException e) {
                getLogger().error("Failed to create file. Reasons: " + e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }
        }
        attributes.put("azure.filesystem", fileSystem);
        attributes.put("azure.filename", fileName);
        attributes.put("azure.primaryUri", fileClient.getFileUrl().toString());
        attributes.put("azure.length", String.valueOf(length));
        fileClient.flush(length);

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
          }
        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, fileClient.getFileUrl().toString(), transferMillis);
        } catch (IllegalArgumentException e) {
        getLogger().error("Failed to create file. Reasons: " + e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}
}
