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
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.AzureConstants;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ ListAzureBlobStorage.class, FetchAzureBlobStorage.class })
@CapabilityDescription("Puts content into an Azure Storage Blob")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({ @WritesAttribute(attribute = "azure.container", description = "The name of the Azure container"),
        @WritesAttribute(attribute = "azure.blobname", description = "The name of the Azure blob"),
        @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for blob content"),
        @WritesAttribute(attribute = "azure.etag", description = "Etag for the Azure blob"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the blob"),
        @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob")})
public class PutAzureBlobStorage extends AbstractAzureBlobProcessor {

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        String containerName = context.getProperty(AzureConstants.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();

        String blobPath = context.getProperty(BLOB).evaluateAttributeExpressions(flowFile).getValue();

        AtomicReference<Exception> storedException = new AtomicReference<>();
        try {
            CloudStorageAccount storageAccount = createStorageConnection(context, flowFile);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            CloudBlob blob = container.getBlockBlobReference(blobPath);

            final Map<String, String> attributes = new HashMap<>();
            long length = flowFile.getSize();
            session.read(flowFile, rawIn -> {
                InputStream in = rawIn;
                if (!(in instanceof BufferedInputStream)) {
                    // do not double-wrap
                    in = new BufferedInputStream(rawIn);
                }

                try {
                    blob.upload(in, length);
                    BlobProperties properties = blob.getProperties();
                    attributes.put("azure.container", containerName);
                    attributes.put("azure.primaryUri", blob.getSnapshotQualifiedUri().toString());
                    attributes.put("azure.etag", properties.getEtag());
                    attributes.put("azure.length", String.valueOf(length));
                    attributes.put("azure.timestamp", String.valueOf(properties.getLastModified()));
                } catch (StorageException | URISyntaxException e) {
                    storedException.set(e);
                    throw new IOException(e);
                }
            });

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, blob.getSnapshotQualifiedUri().toString(), transferMillis);

        } catch (IllegalArgumentException | URISyntaxException | StorageException | ProcessException e) {
            if (e instanceof ProcessException && storedException.get() == null) {
                throw (ProcessException) e;
            } else {
                Exception failureException = Optional.ofNullable(storedException.get()).orElse(e);
                getLogger().error("Failed to put Azure blob {}", new Object[]{blobPath}, failureException);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }

    }
}
