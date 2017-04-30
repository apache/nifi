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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.AzureConstants;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@CapabilityDescription("Retrieves contents of an Azure Storage Blob, writing the contents to the content of the FlowFile")
@SeeAlso({ ListAzureBlobStorage.class, PutAzureBlobStorage.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "azure.length", description = "The length of the blob fetched")
})
public class FetchAzureBlobStorage extends AbstractAzureBlobProcessor {

    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, BLOB));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
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

            final Map<String, String> attributes = new HashMap<>();
            final CloudBlob blob = container.getBlockBlobReference(blobPath);

            // TODO - we may be able do fancier things with ranges and
            // distribution of download over threads, investigate
            flowFile = session.write(flowFile, os -> {
                try {
                    blob.download(os);
                } catch (StorageException e) {
                    storedException.set(e);
                    throw new IOException(e);
                }
            });

            long length = blob.getProperties().getLength();
            attributes.put("azure.length", String.valueOf(length));

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            session.transfer(flowFile, REL_SUCCESS);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, blob.getSnapshotQualifiedUri().toString(), transferMillis);
        } catch (IllegalArgumentException | URISyntaxException | StorageException | ProcessException e) {
            if (e instanceof ProcessException && storedException.get() == null) {
                throw (ProcessException) e;
            } else {
                Exception failureException = Optional.ofNullable(storedException.get()).orElse(e);
                getLogger().error("Failure to fetch Azure blob {}",  new Object[]{blobPath}, failureException);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }
}
