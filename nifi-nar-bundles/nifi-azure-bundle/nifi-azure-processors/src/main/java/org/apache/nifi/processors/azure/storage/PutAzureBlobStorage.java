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
import java.io.FilterInputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.OperationContext;
import org.apache.commons.codec.DecoderException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.BlobRequestOptions;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ ListAzureBlobStorage.class, FetchAzureBlobStorage.class, DeleteAzureBlobStorage.class })
@CapabilityDescription("Puts content into an Azure Storage Blob")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({ @WritesAttribute(attribute = "azure.container", description = "The name of the Azure container"),
        @WritesAttribute(attribute = "azure.blobname", description = "The name of the Azure blob"),
        @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for blob content"),
        @WritesAttribute(attribute = "azure.etag", description = "Etag for the Azure blob"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the blob"),
        @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob")})
public class PutAzureBlobStorage extends AbstractAzureBlobProcessor {

    public static final PropertyDescriptor BLOB_NAME = new PropertyDescriptor.Builder()
            .name("blob")
            .displayName("Blob")
            .description("The filename of the blob")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor CREATE_CONTAINER = new PropertyDescriptor.Builder()
            .name("azure-create-container")
            .displayName("Create Container")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to check if the container exists and to automatically create it if it does not. " +
                  "Permission to list containers is required. If false, this check is not made, but the Put operation " +
                  "will fail if the container does not exist.")
            .build();

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        results.addAll(AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext));
        return results;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.remove(BLOB);
        properties.add(BLOB_NAME);
        properties.add(CREATE_CONTAINER);
        properties.add(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE);
        properties.add(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID);
        properties.add(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX);
        return properties;
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();

        String blobPath = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final boolean createContainer = context.getProperty(CREATE_CONTAINER).asBoolean();

        AtomicReference<Exception> storedException = new AtomicReference<>();
        try {
            CloudBlobClient blobClient = AzureStorageUtils.createCloudBlobClient(context, getLogger(), flowFile);
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            if (createContainer)
                container.createIfNotExists();

            CloudBlob blob = container.getBlockBlobReference(blobPath);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            BlobRequestOptions blobRequestOptions = createBlobRequestOptions(context);

            final Map<String, String> attributes = new HashMap<>();
            long length = flowFile.getSize();
            session.read(flowFile, rawIn -> {
                InputStream in = rawIn;
                if (!(in instanceof BufferedInputStream)) {
                    // do not double-wrap
                    in = new BufferedInputStream(rawIn);
                }

                // If markSupported() is true and a file length is provided,
                // Blobs are not uploaded in blocks resulting in OOME for large
                // files. The UnmarkableInputStream wrapper class disables
                // mark() and reset() to help force uploading files in chunks.
                if (in.markSupported()) {
                    in = new UnmarkableInputStream(in);
                }

                try {
                    uploadBlob(blob, operationContext, blobRequestOptions, in);
                    BlobProperties properties = blob.getProperties();
                    attributes.put("azure.container", containerName);
                    attributes.put("azure.primaryUri", blob.getSnapshotQualifiedUri().toString());
                    attributes.put("azure.etag", properties.getEtag());
                    attributes.put("azure.length", String.valueOf(length));
                    attributes.put("azure.timestamp", String.valueOf(properties.getLastModified()));
                } catch (StorageException | URISyntaxException | IOException e) {
                    storedException.set(e);
                    throw e instanceof IOException ? (IOException) e : new IOException(e);
                }
            });

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, blob.getSnapshotQualifiedUri().toString(), transferMillis);

        } catch (IllegalArgumentException | URISyntaxException | StorageException | ProcessException | DecoderException e) {
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

    @VisibleForTesting
    void uploadBlob(CloudBlob blob, OperationContext operationContext, BlobRequestOptions blobRequestOptions, InputStream in) throws StorageException, IOException {
        blob.upload(in, -1, null, blobRequestOptions, operationContext);
    }

    // Used to help force Azure Blob SDK to write in blocks
    private static class UnmarkableInputStream extends FilterInputStream {
        public UnmarkableInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void mark(int readlimit) {
        }

        @Override
        public void reset() throws IOException {
        }

        @Override
        public boolean markSupported() {
            return false;
        }
    }
}
