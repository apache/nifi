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

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
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

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
            .displayName("Blob Name")
            .description("The filename of the blob")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${filename}")
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

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        AzureStorageUtils.CONTAINER,
        BLOB_NAME,
        CREATE_CONTAINER,
        AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE,
        AzureStorageUtils.ACCOUNT_NAME,
        AzureStorageUtils.ACCOUNT_KEY,
        AzureStorageUtils.PROP_SAS_TOKEN,
        AzureStorageUtils.ENDPOINT_SUFFIX,
        AzureStorageUtils.PROXY_CONFIGURATION_SERVICE,
        AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE,
        AzureBlobClientSideEncryptionUtils.CSE_KEY_ID,
        AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX
    ));

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        results.addAll(AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext));
        return results;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final String blobPath = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final boolean createContainer = context.getProperty(CREATE_CONTAINER).asBoolean();

        try {
            final CloudBlobClient blobClient = AzureStorageUtils.createCloudBlobClient(context, getLogger(), flowFile);
            final CloudBlobContainer container = blobClient.getContainerReference(containerName);

            if (createContainer) {
                container.createIfNotExists();
            }

            final CloudBlob blob = container.getBlockBlobReference(blobPath);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            final BlobRequestOptions blobRequestOptions = createBlobRequestOptions(context);

            try (final InputStream rawIn = session.read(flowFile);
                 final InputStream bufferedIn = new BufferedInputStream(rawIn);
                 final InputStream in = new UnmarkableInputStream(bufferedIn)) {

                uploadBlob(blob, in, blobRequestOptions, operationContext);
            }

            final Map<String, String> attributes = getAttributes(blob, flowFile, containerName);
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, blob.getSnapshotQualifiedUri().toString(), transferMillis);
        } catch (final Exception e) {
            getLogger().error("Failed to put Azure blob {} for {}", blobPath, flowFile, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    // Method to uplaod the blob. Exists solely for the purpose of mocking out the interaction within unit tests
    protected void uploadBlob(final CloudBlob blob, final InputStream in, final BlobRequestOptions blobRequestOptions, final OperationContext operationContext) throws IOException, StorageException {
        blob.upload(in, -1, null, blobRequestOptions, operationContext);
    }

    private Map<String, String> getAttributes(final CloudBlob blob, final FlowFile flowFile, final String containerName) throws URISyntaxException, StorageException {
        final Map<String, String> attributes = new HashMap<>();
        final long length = flowFile.getSize();
        final BlobProperties properties = blob.getProperties();
        attributes.put("azure.container", containerName);
        attributes.put("azure.primaryUri", blob.getSnapshotQualifiedUri().toString());
        attributes.put("azure.etag", properties.getEtag());
        attributes.put("azure.length", String.valueOf(length));
        attributes.put("azure.timestamp", String.valueOf(properties.getLastModified()));

        return attributes;
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
        public void reset() {
        }

        @Override
        public boolean markSupported() {
            return false;
        }
    }
}
