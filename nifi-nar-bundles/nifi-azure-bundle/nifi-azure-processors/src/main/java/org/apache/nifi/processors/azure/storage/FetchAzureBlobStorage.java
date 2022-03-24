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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.BlobRequestOptions;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@CapabilityDescription("Retrieves contents of an Azure Storage Blob, writing the contents to the content of the FlowFile")
@SeeAlso({ ListAzureBlobStorage.class, PutAzureBlobStorage.class, DeleteAzureBlobStorage.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "azure.length", description = "The length of the blob fetched")
})
public class FetchAzureBlobStorage extends AbstractAzureBlobProcessor {

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("The byte position at which to start reading from the blob. An empty value or a value of " +
                    "zero will start reading at the beginning of the blob.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("The number of bytes to download from the blob, starting from the Range Start. An empty " +
                    "value or a value that extends beyond the end of the blob will read to the end of the blob.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
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
        properties.add(RANGE_START);
        properties.add(RANGE_LENGTH);
        properties.add(AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE);
        properties.add(AzureBlobClientSideEncryptionUtils.CSE_KEY_ID);
        properties.add(AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX);
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final String blobPath = context.getProperty(BLOB).evaluateAttributeExpressions(flowFile).getValue();
        final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : 0L);
        final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : null);

        AtomicReference<Exception> storedException = new AtomicReference<>();
        try {
            CloudBlobClient blobClient = AzureStorageUtils.createCloudBlobClient(context, getLogger(), flowFile);
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            final Map<String, String> attributes = new HashMap<>();
            final CloudBlob blob = container.getBlockBlobReference(blobPath);

            BlobRequestOptions blobRequestOptions = createBlobRequestOptions(context);

            // TODO - we may be able do fancier things with ranges and
            // distribution of download over threads, investigate
            flowFile = session.write(flowFile, os -> {
                try {
                    blob.downloadRange(rangeStart, rangeLength, os, null, blobRequestOptions, operationContext);
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
        } catch (IllegalArgumentException | URISyntaxException | StorageException | ProcessException | DecoderException e) {
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
