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
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo.Builder;
import org.apache.nifi.processors.standard.AbstractListProcessor;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

@TriggerSerially
@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ FetchAzureBlobStorage.class })
@CapabilityDescription("Lists blobs in an Azure Storage container. Listing details are attached to an empty FlowFile for use with FetchAzureBlobStorage")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({ @WritesAttribute(attribute = "azure.container", description = "The name of the azure container"),
        @WritesAttribute(attribute = "azure.blobname", description = "The name of the azure blob"), @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for blob content"),
        @WritesAttribute(attribute = "azure.secondaryUri", description = "Secondary location for blob content"), @WritesAttribute(attribute = "azure.etag", description = "Etag for the Azure blob"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the blob"), @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob"),
        @WritesAttribute(attribute = "mime.type", description = "MimeType of the content"), @WritesAttribute(attribute = "lang", description = "Language code for the content"),
        @WritesAttribute(attribute = "azure.blobtype", description = "This is the type of blob and can be either page or block type") })
@Stateful(scopes = { Scope.LOCAL, Scope.CLUSTER }, description = "After performing a listing of blobs, the timestamp of the newest blob is stored. "
        + "This allows the Processor to list only blobs that have been added or modified after " + "this date the next time that the Processor is run.")
public class ListAzureBlobStorage extends AbstractListProcessor<BlobInfo> {

    private static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder().name("Prefix").description("Search prefix for listing").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true).required(false).build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, PREFIX));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Map<String, String> createAttributes(BlobInfo entity, ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.etag", entity.getEtag());
        attributes.put("azure.primaryUri", entity.getPrimaryUri());
        attributes.put("azure.secondaryUri", entity.getSecondaryUri());
        attributes.put("azure.blobname", entity.getName());
        attributes.put("azure.blobtype", entity.getBlobType());
        attributes.put("azure.length", String.valueOf(entity.getLength()));
        attributes.put("azure.timestamp", String.valueOf(entity.getTimestamp()));
        attributes.put("mime.type", entity.getContentType());
        attributes.put("lang", entity.getContentLanguage());

        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(AzureConstants.CONTAINER).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        // TODO - implement
        return false;
    }

    @Override
    protected Scope getStateScope(final ProcessContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected List<BlobInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        String containerName = context.getProperty(AzureConstants.CONTAINER).evaluateAttributeExpressions().getValue();
        String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();
        if (prefix == null) {
            prefix = "";
        }
        final List<BlobInfo> listing = new ArrayList<>();
        try {
            CloudStorageAccount storageAccount = createStorageConnection(context);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            BlobRequestOptions blobRequestOptions = null;
            OperationContext operationContext = null;

            for (ListBlobItem blob : container.listBlobs(prefix, true, EnumSet.of(BlobListingDetails.METADATA), blobRequestOptions, operationContext)) {
                if (blob instanceof CloudBlob) {
                    CloudBlob cloudBlob = (CloudBlob) blob;
                    BlobProperties properties = cloudBlob.getProperties();
                    StorageUri uri = cloudBlob.getSnapshotQualifiedStorageUri();

                    Builder builder = new BlobInfo.Builder().primaryUri(uri.getPrimaryUri().toString()).secondaryUri(uri.getSecondaryUri().toString()).contentType(properties.getContentType())
                            .contentLanguage(properties.getContentLanguage()).etag(properties.getEtag()).lastModifiedTime(properties.getLastModified().getTime()).length(properties.getLength());

                    if (blob instanceof CloudBlockBlob) {
                        builder.blobType(AzureConstants.BLOCK);
                    } else {
                        builder.blobType(AzureConstants.PAGE);
                    }
                    listing.add(builder.build());
                }
            }
        } catch (IllegalArgumentException | URISyntaxException | StorageException e) {
            throw (new IOException(e));
        }
        return listing;
    }

    protected static CloudStorageAccount createStorageConnection(ProcessContext context) {
        final String accountName = context.getProperty(AzureConstants.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String accountKey = context.getProperty(AzureConstants.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        final String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s", accountName, accountKey);
        try {
            return createStorageAccountFromConnectionString(storageConnectionString);
        } catch (InvalidKeyException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Validates the connection string and returns the storage account. The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString
     *            Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     */
    private static CloudStorageAccount createStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException {

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
        } catch (IllegalArgumentException | URISyntaxException e) {
            System.out.println("\nConnection string specifies an invalid URI.");
            System.out.println("Please confirm the connection string is in the Azure connection string format.");
            throw e;
        } catch (InvalidKeyException e) {
            System.out.println("\nConnection string specifies an invalid key.");
            System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
            throw e;
        }
        return storageAccount;
    }

}
