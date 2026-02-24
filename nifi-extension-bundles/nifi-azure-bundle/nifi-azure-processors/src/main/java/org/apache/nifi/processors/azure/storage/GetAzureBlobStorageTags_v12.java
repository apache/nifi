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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, PutAzureBlobStorage_v12.class,
        CopyAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class, GetAzureBlobStorageMetadata_v12.class })
@CapabilityDescription("Retrieves user metadata and/or tags from the specified blob from Azure Blob Storage. The processor uses Azure Blob Storage client library v12.")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class GetAzureBlobStorageTags_v12 extends AbstractAzureBlobProcessor_v12 {

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.CONTAINER)
            .defaultValue(String.format("${%s}", ATTR_NAME_CONTAINER))
            .build();

    public static final PropertyDescriptor BLOB_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractAzureBlobProcessor_v12.BLOB_NAME)
            .defaultValue(String.format("${%s}", ATTR_NAME_BLOBNAME))
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            BLOB_STORAGE_CREDENTIALS_SERVICE,
            CONTAINER,
            BLOB_NAME,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    );

    static Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("A blob with the supplied name was found in the container")
            .build();

    static Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("No blob was found with the supplied name in the container")
            .build();

    private static final Set<Relationship> relationships = Set.of(REL_FOUND, REL_NOT_FOUND,  REL_FAILURE);

    private static final String ATTRIBUTE_FORMAT_TAG = "azure.tag.%s";

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

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

        String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        String blobName = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final Map<String, String> newAttributes = new HashMap<>();

        try {
            BlobServiceClient storageClient = getStorageClient(context, flowFile);
            BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            Map<String, String> tags = blobClient.getTags();
            tags.forEach((key, value) -> {
                newAttributes.put(ATTRIBUTE_FORMAT_TAG.formatted(key), value);
            });

            flowFile = session.putAllAttributes(flowFile, newAttributes);
            session.transfer(flowFile, REL_FOUND);
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                getLogger().warn("Specified blob ({}) does not exist, routing to not found.", blobName);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Failed to retrieve metadata for the specified blob ({}) from Azure Blob Storage. Routing to failure", blobName, e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }
}

