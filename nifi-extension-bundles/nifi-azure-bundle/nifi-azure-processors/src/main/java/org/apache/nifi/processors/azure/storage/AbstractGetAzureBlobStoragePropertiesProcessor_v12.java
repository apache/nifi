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

public abstract class AbstractGetAzureBlobStoragePropertiesProcessor_v12 extends AbstractAzureBlobProcessor_v12 {

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

    static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("A blob with the supplied name was found in the container")
            .build();

    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("No blob was found with the supplied name in the container")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_FOUND, REL_NOT_FOUND, REL_FAILURE);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    protected abstract String getAttributePrefix();

    protected abstract Map<String, String> fetchProperties(BlobClient blobClient);

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String containerName = context.getProperty(CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final String blobName = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();

        try {
            final BlobServiceClient storageClient = getStorageClient(context, flowFile);
            final BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
            final BlobClient blobClient = containerClient.getBlobClient(blobName);

            final Map<String, String> newAttributes = new HashMap<>();
            fetchProperties(blobClient).forEach((key, value) ->
                newAttributes.put(getAttributePrefix().formatted(key), value)
            );

            flowFile = session.putAllAttributes(flowFile, newAttributes);
            final String eventDetails = String.format("Attributes added [%s]", String.join(", ", newAttributes.keySet()));
            session.getProvenanceReporter().modifyAttributes(flowFile, eventDetails);
            session.transfer(flowFile, REL_FOUND);
        } catch (final BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND) {
                getLogger().warn("Specified blob ({}) does not exist, routing to not found.", blobName);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Failed to retrieve properties for the specified blob ({}) from Azure Blob Storage. Routing to failure", blobName, e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }
}
