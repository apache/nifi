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
package org.apache.nifi.processors.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobProperties;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.BlobServiceClientFactory;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.getProxyOptions;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

public abstract class AbstractAzureBlobProcessor_v12 extends AbstractProcessor {

    public static final PropertyDescriptor BLOB_NAME = new PropertyDescriptor.Builder()
            .name("blob-name")
            .displayName("Blob Name")
            .description("The full name of the blob")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully processed FlowFiles are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Unsuccessful operations will be transferred to the failure relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile BlobServiceClientFactory clientFactory;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        clientFactory = new BlobServiceClientFactory(getLogger(), getProxyOptions(context));
    }

    @OnStopped
    public void onStopped() {
        clientFactory = null;
    }

    protected BlobServiceClient getStorageClient(PropertyContext context, FlowFile flowFile) {
        return getStorageClient(context, BLOB_STORAGE_CREDENTIALS_SERVICE, flowFile);
    }

    protected BlobServiceClient getStorageClient(PropertyContext context, PropertyDescriptor storageCredentialsServiceProperty, FlowFile flowFile) {
        final Map<String, String> attributes = flowFile != null ? flowFile.getAttributes() : Collections.emptyMap();

        final AzureStorageCredentialsService_v12 credentialsService = context.getProperty(storageCredentialsServiceProperty).asControllerService(AzureStorageCredentialsService_v12.class);
        final AzureStorageCredentialsDetails_v12 credentialsDetails = credentialsService.getCredentialsDetails(attributes);

        return clientFactory.getStorageClient(credentialsDetails);
    }

    protected Map<String, String> createBlobAttributesMap(BlobClient blobClient) {
        final Map<String, String> attributes = new HashMap<>();
        applyStandardBlobAttributes(attributes, blobClient);
        applyBlobMetadata(attributes, blobClient);
        return attributes;
    }

    protected void applyStandardBlobAttributes(Map<String, String> attributes, BlobClient blobClient) {
        String primaryUri = blobClient.getBlobUrl().replace("%2F", "/");
        attributes.put(ATTR_NAME_CONTAINER, blobClient.getContainerName());
        attributes.put(ATTR_NAME_BLOBNAME, blobClient.getBlobName());
        attributes.put(ATTR_NAME_PRIMARY_URI, primaryUri);
    }

    protected void applyBlobMetadata(Map<String, String> attributes, BlobClient blobClient) {
        Supplier<BlobProperties> props = new Supplier<>() {
            BlobProperties properties;
            public BlobProperties get() {
                if (properties == null) {
                    properties = blobClient.getProperties();
                }
                return properties;
            }
        };

        attributes.computeIfAbsent(ATTR_NAME_ETAG, key -> props.get().getETag());
        attributes.computeIfAbsent(ATTR_NAME_BLOBTYPE, key -> props.get().getBlobType().toString());
        attributes.computeIfAbsent(ATTR_NAME_MIME_TYPE, key -> props.get().getContentType());
        attributes.computeIfAbsent(ATTR_NAME_TIMESTAMP, key -> String.valueOf(props.get().getLastModified()));
        attributes.computeIfAbsent(ATTR_NAME_LENGTH, key -> String.valueOf(props.get().getBlobSize()));

        // The LANG attribute is a special case because we allow it to be null.
        if (!attributes.containsKey(ATTR_NAME_LANG)) {
            attributes.put(ATTR_NAME_LANG, props.get().getContentLanguage());
        }
    }
}
