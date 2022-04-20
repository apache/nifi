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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_TIMESTAMP;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class})
@CapabilityDescription("Puts content into a blob on Azure Blob Storage. The processor uses Azure Blob Storage client library v12.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_CONTAINER, description = ATTR_DESCRIPTION_CONTAINER),
        @WritesAttribute(attribute = ATTR_NAME_BLOBNAME, description = ATTR_DESCRIPTION_BLOBNAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_ETAG, description = ATTR_DESCRIPTION_ETAG),
        @WritesAttribute(attribute = ATTR_NAME_BLOBTYPE, description = ATTR_DESCRIPTION_BLOBTYPE),
        @WritesAttribute(attribute = ATTR_NAME_MIME_TYPE, description = ATTR_DESCRIPTION_MIME_TYPE),
        @WritesAttribute(attribute = ATTR_NAME_LANG, description = ATTR_DESCRIPTION_LANG),
        @WritesAttribute(attribute = ATTR_NAME_TIMESTAMP, description = ATTR_DESCRIPTION_TIMESTAMP),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH)})
public class PutAzureBlobStorage_v12 extends AbstractAzureBlobProcessor_v12 {

    public static final PropertyDescriptor CREATE_CONTAINER = new PropertyDescriptor.Builder()
            .name("create-container")
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

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            AzureStorageUtils.CONTAINER,
            CREATE_CONTAINER,
            BLOB_NAME
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.of(super.getSupportedPropertyDescriptors(), PROPERTIES)
                .flatMap(Collection::stream)
                .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        boolean createContainer = context.getProperty(CREATE_CONTAINER).asBoolean();
        String blobName = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();

        long startNanos = System.nanoTime();
        try {
            BlobServiceClient storageClient = getStorageClient();
            BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
            if (createContainer && !containerClient.exists()) {
                containerClient.create();
            }
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            long length = flowFile.getSize();

            try (InputStream rawIn = session.read(flowFile);
                 BufferedInputStream bufferedIn = new BufferedInputStream(rawIn)) {
                blobClient.upload(bufferedIn, length);
            }

            Map<String, String> attributes = createBlobAttributesMap(blobClient);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.transfer(flowFile, REL_SUCCESS);

            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            String transitUri = attributes.get(ATTR_NAME_PRIMARY_URI);
            session.getProvenanceReporter().send(flowFile, transitUri, transferMillis);
        } catch (Exception e) {
            getLogger().error("Failed to create blob on Azure Blob Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
