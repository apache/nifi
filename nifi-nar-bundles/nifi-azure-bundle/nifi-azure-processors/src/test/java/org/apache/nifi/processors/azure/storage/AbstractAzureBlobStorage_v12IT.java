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
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobType;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.AzureServiceEndpoints;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.BlobAttributes;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.processors.azure.AzureServiceEndpoints.DEFAULT_BLOB_ENDPOINT_SUFFIX;

public abstract class AbstractAzureBlobStorage_v12IT extends AbstractAzureStorageIT {

    protected static final String BLOB_NAME = "blob1";
    protected static final byte[] BLOB_DATA = "0123456789".getBytes();

    protected static final String EL_CONTAINER_NAME = "az.containername";
    protected static final String EL_BLOB_NAME = "az.blobname";

    protected static final byte[] EMPTY_CONTENT = new byte[0];

    private static final String TEST_CONTAINER_NAME_PREFIX = "nifi-test-container";

    private BlobServiceClient storageClient;
    private BlobContainerClient containerClient;
    private String containerName;

    @Override
    protected String getDefaultEndpointSuffix() {
        return DEFAULT_BLOB_ENDPOINT_SUFFIX;
    }

    @Override
    protected void setUpCredentials() throws Exception {
        String serviceId = "credentials-service";
        AzureStorageCredentialsService_v12 service = new AzureStorageCredentialsControllerService_v12();
        runner.addControllerService(serviceId, service);
        runner.setProperty(service, AzureStorageCredentialsControllerService_v12.ACCOUNT_NAME, getAccountName());
        if (getEndpointSuffix() != null) {
            runner.setProperty(service, AzureStorageCredentialsControllerService_v12.ENDPOINT_SUFFIX, getEndpointSuffix());
        }
        runner.setProperty(service, AzureStorageCredentialsControllerService_v12.CREDENTIALS_TYPE, AzureStorageCredentialsType.ACCOUNT_KEY.getAllowableValue());
        runner.setProperty(service, AzureStorageCredentialsControllerService_v12.ACCOUNT_KEY, getAccountKey());
        runner.enableControllerService(service);

        runner.setProperty(AbstractAzureBlobProcessor_v12.STORAGE_CREDENTIALS_SERVICE, serviceId);
    }

    @BeforeEach
    public void setUpAzureBlobStorage_v12IT() {
        containerName = generateContainerName();

        runner.setProperty(AzureStorageUtils.CONTAINER, containerName);

        storageClient = createStorageClient();
        containerClient = storageClient.createBlobContainer(containerName);
    }

    @AfterEach
    public void tearDownAzureBlobStorage_v12IT() {
        containerClient.delete();
    }

    protected String generateContainerName() {
        return String.format("%s-%s", TEST_CONTAINER_NAME_PREFIX, UUID.randomUUID());
    }

    protected BlobServiceClient getStorageClient() {
        return storageClient;
    }

    protected BlobContainerClient getContainerClient() {
        return containerClient;
    }

    protected String getContainerName() {
        return containerName;
    }

    private BlobServiceClient createStorageClient() {
        return new BlobServiceClientBuilder()
                .endpoint(AzureServiceEndpoints.getAzureBlobStorageEndpoint(getAccountName(), getEndpointSuffix()))
                .credential(new StorageSharedKeyCredential(getAccountName(), getAccountKey()))
                .buildClient();
    }

    protected BlobClient uploadBlob(String blobName, byte[] blobData) throws Exception {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.upload(new ByteArrayInputStream(blobData), blobData.length);

        // waiting for the blob to be available
        Thread.sleep(1000);

        return blobClient;
    }

    protected Map<String, String> initCommonExpressionLanguageAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(EL_CONTAINER_NAME, getContainerName());
        attributes.put(EL_BLOB_NAME, BLOB_NAME);

        runner.setProperty(AzureStorageUtils.CONTAINER, String.format("${%s}", EL_CONTAINER_NAME));
        runner.setProperty(AbstractAzureBlobProcessor_v12.BLOB_NAME, String.format("${%s}", EL_BLOB_NAME));

        return attributes;
    }

    protected void assertFlowFileBlobAttributes(MockFlowFile flowFile, String containerName, String blobName, int blobLength) {
        flowFile.assertAttributeEquals(BlobAttributes.ATTR_NAME_CONTAINER, containerName);
        flowFile.assertAttributeEquals(BlobAttributes.ATTR_NAME_BLOBNAME, blobName);
        flowFile.assertAttributeEquals(BlobAttributes.ATTR_NAME_PRIMARY_URI, String.format("https://%s.blob.core.windows.net/%s/%s", getAccountName(), containerName, blobName));
        flowFile.assertAttributeExists(BlobAttributes.ATTR_NAME_ETAG);
        flowFile.assertAttributeEquals(BlobAttributes.ATTR_NAME_BLOBTYPE, BlobType.BLOCK_BLOB.toString());
        flowFile.assertAttributeEquals(BlobAttributes.ATTR_NAME_MIME_TYPE, "application/octet-stream");
        flowFile.assertAttributeExists(BlobAttributes.ATTR_NAME_LANG);
        flowFile.assertAttributeExists(BlobAttributes.ATTR_NAME_TIMESTAMP);
        flowFile.assertAttributeEquals(BlobAttributes.ATTR_NAME_LENGTH, String.valueOf(blobLength));
    }
}
