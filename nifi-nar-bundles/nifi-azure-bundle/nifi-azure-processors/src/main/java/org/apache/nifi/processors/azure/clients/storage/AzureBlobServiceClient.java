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
package org.apache.nifi.processors.azure.clients.storage;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.azure.clients.AbstractAzureServiceClient;
import org.apache.nifi.processors.azure.storage.utils.AzureProxyUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails;

public class AzureBlobServiceClient extends AbstractAzureServiceClient<BlobServiceClient> {

    public AzureBlobServiceClient(PropertyContext context, FlowFile flowFile) {
        super(context, flowFile);
    }

    @Override
    public void setServiceClient(PropertyContext context, FlowFile flowFile) {
        final AzureStorageCredentialsDetails storageCredentialsDetails = AzureStorageUtils.getStorageCredentialsDetails(context, flowFile);

        final String storageSuffix = StringUtils.isNotBlank(storageCredentialsDetails.getStorageSuffix())
                ? storageCredentialsDetails.getStorageSuffix()
                : "blob.core.windows.net";
        final String endpoint = String.format("https://%s.%s", storageCredentialsDetails.getStorageAccountName(),
                storageSuffix);

        // use HttpClient object to allow proxy setting
        final HttpClient httpClient = AzureProxyUtils.createHttpClient(context);
        final BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .httpClient(httpClient);

        switch (storageCredentialsDetails.getCredentialType()) {
            case SAS_TOKEN:
                this.client = blobServiceClientBuilder.sasToken(storageCredentialsDetails.getSasToken())
                        .buildClient();
                break;
            case STORAGE_ACCOUNT_KEY:
                this.client = blobServiceClientBuilder.credential(storageCredentialsDetails.getStorageSharedKeyCredential())
                        .buildClient();
                break;
            default:
                throw new IllegalArgumentException(String.format("Invalid credential type '%s'!", storageCredentialsDetails.getCredentialType().toString()));
        }
    }

    public BlobContainerClient getContainerClient(final String containerName) {
        return this.client.getBlobContainerClient(containerName);
    }
}
