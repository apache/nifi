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
package org.apache.nifi.services.azure.storage;

import com.azure.core.credential.AccessToken;
import com.microsoft.azure.storage.StorageCredentials;

public class AzureStorageCredentialsDetails {
    private final String storageAccountName;

    private final String accountKey;
    private final String sasToken;
    private final String storageSuffix;

    private final AccessToken accessToken;
    private final StorageCredentials storageCredentials;

    @Deprecated
    public AzureStorageCredentialsDetails(String storageAccountName, StorageCredentials storageCredentials) {
        this(storageAccountName, null, storageCredentials);
    }

    public AzureStorageCredentialsDetails(String storageAccountName, String storageSuffix, StorageCredentials storageCredentials) {
        this(storageAccountName, null, null, storageSuffix, null, storageCredentials);
    }

    public AzureStorageCredentialsDetails(
        String storageAccountName,
        String accountKey,
        String sasToken,
        String storageSuffix,
        AccessToken accessToken,
        StorageCredentials storageCredentials
    ) {
        this.storageAccountName = storageAccountName;
        this.accountKey = accountKey;
        this.sasToken = sasToken;
        this.accessToken = accessToken;
        this.storageSuffix = storageSuffix;
        this.storageCredentials = storageCredentials;
    }

    public String getStorageAccountName() {
        return storageAccountName;
    }

    public String getStorageSuffix() {
        return storageSuffix;
    }

    public String getAccountKey() {
        return accountKey;
    }

    public String getSasToken() {
        return sasToken;
    }

    public AccessToken getAccessToken() {
        return accessToken;
    }

    public StorageCredentials getStorageCredentials() {
        return storageCredentials;
    }

    public static class Builder {
        private String accountName;
        private String endpointSuffix;
        private String accountKey;
        private String sasToken;
        private AccessToken accessToken;
        private StorageCredentials storageCredentials;

        private Builder() {}

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setAccountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder setEndpointSuffix(String endpointSuffix) {
            this.endpointSuffix = endpointSuffix;
            return this;
        }

        public Builder setAccountKey(String accountKey) {
            this.accountKey = accountKey;
            return this;
        }

        public Builder setSasToken(String sasToken) {
            this.sasToken = sasToken;
            return this;
        }

        public Builder setAccessToken(AccessToken accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder setStorageCredentials(StorageCredentials storageCredentials) {
            this.storageCredentials = storageCredentials;
            return this;
        }

        public AzureStorageCredentialsDetails build() {
            return new AzureStorageCredentialsDetails(accountName, accountKey, sasToken, endpointSuffix, accessToken, storageCredentials);
        }
    }
}
