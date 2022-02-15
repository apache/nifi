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

public class AzureStorageCredentialsDetails_v12 {

    private final String accountName;
    private final String endpointSuffix;
    private final AzureStorageCredentialsType credentialsType;
    private final String accountKey;
    private final String sasToken;
    private final String tenantId;
    private final String clientId;
    private final String clientSecret;
    private final AccessToken accessToken;

    private AzureStorageCredentialsDetails_v12(
            String accountName, String endpointSuffix, AzureStorageCredentialsType credentialsType, String accountKey,
            String sasToken, String tenantId, String clientId, String clientSecret, AccessToken accessToken) {
        this.accountName = accountName;
        this.endpointSuffix = endpointSuffix;
        this.credentialsType = credentialsType;
        this.accountKey = accountKey;
        this.sasToken = sasToken;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.accessToken = accessToken;
    }

    public String getAccountName() {
        return accountName;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public AzureStorageCredentialsType getCredentialsType() {
        return credentialsType;
    }

    public String getAccountKey() {
        return accountKey;
    }

    public String getSasToken() {
        return sasToken;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public AccessToken getAccessToken() {
        return accessToken;
    }

    public static AzureStorageCredentialsDetails_v12 createWithAccountKey(
            String accountName,
            String endpointSuffix,
            String accountKey) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.ACCOUNT_KEY, accountKey, null, null, null, null, null);
    }

    public static AzureStorageCredentialsDetails_v12 createWithSasToken(
            String accountName,
            String endpointSuffix,
            String sasToken) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.SAS_TOKEN, null, sasToken, null, null, null, null);
    }

    public static AzureStorageCredentialsDetails_v12 createWithManagedIdentity(
            String accountName,
            String endpointSuffix) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.MANAGED_IDENTITY, null, null, null, null, null, null);
    }

    public static AzureStorageCredentialsDetails_v12 createWithServicePrincipal(
            String accountName,
            String endpointSuffix,
            String tenantId,
            String clientId,
            String clientSecret) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.SERVICE_PRINCIPAL, null, null, tenantId, clientId, clientSecret, null);
    }

    public static AzureStorageCredentialsDetails_v12 createWithAccessToken(
            String accountName,
            String endpointSuffix,
            AccessToken accessToken) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.ACCESS_TOKEN, null, null, null, null, null, accessToken);
    }
}
