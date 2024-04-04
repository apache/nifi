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
import com.azure.core.http.ProxyOptions;

import java.util.Objects;

import static org.apache.nifi.services.azure.util.ProxyOptionsUtils.equalsProxyOptions;
import static org.apache.nifi.services.azure.util.ProxyOptionsUtils.hashCodeProxyOptions;

public class AzureStorageCredentialsDetails_v12 {

    private final String accountName;
    private final String endpointSuffix;
    private final AzureStorageCredentialsType credentialsType;
    private final String accountKey;
    private final String sasToken;
    private final String managedIdentityClientId;
    private final String servicePrincipalTenantId;
    private final String servicePrincipalClientId;
    private final String servicePrincipalClientSecret;
    private final AccessToken accessToken;
    private final ProxyOptions proxyOptions;

    private AzureStorageCredentialsDetails_v12(
            String accountName, String endpointSuffix, AzureStorageCredentialsType credentialsType, String accountKey, String sasToken, String managedIdentityClientId,
            String servicePrincipalTenantId, String servicePrincipalClientId, String servicePrincipalClientSecret, AccessToken accessToken, ProxyOptions proxyOptions) {
        this.accountName = accountName;
        this.endpointSuffix = endpointSuffix;
        this.credentialsType = credentialsType;
        this.accountKey = accountKey;
        this.sasToken = sasToken;
        this.managedIdentityClientId = managedIdentityClientId;
        this.servicePrincipalTenantId = servicePrincipalTenantId;
        this.servicePrincipalClientId = servicePrincipalClientId;
        this.servicePrincipalClientSecret = servicePrincipalClientSecret;
        this.accessToken = accessToken;
        this.proxyOptions = proxyOptions;
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

    public String getManagedIdentityClientId() {
        return managedIdentityClientId;
    }

    public String getServicePrincipalTenantId() {
        return servicePrincipalTenantId;
    }

    public String getServicePrincipalClientId() {
        return servicePrincipalClientId;
    }

    public String getServicePrincipalClientSecret() {
        return servicePrincipalClientSecret;
    }

    public AccessToken getAccessToken() {
        return accessToken;
    }

    public ProxyOptions getProxyOptions() {
        return proxyOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AzureStorageCredentialsDetails_v12 that = (AzureStorageCredentialsDetails_v12) o;
        return credentialsType == that.credentialsType
                && Objects.equals(accountName, that.accountName)
                && Objects.equals(endpointSuffix, that.endpointSuffix)
                && Objects.equals(accountKey, that.accountKey)
                && Objects.equals(sasToken, that.sasToken)
                && Objects.equals(managedIdentityClientId, that.managedIdentityClientId)
                && Objects.equals(servicePrincipalTenantId, that.servicePrincipalTenantId)
                && Objects.equals(servicePrincipalClientId, that.servicePrincipalClientId)
                && Objects.equals(servicePrincipalClientSecret, that.servicePrincipalClientSecret)
                && Objects.equals(accessToken, that.accessToken)
                && equalsProxyOptions(proxyOptions, that.proxyOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                credentialsType,
                accountName,
                endpointSuffix,
                accountKey,
                sasToken,
                managedIdentityClientId,
                servicePrincipalTenantId,
                servicePrincipalClientId,
                servicePrincipalClientSecret,
                accessToken,
                hashCodeProxyOptions(proxyOptions)
        );
    }

    public static AzureStorageCredentialsDetails_v12 createWithAccountKey(
            String accountName,
            String endpointSuffix,
            String accountKey) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.ACCOUNT_KEY, accountKey, null, null, null, null, null, null, null);
    }

    public static AzureStorageCredentialsDetails_v12 createWithSasToken(
            String accountName,
            String endpointSuffix,
            String sasToken) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.SAS_TOKEN, null, sasToken, null, null, null, null, null, null);
    }

    public static AzureStorageCredentialsDetails_v12 createWithManagedIdentity(
            String accountName,
            String endpointSuffix,
            String managedIdentityClientId,
            ProxyOptions proxyOptions) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.MANAGED_IDENTITY, null, null, managedIdentityClientId,
                null, null, null, null, proxyOptions);
    }

    public static AzureStorageCredentialsDetails_v12 createWithServicePrincipal(
            String accountName,
            String endpointSuffix,
            String servicePrincipalTenantId,
            String servicePrincipalClientId,
            String servicePrincipalClientSecret,
            ProxyOptions proxyOptions) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.SERVICE_PRINCIPAL, null, null, null,
                servicePrincipalTenantId, servicePrincipalClientId, servicePrincipalClientSecret, null, proxyOptions);
    }

    public static AzureStorageCredentialsDetails_v12 createWithAccessToken(
            String accountName,
            String endpointSuffix,
            AccessToken accessToken) {
        return new AzureStorageCredentialsDetails_v12(accountName, endpointSuffix, AzureStorageCredentialsType.ACCESS_TOKEN, null, null, null, null, null, null, accessToken, null);
    }
}
