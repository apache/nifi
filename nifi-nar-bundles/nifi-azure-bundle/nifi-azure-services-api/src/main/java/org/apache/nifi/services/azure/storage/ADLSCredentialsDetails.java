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

public class ADLSCredentialsDetails {
    private final String accountName;

    private final String accountKey;
    private final String sasToken;
    private final String endpointSuffix;

    private final AccessToken accessToken;

    private final boolean useManagedIdentity;

    private final String servicePrincipalTenantId;
    private final String servicePrincipalClientId;
    private final String servicePrincipalClientSecret;

    public ADLSCredentialsDetails(
            String accountName,
            String accountKey,
            String sasToken,
            String endpointSuffix,
            AccessToken accessToken,
            boolean useManagedIdentity,
            String servicePrincipalTenantId,
            String servicePrincipalClientId,
            String servicePrincipalClientSecret
    ) {
        this.accountName = accountName;
        this.accountKey = accountKey;
        this.sasToken = sasToken;
        this.endpointSuffix = endpointSuffix;
        this.accessToken = accessToken;
        this.useManagedIdentity = useManagedIdentity;
        this.servicePrincipalTenantId = servicePrincipalTenantId;
        this.servicePrincipalClientId = servicePrincipalClientId;
        this.servicePrincipalClientSecret = servicePrincipalClientSecret;
    }

    public String getAccountName() {
        return accountName;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
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

    public boolean getUseManagedIdentity() {
        return useManagedIdentity;
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

    public static class Builder {
        private String accountName;
        private String accountKey;
        private String sasToken;
        private String endpointSuffix;
        private AccessToken accessToken;
        private boolean useManagedIdentity;
        private String servicePrincipalTenantId;
        private String servicePrincipalClientId;
        private String servicePrincipalClientSecret;

        private Builder() {}

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setAccountName(String accountName) {
            this.accountName = accountName;
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

        public Builder setEndpointSuffix(String endpointSuffix) {
            this.endpointSuffix = endpointSuffix;
            return this;
        }

        public Builder setAccessToken(AccessToken accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder setUseManagedIdentity(boolean useManagedIdentity) {
            this.useManagedIdentity = useManagedIdentity;
            return this;
        }

        public Builder setServicePrincipalTenantId(String servicePrincipalTenantId) {
            this.servicePrincipalTenantId = servicePrincipalTenantId;
            return this;
        }

        public Builder setServicePrincipalClientId(String servicePrincipalClientId) {
            this.servicePrincipalClientId = servicePrincipalClientId;
            return this;
        }

        public Builder setServicePrincipalClientSecret(String servicePrincipalClientSecret) {
            this.servicePrincipalClientSecret = servicePrincipalClientSecret;
            return this;
        }

        public ADLSCredentialsDetails build() {
            return new ADLSCredentialsDetails(accountName, accountKey, sasToken, endpointSuffix, accessToken, useManagedIdentity,
                    servicePrincipalTenantId, servicePrincipalClientId, servicePrincipalClientSecret);
        }
    }
}
