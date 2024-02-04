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

import org.apache.nifi.components.DescribedValue;

public enum AzureStorageCredentialsType implements DescribedValue {

    ACCOUNT_KEY("Account Key", "The primary or secondary Account Key of the storage account that provides full access to the resources in the account"),
    SAS_TOKEN("SAS Token", "SAS (Shared Access Signature) Token generated for accessing resources in the storage account"),
    MANAGED_IDENTITY("Managed Identity", "Azure Virtual Machine Managed Identity (it can only be used when NiFi is running on Azure)"),
    SERVICE_PRINCIPAL("Service Principal", "Azure Active Directory Service Principal with Client Id / Client Secret of a registered application"),
    ACCESS_TOKEN("Access Token", "Access Token provided by custom controller service implementations");

    private final String displayName;
    private final String description;

    AzureStorageCredentialsType(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}

