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
package org.apache.nifi.services.azure.keyvault;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureKeyVaultUtils {

    private static final String DEFAULT_KEYVAULT_ENDPOINT_SUFFIX = ".vault.azure.net";
    public static final PropertyDescriptor KEYVAULT_NAME = new PropertyDescriptor.Builder()
            .name("azure-keyvault-name")
            .displayName("Key Vault Name")
            .description("The name of the Azure Key Vault to get secrets from.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SP_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("azure-service-principal-client-id")
            .displayName("Service Principal Client ID")
            .description("The Azure Active Directory Tenant ID to use when using Service Principal authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SP_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("azure-service-principal-client-secret")
            .displayName("Service Principal Client Secret")
            .description("The Azure Service Principal Client Secret to use when using Service Principal authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder()
            .name("azure-tenant-id")
            .displayName("Azure Tenant ID")
            .description("This processor will use Azure Service Principal for authentication. " +
                    "Please provide Azure Tenant ID for authentication")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .name("keyvault-uri-suffix")
            .displayName("Endpoint Suffix")
            .description("The Endpoint or FQDN Suffix or to use when connecting Azure Key Vault." +
                    "Override this endpoint suffix with a different suffix in certain " +
                    "circumstances (like Azure Stack or non-public Azure regions). ")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .defaultValue(DEFAULT_KEYVAULT_ENDPOINT_SUFFIX)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor USE_MANAGED_IDENTITY = new PropertyDescriptor.Builder()
            .name("azure-use-managed-identity")
            .displayName("Use Azure Managed Identity")
            .description("Choose whether or not to use the managed identity of Azure VM/VMSS. " +
                    "If using managed identity user will not be required to provide service principal details")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Maximum number of secrets to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_WRITE = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-write")
            .displayName("Cache TTL after Write")
            .description("The cache TTL (time-to-live) or how long to keep secret in the cache after it was written.")
            .required(true)
            .defaultValue("5 mins")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
}

