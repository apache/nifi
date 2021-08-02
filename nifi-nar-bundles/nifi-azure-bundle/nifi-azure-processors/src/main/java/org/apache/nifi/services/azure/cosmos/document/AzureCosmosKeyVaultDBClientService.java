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

package org.apache.nifi.services.azure.cosmos.document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.cosmos.document.AzureCosmosDBUtils;
import org.apache.nifi.services.azure.keyvault.AzureKeyVaultConnectionService;
import org.apache.nifi.util.StringUtils;

@Tags({"azure", "cosmos", "document", "service", "secure"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Cosmos DB (Core SQL API) " +
                " and provides access to that connection to other Cosmos DB-related components."
)
public class AzureCosmosKeyVaultDBClientService extends AbstractCosmosDBClientService {

    private String uriSecret;
    private String accessKeySecret;
    private String consistencyLevel;
    private AzureKeyVaultConnectionService keyVaultClientService;

    public static final PropertyDescriptor URI_SECRET = new PropertyDescriptor.Builder()
            .name("azure-cosmos-db-uri-secret")
            .displayName("Cosmos DB URI Secret Name")
            .description("The controller service will get the value of secret from Keyvault. " +
                    "Provide the name of secret which stores Cosmos DB URI.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor DB_ACCESS_KEY_SECRET = new PropertyDescriptor.Builder()
            .name("azure-cosmos-db-key-secret")
            .displayName("Cosmos DB Access Key Secret Name")
            .description("The controller service will get the value of secret from Keyvault. " +
                    "Provide the name of secret which stores Cosmos DB Access Key.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    static final PropertyDescriptor KEYVAULT_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("azure-keyvault-connection-service")
            .displayName("KeyVault Connection Service")
            .description("The controller service will get the value of secrets from Keyvault. " +
                    "Provide the name of keyvault controller service.")
            .required(true)
            .identifiesControllerService(AzureKeyVaultConnectionService.class)
            .build();

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(KEYVAULT_CONNECTION_SERVICE);
        descriptors.add(URI_SECRET);
        descriptors.add(DB_ACCESS_KEY_SECRET);
        descriptors.add(AzureCosmosDBUtils.CONSISTENCY);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.uriSecret = context.getProperty(URI_SECRET).getValue();
        this.accessKeySecret = context.getProperty(DB_ACCESS_KEY_SECRET).getValue();
        this.consistencyLevel = context.getProperty(
                AzureCosmosDBUtils.CONSISTENCY).getValue();
        this.keyVaultClientService = context.getProperty(
                KEYVAULT_CONNECTION_SERVICE
        ).asControllerService(AzureKeyVaultConnectionService.class);

        initCosmosClient(getURI(), getAccessKey(), getConsistencyLevel()
        );
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public String getURI() {
        if (keyVaultClientService == null) {
            throw new IllegalArgumentException(String.format(
                    "Cannot get '%s'.", KEYVAULT_CONNECTION_SERVICE.getDisplayName()));
        }
        return keyVaultClientService.getSecret(uriSecret);
    }

    @Override
    public String getAccessKey() {
        if (keyVaultClientService == null) {
            throw new IllegalArgumentException(String.format(
                    "Cannot get '%s'.", KEYVAULT_CONNECTION_SERVICE.getDisplayName()));
        }
        if (StringUtils.isBlank(accessKeySecret)) {
            throw new IllegalArgumentException(String.format(
                    "'%s' must not be empty.", DB_ACCESS_KEY_SECRET.getDisplayName()));
        }
        return keyVaultClientService.getSecret(accessKeySecret);
    }

    @Override
    public String getConsistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String uriSecret = validationContext.getProperty(URI_SECRET).getValue();
        final String accessKeySecret = validationContext.getProperty(DB_ACCESS_KEY_SECRET).getValue();

        if (StringUtils.isBlank(uriSecret) || StringUtils.isBlank(accessKeySecret)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation(
                            "both " + URI_SECRET.getDisplayName()
                                    + DB_ACCESS_KEY_SECRET.getDisplayName()
                                    + " are required")
                    .build());
        }
        return results;
    }
}

