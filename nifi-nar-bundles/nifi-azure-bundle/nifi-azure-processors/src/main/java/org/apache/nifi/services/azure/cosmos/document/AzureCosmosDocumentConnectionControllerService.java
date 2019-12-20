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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDocumentConnectionService;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Tags({"azure", "cosmos", "document", "service"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Azure Cosmos Document DB (Document API or Core SQL API recently renamed) " +
                " and provides access to that connection to other CosmosDB-related components."
)
public class AzureCosmosDocumentConnectionControllerService extends AbstractControllerService implements AzureCosmosDocumentConnectionService {
    private String uri;
    private String accessKey;

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
    .name("Cosmos Document DB URI")
    .displayName("Cosmos Document DB URI")
    .description("CosmosURI, typically of the form: https://{databaseaccount}.documents.azure.com:443/. Note this is not for Cosmos DB with Mongo API")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

    public static final PropertyDescriptor DB_ACCESS_KEY = new PropertyDescriptor.Builder()
        .name("Cosmos Document DB Access Key")
        .displayName("Cosmos Document DB Access Key")
        .description("Cosmos DB Access Key from Azure Portal (Settings->Keys)")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.uri = context.getProperty(URI).getValue();
        this.accessKey = context.getProperty(DB_ACCESS_KEY).getValue();
    }

    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(URI);
        descriptors.add(DB_ACCESS_KEY);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public String getURI() {
        return this.uri;
    }

    @Override
    public String getAccessKey() {
        return this.accessKey;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String uri = validationContext.getProperty(AzureCosmosDocumentConnectionControllerService.URI).getValue();
        final String db_access_key = validationContext.getProperty(AzureCosmosDocumentConnectionControllerService.DB_ACCESS_KEY).getValue();

        if (StringUtils.isBlank(uri) || StringUtils.isBlank(db_access_key)) {
            results.add(new ValidationResult.Builder().subject("AzureStorageCredentialsControllerService")
                    .valid(false)
                    .explanation(
                        "either " + AzureCosmosDocumentConnectionControllerService.URI.getDisplayName()
                        + " or " + AzureCosmosDocumentConnectionControllerService.DB_ACCESS_KEY.getDisplayName() + " is required")
                    .build());
        }
        return results;
    }

}
