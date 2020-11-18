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
import org.apache.nifi.processors.azure.cosmos.document.AzureCosmosDBUtils;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDBConnectionService;
import org.apache.nifi.util.StringUtils;

@Tags({"azure", "cosmos", "document", "service"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Cosmos DB (Core SQL API) " +
        " and provides access to that connection to other Cosmos DB-related components."
)
public class AzureCosmosDBClientService
        extends AbstractCosmosDBClientService
        implements AzureCosmosDBConnectionService {
    private String uri;
    private String accessKey;
    private String consistencyLevel;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.uri = context.getProperty(AzureCosmosDBUtils.URI).getValue();
        this.accessKey = context.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).getValue();
        this.consistencyLevel = context.getProperty(
                AzureCosmosDBUtils.CONSISTENCY).getValue();

        if (this.cosmosClient != null) {
            onStopped();
        }
        createCosmosClient(
                getURI(),
                getAccessKey(),
                getConsistencyLevel()
        );
    }


    static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(AzureCosmosDBUtils.URI);
        descriptors.add(AzureCosmosDBUtils.DB_ACCESS_KEY);
        descriptors.add(AzureCosmosDBUtils.CONSISTENCY);
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
    public String getConsistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String uri = validationContext.getProperty(AzureCosmosDBUtils.URI).getValue();
        final String accessKey = validationContext.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).getValue();

        if (StringUtils.isBlank(uri) || StringUtils.isBlank(accessKey)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation(
                        "either " + AzureCosmosDBUtils.URI.getDisplayName()
                        + " or " + AzureCosmosDBUtils.DB_ACCESS_KEY.getDisplayName() + " is required")
                    .build());
        }
        return results;
    }
}
