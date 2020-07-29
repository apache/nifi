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

import java.util.Map;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.service.lookup.AbstractSingleAttributeBasedControllerServiceLookup;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Provides an AzureStorageCredentialsService that can be used to dynamically select another AzureStorageCredentialsService. " +
        "This service requires an attribute named 'azure.storage.credentials.name' to be passed in, and will throw an exception if the attribute is missing. " +
        "The value of 'azure.storage.credentials.name' will be used to select the AzureStorageCredentialsService that has been registered with that name. " +
        "This will allow multiple AzureStorageCredentialsServices to be defined and registered, and then selected dynamically at runtime by tagging flow files " +
        "with the appropriate 'azure.storage.credentials.name' attribute.")
@DynamicProperty(name = "The name to register AzureStorageCredentialsService", value = "The AzureStorageCredentialsService",
        description = "If '" + AzureStorageCredentialsControllerServiceLookup.AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE + "' attribute contains " +
                "the name of the dynamic property, then the AzureStorageCredentialsService (registered in the value) will be selected.",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class AzureStorageCredentialsControllerServiceLookup
        extends AbstractSingleAttributeBasedControllerServiceLookup<AzureStorageCredentialsService> implements AzureStorageCredentialsService {

    public static final String AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE = "azure.storage.credentials.name";

    @Override
    protected String getLookupAttribute() {
        return AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE;
    }

    @Override
    public Class<AzureStorageCredentialsService> getServiceType() {
        return AzureStorageCredentialsService.class;
    }

    @Override
    public AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) {
        return lookupService(attributes).getStorageCredentialsDetails(attributes);
    }
}
