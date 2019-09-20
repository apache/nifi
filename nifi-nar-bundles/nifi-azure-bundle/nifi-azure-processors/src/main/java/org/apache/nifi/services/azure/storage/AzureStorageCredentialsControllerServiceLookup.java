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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Provides an AzureStorageCredentialsService that can be used to dynamically select another AzureStorageCredentialsService. " +
        "This service requires an attribute named 'azure.storage.credentials.name' to be passed in, and will throw an exception if the attribute is missing. " +
        "The value of 'azure.storage.credentials.name' will be used to select the AzureStorageCredentialsService that has been registered with that name. " +
        "This will allow multiple AzureStorageCredentialsServices to be defined and registered, and then selected dynamically at runtime by tagging flow files " +
        "with the appropriate 'azure.storage.credentials.name' attribute.")
@DynamicProperty(name = "The name to register AzureStorageCredentialsService", value = "The AzureStorageCredentialsService",
        description = "If 'azure.storage.credentials.name' attribute contains the name of the dynamic property, then the AzureStorageCredentialsService (registered in the value) will be selected.",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class AzureStorageCredentialsControllerServiceLookup extends AbstractControllerService implements AzureStorageCredentialsService {

    public static final String AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE = "azure.storage.credentials.name";

    private volatile Map<String, AzureStorageCredentialsService> serviceMap;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The " + AzureStorageCredentialsService.class.getSimpleName() + " to return when " + AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE + " = '" + propertyDescriptorName + "'")
                .identifiesControllerService(AzureStorageCredentialsService.class)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        int numDefinedServices = 0;
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                numDefinedServices++;
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getDisplayName())
                        .explanation("the current service cannot be registered as an " + AzureStorageCredentialsService.class.getSimpleName() + " to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (numDefinedServices == 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("at least one " + AzureStorageCredentialsService.class.getSimpleName() + " must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String, AzureStorageCredentialsService> map = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final AzureStorageCredentialsService service = context.getProperty(descriptor).asControllerService(AzureStorageCredentialsService.class);
                map.put(descriptor.getName(), service);
            }
        }

        serviceMap = Collections.unmodifiableMap(map);
    }

    @OnDisabled
    public void onDisabled() {
        serviceMap = null;
    }

    @Override
    public AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) throws ProcessException {
        final AzureStorageCredentialsService service = lookupAzureStorageCredentialsService(attributes);

        return service.getStorageCredentialsDetails(attributes);
    }

    private AzureStorageCredentialsService lookupAzureStorageCredentialsService(Map<String, String> attributes) {
        if (!attributes.containsKey(AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE)) {
            throw new ProcessException("Attributes must contain an attribute name '" + AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE + "'");
        }

        final String storageCredentialService = attributes.get(AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE);
        if (StringUtils.isBlank(storageCredentialService)) {
            throw new ProcessException(AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE + " cannot be null or blank");
        }

        final AzureStorageCredentialsService service = serviceMap.get(storageCredentialService);
        if (service == null) {
            throw new ProcessException("No " + AzureStorageCredentialsService.class.getSimpleName() + " was found for " +
                    AZURE_STORAGE_CREDENTIALS_NAME_ATTRIBUTE + " '" + storageCredentialService + "'");
        }

        return service;
    }
}
