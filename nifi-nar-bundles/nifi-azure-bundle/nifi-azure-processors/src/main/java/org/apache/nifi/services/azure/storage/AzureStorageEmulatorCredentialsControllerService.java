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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "azure", "microsoft", "emulator", "storage", "blob", "queue", "credentials" })
@CapabilityDescription("Defines credentials for Azure Storage processors that connects to Azurite emulator.")
public class AzureStorageEmulatorCredentialsControllerService extends AbstractControllerService implements AzureStorageCredentialsService {

    public static final PropertyDescriptor DEVELOPMENT_STORAGE_PROXY_URI = new PropertyDescriptor.Builder()
            .name("azurite-uri")
            .displayName("Storage Emulator URI")
            .description("URI to connect to Azure Storage Emulator (Azurite)")
            .required(false)
            .sensitive(false)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES =
            Collections.unmodifiableList(Arrays.asList(DEVELOPMENT_STORAGE_PROXY_URI));

    private String azuriteProxyUri;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.azuriteProxyUri = context.getProperty(DEVELOPMENT_STORAGE_PROXY_URI).getValue();
    }

    public String getProxyUri() {
        return azuriteProxyUri;
    }

    @Override
    public AzureStorageCredentialsDetails getStorageCredentialsDetails(final Map<String, String> attributes) {
        return new AzureStorageEmulatorCredentialsDetails(azuriteProxyUri);

    }
}
