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
package org.apache.nifi.services.iceberg.azure;

import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.iceberg.IcebergFileIOProvider;
import org.apache.nifi.services.iceberg.ProviderContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Tags({"adls", "iceberg", "azure"})
@CapabilityDescription("Provides Azure Data Lake Storage file input and output support for Apache Iceberg tables")
public class ADLSIcebergFileIOProvider extends AbstractControllerService implements IcebergFileIOProvider {

    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .description("Strategy for authenticating with Azure Data Lake Storage services")
            .required(true)
            .allowableValues(AuthenticationStrategy.class)
            .defaultValue(AuthenticationStrategy.VENDED_CREDENTIALS)
            .build();

    static final PropertyDescriptor STORAGE_ACCOUNT = new PropertyDescriptor.Builder()
            .name("Storage Account")
            .description("Azure Storage Account name for authentication to Azure Data Lake Storage services")
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                    AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.SHARED_ACCESS_SIGNATURE_TOKEN
            )
            .build();

    static final PropertyDescriptor SHARED_ACCESS_SIGNATURE_TOKEN = new PropertyDescriptor.Builder()
            .name("Shared Access Signature Token")
            .description("Shared Access Signature (SAS) Token for authentication to Azure Data Lake Storage services")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(
                    AUTHENTICATION_STRATEGY,
                    AuthenticationStrategy.SHARED_ACCESS_SIGNATURE_TOKEN
            )
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AUTHENTICATION_STRATEGY,
            STORAGE_ACCOUNT,
            SHARED_ACCESS_SIGNATURE_TOKEN
    );

    private static final String SAS_TOKEN_PROPERTY_NAME_FORMAT = "%s.%s";

    private final Map<String, String> standardProperties = new ConcurrentHashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String, String> configuredProperties = getConfiguredProperties(context);
        standardProperties.putAll(configuredProperties);
    }

    @OnDisabled
    public void onDisabled() {
        standardProperties.clear();
    }

    @Override
    public FileIO getFileIO(final ProviderContext providerContext) {
        Objects.requireNonNull(providerContext, "Provider Context required");
        final Map<String, String> contextProperties = providerContext.getProperties();
        Objects.requireNonNull(contextProperties, "Context properties required");

        final Map<String, String> mergedProperties = new HashMap<>(standardProperties);
        mergedProperties.putAll(contextProperties);
        final ADLSFileIO fileIO = new ADLSFileIO();
        fileIO.initialize(mergedProperties);
        return fileIO;
    }

    private Map<String, String> getConfiguredProperties(final ConfigurationContext context) {
        final Map<String, String> contextProperties = new HashMap<>();
        final AuthenticationStrategy authenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(AuthenticationStrategy.class);
        if (AuthenticationStrategy.SHARED_ACCESS_SIGNATURE_TOKEN == authenticationStrategy) {
            final String storageAccount = context.getProperty(STORAGE_ACCOUNT).getValue();
            final String sharedAccessSignatureToken = context.getProperty(SHARED_ACCESS_SIGNATURE_TOKEN).getValue();

            final String storageAccountProperty = SAS_TOKEN_PROPERTY_NAME_FORMAT.formatted(AzureProperties.ADLS_SAS_TOKEN_PREFIX, storageAccount);
            contextProperties.put(storageAccountProperty, sharedAccessSignatureToken);
        }

        return contextProperties;
    }
}
