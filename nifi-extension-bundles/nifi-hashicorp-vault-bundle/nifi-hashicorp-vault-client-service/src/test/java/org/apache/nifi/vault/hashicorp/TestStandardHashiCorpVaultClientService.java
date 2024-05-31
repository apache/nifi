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
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardHashiCorpVaultClientService {

    private StandardHashiCorpVaultClientService clientService;

    private ConfigurationContext mockContext(final Map<PropertyDescriptor, String> properties) {
        final ConfigurationContext context = mock(ConfigurationContext.class);
        properties.forEach((key, value) -> mockProperty(context, key, value));
        if (properties.containsKey(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES)) {
            final PropertyValue propertiesFilesProperty = mock(PropertyValue.class);
            final ResourceReferences resources = mock(ResourceReferences.class);
            when(resources.asList()).thenReturn(Arrays.stream(properties.get(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES)
                    .split(","))
                    .map(SimpleResourceReference::new)
                    .collect(Collectors.toList()));
            when(propertiesFilesProperty.asResources()).thenReturn(resources);
            when(context.getProperty(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES)).thenReturn(propertiesFilesProperty);
        }

        final PropertyValue sslContextServicePropertyValue = mock(PropertyValue.class);
        when(sslContextServicePropertyValue.isSet()).thenReturn(false);
        when(context.getProperty(HashiCorpVaultClientService.SSL_CONTEXT_SERVICE)).thenReturn(sslContextServicePropertyValue);
        when(context.getAllProperties()).thenReturn(properties.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue)));
        return context;
    }

    private void mockProperty(final ConfigurationContext context, final PropertyDescriptor descriptor, final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(value);
        when(context.getProperty(descriptor)).thenReturn(propertyValue);
    }

    @BeforeEach
    public void init() {
        clientService = new StandardHashiCorpVaultClientService() {
            @Override
            protected ComponentLog getLogger() {
                return mock(ComponentLog.class);
            }
        };
    }

    @Test
    public void onEnabledHttpDirect() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final PropertyDescriptor vaultToken = new PropertyDescriptor.Builder().name("vault.token").build();
        properties.put(HashiCorpVaultClientService.CONFIGURATION_STRATEGY, HashiCorpVaultClientService.DIRECT_PROPERTIES.getValue());
        properties.put(HashiCorpVaultClientService.VAULT_URI, "http://localhost:8200");
        properties.put(vaultToken, "myToken");
        properties.put(HashiCorpVaultClientService.VAULT_AUTHENTICATION, "TOKEN");
        final ConfigurationContext context = mockContext(properties);
        clientService.onEnabled(context);
        assertNotNull(clientService.getHashiCorpVaultCommunicationService());
        clientService.onDisabled();
        assertNull(clientService.getHashiCorpVaultCommunicationService());
    }

    private record SimpleResourceReference(String filename) implements ResourceReference {

        @Override
            public File asFile() {
                return null;
            }

            @Override
            public URL asURL() {
                return null;
            }

            @Override
            public InputStream read() {
                return null;
            }

            @Override
            public boolean isAccessible() {
                return false;
            }

            @Override
            public String getLocation() {
                return filename;
            }

            @Override
            public ResourceType getResourceType() {
                return null;
            }
        }
}
