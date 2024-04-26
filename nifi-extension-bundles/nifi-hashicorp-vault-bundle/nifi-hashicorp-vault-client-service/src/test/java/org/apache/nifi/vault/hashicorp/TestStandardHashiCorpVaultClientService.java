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
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardHashiCorpVaultClientService {

    private static Path bootstrapHashiCorpVaultConf;
    private static TlsConfiguration tlsConfiguration;

    private StandardHashiCorpVaultClientService clientService;

    private ConfigurationContext mockContext(final Map<PropertyDescriptor, String> properties, final boolean includeSSL) {
        final ConfigurationContext context = mock(ConfigurationContext.class);
        properties.entrySet().forEach(entry -> mockProperty(context, entry.getKey(), entry.getValue()));
        if (properties.containsKey(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES)) {
            final PropertyValue propertiesFilesProperty = mock(PropertyValue.class);
            final ResourceReferences resources = mock(ResourceReferences.class);
            when(resources.asList()).thenReturn(Arrays.asList(properties.get(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES).split(","))
                    .stream().map(SimpleResourceReference::new)
                    .collect(Collectors.toList()));
            when(propertiesFilesProperty.asResources()).thenReturn(resources);
            when(context.getProperty(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES)).thenReturn(propertiesFilesProperty);
        }
        if (includeSSL) {
            final SSLContextService sslContextService = mock(SSLContextService.class);
            when(sslContextService.getKeyStoreFile()).thenReturn(tlsConfiguration.getKeystorePath());
            when(sslContextService.getKeyStoreType()).thenReturn(tlsConfiguration.getKeystoreType().getType());
            when(sslContextService.getKeyStorePassword()).thenReturn(tlsConfiguration.getKeystorePassword());
            when(sslContextService.getTrustStoreFile()).thenReturn(tlsConfiguration.getTruststorePath());
            when(sslContextService.getTrustStoreType()).thenReturn(tlsConfiguration.getTruststoreType().getType());
            when(sslContextService.getTrustStorePassword()).thenReturn(tlsConfiguration.getTruststorePassword());
            when(sslContextService.getSslAlgorithm()).thenReturn(tlsConfiguration.getProtocol());

            final PropertyValue sslContextServicePropertyValue = mock(PropertyValue.class);
            when(sslContextServicePropertyValue.isSet()).thenReturn(true);
            when(sslContextServicePropertyValue.asControllerService(SSLContextService.class)).thenReturn(sslContextService);
            when(context.getProperty(HashiCorpVaultClientService.SSL_CONTEXT_SERVICE)).thenReturn(sslContextServicePropertyValue);
        } else {
            final PropertyValue sslContextServicePropertyValue = mock(PropertyValue.class);
            when(sslContextServicePropertyValue.isSet()).thenReturn(false);
            when(context.getProperty(HashiCorpVaultClientService.SSL_CONTEXT_SERVICE)).thenReturn(sslContextServicePropertyValue);
        }
        when(context.getAllProperties()).thenReturn(properties.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue)));
        return context;
    }

    private void mockProperty(final ConfigurationContext context, final PropertyDescriptor descriptor, final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn(value);
        when(context.getProperty(descriptor)).thenReturn(propertyValue);
    }

    @BeforeAll
    public static void initOnce() throws IOException, GeneralSecurityException {
        bootstrapHashiCorpVaultConf = Files.createTempFile("bootstrap-hashicorp-vault", "conf");
        final File keyStoreFile = File.createTempFile(TestStandardHashiCorpVaultClientService.class.getSimpleName(), ".keystore.p12");
        final File trustStoreFile = File.createTempFile(TestStandardHashiCorpVaultClientService.class.getSimpleName(), ".truststore.p12");
        final TlsConfiguration requestedTlsConfig = new StandardTlsConfiguration();
        tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(requestedTlsConfig, 7, null);
        // This should be overridden by any explicit properties
        Files.write(bootstrapHashiCorpVaultConf, (String.format("vault.uri=https://localhost:8200\n" +
                "vault.authentication=TOKEN\n" +
                "vault.token=my-token\n" +
                "vault.ssl.key-store=%s\n" +
                "vault.ssl.key-store-password=%s\n" +
                "vault.ssl.key-store-type=%s\n" +
                "vault.ssl.trust-store=%s\n" +
                "vault.ssl.trust-store-password=%s\n" +
                "vault.ssl.trust-store-type=%s\n",
                tlsConfiguration.getKeystorePath().replace("\\", "\\\\"),
                tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getKeystoreType().getType(),
                tlsConfiguration.getTruststorePath().replace("\\", "\\\\"),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType().getType())).getBytes(StandardCharsets.UTF_8));
    }

    @AfterAll
    public static void tearDownOnce() throws IOException {
        Files.deleteIfExists(bootstrapHashiCorpVaultConf);
        Files.deleteIfExists(Paths.get(tlsConfiguration.getKeystorePath()));
        Files.deleteIfExists(Paths.get(tlsConfiguration.getTruststorePath()));
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
        final ConfigurationContext context = mockContext(properties, false);
        clientService.onEnabled(context);
        assertNotNull(clientService.getHashiCorpVaultCommunicationService());
        clientService.onDisabled();
        assertNull(clientService.getHashiCorpVaultCommunicationService());
    }

    @Test
    public void onEnabledHttpsDirect() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        final PropertyDescriptor vaultToken = new PropertyDescriptor.Builder().name("vault.token").build();
        properties.put(HashiCorpVaultClientService.CONFIGURATION_STRATEGY, HashiCorpVaultClientService.DIRECT_PROPERTIES.getValue());
        properties.put(HashiCorpVaultClientService.VAULT_URI, "https://localhost:8200");
        properties.put(vaultToken, "myToken");
        properties.put(HashiCorpVaultClientService.VAULT_AUTHENTICATION, "TOKEN");
        final ConfigurationContext context = mockContext(properties, true);
        clientService.onEnabled(context);
        assertNotNull(clientService.getHashiCorpVaultCommunicationService());
        clientService.onDisabled();
        assertNull(clientService.getHashiCorpVaultCommunicationService());
    }

    @Test
    public void onEnabledHttpsPropertiesFiles() throws InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HashiCorpVaultClientService.CONFIGURATION_STRATEGY, HashiCorpVaultClientService.PROPERTIES_FILES.getValue());
        properties.put(HashiCorpVaultClientService.VAULT_PROPERTIES_FILES, bootstrapHashiCorpVaultConf.toString());
        final ConfigurationContext context = mockContext(properties, true);
        clientService.onEnabled(context);
        assertNotNull(clientService.getHashiCorpVaultCommunicationService());
        clientService.onDisabled();
        assertNull(clientService.getHashiCorpVaultCommunicationService());
    }

    private class SimpleResourceReference implements ResourceReference {
        private final String filename;

        private SimpleResourceReference(String filename) {
            this.filename = filename;
        }

        @Override
        public File asFile() {
            return null;
        }

        @Override
        public URL asURL() {
            return null;
        }

        @Override
        public InputStream read() throws IOException {
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
