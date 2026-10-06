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

import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.springframework.vault.VaultException;
import org.springframework.vault.core.VaultKeyValueOperations;
import org.springframework.vault.core.VaultKeyValueOperationsSupport.KeyValueBackend;
import org.springframework.vault.core.VaultTemplate;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

public class TestStandardHashiCorpVaultCommunicationService {
    public static final String URI_VALUE = "http://127.0.0.1:8200";

    private HashiCorpVaultProperties properties;
    private File authProps;

    @BeforeEach
    public void init() throws IOException {
        authProps = TestHashiCorpVaultConfiguration.writeBasicVaultAuthProperties();

        properties = Mockito.mock(HashiCorpVaultProperties.class);

        when(properties.getUri()).thenReturn(URI_VALUE);
        when(properties.getAuthPropertiesFilename()).thenReturn(authProps.getAbsolutePath());
        when(properties.getKvVersion()).thenReturn(1);
    }

    @AfterEach
    public void cleanUp() throws IOException {
        Files.deleteIfExists(authProps.toPath());
    }

    private StandardHashiCorpVaultCommunicationService configureService() {
        return new StandardHashiCorpVaultCommunicationService(properties);
    }

    @Test
    public void testDefaultSecretPathPrefixMethodPreservesExistingImplementations() {
        final HashiCorpVaultCommunicationService communicationService = Mockito.mock(HashiCorpVaultCommunicationService.class);
        when(communicationService.listKeyValueSecrets("kv", KeyValueBackend.KV_1.name())).thenReturn(List.of("secret"));
        doCallRealMethod().when(communicationService).listKeyValueSecrets(eq("kv"), eq(KeyValueBackend.KV_1.name()), anyString());

        assertEquals(List.of("secret"), communicationService.listKeyValueSecrets("kv", KeyValueBackend.KV_1.name(), ""));
        assertThrows(UnsupportedOperationException.class,
                () -> communicationService.listKeyValueSecrets("kv", KeyValueBackend.KV_1.name(), "nested"));
    }

    @Test
    public void testBasicConfiguration() {
        try (StandardHashiCorpVaultCommunicationService ignored = this.configureService()) {
            // Once to check if the URI is https, and once to resolve the Vault endpoint shared by the client
            Mockito.verify(properties, Mockito.times(2)).getUri();

            // Once to check if the property is set, and once to retrieve the value
            Mockito.verify(properties, Mockito.times(2)).getAuthPropertiesFilename();
        }
    }

    @Test
    public void testTimeouts() {
        when(properties.getConnectionTimeout()).thenReturn(Optional.of("20 secs"));
        when(properties.getReadTimeout()).thenReturn(Optional.of("40 secs"));
        try (StandardHashiCorpVaultCommunicationService ignored = this.configureService()) {
            // Intentionally empty
        }
    }

    @Test
    public void testListKeyValueSecretsRecursesNestedPaths() throws Exception {
        when(properties.getKvVersion()).thenReturn(2);

        try (StandardHashiCorpVaultCommunicationService service = this.configureService()) {

            final VaultTemplate vaultTemplate = Mockito.mock(VaultTemplate.class);
            final VaultKeyValueOperations keyValueOperations = Mockito.mock(VaultKeyValueOperations.class);

            final Field vaultTemplateField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("vaultTemplate");
            vaultTemplateField.setAccessible(true);
            vaultTemplateField.set(service, vaultTemplate);

            final Field keyValueBackendField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("keyValueBackend");
            keyValueBackendField.setAccessible(true);
            final KeyValueBackend keyValueBackend = (KeyValueBackend) keyValueBackendField.get(service);

            when(vaultTemplate.opsForKeyValue("kv", keyValueBackend)).thenReturn(keyValueOperations);
            when(keyValueOperations.list("/")).thenReturn(Arrays.asList("test", "nested/"));
            when(keyValueOperations.list("nested/")).thenReturn(List.of("nifi"));

            final List<String> secrets = service.listKeyValueSecrets("kv", keyValueBackend.name());
            assertEquals(Arrays.asList("test", "nested/nifi"), secrets);
        }
    }

    @ParameterizedTest
    @EnumSource(KeyValueBackend.class)
    public void testListKeyValueSecretsStartsAtSecretPathPrefix(final KeyValueBackend backend) throws Exception {
        when(properties.getKvVersion()).thenReturn(backend == KeyValueBackend.KV_1 ? 1 : 2);

        try (StandardHashiCorpVaultCommunicationService service = this.configureService()) {

            final VaultTemplate vaultTemplate = Mockito.mock(VaultTemplate.class);
            final VaultKeyValueOperations keyValueOperations = Mockito.mock(VaultKeyValueOperations.class);

            final Field vaultTemplateField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("vaultTemplate");
            vaultTemplateField.setAccessible(true);
            vaultTemplateField.set(service, vaultTemplate);

            final Field keyValueBackendField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("keyValueBackend");
            keyValueBackendField.setAccessible(true);
            final KeyValueBackend keyValueBackend = (KeyValueBackend) keyValueBackendField.get(service);

            when(vaultTemplate.opsForKeyValue("kv", keyValueBackend)).thenReturn(keyValueOperations);
            when(keyValueOperations.list("groups/my-group/")).thenReturn(Arrays.asList("app", "nested/"));
            when(keyValueOperations.list("groups/my-group/nested/")).thenReturn(List.of("nifi"));

            final List<String> secrets = service.listKeyValueSecrets("kv", keyValueBackend.name(), "groups/my-group");
            assertEquals(Arrays.asList("groups/my-group/app", "groups/my-group/nested/nifi"), secrets);
            Mockito.verify(keyValueOperations, Mockito.never()).list("/");
            Mockito.verify(keyValueOperations, Mockito.never()).list("groups/");
            Mockito.verify(keyValueOperations).list("groups/my-group/");
            Mockito.verify(keyValueOperations).list("groups/my-group/nested/");
            Mockito.verifyNoMoreInteractions(keyValueOperations);
        }
    }

    @ParameterizedTest
    @EnumSource(KeyValueBackend.class)
    public void testListKeyValueSecretsNormalizesSecretPathPrefixSlashes(final KeyValueBackend backend) throws Exception {
        when(properties.getKvVersion()).thenReturn(backend == KeyValueBackend.KV_1 ? 1 : 2);

        try (StandardHashiCorpVaultCommunicationService service = this.configureService()) {

            final VaultTemplate vaultTemplate = Mockito.mock(VaultTemplate.class);
            final VaultKeyValueOperations keyValueOperations = Mockito.mock(VaultKeyValueOperations.class);

            final Field vaultTemplateField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("vaultTemplate");
            vaultTemplateField.setAccessible(true);
            vaultTemplateField.set(service, vaultTemplate);

            final Field keyValueBackendField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("keyValueBackend");
            keyValueBackendField.setAccessible(true);
            final KeyValueBackend keyValueBackend = (KeyValueBackend) keyValueBackendField.get(service);

            when(vaultTemplate.opsForKeyValue("kv", keyValueBackend)).thenReturn(keyValueOperations);
            when(keyValueOperations.list("groups/my-group/")).thenReturn(List.of("app"));

            for (final String prefix : List.of("groups/my-group", "groups/my-group/", "/groups/my-group", "/groups/my-group/")) {
                assertEquals(List.of("groups/my-group/app"), service.listKeyValueSecrets("kv", keyValueBackend.name(), prefix));
            }
        }
    }

    @Test
    public void testListKeyValueSecretsBlankSecretPathPrefixListsRoot() throws Exception {
        try (StandardHashiCorpVaultCommunicationService service = this.configureService()) {

            final VaultTemplate vaultTemplate = Mockito.mock(VaultTemplate.class);
            final VaultKeyValueOperations keyValueOperations = Mockito.mock(VaultKeyValueOperations.class);

            final Field vaultTemplateField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("vaultTemplate");
            vaultTemplateField.setAccessible(true);
            vaultTemplateField.set(service, vaultTemplate);

            final Field keyValueBackendField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("keyValueBackend");
            keyValueBackendField.setAccessible(true);
            final KeyValueBackend keyValueBackend = (KeyValueBackend) keyValueBackendField.get(service);

            when(vaultTemplate.opsForKeyValue("kv", keyValueBackend)).thenReturn(keyValueOperations);
            when(keyValueOperations.list("/")).thenReturn(List.of("test"));

            assertEquals(List.of("test"), service.listKeyValueSecrets("kv", keyValueBackend.name(), null));
            assertEquals(List.of("test"), service.listKeyValueSecrets("kv", keyValueBackend.name(), ""));
            assertEquals(List.of("test"), service.listKeyValueSecrets("kv", keyValueBackend.name(), "/"));
        }
    }

    @Test
    public void testListKeyValueSecretsMissingSecretPathPrefixReturnsEmpty() throws Exception {
        try (StandardHashiCorpVaultCommunicationService service = this.configureService()) {

            final VaultTemplate vaultTemplate = Mockito.mock(VaultTemplate.class);
            final VaultKeyValueOperations keyValueOperations = Mockito.mock(VaultKeyValueOperations.class);

            final Field vaultTemplateField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("vaultTemplate");
            vaultTemplateField.setAccessible(true);
            vaultTemplateField.set(service, vaultTemplate);

            final Field keyValueBackendField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("keyValueBackend");
            keyValueBackendField.setAccessible(true);
            final KeyValueBackend keyValueBackend = (KeyValueBackend) keyValueBackendField.get(service);

            when(vaultTemplate.opsForKeyValue("kv", keyValueBackend)).thenReturn(keyValueOperations);
            when(keyValueOperations.list("groups/unknown/")).thenReturn(null);

            assertEquals(List.of(), service.listKeyValueSecrets("kv", keyValueBackend.name(), "groups/unknown"));
        }
    }

    @Test
    public void testListKeyValueSecretsDeniedSecretPathPrefixDoesNotFallBackToRoot() throws Exception {
        try (StandardHashiCorpVaultCommunicationService service = this.configureService()) {
            final VaultTemplate vaultTemplate = Mockito.mock(VaultTemplate.class);
            final VaultKeyValueOperations keyValueOperations = Mockito.mock(VaultKeyValueOperations.class);

            final Field vaultTemplateField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("vaultTemplate");
            vaultTemplateField.setAccessible(true);
            vaultTemplateField.set(service, vaultTemplate);

            final Field keyValueBackendField = StandardHashiCorpVaultCommunicationService.class.getDeclaredField("keyValueBackend");
            keyValueBackendField.setAccessible(true);
            final KeyValueBackend keyValueBackend = (KeyValueBackend) keyValueBackendField.get(service);

            when(vaultTemplate.opsForKeyValue("kv", keyValueBackend)).thenReturn(keyValueOperations);
            when(keyValueOperations.list("groups/my-group/")).thenThrow(new VaultException("Permission denied"));

            assertThrows(VaultException.class, () -> service.listKeyValueSecrets("kv", keyValueBackend.name(), "groups/my-group"));
            Mockito.verify(keyValueOperations).list("groups/my-group/");
            Mockito.verifyNoMoreInteractions(keyValueOperations);
        }
    }
}
