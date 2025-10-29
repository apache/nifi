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
import org.mockito.Mockito;
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

    private HashiCorpVaultCommunicationService configureService() {
        return new StandardHashiCorpVaultCommunicationService(properties);
    }

    @Test
    public void testBasicConfiguration() {
        this.configureService();

        // Once to check if the URI is https, once by VaultTemplate, and once to validate
        Mockito.verify(properties, Mockito.times(3)).getUri();

        // Once to check if the property is set, and once to retrieve the value
        Mockito.verify(properties, Mockito.times(2)).getAuthPropertiesFilename();
    }

    @Test
    public void testTimeouts() {
        when(properties.getConnectionTimeout()).thenReturn(Optional.of("20 secs"));
        when(properties.getReadTimeout()).thenReturn(Optional.of("40 secs"));
        this.configureService();
    }

    @Test
    public void testListKeyValueSecretsRecursesNestedPaths() throws Exception {
        when(properties.getKvVersion()).thenReturn(2);

        final StandardHashiCorpVaultCommunicationService service = (StandardHashiCorpVaultCommunicationService) this.configureService();

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
