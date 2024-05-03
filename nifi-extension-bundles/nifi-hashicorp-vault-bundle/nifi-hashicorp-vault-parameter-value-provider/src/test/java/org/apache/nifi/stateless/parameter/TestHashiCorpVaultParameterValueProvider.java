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
package org.apache.nifi.stateless.parameter;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.vault.hashicorp.HashiCorpVaultCommunicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHashiCorpVaultParameterValueProvider {

    private static final String PATH = "kv";
    private static final String CONFIG_FILE = "bootstrap-hashicorp-vault.conf";
    private static final String DEFAULT_SECRET_NAME = "Test";

    @Mock
    private HashiCorpVaultCommunicationService vaultCommunicationService;

    private HashiCorpVaultParameterValueProvider parameterProvider;

    @BeforeEach
    public void init() {
        final HashiCorpVaultParameterValueProvider realParameterProvider = new HashiCorpVaultParameterValueProvider();
        parameterProvider = Mockito.spy(realParameterProvider);

        doAnswer(args -> {
            parameterProvider.setPath(PATH);
            parameterProvider.setVaultCommunicationService(vaultCommunicationService);
            return null;
        }).when(parameterProvider).configure(CONFIG_FILE);
        parameterProvider.init(new ParameterValueProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return null;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                if (descriptor.equals(HashiCorpVaultParameterValueProvider.VAULT_CONFIG_FILE)) {
                    return new StandardPropertyValue(CONFIG_FILE, null, null);
                } else if (descriptor.equals(HashiCorpVaultParameterValueProvider.DEFAULT_SECRET_NAME)) {
                    return new StandardPropertyValue(DEFAULT_SECRET_NAME, null, null);
                }
                return null;
            }

            @Override
            public Map<String, String> getAllProperties() {
                return Stream.of(HashiCorpVaultParameterValueProvider.VAULT_CONFIG_FILE, HashiCorpVaultParameterValueProvider.DEFAULT_SECRET_NAME)
                        .collect(Collectors.toMap(PropertyDescriptor::getName, descriptor -> getProperty(descriptor).getValue()));
            }
        });
    }

    @Test
    public void testGetParameterValue() {
        final String value = "value";
        final Map<String, String> keyValues = new HashMap<>();
        keyValues.put("param", value);
        when(vaultCommunicationService.readKeyValueSecretMap(PATH, "context")).thenReturn(keyValues);

        assertEquals(value, parameterProvider.getParameterValue("context", "param"));
    }

    @Test
    public void testGetParameterValueNoSecret() {
        when(vaultCommunicationService.readKeyValueSecretMap(PATH, "context")).thenReturn(Collections.emptyMap());

        assertNull(parameterProvider.getParameterValue("context", "param"));
    }

    @Test
    public void testGetParameterValueNoMatchingKey() {
        final String value = "value";
        final Map<String, String> keyValues = new HashMap<>();
        keyValues.put("differentParam", value);
        when(vaultCommunicationService.readKeyValueSecretMap(PATH, "context")).thenReturn(keyValues);

        assertNull(parameterProvider.getParameterValue("context", "param"));
    }

    @Test
    public void testGetParameterValueDefaultSecret() {
        final String value = "value";
        final String defaultValue = "defaultValue";
        final Map<String, String> keyValues = new HashMap<>();
        keyValues.put("differentParam", value);

        final Map<String, String> defaultKeyValues = new HashMap<>();
        defaultKeyValues.put("param", defaultValue);

        // Although 'context' does not contain 'param', the default secret does, so it should fall back to that
        when(vaultCommunicationService.readKeyValueSecretMap(PATH, "context")).thenReturn(keyValues);
        when(vaultCommunicationService.readKeyValueSecretMap(PATH, DEFAULT_SECRET_NAME)).thenReturn(defaultKeyValues);

        assertEquals(defaultValue, parameterProvider.getParameterValue("context", "param"));
    }

    @Test
    public void testIsParameterDefined() {
        final String value = "value";
        final Map<String, String> keyValues = new HashMap<>();
        keyValues.put("param", value);
        when(vaultCommunicationService.readKeyValueSecretMap(PATH, "context")).thenReturn(keyValues);

        assertTrue(parameterProvider.isParameterDefined("context", "param"));
    }
}
