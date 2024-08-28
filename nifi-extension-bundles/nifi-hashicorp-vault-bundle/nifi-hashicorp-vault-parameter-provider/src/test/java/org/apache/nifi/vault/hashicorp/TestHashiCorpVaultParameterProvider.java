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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHashiCorpVaultParameterProvider {

    private HashiCorpVaultParameterProvider parameterProvider;

    @Mock
    private HashiCorpVaultCommunicationService vaultCommunicationService;

    private List<ParameterGroup> mockedGroups;

    @BeforeEach
    public void init() {
        vaultCommunicationService = mock(HashiCorpVaultCommunicationService.class);

        parameterProvider = new HashiCorpVaultParameterProvider() {
            @Override
            HashiCorpVaultCommunicationService getVaultCommunicationService(final ConfigurationContext context) {
                return vaultCommunicationService;
            }

            @Override
            protected ComponentLog getLogger() {
                return mock(ComponentLog.class);
            }
        };
        mockedGroups = new ArrayList<>();
        mockedGroups.add(new ParameterGroup("groupA", Arrays.asList(
                createParameter("paramA", "valueA"),
                createParameter("paramB", "valueB"),
                createParameter("otherParam", "valueC"))));
        mockedGroups.add(new ParameterGroup("groupA", Arrays.asList(
                createParameter("paramC", "valueC"),
                createParameter("paramD", "valueD"),
                createParameter("otherParam2", "valueE"))));
        mockedGroups.add(new ParameterGroup("groupB", Arrays.asList(
                createParameter("paramC", "valueC"),
                createParameter("paramD", "valueD"),
                createParameter("otherParam", "valueE"))));
    }

    @Test
    public void testFetchParameters() {
        final String kvVersion = "KV_1";
        mockSecrets("kv2", kvVersion, mockedGroups);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HashiCorpVaultParameterProvider.KV_PATH, "kv2");
        properties.put(HashiCorpVaultParameterProvider.KV_VERSION, kvVersion);
        properties.put(HashiCorpVaultParameterProvider.VAULT_CLIENT_SERVICE, "service");
        properties.put(HashiCorpVaultParameterProvider.SECRET_NAME_PATTERN, ".*");
        final ConfigurationContext context = mockContext(properties);

        final List<ParameterGroup> results = parameterProvider.fetchParameters(context);
        assertEquals(3, results.size());
        results.forEach(group -> {
            assertEquals(3, group.getParameters().size());
        });
    }

    @Test
    public void testFetchParametersSecretRegex() {
        final String kvVersion = "KV_2";
        mockSecrets("kv2", kvVersion, mockedGroups);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HashiCorpVaultParameterProvider.KV_PATH, "kv2");
        properties.put(HashiCorpVaultParameterProvider.KV_VERSION, kvVersion);
        properties.put(HashiCorpVaultParameterProvider.VAULT_CLIENT_SERVICE, "service");
        properties.put(HashiCorpVaultParameterProvider.SECRET_NAME_PATTERN, ".*A");
        final ConfigurationContext context = mockContext(properties);

        final List<ParameterGroup> results = parameterProvider.fetchParameters(context);
        assertEquals(2, results.size());
        results.forEach(group -> {
            assertEquals(3, group.getParameters().size());
        });
    }

    @Test
    public void testVerifyParameters() {
        final String kvVersion = "KV_1";
        mockSecrets("kv2", kvVersion, mockedGroups);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(HashiCorpVaultParameterProvider.KV_PATH, "kv2");
        properties.put(HashiCorpVaultParameterProvider.KV_VERSION, kvVersion);
        properties.put(HashiCorpVaultParameterProvider.VAULT_CLIENT_SERVICE, "service");
        properties.put(HashiCorpVaultParameterProvider.SECRET_NAME_PATTERN, ".*");
        final ConfigurationContext context = mockContext(properties);

        final List<ConfigVerificationResult> results = parameterProvider.verify(context, mock(ComponentLog.class));
        assertEquals(1, results.size());
        final String explanation = results.get(0).getExplanation();
        assertTrue(explanation.contains("3 secrets"));
        assertTrue(explanation.contains("9 Parameters"));
    }

    private ConfigurationContext mockContext(final Map<PropertyDescriptor, String> properties) {
        final ConfigurationContext context = mock(ConfigurationContext.class);
        properties.entrySet().forEach(entry -> mockProperty(context, entry.getKey(), entry.getValue()));
        return context;
    }

    private void mockProperty(final ConfigurationContext context, final PropertyDescriptor descriptor, final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        lenient().when(propertyValue.getValue()).thenReturn(value);
        lenient().when(context.getProperty(descriptor)).thenReturn(propertyValue);
    }

    private void mockSecrets(final String kvPath, final String kvVersion, final List<ParameterGroup> parameterGroups) {
        when(vaultCommunicationService.listKeyValueSecrets(kvPath, kvVersion))
                .thenReturn(parameterGroups.stream().map(group -> group.getGroupName()).collect(Collectors.toList()));
        for (final ParameterGroup parameterGroup : parameterGroups) {
            final Map<String, String> keyValues = parameterGroup.getParameters().stream()
                    .collect(Collectors.toMap(parameter -> parameter.getDescriptor().getName(), parameter -> parameter.getValue()));
            lenient().when(vaultCommunicationService.readKeyValueSecretMap(kvPath, parameterGroup.getGroupName(), kvVersion)).thenReturn(keyValues);
        }
    }

    private Parameter createParameter(final String name, final String value) {
        return new Parameter.Builder()
            .name(name)
            .value(value)
            .build();
    }
}
