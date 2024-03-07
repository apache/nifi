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
package org.apache.nifi.parameter.azure;

import com.azure.core.http.rest.PagedIterable;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.implementation.SecretPropertiesHelper;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.security.keyvault.secrets.models.SecretProperties;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestAzureKeyVaultSecretsParameterProvider {

    @Mock
    private SecretClient secretClient;
    @Spy
    private AzureKeyVaultSecretsParameterProvider parameterProvider;

    private final List<Parameter> mySecretParameters = List.of(
            parameter("paramA", "valueA"),
            parameter("paramB", "valueB"),
            parameter("otherC", "valueOther"),
            parameter("paramD", "valueD"),
            parameter("nonSensitiveE", "valueE"),
            parameter("otherF", "valueF")
    );
    private final List<Parameter> otherSecretParameters = List.of(
            parameter("paramG", "valueG"),
            parameter("otherH", "valueOther")
    );
    private final List<ParameterGroup> mockParameterGroups = List.of(
            new ParameterGroup("MySecret", mySecretParameters),
            new ParameterGroup("OtherSecret", otherSecretParameters)
    );

    @BeforeEach
    public void setup() {
        doReturn(secretClient).when(parameterProvider).configureSecretClient(any());
    }

    @Test
    public void testFetchParametersWithNoSecrets() throws InitializationException {
        final List<ParameterGroup> parameterGroups = List.of(new ParameterGroup("MySecret", Collections.emptyList()));
        mockSecretClient(parameterGroups);
        runProviderTest(0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParameters() throws InitializationException {
        mockSecretClient(mockParameterGroups);
        runProviderTest(8, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchDisabledParameters() throws InitializationException {
        final List<SecretProperties> secretPropertiesList = Stream
                .generate(() -> new SecretProperties().setEnabled(false))
                .limit(mockParameterGroups.stream().mapToInt(group -> group.getParameters().size()).sum())
                .toList();

        mockListPropertiesOfSecrets(secretPropertiesList);
        runProviderTest(0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParametersWithNullTagsShouldNotThrowError() throws InitializationException {
        final List<SecretProperties> secretPropertiesList = new ArrayList<>();
        for (final ParameterGroup group : mockParameterGroups) {
            for (final Parameter parameter : group.getParameters()) {
                final String parameterName = parameter.getDescriptor().getName();
                final String parameterValue = parameter.getValue();

                final SecretProperties secretProperties = new SecretProperties();
                SecretPropertiesHelper.setName(secretProperties, parameterName);
                secretProperties.setEnabled(true);

                final KeyVaultSecret secret = new KeyVaultSecret(parameterName, parameterValue);
                secret.setProperties(secretProperties);

                when(secretClient.getSecret(eq(parameterName), any())).thenReturn(secret);
                secretPropertiesList.add(secretProperties);
            }
        }

        mockListPropertiesOfSecrets(secretPropertiesList);
        runProviderTest(0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParametersListFailure() throws InitializationException {
        when(secretClient.listPropertiesOfSecrets()).thenThrow(new RuntimeException("Fake RuntimeException"));
        runProviderTest(0, ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    public void testFetchParametersWithGroupNameRegex() throws InitializationException {
        mockSecretClient(mockParameterGroups);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(AzureKeyVaultSecretsParameterProvider.GROUP_NAME_PATTERN, "MySecret");
        runProviderTestWithProperties(6, ConfigVerificationResult.Outcome.SUCCESSFUL, properties);
    }

    private void mockSecretClient(final List<ParameterGroup> mockGroups) {
        final List<SecretProperties> secretPropertiesList = new ArrayList<>();
        for (final ParameterGroup group : mockGroups) {
            for (final Parameter parameter : group.getParameters()) {
                final String parameterName = parameter.getDescriptor().getName();
                final String parameterValue = parameter.getValue();

                final SecretProperties secretProperties = new SecretProperties();
                SecretPropertiesHelper.setName(secretProperties, parameterName);
                secretProperties.setTags(
                        Map.of(AzureKeyVaultSecretsParameterProvider.GROUP_NAME_TAG, group.getGroupName())
                );
                secretProperties.setEnabled(true);

                final KeyVaultSecret secret = new KeyVaultSecret(parameterName, parameterValue);
                secret.setProperties(secretProperties);

                when(secretClient.getSecret(eq(parameterName), any())).thenReturn(secret);
                secretPropertiesList.add(secretProperties);
            }
        }

        mockListPropertiesOfSecrets(secretPropertiesList);
    }

    private void mockListPropertiesOfSecrets(final List<SecretProperties> secretPropertiesList) {
        final PagedIterable<SecretProperties> mockIterable = mock(PagedIterable.class);
        when(mockIterable.iterator()).thenReturn(secretPropertiesList.iterator());
        when(secretClient.listPropertiesOfSecrets()).thenReturn(mockIterable);
    }

    private void runProviderTest(final int expectedCount, final ConfigVerificationResult.Outcome expectedOutcome)
            throws InitializationException {
        runProviderTestWithProperties(
                expectedCount, expectedOutcome,
                Map.of(AzureKeyVaultSecretsParameterProvider.GROUP_NAME_PATTERN, ".*")
        );
    }

    private void runProviderTestWithProperties(final int expectedCount,
                                               final ConfigVerificationResult.Outcome expectedOutcome,
                                               final Map<PropertyDescriptor, String> properties)
            throws InitializationException {
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);

        List<ParameterGroup> parameterGroups = new ArrayList<>();
        // Verify parameter fetching
        if (expectedOutcome == ConfigVerificationResult.Outcome.FAILED) {
            assertThrows(RuntimeException.class, () -> parameterProvider.fetchParameters(mockConfigurationContext));
        } else {
            parameterGroups = parameterProvider.fetchParameters(mockConfigurationContext);
            final int parameterCount = parameterGroups.stream()
                    .mapToInt(group -> group.getParameters().size())
                    .sum();
            assertEquals(expectedCount, parameterCount);
        }

        // Verify config verification
        final List<ConfigVerificationResult> results = ((VerifiableParameterProvider) parameterProvider).verify(mockConfigurationContext, initContext.getLogger());

        assertEquals(1, results.size());
        assertEquals(expectedOutcome, results.getFirst().getOutcome());
    }

    private static Parameter parameter(final String name, final String value) {
        return new Parameter.Builder()
            .name(name)
            .value(value)
            .build();
    }
}
