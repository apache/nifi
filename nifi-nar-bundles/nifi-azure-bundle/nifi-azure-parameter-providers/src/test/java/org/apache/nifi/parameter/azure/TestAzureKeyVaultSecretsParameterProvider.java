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
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.security.keyvault.secrets.models.SecretProperties;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final List<Parameter> mySecretParameters = Arrays.asList(
            parameter("paramA", "valueA"),
            parameter("paramB", "valueB"),
            parameter("otherC", "valueOther"),
            parameter("paramD", "valueD"),
            parameter("nonSensitiveE", "valueE"),
            parameter("otherF", "valueF")
    );
    private final List<Parameter> otherSecretParameters = Arrays.asList(
            parameter("paramG", "valueG"),
            parameter("otherH", "valueOther")
    );
    private final List<ParameterGroup> mockParameterGroups = Arrays.asList(
            new ParameterGroup("MySecret", mySecretParameters),
            new ParameterGroup("OtherSecret", otherSecretParameters)
    );

    @BeforeEach
    public void setup() {
        doReturn(secretClient).when(parameterProvider).configureSecretClient(any());
    }

    @Test
    public void testFetchParametersWithNoSecrets() throws IOException, InitializationException {
        final List<ParameterGroup> parameterGroups = Collections.singletonList(new ParameterGroup("MySecret", Collections.emptyList()));
        mockSecretClient(parameterGroups);
        runProviderTest(0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParameters() throws IOException, InitializationException {
        mockSecretClient(mockParameterGroups);
        runProviderTest( 8, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchDisabledParameters() throws IOException, InitializationException {
        final List<SecretProperties> secretPropertiesList = new ArrayList<>();
        for (final ParameterGroup group : mockParameterGroups) {
            for (final Parameter parameter : group.getParameters()) {
                final SecretProperties secretProperties = mock(SecretProperties.class);

                when(secretProperties.isEnabled()).thenReturn(false);

                secretPropertiesList.add(secretProperties);
            }

        }

        final PagedIterable<SecretProperties> mockIterable = mock(PagedIterable.class);
        when(secretClient.listPropertiesOfSecrets()).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(secretPropertiesList.iterator());
        runProviderTest( 0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParametersWithNullTagsShouldNotThrowError() throws IOException, InitializationException {
        final List<SecretProperties> secretPropertiesList = new ArrayList<>();
        for (final ParameterGroup group : mockParameterGroups) {
            for (final Parameter parameter : group.getParameters()) {
                final String parameterName = parameter.getDescriptor().getName();
                final String parameterValue = parameter.getValue();
                final KeyVaultSecret secret = mock(KeyVaultSecret.class);
                when(secret.getName()).thenReturn(parameterName);
                when(secret.getValue()).thenReturn(parameterValue);

                final SecretProperties secretProperties = mock(SecretProperties.class);
                when(secret.getProperties()).thenReturn(secretProperties);

                final Map<String, String> tags = null;
                when(secretProperties.getTags()).thenReturn(tags);

                when(secretProperties.getName()).thenReturn(parameterName);
                when(secretProperties.getVersion()).thenReturn(null);
                when(secretProperties.isEnabled()).thenReturn(true);
                when(secretClient.getSecret(eq(parameterName), any())).thenReturn(secret);

                secretPropertiesList.add(secretProperties);
            }

        }

        final PagedIterable<SecretProperties> mockIterable = mock(PagedIterable.class);
        when(secretClient.listPropertiesOfSecrets()).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(secretPropertiesList.iterator());
        runProviderTest( 0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParametersListFailure() throws IOException, InitializationException {
        when(secretClient.listPropertiesOfSecrets()).thenThrow(new RuntimeException("Fake RuntimeException"));
        runProviderTest(0, ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    public void testFetchParametersWithGroupNameRegex() throws IOException, InitializationException {
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
                final KeyVaultSecret secret = mock(KeyVaultSecret.class);
                when(secret.getName()).thenReturn(parameterName);
                when(secret.getValue()).thenReturn(parameterValue);

                final SecretProperties secretProperties = mock(SecretProperties.class);
                when(secret.getProperties()).thenReturn(secretProperties);

                final Map<String, String> tags = new HashMap<>();
                tags.put(AzureKeyVaultSecretsParameterProvider.GROUP_NAME_TAG, group.getGroupName());
                when(secretProperties.getTags()).thenReturn(tags);

                when(secretProperties.getName()).thenReturn(parameterName);
                when(secretProperties.getVersion()).thenReturn(null);
                when(secretProperties.isEnabled()).thenReturn(true);
                when(secretClient.getSecret(eq(parameterName), any())).thenReturn(secret);

                secretPropertiesList.add(secretProperties);
            }

        }

        final PagedIterable<SecretProperties> mockIterable = mock(PagedIterable.class);
        when(secretClient.listPropertiesOfSecrets()).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(secretPropertiesList.iterator());
    }

    private List<ParameterGroup> runProviderTest(final int expectedCount,
                                                 final ConfigVerificationResult.Outcome expectedOutcome)
            throws IOException, InitializationException {
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(AzureKeyVaultSecretsParameterProvider.GROUP_NAME_PATTERN, ".*");
        return runProviderTestWithProperties(expectedCount, expectedOutcome, properties);
    }

    private List<ParameterGroup> runProviderTestWithProperties(final int expectedCount,
                                                 final ConfigVerificationResult.Outcome expectedOutcome,
                                                 final Map<PropertyDescriptor, String> properties)
            throws InitializationException, IOException {
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null);

        List<ParameterGroup> parameterGroups = new ArrayList<>();
        // Verify parameter fetching
        if (expectedOutcome == ConfigVerificationResult.Outcome.FAILED) {
            assertThrows(RuntimeException.class, () -> parameterProvider.fetchParameters(mockConfigurationContext));
        } else {
            parameterGroups = parameterProvider.fetchParameters(mockConfigurationContext);
            final int parameterCount = (int) parameterGroups.stream()
                    .flatMap(group -> group.getParameters().stream())
                    .count();
            assertEquals(expectedCount, parameterCount);
        }

        // Verify config verification
        final List<ConfigVerificationResult> results = ((VerifiableParameterProvider) parameterProvider).verify(mockConfigurationContext, initContext.getLogger());

        assertEquals(1, results.size());
        assertEquals(expectedOutcome, results.get(0).getOutcome());

        return parameterGroups;
    }

    private static Parameter parameter(final String name, final String value) {
        return new Parameter(new ParameterDescriptor.Builder().name(name).build(), value);
    }
}
