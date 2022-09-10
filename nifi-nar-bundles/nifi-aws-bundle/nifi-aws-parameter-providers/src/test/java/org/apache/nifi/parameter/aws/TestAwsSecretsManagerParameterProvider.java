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
package org.apache.nifi.parameter.aws;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ListSecretsResult;
import com.amazonaws.services.secretsmanager.model.SecretListEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestAwsSecretsManagerParameterProvider {
    final ObjectMapper objectMapper = new ObjectMapper();

    final List<Parameter> mySecretParameters = Arrays.asList(
            parameter("paramA", "valueA"),
            parameter("paramB", "valueB"),
            parameter("otherC", "valueOther"),
            parameter("paramD", "valueD"),
            parameter("nonSensitiveE", "valueE"),
            parameter("otherF", "valueF")
    );
    final List<Parameter> otherSecretParameters = Arrays.asList(
            parameter("paramG", "valueG"),
            parameter("otherH", "valueOther")
    );
    final List<ParameterGroup> mockParameterGroups = Arrays.asList(
            new ParameterGroup("MySecret", mySecretParameters),
            new ParameterGroup("OtherSecret", otherSecretParameters)
    );

    private AwsSecretsManagerParameterProvider getParameterProvider() {
        return spy(new AwsSecretsManagerParameterProvider());
    }

    private AWSSecretsManager mockSecretsManager(final List<ParameterGroup> mockGroup) {
        final AWSSecretsManager secretsManager = mock(AWSSecretsManager.class);

        final ListSecretsResult listSecretsResult = mock(ListSecretsResult.class);
        final List<SecretListEntry> secretList = mockGroup.stream()
                .map(group -> new SecretListEntry().withName(group.getGroupName()))
                .collect(Collectors.toList());
        when(listSecretsResult.getSecretList()).thenReturn(secretList);
        when(secretsManager.listSecrets(any())).thenReturn(listSecretsResult);

        mockGroup.forEach(group -> {
            final String groupName = group.getGroupName();
            final Map<String, String> keyValues = group.getParameters().stream().collect(Collectors.toMap(
                    param -> param.getDescriptor().getName(),
                    Parameter::getValue));
            final String secretString;
            try {
                secretString = objectMapper.writeValueAsString(keyValues);
                final GetSecretValueResult result = new GetSecretValueResult().withName(groupName).withSecretString(secretString);
                when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest(groupName))))
                        .thenReturn(result);
            } catch (final JsonProcessingException e) {
                // Ignore
            }
        });
        return secretsManager;
    }

    private List<ParameterGroup> runProviderTest(final AWSSecretsManager secretsManager, final int expectedCount,
                                                 final ConfigVerificationResult.Outcome expectedOutcome) throws InitializationException {

        final AwsSecretsManagerParameterProvider parameterProvider = getParameterProvider();
        doReturn(secretsManager).when(parameterProvider).configureClient(any());
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
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

    @Test
    public void testFetchParametersWithNoSecrets() throws InitializationException {
        final List<ParameterGroup> expectedGroups = Collections.singletonList(new ParameterGroup("MySecret", Collections.emptyList()));
        runProviderTest(mockSecretsManager(expectedGroups), 0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParameters() throws InitializationException {
        runProviderTest(mockSecretsManager(mockParameterGroups), 8, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParametersListFailure() throws InitializationException {
        final AWSSecretsManager mockSecretsManager = mock(AWSSecretsManager.class);
        when(mockSecretsManager.listSecrets(any())).thenThrow(new AWSSecretsManagerException("Fake exception"));
        runProviderTest(mockSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    public void testFetchParametersGetSecretFailure() throws InitializationException {
        final AWSSecretsManager mockSecretsManager = mock(AWSSecretsManager.class);
        final ListSecretsResult listSecretsResult = mock(ListSecretsResult.class);
        final List<SecretListEntry> secretList = Collections.singletonList(new SecretListEntry().withName("MySecret"));
        when(listSecretsResult.getSecretList()).thenReturn(secretList);
        when(mockSecretsManager.listSecrets(any())).thenReturn(listSecretsResult);
        when(mockSecretsManager.getSecretValue(argThat(matchesGetSecretValueRequest("MySecret")))).thenThrow(new AWSSecretsManagerException("Fake exception"));
        runProviderTest(mockSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED);
    }

    private static Parameter parameter(final String name, final String value) {
        return new Parameter(new ParameterDescriptor.Builder().name(name).build(), value);
    }

    private static ArgumentMatcher<GetSecretValueRequest> matchesGetSecretValueRequest(final String groupName) {
        return new GetSecretValueRequestMatcher(groupName);
    }

    private static class GetSecretValueRequestMatcher implements ArgumentMatcher<GetSecretValueRequest> {

        private final String secretId;

        private GetSecretValueRequestMatcher(final String secretId) {
            this.secretId = secretId;
        }

        @Override
        public boolean matches(final GetSecretValueRequest argument) {
            return argument != null && argument.getSecretId().equals(secretId);
        }
    }
}
