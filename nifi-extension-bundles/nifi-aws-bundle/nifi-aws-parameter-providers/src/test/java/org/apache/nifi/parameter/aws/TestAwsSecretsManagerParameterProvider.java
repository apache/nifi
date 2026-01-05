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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ListSecretsRequest;
import software.amazon.awssdk.services.secretsmanager.model.ListSecretsResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretListEntry;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestAwsSecretsManagerParameterProvider {

    @Mock
    private SecretsManagerClient defaultSecretsManager;

    @Mock
    private ListSecretsResponse emptyListSecretsResponse;

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

    @Test
    public void testFetchParametersWithNoSecrets() throws InitializationException {
        final List<ParameterGroup> expectedGroups = Collections.singletonList(new ParameterGroup("MySecret", Collections.emptyList()));
        runProviderTest(mockSecretsManager(expectedGroups), 0, ConfigVerificationResult.Outcome.SUCCESSFUL, "PATTERN", null);
    }

    @Test
    public void testFetchParameters() throws InitializationException {
        runProviderTest(mockSecretsManager(mockParameterGroups), 8, ConfigVerificationResult.Outcome.SUCCESSFUL, "PATTERN", null);
    }

    @Test
    public void testFetchSpecificSecret() throws InitializationException {
        runProviderTest(mockSecretsManagerNoList(mockParameterGroups, "MySecret"), 6, ConfigVerificationResult.Outcome.SUCCESSFUL, "ENUMERATION", "MySecret");
    }

    @Test
    public void testFetchTwoSecrets() throws InitializationException {
        runProviderTest(mockSecretsManagerNoList(mockParameterGroups, "MySecret,OtherSecret"), 8, ConfigVerificationResult.Outcome.SUCCESSFUL, "ENUMERATION", "MySecret,OtherSecret");
    }

    @Test
    public void testFetchNonExistentSecret() throws InitializationException {
        when(defaultSecretsManager.getSecretValue(argThat(matchesGetSecretValueRequest("MySecretDoesNotExist")))).thenThrow(ResourceNotFoundException.builder().message("Fake exception").build());
        runProviderTest(defaultSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED, "ENUMERATION", "BadSecret");
    }

    @Test
    public void testFetchParametersListFailure() throws InitializationException {
        when(defaultSecretsManager.listSecrets(any(ListSecretsRequest.class))).thenThrow(SecretsManagerException.builder().message("Fake exception").build());
        runProviderTest(defaultSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED, "PATTERN", null);
    }

    @Test
    public void testFetchParametersGetSecretFailure() throws InitializationException {
        final List<SecretListEntry> secretList = Collections.singletonList(SecretListEntry.builder().name("MySecret").build());
        final ListSecretsResponse listSecretsResponse = mock(ListSecretsResponse.class);
        when(listSecretsResponse.secretList()).thenReturn(secretList);
        when(defaultSecretsManager.listSecrets(argThat(ListSecretsRequestMatcher.hasToken(null)))).thenReturn(listSecretsResponse);
        when(defaultSecretsManager.getSecretValue(argThat(matchesGetSecretValueRequest("MySecret")))).thenThrow(SecretsManagerException.builder().message("Fake exception").build());
        runProviderTest(defaultSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED, "PATTERN", null);
    }

    @Test
    public void testFetchParametersWithNonStringValues() throws InitializationException {
        // JSON with string, number, boolean, and null values
        final String secretString = "{ \"stringParam\": \"stringValue\", \"numberParam\": 5432, \"booleanParam\": true, \"nullParam\": null }";

        final SecretsManagerClient secretsManager = mock(SecretsManagerClient.class);
        final GetSecretValueResponse response = GetSecretValueResponse.builder()
                .name("MixedSecret")
                .secretString(secretString)
                .build();
        when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest("MixedSecret")))).thenReturn(response);

        final List<ParameterGroup> parameterGroups = runProviderTest(secretsManager, 3, ConfigVerificationResult.Outcome.SUCCESSFUL, "ENUMERATION", "MixedSecret");

        assertEquals(1, parameterGroups.size());
        final ParameterGroup group = parameterGroups.get(0);
        assertEquals("MixedSecret", group.getGroupName());

        final Map<String, String> parameterValues = group.getParameters().stream()
                .collect(Collectors.toMap(p -> p.getDescriptor().getName(), Parameter::getValue));

        assertEquals("stringValue", parameterValues.get("stringParam"));
        assertEquals("5432", parameterValues.get("numberParam"));
        assertEquals("true", parameterValues.get("booleanParam"));
        // nullParam should not be included
        assertEquals(3, parameterValues.size());
    }

    @Test
    public void testFetchParametersWithNestedObjectsIgnored() throws InitializationException {
        // JSON with nested objects and arrays that should be ignored
        final String secretString = "{ \"validParam\": \"validValue\", \"nestedObject\": { \"inner\": \"value\" }, \"arrayParam\": [1, 2, 3] }";

        final SecretsManagerClient secretsManager = mock(SecretsManagerClient.class);
        final GetSecretValueResponse response = GetSecretValueResponse.builder()
                .name("NestedSecret")
                .secretString(secretString)
                .build();
        when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest("NestedSecret")))).thenReturn(response);

        final List<ParameterGroup> parameterGroups = runProviderTest(secretsManager, 1, ConfigVerificationResult.Outcome.SUCCESSFUL, "ENUMERATION", "NestedSecret");

        assertEquals(1, parameterGroups.size());
        final ParameterGroup group = parameterGroups.get(0);

        final Map<String, String> parameterValues = group.getParameters().stream()
                .collect(Collectors.toMap(p -> p.getDescriptor().getName(), Parameter::getValue));

        // Only the simple value param should be included
        assertEquals(1, parameterValues.size());
        assertEquals("validValue", parameterValues.get("validParam"));
    }

    private AwsSecretsManagerParameterProvider getParameterProvider() {
        return spy(new AwsSecretsManagerParameterProvider());
    }

    private SecretsManagerClient mockSecretsManagerNoList(final List<ParameterGroup> mockParameterGroups, final String secretNames) {
        final SecretsManagerClient secretsManager = mock(SecretsManagerClient.class);

        mockParameterGroups.forEach(group -> {
            final String groupName = group.getGroupName();
            for (String secretName : Arrays.asList(secretNames.split(","))) {
                if (groupName.equalsIgnoreCase(secretName)) {
                    final Map<String, String> keyValues = group.getParameters().stream().collect(Collectors.toMap(
                            param -> param.getDescriptor().getName(),
                            Parameter::getValue));
                    final String secretString;
                    try {
                        secretString = objectMapper.writeValueAsString(keyValues);
                        final GetSecretValueResponse response = GetSecretValueResponse.builder().name(groupName).secretString(secretString).build();
                        when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest(groupName))))
                                .thenReturn(response);
                    } catch (final JsonProcessingException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
        });
        return secretsManager;
    }
    private SecretsManagerClient mockSecretsManager(final List<ParameterGroup> mockParameterGroups) {
        final SecretsManagerClient secretsManager = mock(SecretsManagerClient.class);
        when(emptyListSecretsResponse.secretList()).thenReturn(Collections.emptyList());

        String currentToken = null;
        for (int i = 0; i < mockParameterGroups.size(); i++) {
            final ParameterGroup group = mockParameterGroups.get(i);
            final List<SecretListEntry> secretList = Collections.singletonList(SecretListEntry.builder().name(group.getGroupName()).build());
            final ListSecretsResponse listSecretsResponse = mock(ListSecretsResponse.class);
            when(listSecretsResponse.secretList()).thenReturn(secretList);
            when(secretsManager.listSecrets(argThat(ListSecretsRequestMatcher.hasToken(currentToken)))).thenReturn(listSecretsResponse);

            currentToken = "token-" + i;
            when(listSecretsResponse.nextToken()).thenReturn(currentToken);
        }
        when(secretsManager.listSecrets(argThat(ListSecretsRequestMatcher.hasToken(currentToken)))).thenReturn(emptyListSecretsResponse);

        mockParameterGroups.forEach(group -> {
            final String groupName = group.getGroupName();
            final Map<String, String> keyValues = group.getParameters().stream().collect(Collectors.toMap(
                    param -> param.getDescriptor().getName(),
                    Parameter::getValue));
            final String secretString;
            try {
                secretString = objectMapper.writeValueAsString(keyValues);
                final GetSecretValueResponse response = GetSecretValueResponse.builder().name(groupName).secretString(secretString).build();
                when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest(groupName))))
                        .thenReturn(response);
            } catch (final JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        });
        return secretsManager;
    }

    private List<ParameterGroup> runProviderTest(final SecretsManagerClient secretsManager,
                                                 final int expectedCount,
                                                 final ConfigVerificationResult.Outcome expectedOutcome,
                                                 final String listingStrategy,
                                                 final String secretNames) throws InitializationException {

        final AwsSecretsManagerParameterProvider parameterProvider = getParameterProvider();
        doReturn(secretsManager).when(parameterProvider).configureClient(any());
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);
        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        if (listingStrategy != null) {
            properties.put(AwsSecretsManagerParameterProvider.SECRET_LISTING_STRATEGY, listingStrategy);
        }
        if (secretNames != null) {
            properties.put(AwsSecretsManagerParameterProvider.SECRET_NAMES, secretNames);
        }
        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);

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
        return new Parameter.Builder()
            .name(name)
            .value(value)
            .build();
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
            return argument != null && argument.secretId().equals(secretId);
        }
    }

    private static class ListSecretsRequestMatcher implements ArgumentMatcher<ListSecretsRequest> {

        private static ListSecretsRequestMatcher hasToken(final String token) {
            return new ListSecretsRequestMatcher(token);
        }

        private final String token;

        private ListSecretsRequestMatcher(final String token) {
            this.token = token;
        }

        @Override
        public boolean matches(final ListSecretsRequest argument) {
            return argument != null && Objects.equals(argument.nextToken(), token);
        }
    }
}
