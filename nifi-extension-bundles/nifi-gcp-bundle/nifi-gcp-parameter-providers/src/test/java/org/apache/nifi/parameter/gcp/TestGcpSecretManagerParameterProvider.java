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
package org.apache.nifi.parameter.gcp;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient.ListSecretsPage;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient.ListSecretsPagedResponse;
import com.google.cloud.secretmanager.v1.SecretPayload;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.ByteString;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestGcpSecretManagerParameterProvider {

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
    final List<Parameter> unrelatedParameters = Collections.singletonList(parameter("paramK", "unused"));
    final List<ParameterGroup> mockParameterGroups = Arrays.asList(
            new ParameterGroup("MySecret", mySecretParameters),
            new ParameterGroup("OtherSecret", otherSecretParameters),
            new ParameterGroup("Unrelated", unrelatedParameters) // will not be picked up
    );

    @Test
    public void testFetchParametersWithNoSecrets() throws InitializationException, IOException {
        final List<ParameterGroup> expectedGroups = Collections.singletonList(new ParameterGroup("MySecret", Collections.emptyList()));
        runProviderTest(mockSecretManagerClient(expectedGroups), 0, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParameters() throws InitializationException, IOException {
        runProviderTest(mockSecretManagerClient(mockParameterGroups), 8, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testFetchParametersListFailure() throws InitializationException, IOException {
        final SecretManagerServiceClient mockSecretsManager = mock(SecretManagerServiceClient.class);
        when(mockSecretsManager.listSecrets(any(ProjectName.class))).thenThrow(new RuntimeException("Fake exception"));
        runProviderTest(mockSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    public void testFetchParametersGetSecretFailure() throws InitializationException, IOException {
        final SecretManagerServiceClient mockSecretsManager = mock(SecretManagerServiceClient.class);
        final ListSecretsPagedResponse listSecretsPagedResponse = mock(ListSecretsPagedResponse.class);
        final ListSecretsPage page = mock(ListSecretsPage.class);
        when(listSecretsPagedResponse.getPage()).thenReturn(page);

        when(page.hasNextPage()).thenReturn(false);
        final Secret secret = mock(Secret.class);
        when(secret.getName()).thenReturn("paramA");
        when(secret.getLabelsOrDefault("group-name", null)).thenReturn("Secret");
        when(page.getValues()).thenReturn(Collections.singletonList(secret));

        when(mockSecretsManager.accessSecretVersion(any(SecretVersionName.class))).thenThrow(new RuntimeException("Fake exception"));
        runProviderTest(mockSecretsManager, 0, ConfigVerificationResult.Outcome.FAILED);
    }

    private GcpSecretManagerParameterProvider getParameterProvider() {
        return spy(new GcpSecretManagerParameterProvider());
    }

    private SecretManagerServiceClient mockSecretManagerClient(final List<ParameterGroup> mockGroups) {
        final SecretManagerServiceClient secretManager = mock(SecretManagerServiceClient.class);

        final ListSecretsPagedResponse listSecretsPagedResponse = mock(ListSecretsPagedResponse.class);
        when(secretManager.listSecrets(any(ProjectName.class))).thenReturn(listSecretsPagedResponse);

        boolean mockedFirstPage = false;
        ListSecretsPage currentPage;
        ListSecretsPage previousPage = null;
        for (final Iterator<ParameterGroup> it = mockGroups.iterator(); it.hasNext(); ) {
            final ParameterGroup group = it.next();
            currentPage = mock(ListSecretsPage.class);
            if (mockedFirstPage) {
                when(previousPage.getNextPage()).thenReturn(currentPage);
            } else {
                when(listSecretsPagedResponse.getPage()).thenReturn(currentPage);
                mockedFirstPage = true;
            }
            final List<Secret> values = new ArrayList<>();
            values.addAll(group.getParameters().stream()
                    .map(parameter -> mockSecret(secretManager, group.getGroupName(), parameter))
                    .collect(Collectors.toList()));
            when(currentPage.getValues()).thenReturn(values);
            if (it.hasNext()) {
                when(currentPage.hasNextPage()).thenReturn(true);
                previousPage = currentPage;
            } else {
                when(currentPage.hasNextPage()).thenReturn(false);
            }
        }

        return secretManager;
    }

    final Secret mockSecret(final SecretManagerServiceClient secretManager, final String groupName, final Parameter parameter) {
        final Secret secret = mock(Secret.class);

        final String parameterName = parameter.getDescriptor().getName();
        when(secret.getName()).thenReturn("projects/project/secrets/" + parameterName);
        when(secret.getLabelsOrDefault("group-name", null)).thenReturn(groupName);

        final AccessSecretVersionResponse response = mock(AccessSecretVersionResponse.class);
        doReturn(response).when(secretManager).accessSecretVersion(argThat((SecretVersionName secretVersionName) -> secretVersionName.getSecret().equals(parameterName)));
        final SecretPayload payload = mock(SecretPayload.class);
        final ByteString data = ByteString.copyFromUtf8(parameter.getValue());
        when(payload.getData()).thenReturn(data);
        when(response.getPayload()).thenReturn(payload);
        return secret;
    }

    private List<ParameterGroup> runProviderTest(final SecretManagerServiceClient secretsManager, final int expectedCount,
                                                 final ConfigVerificationResult.Outcome expectedOutcome) throws InitializationException, IOException {

        final GcpSecretManagerParameterProvider parameterProvider = getParameterProvider();
        doReturn(secretsManager).when(parameterProvider).configureClient(any());
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(GcpSecretManagerParameterProvider.GROUP_NAME_PATTERN, ".*Secret");
        properties.put(GcpSecretManagerParameterProvider.PROJECT_ID, "my-project");
        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);

        List<ParameterGroup> parameterGroups = new ArrayList<>();
        // Verify parameter fetching
        if (expectedOutcome == ConfigVerificationResult.Outcome.FAILED) {
            assertThrows(RuntimeException.class, () -> parameterProvider.fetchParameters(mockConfigurationContext));
        } else {
            parameterGroups = parameterProvider.fetchParameters(mockConfigurationContext);
            final int count = (int) parameterGroups.stream()
                    .flatMap(group -> group.getParameters().stream())
                    .count();
            assertEquals(expectedCount, count);
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

}
