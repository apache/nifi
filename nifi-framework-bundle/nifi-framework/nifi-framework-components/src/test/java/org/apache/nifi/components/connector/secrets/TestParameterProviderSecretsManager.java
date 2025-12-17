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
package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestParameterProviderSecretsManager {

    private static final String PROVIDER_1_ID = "provider-1-id";
    private static final String PROVIDER_1_NAME = "Provider One";
    private static final String PROVIDER_2_ID = "provider-2-id";
    private static final String PROVIDER_2_NAME = "Provider Two";

    private static final String GROUP_1_NAME = "Group One";
    private static final String GROUP_2_NAME = "Group Two";

    private static final String SECRET_1_NAME = "secret-one";
    private static final String SECRET_1_DESCRIPTION = "First secret";
    private static final String SECRET_1_VALUE = "secret-value-one";

    private static final String SECRET_2_NAME = "secret-two";
    private static final String SECRET_2_DESCRIPTION = "Second secret";
    private static final String SECRET_2_VALUE = "secret-value-two";

    private static final String SECRET_3_NAME = "secret-three";
    private static final String SECRET_3_DESCRIPTION = "Third secret";
    private static final String SECRET_3_VALUE = "secret-value-three";

    private ParameterProviderSecretsManager secretsManager;

    @BeforeEach
    public void setup() {
        final FlowManager flowManager = mock(FlowManager.class);
        final ParameterProviderNode providerNode1 = createMockedParameterProviderNode(PROVIDER_1_ID, PROVIDER_1_NAME, GROUP_1_NAME,
            createParameter(SECRET_1_NAME, SECRET_1_DESCRIPTION, SECRET_1_VALUE),
            createParameter(SECRET_2_NAME, SECRET_2_DESCRIPTION, SECRET_2_VALUE));
        final ParameterProviderNode providerNode2 = createMockedParameterProviderNode(PROVIDER_2_ID, PROVIDER_2_NAME, GROUP_2_NAME,
            createParameter(SECRET_3_NAME, SECRET_3_DESCRIPTION, SECRET_3_VALUE));

        final Set<ParameterProviderNode> providers = new HashSet<>();
        providers.add(providerNode1);
        providers.add(providerNode2);
        when(flowManager.getAllParameterProviders()).thenReturn(providers);

        secretsManager = new ParameterProviderSecretsManager();
        final SecretsManagerInitializationContext initContext = new StandardSecretsManagerInitializationContext(flowManager);
        secretsManager.initialize(initContext);
    }

    private ParameterProviderNode createMockedParameterProviderNode(final String id, final String name, final String groupName, final Parameter... parameters) {
        final ParameterProviderNode node = mock(ParameterProviderNode.class);
        when(node.getIdentifier()).thenReturn(id);
        when(node.getName()).thenReturn(name);

        final List<Parameter> parameterList = List.of(parameters);
        final ParameterGroup group = new ParameterGroup(groupName, parameterList);
        final List<ParameterGroup> groups = List.of(group);

        when(node.fetchParameterValues()).thenReturn(groups);

        when(node.fetchParameterValues(anyList())).thenAnswer(invocation -> {
            final List<String> requestedNames = invocation.getArgument(0);
            final List<Parameter> matchingParameters = parameterList.stream()
                    .filter(p -> requestedNames.contains(groupName + "." + p.getDescriptor().getName()))
                    .toList();
            if (matchingParameters.isEmpty()) {
                return List.of();
            }
            return List.of(new ParameterGroup(groupName, matchingParameters));
        });

        return node;
    }

    private Parameter createParameter(final String name, final String description, final String value) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder()
            .name(name)
            .description(description)
            .build();
        return new Parameter.Builder()
            .descriptor(descriptor)
            .value(value)
            .build();
    }

    private SecretReference createSecretReference(final String providerId, final String providerName, final String secretName) {
        final String groupName = getGroupName(secretName);
        return new SecretReference(providerId, providerName, secretName, groupName + "." + secretName);
    }

    private String getGroupName(final String secretName) {
        if (secretName.equals(SECRET_1_NAME) || secretName.equals(SECRET_2_NAME)) {
            return GROUP_1_NAME;
        }

        return GROUP_2_NAME;
    }

    @Test
    public void testGetSecretProvidersReturnsOneProviderPerMockedParameterProvider() {
        final Set<SecretProvider> providers = secretsManager.getSecretProviders();

        assertEquals(2, providers.size());

        boolean foundProvider1 = false;
        boolean foundProvider2 = false;
        for (final SecretProvider provider : providers) {
            if (PROVIDER_1_ID.equals(provider.getProviderId())) {
                assertEquals(PROVIDER_1_NAME, provider.getProviderName());
                foundProvider1 = true;
            } else if (PROVIDER_2_ID.equals(provider.getProviderId())) {
                assertEquals(PROVIDER_2_NAME, provider.getProviderName());
                foundProvider2 = true;
            }
        }

        assertTrue(foundProvider1);
        assertTrue(foundProvider2);
    }

    @Test
    public void testGetAllSecretsRetrievesSecretsFromAllProviders() {
        final List<Secret> allSecrets = secretsManager.getAllSecrets();

        assertEquals(3, allSecrets.size());

        boolean foundSecret1 = false;
        boolean foundSecret2 = false;
        boolean foundSecret3 = false;

        for (final Secret secret : allSecrets) {
            if (SECRET_1_NAME.equals(secret.getName())) {
                assertEquals(PROVIDER_1_NAME, secret.getProviderName());
                assertEquals(GROUP_1_NAME, secret.getGroupName());
                assertEquals(SECRET_1_VALUE, secret.getValue());
                foundSecret1 = true;
            } else if (SECRET_2_NAME.equals(secret.getName())) {
                assertEquals(PROVIDER_1_NAME, secret.getProviderName());
                assertEquals(GROUP_1_NAME, secret.getGroupName());
                assertEquals(SECRET_2_VALUE, secret.getValue());
                foundSecret2 = true;
            } else if (SECRET_3_NAME.equals(secret.getName())) {
                assertEquals(PROVIDER_2_NAME, secret.getProviderName());
                assertEquals(GROUP_2_NAME, secret.getGroupName());
                assertEquals(SECRET_3_VALUE, secret.getValue());
                foundSecret3 = true;
            }
        }

        assertTrue(foundSecret1);
        assertTrue(foundSecret2);
        assertTrue(foundSecret3);
    }

    @Test
    public void testGetSecretReturnsPopulatedOptionalWhenSecretIsFoundById() {
        final SecretReference reference = createSecretReference(PROVIDER_1_ID, null, SECRET_1_NAME);

        final Optional<Secret> result = secretsManager.getSecret(reference);

        assertTrue(result.isPresent());
        final Secret secret = result.get();
        assertEquals(SECRET_1_NAME, secret.getName());
        assertEquals(SECRET_1_VALUE, secret.getValue());
        assertEquals(PROVIDER_1_NAME, secret.getProviderName());
    }

    @Test
    public void testGetSecretReturnsPopulatedOptionalWhenSecretIsFoundByName() {
        final SecretReference reference = createSecretReference(null, PROVIDER_2_NAME, SECRET_3_NAME);

        final Optional<Secret> result = secretsManager.getSecret(reference);

        assertTrue(result.isPresent());
        final Secret secret = result.get();
        assertEquals(SECRET_3_NAME, secret.getName());
        assertEquals(SECRET_3_VALUE, secret.getValue());
        assertEquals(PROVIDER_2_NAME, secret.getProviderName());
    }

    @Test
    public void testGetSecretReturnsEmptyOptionalWhenInvalidSecretNameProvided() {
        final SecretReference reference = createSecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, "non-existent-secret");
        final Optional<Secret> result = secretsManager.getSecret(reference);
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetSecretReturnsEmptyOptionalWhenProviderNotFound() {
        final SecretReference reference = createSecretReference("invalid-provider-id", "Invalid Provider", SECRET_1_NAME);
        final Optional<Secret> result = secretsManager.getSecret(reference);
        assertFalse(result.isPresent());
    }

    @Test
    public void testGetSecretsReturnsAllSecretsProperlyMappedWhenAllAreFound() {
        final SecretReference reference1 = createSecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, SECRET_1_NAME);
        final SecretReference reference2 = createSecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, SECRET_2_NAME);
        final SecretReference reference3 = createSecretReference(PROVIDER_2_ID, PROVIDER_2_NAME, SECRET_3_NAME);
        final Set<SecretReference> references = Set.of(reference1, reference2, reference3);

        final Map<SecretReference, Secret> results = secretsManager.getSecrets(references);
        assertEquals(3, results.size());

        final Secret secret1 = results.get(reference1);
        assertNotNull(secret1);
        assertEquals(SECRET_1_NAME, secret1.getName());
        assertEquals(SECRET_1_VALUE, secret1.getValue());

        final Secret secret2 = results.get(reference2);
        assertNotNull(secret2);
        assertEquals(SECRET_2_NAME, secret2.getName());
        assertEquals(SECRET_2_VALUE, secret2.getValue());

        final Secret secret3 = results.get(reference3);
        assertNotNull(secret3);
        assertEquals(SECRET_3_NAME, secret3.getName());
        assertEquals(SECRET_3_VALUE, secret3.getValue());
    }

    @Test
    public void testGetSecretsReturnsNullValueForSecretWithInvalidName() {
        final SecretReference validReference = createSecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, SECRET_1_NAME);
        final SecretReference invalidReference = createSecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, "non-existent-secret");
        final Set<SecretReference> references = Set.of(validReference, invalidReference);

        final Map<SecretReference, Secret> results = secretsManager.getSecrets(references);
        assertEquals(2, results.size());

        final Secret validSecret = results.get(validReference);
        assertNotNull(validSecret);
        assertEquals(SECRET_1_NAME, validSecret.getName());

        assertTrue(results.containsKey(invalidReference));
        assertNull(results.get(invalidReference));
    }

    @Test
    public void testGetSecretsReturnsNullValueForSecretWithInvalidProvider() {
        final SecretReference validReference = createSecretReference(PROVIDER_1_ID, PROVIDER_1_NAME, SECRET_1_NAME);
        final SecretReference invalidProviderReference = createSecretReference("invalid-id", "Invalid Provider", SECRET_1_NAME);
        final Set<SecretReference> references = Set.of(validReference, invalidProviderReference);

        final Map<SecretReference, Secret> results = secretsManager.getSecrets(references);
        assertEquals(2, results.size());

        final Secret validSecret = results.get(validReference);
        assertNotNull(validSecret);

        assertTrue(results.containsKey(invalidProviderReference));
        assertNull(results.get(invalidProviderReference));
    }

    @Test
    public void testGetSecretsFromMultipleProvidersWithMixedResults() {
        final SecretReference provider1Secret1 = createSecretReference(PROVIDER_1_ID, null, SECRET_1_NAME);
        final SecretReference provider1Invalid = createSecretReference(PROVIDER_1_ID, null, "invalid-secret");
        final SecretReference provider2Secret3 = createSecretReference(PROVIDER_2_ID, null, SECRET_3_NAME);
        final SecretReference invalidProvider = createSecretReference("invalid-id", null, SECRET_1_NAME);

        final Set<SecretReference> references = Set.of(provider1Secret1, provider1Invalid, provider2Secret3, invalidProvider);
        final Map<SecretReference, Secret> results = secretsManager.getSecrets(references);
        assertEquals(4, results.size());

        assertNotNull(results.get(provider1Secret1));
        assertEquals(SECRET_1_NAME, results.get(provider1Secret1).getName());

        assertTrue(results.containsKey(provider1Invalid));
        assertNull(results.get(provider1Invalid));

        assertNotNull(results.get(provider2Secret3));
        assertEquals(SECRET_3_NAME, results.get(provider2Secret3).getName());

        assertTrue(results.containsKey(invalidProvider));
        assertNull(results.get(invalidProvider));
    }

    @Test
    public void testGetSecretProviderSearchesByIdFirst() {
        final SecretReference referenceWithBothIdAndName = createSecretReference(PROVIDER_1_ID, PROVIDER_2_NAME, SECRET_1_NAME);
        final Optional<Secret> result = secretsManager.getSecret(referenceWithBothIdAndName);
        assertTrue(result.isPresent());
        assertEquals(PROVIDER_1_NAME, result.get().getProviderName());
    }
}

