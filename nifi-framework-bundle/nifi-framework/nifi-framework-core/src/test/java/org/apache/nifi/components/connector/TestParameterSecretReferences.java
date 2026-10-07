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

package org.apache.nifi.components.connector;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestParameterSecretReferences {

    @Test
    void testMatchSensitiveParametersToUniqueSecretReferences() {
        final SecretReference firstReference = new SecretReference("provider-1", "Provider 1", "first", "Provider 1.group.first");
        final SecretReference secondReference = new SecretReference("provider-2", "Provider 2", "second", "Provider 2.group.second");
        final Secret firstSecret = secret("shared");
        final Secret secondSecret = secret("shared");
        final ParameterContext parameterContext = parameterContext(
            parameter("Bound One", "unique", true),
            parameter("Bound Two", "unique", true),
            parameter("Ambiguous", "shared", true),
            parameter("Plain", "unique", false),
            parameter("Empty", "", true),
            assetParameter("Asset"));

        final Map<String, SecretReference> matched = ParameterSecretReferences.matchSensitiveParametersToSecretReferences(parameterContext,
            Map.of(firstReference, secret("unique"), secondReference, secret("other")));
        assertEquals(Map.of("Bound One", firstReference, "Bound Two", firstReference), matched);

        final Map<String, SecretReference> ambiguous = ParameterSecretReferences.matchSensitiveParametersToSecretReferences(parameterContext,
            Map.of(firstReference, firstSecret, secondReference, secondSecret));
        assertFalse(ambiguous.containsKey("Ambiguous"));
    }

    @Test
    void testStripSensitiveValuesAndPreservesInput() {
        final VersionedParameterContext context = versionedContext(
            versionedParameter("Bound", "secret", true),
            versionedParameter("Unbound", "generated", true),
            versionedParameter("Plain", "plain", false));

        final VersionedParameterContext stripped = ParameterSecretReferences.stripSensitiveValues(context);

        assertNull(getParameter(stripped, "Bound").getValue());
        assertFalse(getParameter(stripped, "Bound").isProvided());
        assertNull(getParameter(stripped, "Unbound").getValue());
        assertEquals("plain", getParameter(stripped, "Plain").getValue());
        assertEquals("secret", getParameter(context, "Bound").getValue());
        assertEquals("generated", getParameter(context, "Unbound").getValue());
    }

    @Test
    void testResolveCurrentSecretsAndReportMissingReferences() {
        final SecretReference resolvedReference = new SecretReference("provider", "Provider", "resolved", "Provider.group.resolved");
        final SecretReference missingReference = new SecretReference("provider", "Provider", "missing", "Provider.group.missing");
        final SecretReference nullableReference = new SecretReference(null, "Provider", "nullable", null);
        final VersionedParameterContext stripped = ParameterSecretReferences.stripSensitiveValues(
            versionedContext(
                versionedParameter("Resolved", "old", true),
                versionedParameter("Missing", "old", true),
                versionedParameter("Generated", "generated", true),
                versionedParameter("Nullable", "old", true),
                versionedParameter("Plain", "plain", false)));
        final SecretsManager secretsManager = mock(SecretsManager.class);
        final Secret currentSecret = secret("current");
        final Secret nullableSecret = secret("nullable");
        when(secretsManager.getSecrets(anySet(), eq(false))).thenReturn(Map.of(resolvedReference, currentSecret, nullableReference, nullableSecret));

        final ParameterSecretReferences.Resolution resolution = ParameterSecretReferences.resolve(stripped,
            Map.of("Resolved", resolvedReference, "Missing", missingReference, "Nullable", nullableReference), secretsManager);

        assertEquals("current", getParameter(resolution.parameterContext(), "Resolved").getValue());
        assertNull(getParameter(resolution.parameterContext(), "Missing").getValue());
        assertNull(getParameter(resolution.parameterContext(), "Generated").getValue());
        assertEquals("nullable", getParameter(resolution.parameterContext(), "Nullable").getValue());
        assertEquals("plain", getParameter(resolution.parameterContext(), "Plain").getValue());
        assertFalse(getParameter(resolution.parameterContext(), "Resolved").isProvided());
        assertEquals(Map.of("Missing", missingReference), resolution.unresolvedReferences());
        assertNull(getParameter(stripped, "Resolved").getValue());
    }

    private static ParameterContext parameterContext(final Parameter... parameters) {
        final ParameterContext parameterContext = mock(ParameterContext.class);
        final Map<ParameterDescriptor, Parameter> parameterMap = new HashMap<>();
        for (final Parameter parameter : parameters) {
            parameterMap.put(parameter.getDescriptor(), parameter);
        }
        when(parameterContext.getParameters()).thenReturn(parameterMap);
        return parameterContext;
    }

    private static Parameter parameter(final String name, final String value, final boolean sensitive) {
        return new Parameter.Builder()
            .name(name)
            .value(value)
            .sensitive(sensitive)
            .build();
    }

    private static Parameter assetParameter(final String name) {
        final Asset asset = mock(Asset.class);
        when(asset.getIdentifier()).thenReturn("asset-id");
        when(asset.getName()).thenReturn("asset");
        when(asset.getFile()).thenReturn(new File("target/asset"));
        return new Parameter.Builder()
            .name(name)
            .sensitive(true)
            .referencedAssets(List.of(asset))
            .build();
    }

    private static VersionedParameterContext versionedContext(final VersionedParameter... parameters) {
        final VersionedParameterContext context = new VersionedParameterContext();
        context.setIdentifier("context-id");
        context.setName("Managed Parameters");
        context.setParameters(Set.of(parameters));
        return context;
    }

    private static VersionedParameter versionedParameter(final String name, final String value, final boolean sensitive) {
        final VersionedParameter parameter = new VersionedParameter();
        parameter.setName(name);
        parameter.setValue(value);
        parameter.setSensitive(sensitive);
        return parameter;
    }

    private static VersionedParameter getParameter(final VersionedParameterContext context, final String name) {
        return context.getParameters().stream()
            .filter(parameter -> name.equals(parameter.getName()))
            .findFirst()
            .orElseThrow();
    }

    private static Secret secret(final String value) {
        final Secret secret = mock(Secret.class);
        when(secret.getValue()).thenReturn(value);
        return secret;
    }
}
