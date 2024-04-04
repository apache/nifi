package org.apache.nifi.parameter;/*
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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestEnvironmentVariableParameterProvider {
    private ParameterProvider getParameterProvider() {
        return new EnvironmentVariableParameterProvider();
    }

    @Test
    public void testFetchParametersCommaSeparated() throws InitializationException, IOException {
        final String commaSeparatedList = "HOME, USER";
        testFetchParameters(new EnvironmentVariableParameterProvider.CommaSeparatedEnvironmentVariableInclusionStrategy(commaSeparatedList), commaSeparatedList,
                "comma-separated");

        assertThrows(IllegalArgumentException.class, () -> testFetchParameters(
                new EnvironmentVariableParameterProvider.CommaSeparatedEnvironmentVariableInclusionStrategy(commaSeparatedList), null,
                "comma-separated"));

    }

    @Test
    public void testFetchParametersRegex() throws InitializationException, IOException {
        final String regex = "H.*";
        testFetchParameters(new EnvironmentVariableParameterProvider.RegexEnvironmentVariableInclusionStrategy(regex), regex, "regex");

        assertThrows(IllegalArgumentException.class, () -> testFetchParameters(
                new EnvironmentVariableParameterProvider.RegexEnvironmentVariableInclusionStrategy(regex), null,
                "regex"));
    }

    @Test
    public void testFetchParametersIncludeAll() throws InitializationException, IOException {
        testFetchParameters(new EnvironmentVariableParameterProvider.IncludeAllEnvironmentVariableInclusionStrategy(null), null, "include-all");
    }

    private void testFetchParameters(final EnvironmentVariableParameterProvider.EnvironmentVariableInclusionStrategy inclusionStrategy,
                                     final String includeEnvironmentVariables, final String strategy) throws InitializationException, IOException {
        final Map<String, String> env = System.getenv();

        final long expectedCount = env.keySet().stream().filter(variable -> inclusionStrategy.include(variable)).count();

        final ParameterProvider parameterProvider = getParameterProvider();
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(EnvironmentVariableParameterProvider.PARAMETER_GROUP_NAME, "environment variables");
        properties.put(EnvironmentVariableParameterProvider.INCLUDE_ENVIRONMENT_VARIABLES, includeEnvironmentVariables);
        properties.put(EnvironmentVariableParameterProvider.ENVIRONMENT_VARIABLE_INCLUSION_STRATEGY, strategy);
        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null, null);

        // Verify parameter fetching
        final List<ParameterGroup> parameterGroups = parameterProvider.fetchParameters(mockConfigurationContext);
        final ParameterGroup group = parameterGroups.stream().findFirst().get();
        assertEquals(expectedCount, group.getParameters().size());
        assertEquals("environment variables", group.getGroupName());

        // Verify config verification
        final List<ConfigVerificationResult> results = ((VerifiableParameterProvider) parameterProvider).verify(mockConfigurationContext, initContext.getLogger());

        assertEquals(1, results.size());
        assertEquals(ConfigVerificationResult.Outcome.SUCCESSFUL, results.get(0).getOutcome());
    }

}
