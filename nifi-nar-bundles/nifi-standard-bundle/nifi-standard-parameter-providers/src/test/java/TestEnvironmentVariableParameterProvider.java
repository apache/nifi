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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.parameter.EnvironmentVariableParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ProvidedParameterGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestEnvironmentVariableParameterProvider {
    private ParameterProvider getParameterProvider() {
        return new EnvironmentVariableParameterProvider();
    }

    private void runProviderTest(final String includePattern, final String excludePattern) throws InitializationException, IOException {
        final Map<String, String> env = System.getenv();
        final Map<String, String> filteredVariables = env.entrySet().stream()
                .filter(entry -> entry.getKey().matches(includePattern))
                .filter(entry -> excludePattern == null || !entry.getKey().matches(excludePattern))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final ParameterProvider parameterProvider = getParameterProvider();
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(EnvironmentVariableParameterProvider.INCLUDE_REGEX, includePattern);
        if (excludePattern != null) {
            properties.put(EnvironmentVariableParameterProvider.EXCLUDE_REGEX, excludePattern);
        }
        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null);

        // Verify parameter fetching
        final List<ProvidedParameterGroup> parameterGroups = parameterProvider.fetchParameters(mockConfigurationContext);
        final List<Parameter> parameters = parameterGroups.get(0).getParameters();
        assertEquals(filteredVariables.size(), parameters.size());

        // Verify config verification
        final List<ConfigVerificationResult> results = ((VerifiableParameterProvider) parameterProvider).verify(mockConfigurationContext, initContext.getLogger());

        assertEquals(1, results.size());
    }

    @Test
    public void testParameterProviderWithoutExclude() throws InitializationException, IOException {
        runProviderTest("P.*", null);
    }

    @Test
    public void testParameterProviderWithExclude() throws InitializationException, IOException {
        runProviderTest(".*", "P.*");
    }

    @Test
    public void testParameterProviderWithOverlappingExclude() throws InitializationException, IOException {
        runProviderTest("[A-Z_]+", "[J-N][\\w]+");
    }
}
