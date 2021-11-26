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
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterSensitivity;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TestEnvironmentVariableParameterProvider {
    private ParameterProvider getParameterProvider() {
        return new EnvironmentVariableParameterProvider();
    }

    private void runProviderTest(final String sensitivePattern, final String nonSensitivePattern, final ConfigVerificationResult.Outcome expectedOutcome) throws InitializationException, IOException {
        final Map<String, String> env = System.getenv();
        final long expectedSensitiveCount = env.keySet().stream()
                .filter(key -> sensitivePattern != null && key.matches(sensitivePattern))
                .count();
        final long expectedNonSensitiveCount = env.keySet().stream()
                .filter(key -> sensitivePattern == null || !key.matches(sensitivePattern))
                .filter(key -> nonSensitivePattern != null && key.matches(nonSensitivePattern))
                .count();

        final ParameterProvider parameterProvider = getParameterProvider();
        final MockParameterProviderInitializationContext initContext = new MockParameterProviderInitializationContext("id", "name",
                new MockComponentLog("providerId", parameterProvider));
        parameterProvider.initialize(initContext);

        final Map<PropertyDescriptor, String> properties = new HashMap<>();
        properties.put(EnvironmentVariableParameterProvider.SENSITIVE_PARAMETER_REGEX, sensitivePattern);
        properties.put(EnvironmentVariableParameterProvider.NON_SENSITIVE_PARAMETER_REGEX, nonSensitivePattern);
        final MockConfigurationContext mockConfigurationContext = new MockConfigurationContext(properties, null);

        // Verify parameter fetching
        if (expectedOutcome == ConfigVerificationResult.Outcome.FAILED) {
            assertThrows(IllegalArgumentException.class, () -> parameterProvider.fetchParameters(mockConfigurationContext));
        } else {
            final List<ProvidedParameterGroup> parameterGroups = parameterProvider.fetchParameters(mockConfigurationContext);
            final ProvidedParameterGroup sensitiveGroup = parameterGroups.stream().filter(group -> group.getGroupKey().getSensitivity() == ParameterSensitivity.SENSITIVE).findFirst().get();
            final ProvidedParameterGroup nonSensitiveGroup = parameterGroups.stream().filter(group -> group.getGroupKey().getSensitivity() == ParameterSensitivity.NON_SENSITIVE).findFirst().get();
            assertEquals(expectedSensitiveCount, sensitiveGroup.getItems().size());
            assertEquals(expectedNonSensitiveCount, nonSensitiveGroup.getItems().size());
        }

        // Verify config verification
        final List<ConfigVerificationResult> results = ((VerifiableParameterProvider) parameterProvider).verify(mockConfigurationContext, initContext.getLogger());

        assertEquals(1, results.size());
        assertEquals(expectedOutcome, results.get(0).getOutcome());
    }

    @Test
    public void testParameterProviderWithoutOverlap() throws InitializationException, IOException {
        runProviderTest("[A-M].*", ".*", ConfigVerificationResult.Outcome.SUCCESSFUL);
    }


    @Test
    public void testParameterProviderWithOverlap() throws InitializationException, IOException {
        runProviderTest("[A-P].*", "[C-Z].*", ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testParameterProviderWithOnlySensitive() throws InitializationException, IOException {
        runProviderTest("[A-P].*", null, ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    public void testParameterProviderWithOnlyNonSensitive() throws InitializationException, IOException {
        runProviderTest(null, "[N-Z].*", ConfigVerificationResult.Outcome.SUCCESSFUL);
    }
}
