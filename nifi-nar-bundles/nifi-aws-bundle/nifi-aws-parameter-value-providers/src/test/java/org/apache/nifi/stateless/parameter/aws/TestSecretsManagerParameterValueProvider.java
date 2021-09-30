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
package org.apache.nifi.stateless.parameter.aws;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ListSecretsRequest;
import com.amazonaws.services.secretsmanager.model.ListSecretsResult;
import com.amazonaws.services.secretsmanager.model.SecretListEntry;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.stateless.parameter.ParameterValueProviderInitializationContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestSecretsManagerParameterValueProvider {
    private static final String CONTEXT = "context";
    private static final String PARAMETER = "param";
    private static final String VALUE = "secret";

    private static final String CONFIG_FILE = "./conf/my-config.file";

    @Spy
    private SecretsManagerParameterValueProvider provider;

    @Mock
    private AWSSecretsManager secretsManager;

    @Before
    public void init() throws IOException {
        mockListSecrets();

        doReturn(secretsManager).when(provider).configureClient(eq(CONFIG_FILE));
        doReturn(secretsManager).when(provider).configureClient(isNull());
    }

    @Test
    public void testIsParameterDefined() {
        assertFalse(provider.isParameterDefined(CONTEXT, PARAMETER));

        provider.init(createContext(CONFIG_FILE));
        assertTrue(provider.isParameterDefined(CONTEXT, PARAMETER));

        provider.init(createContext(null));
        assertTrue(provider.isParameterDefined(CONTEXT, PARAMETER));
    }

    @Test
    public void testGetParameterValue() {
        mockGetSecretValue();

        runGetParameterValueTest(CONFIG_FILE);

        runGetParameterValueTest(null);
    }

    @Test
    public void testGetParameterValueWithoutContext() {
        mockGetSecretValue(null, PARAMETER, VALUE, true);

        runGetParameterValueTest(null, PARAMETER, CONFIG_FILE);
    }

    @Test
    public void testGetParameterValueWithMissingSecretString() {
        mockGetSecretValue(CONTEXT, PARAMETER, "value", false);

        provider.init(createContext(CONFIG_FILE));
        assertThrows(IllegalStateException.class, () -> provider.getParameterValue(CONTEXT, PARAMETER));
    }

    private void runGetParameterValueTest(final String configFileName) {
        runGetParameterValueTest(CONTEXT, PARAMETER, configFileName);
    }

    private void runGetParameterValueTest(final String context, final String parameterName, final String configFileName) {
        provider.init(createContext(configFileName));
        assertEquals(VALUE, provider.getParameterValue(context, parameterName));

        // Throw an exception because isParameterDefined should already have returned false, preventing getParameterValue from being attempted, so this would really be an error state
        assertThrows(IllegalArgumentException.class, () -> provider.getParameterValue(CONTEXT, "Does not exist"));
    }

    private void mockGetSecretValue() {
        mockGetSecretValue(CONTEXT, PARAMETER, VALUE, true);
    }

    private void mockGetSecretValue(final String context, final String parameterName, final String secretValue, final boolean hasSecretString) {
        GetSecretValueResult result = new GetSecretValueResult();
        if (hasSecretString) {
            result = result.withSecretString(secretValue);
        }
        when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest(context, parameterName)))).thenReturn(result);
    }

    private void mockListSecrets() {
        final ListSecretsResult listSecretsResult = new ListSecretsResult().withSecretList(new SecretListEntry().withName(getSecretName(CONTEXT, PARAMETER)));
        when(secretsManager.listSecrets(any(ListSecretsRequest.class))).thenReturn(listSecretsResult);
    }

    private static String getSecretName(final String context, final String paramName) {
        return context == null ? paramName : String.format("%s/%s", context, paramName);
    }

    private static ParameterValueProviderInitializationContext createContext(final String awsConfigFilename) {
        return new ParameterValueProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return null;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                return descriptor.equals(SecretsManagerParameterValueProvider.AWS_CREDENTIALS_FILE) ? new StandardPropertyValue(awsConfigFilename, null, null) : null;
            }

            @Override
            public Map<String, String> getAllProperties() {
                return null;
            }
        };
    }

    private static ArgumentMatcher<GetSecretValueRequest> matchesGetSecretValueRequest(final String context, final String parameter) {
        return new GetSecretValueRequestMatcher(getSecretName(context, parameter));
    }

    private static class GetSecretValueRequestMatcher implements ArgumentMatcher<GetSecretValueRequest> {

        private final String secretId;

        private GetSecretValueRequestMatcher(final String secretId) {
            this.secretId = secretId;
        }

        @Override
        public boolean matches(final GetSecretValueRequest argument) {
            return argument.getSecretId().equals(secretId);
        }
    }
}
