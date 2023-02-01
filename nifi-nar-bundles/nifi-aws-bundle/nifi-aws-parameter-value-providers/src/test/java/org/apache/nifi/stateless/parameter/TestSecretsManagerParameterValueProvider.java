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
package org.apache.nifi.stateless.parameter;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestSecretsManagerParameterValueProvider {
    private static final String CONTEXT = "context";
    private static final String PARAMETER = "param";
    private static final String VALUE = "secret";
    private static final String DEFAULT_SECRET_NAME = "Test";
    private static final String DEFAULT_VALUE = "DefaultValue";

    private static final String CONFIG_FILE = "./conf/my-config.file";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Spy
    private AwsSecretsManagerParameterValueProvider provider;

    @Mock
    private AWSSecretsManager secretsManager;

    @BeforeEach
    public void init() throws IOException {
        doReturn(secretsManager).when(provider).configureClient(CONFIG_FILE);
    }

    @Test
    public void testIsParameterDefined() throws IOException {
        doReturn(secretsManager).when(provider).configureClient(isNull());
        mockGetSecretValue();
        provider.init(createContext(CONFIG_FILE));

        assertTrue(provider.isParameterDefined(CONTEXT, PARAMETER));

        provider.init(createContext(null));
        assertTrue(provider.isParameterDefined(CONTEXT, PARAMETER));
    }

    @Test
    public void testGetParameterValue() throws IOException {
        doReturn(secretsManager).when(provider).configureClient(isNull());
        mockGetSecretValue();

        runGetParameterValueTest(CONFIG_FILE);

        runGetParameterValueTest(null);
    }

    @Test
    public void testGetParameterValueWithMissingSecretString() throws JsonProcessingException {
        mockGetSecretValue(CONTEXT, PARAMETER, "value", false, false);
        mockGetSecretValue(DEFAULT_SECRET_NAME, PARAMETER, DEFAULT_VALUE, false, false);

        provider.init(createContext(CONFIG_FILE));
        assertNull(provider.getParameterValue(CONTEXT, PARAMETER));
    }

    @Test
    public void testGetParameterValueWithSecretMapping() throws JsonProcessingException {
        final String mappedSecretName = "MyMappedSecretName";
        mockGetSecretValue(mappedSecretName, PARAMETER, VALUE, true, false);

        final Map<String, String> dynamicProperties = new HashMap<>();
        dynamicProperties.put(CONTEXT, mappedSecretName);
        provider.init(createContext(CONFIG_FILE, null, dynamicProperties));
        assertEquals(VALUE, provider.getParameterValue(CONTEXT, PARAMETER));
    }

    @Test
    public void testGetParameterValueWithNoDefault() throws JsonProcessingException {
        mockGetSecretValue("Does not exist", PARAMETER, null, false, true);

        provider.init(createContext(CONFIG_FILE, null, Collections.emptyMap()));

        // Nothing to fall back to here
        assertNull(provider.getParameterValue("Does not exist", PARAMETER));
    }

    private void runGetParameterValueTest(final String configFileName) throws JsonProcessingException {
        runGetParameterValueTest(CONTEXT, PARAMETER, configFileName);
    }

    private void runGetParameterValueTest(final String context, final String parameterName, final String configFileName) throws JsonProcessingException {
        mockGetSecretValue(DEFAULT_SECRET_NAME, PARAMETER, DEFAULT_VALUE, true, false);
        mockGetSecretValue("Does not exist", PARAMETER, null, false, true);

        provider.init(createContext(configFileName));
        assertEquals(VALUE, provider.getParameterValue(context, parameterName));

        // Should fall back to the default context, which does have the parameter
        assertEquals(DEFAULT_VALUE, provider.getParameterValue("Does not exist", PARAMETER));
    }

    private void mockGetSecretValue() throws JsonProcessingException {
        mockGetSecretValue(CONTEXT, PARAMETER, VALUE, true, false);
    }

    private void mockGetSecretValue(final String context, final String parameterName, final String secretValue, final boolean hasSecretString, final boolean resourceNotFound)
            throws JsonProcessingException {
        if (resourceNotFound) {
            when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest(context)))).thenThrow(new ResourceNotFoundException("Not found"));
        } else {
            GetSecretValueResult result = new GetSecretValueResult();
            if (hasSecretString) {
                result = result.withSecretString(getSecretString(parameterName, secretValue));
            }
            when(secretsManager.getSecretValue(argThat(matchesGetSecretValueRequest(context)))).thenReturn(result);
        }
    }

    private static String getSecretName(final String context) {
        return context == null ? DEFAULT_SECRET_NAME : context;
    }

    private static ParameterValueProviderInitializationContext createContext(final String awsConfigFilename) {
        return createContext(awsConfigFilename, DEFAULT_SECRET_NAME, Collections.emptyMap());
    }

    private static ParameterValueProviderInitializationContext createContext(final String awsConfigFilename, final String defaultSecretName, final Map<String, String> dynamicProperties) {
        return new ParameterValueProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return null;
            }

            @Override
            public PropertyValue getProperty(final PropertyDescriptor descriptor) {
                if (descriptor.equals(AwsSecretsManagerParameterValueProvider.AWS_CREDENTIALS_FILE)) {
                    return new StandardPropertyValue(awsConfigFilename, null, null);
                } else if (descriptor.equals(AwsSecretsManagerParameterValueProvider.DEFAULT_SECRET_NAME)) {
                    return new StandardPropertyValue(defaultSecretName, null, null);
                }
                return null;
            }

            @Override
            public Map<String, String> getAllProperties() {
                final Map<String, String> properties = new HashMap<>(dynamicProperties);
                properties.put(AwsSecretsManagerParameterValueProvider.AWS_CREDENTIALS_FILE.getName(), awsConfigFilename);
                properties.put(AwsSecretsManagerParameterValueProvider.DEFAULT_SECRET_NAME.getName(), defaultSecretName);
                return properties;
            }
        };
    }

    private String getSecretString(final String parameterName, final String parameterValue) throws JsonProcessingException {
        final Map<String, String> parameters = new HashMap<>();
        parameters.put(parameterName, parameterValue);
        return getSecretString(parameters);
    }

    private String getSecretString(final Map<String, String> parameters) throws JsonProcessingException {
        final ObjectNode root = objectMapper.createObjectNode();
        for(final Map.Entry<String, String> entry : parameters.entrySet()) {
            root.put(entry.getKey(), entry.getValue());
        }
        return objectMapper.writeValueAsString(root);
    }

    private static ArgumentMatcher<GetSecretValueRequest> matchesGetSecretValueRequest(final String context) {
        return new GetSecretValueRequestMatcher(getSecretName(context));
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
