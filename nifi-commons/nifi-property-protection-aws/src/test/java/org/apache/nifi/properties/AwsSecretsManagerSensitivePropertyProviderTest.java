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
package org.apache.nifi.properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretResponse;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.nio.charset.Charset;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AwsSecretsManagerSensitivePropertyProviderTest {
    private static final String PROPERTY_NAME = "propertyName";
    private static final String PROPERTY_VALUE = "propertyValue";

    private static final String SECRET_STRING = String.format("{ \"%s\": \"%s\" }", PROPERTY_NAME, PROPERTY_VALUE);

    private static final String IDENTIFIER_KEY = "aws/secretsmanager";

    @Mock
    private SecretsManagerClient secretsManagerClient;

    private AwsSecretsManagerSensitivePropertyProvider provider;

    @BeforeEach
    public void setProvider() {
        provider = new AwsSecretsManagerSensitivePropertyProvider(secretsManagerClient);
    }

    @Test
    public void testValidateClientNull() {
        final AwsSecretsManagerSensitivePropertyProvider provider = new AwsSecretsManagerSensitivePropertyProvider(null);
        assertNotNull(provider);
    }

    @Test
    public void testValidateKeyNoSecretString() {
        final GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder()
                .secretBinary(SdkBytes.fromString("binary", Charset.defaultCharset())).build();
        when(secretsManagerClient.getSecretValue(any(Consumer.class))).thenReturn(getSecretValueResponse);

        assertThrows(SensitivePropertyProtectionException.class, () ->
                provider.unprotect("anyValue", ProtectedPropertyContext.defaultContext(PROPERTY_NAME)));
    }

    @Test
    public void testCleanUp() {
        provider.cleanUp();
        verify(secretsManagerClient).close();
    }

    @Test
    public void testProtectCreateSecret() {
        final String secretName = ProtectedPropertyContext.defaultContext(PROPERTY_NAME).getContextKey();

        when(secretsManagerClient.getSecretValue(any(Consumer.class))).thenThrow(ResourceNotFoundException.builder().message("Not found").build());

        final CreateSecretResponse createSecretResponse = CreateSecretResponse.builder()
                .name(secretName).build();
        when(secretsManagerClient.createSecret(any(Consumer.class))).thenReturn(createSecretResponse);

        final String protectedProperty = provider.protect(PROPERTY_VALUE, ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(secretName, protectedProperty);
    }

    @Test
    public void testProtectExistingSecret() {
        final String secretName = ProtectedPropertyContext.defaultContext(PROPERTY_NAME).getContextKey();
        final GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder().secretString(SECRET_STRING).build();
        when(secretsManagerClient.getSecretValue(any(Consumer.class))).thenReturn(getSecretValueResponse);

        final PutSecretValueResponse putSecretValueResponse = PutSecretValueResponse.builder()
                .name(secretName).build();
        when(secretsManagerClient.putSecretValue(any(Consumer.class))).thenReturn(putSecretValueResponse);

        final String protectedProperty = provider.protect(PROPERTY_VALUE, ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(secretName, protectedProperty);
    }

    @Test
    public void testUnprotect() {
        final GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder().secretString(SECRET_STRING).build();
        when(secretsManagerClient.getSecretValue(any(Consumer.class))).thenReturn(getSecretValueResponse);

        final String property = provider.unprotect("anyValue", ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(PROPERTY_VALUE, property);
    }


    @Test
    public void testGetIdentifierKey() {
        final String identifierKey = provider.getIdentifierKey();
        assertEquals(IDENTIFIER_KEY, identifierKey);
    }
}
