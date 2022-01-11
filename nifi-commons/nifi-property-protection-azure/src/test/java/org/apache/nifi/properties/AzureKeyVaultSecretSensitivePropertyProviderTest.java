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

import com.azure.core.exception.AzureException;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.core.http.HttpResponse;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AzureKeyVaultSecretSensitivePropertyProviderTest {
    private static final String PROPERTY_NAME = "property.name";

    private static final String SECRET_NAME = "default-property-name";

    private static final String PROPERTY = String.class.getName();

    private static final String PROTECTED_PROPERTY = KeyVaultSecret.class.getSimpleName();

    private static final String ID = KeyVaultSecret.class.getName();

    private static final String IDENTIFIER_KEY = "azure/keyvault/secret";

    @Mock
    private SecretClient secretClient;

    @Mock
    private KeyVaultSecret keyVaultSecret;

    @Mock
    private HttpResponse httpResponse;

    private AzureKeyVaultSecretSensitivePropertyProvider provider;

    @BeforeEach
    public void setProvider() {
        provider = new AzureKeyVaultSecretSensitivePropertyProvider(secretClient);
    }

    @Test
    public void testClientNull() {
        final AzureKeyVaultSecretSensitivePropertyProvider provider = new AzureKeyVaultSecretSensitivePropertyProvider(null);
        assertNotNull(provider);
        assertFalse(provider.isSupported());
    }

    @Test
    public void testProtect() {
        when(secretClient.setSecret(eq(SECRET_NAME), eq(PROPERTY))).thenReturn(keyVaultSecret);
        when(keyVaultSecret.getId()).thenReturn(ID);

        final ProtectedPropertyContext context = ProtectedPropertyContext.defaultContext(PROPERTY_NAME);
        final String protectedProperty = provider.protect(PROPERTY, context);
        assertEquals(ID, protectedProperty);
    }

    @Test
    public void testProtectException() {
        final ProtectedPropertyContext context = ProtectedPropertyContext.defaultContext(PROPERTY_NAME);
        final String secretName = context.getContextKey();
        when(secretClient.setSecret(eq(secretName), eq(PROPERTY))).thenThrow(new AzureException());

        assertThrows(SensitivePropertyProtectionException.class, () -> provider.protect(PROPERTY, context));
    }

    @Test
    public void testUnprotect() {
        when(secretClient.getSecret(eq(SECRET_NAME))).thenReturn(keyVaultSecret);
        when(keyVaultSecret.getValue()).thenReturn(PROPERTY);

        final ProtectedPropertyContext context = ProtectedPropertyContext.defaultContext(PROPERTY_NAME);
        final String property = provider.unprotect(PROTECTED_PROPERTY, context);
        assertEquals(PROPERTY, property);
    }

    @Test
    public void testUnprotectResourceNotFoundException() {
        when(secretClient.getSecret(eq(SECRET_NAME))).thenThrow(new ResourceNotFoundException(SECRET_NAME, httpResponse));

        final ProtectedPropertyContext context = ProtectedPropertyContext.defaultContext(PROPERTY_NAME);
        assertThrows(SensitivePropertyProtectionException.class, () -> provider.unprotect(PROTECTED_PROPERTY, context));
    }

    @Test
    public void testGetIdentifierKey() {
        final String identifierKey = provider.getIdentifierKey();
        assertEquals(IDENTIFIER_KEY, identifierKey);
    }
}
