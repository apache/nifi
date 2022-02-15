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

import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.azure.security.keyvault.keys.models.KeyProperties;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AzureKeyVaultKeySensitivePropertyProviderTest {
    private static final String PROPERTY_NAME = String.class.getSimpleName();

    private static final String PROPERTY = String.class.getName();

    private static final byte[] PROPERTY_BYTES = PROPERTY.getBytes(StandardCharsets.UTF_8);

    private static final String ENCRYPTED_PROPERTY = Integer.class.getName();

    private static final byte[] ENCRYPTED_BYTES = ENCRYPTED_PROPERTY.getBytes(StandardCharsets.UTF_8);

    private static final String PROTECTED_PROPERTY = Base64.getEncoder().withoutPadding().encodeToString(ENCRYPTED_BYTES);

    private static final String ID = KeyVaultKey.class.getSimpleName();

    private static final Properties PROPERTIES = new Properties();

    private static final EncryptionAlgorithm ALGORITHM = EncryptionAlgorithm.A256GCM;

    private static final String IDENTIFIER_KEY = "azure/keyvault/key";

    static {
        PROPERTIES.setProperty(AzureKeyVaultKeySensitivePropertyProvider.ENCRYPTION_ALGORITHM_PROPERTY, ALGORITHM.toString());
    }

    @Mock
    private CryptographyClient cryptographyClient;

    @Mock
    private KeyVaultKey keyVaultKey;

    @Mock
    private KeyProperties keyProperties;

    private AzureKeyVaultKeySensitivePropertyProvider provider;

    @BeforeEach
    public void setProvider() {
        when(keyProperties.isEnabled()).thenReturn(true);
        when(keyVaultKey.getId()).thenReturn(ID);
        when(keyVaultKey.getProperties()).thenReturn(keyProperties);
        when(keyVaultKey.getKeyOperations()).thenReturn(AzureKeyVaultKeySensitivePropertyProvider.REQUIRED_OPERATIONS);
        when(cryptographyClient.getKey()).thenReturn(keyVaultKey);

        provider = new AzureKeyVaultKeySensitivePropertyProvider(cryptographyClient, PROPERTIES);
    }

    @Test
    public void testValidateClientNull() {
        final AzureKeyVaultKeySensitivePropertyProvider provider = new AzureKeyVaultKeySensitivePropertyProvider(null, PROPERTIES);
        assertNotNull(provider);
    }

    @Test
    public void testProtect() {
        final EncryptResult encryptResult = new EncryptResult(ENCRYPTED_BYTES, ALGORITHM, ID);
        when(cryptographyClient.encrypt(eq(ALGORITHM), any(byte[].class))).thenReturn(encryptResult);

        final String protectedProperty = provider.protect(PROPERTY, ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(PROTECTED_PROPERTY, protectedProperty);
    }

    @Test
    public void testUnprotect() {
        final DecryptResult decryptResult = new DecryptResult(PROPERTY_BYTES, ALGORITHM, ID);
        when(cryptographyClient.decrypt(eq(ALGORITHM), any(byte[].class))).thenReturn(decryptResult);

        final String property = provider.unprotect(PROTECTED_PROPERTY, ProtectedPropertyContext.defaultContext(PROPERTY_NAME));
        assertEquals(PROPERTY, property);
    }

    @Test
    public void testGetIdentifierKey() {
        final String identifierKey = provider.getIdentifierKey();
        assertEquals(IDENTIFIER_KEY, identifierKey);
    }
}
