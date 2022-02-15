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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HashiCorpVaultTransitSensitivePropertyProviderTest {
    private static final String IDENTIFIER_KEY = "hashicorp/vault/transit/null";

    @Mock
    private BootstrapProperties bootstrapProperties;

    @Test
    public void testGetIdentifierKeyPropertiesNotFound() {
        when(bootstrapProperties.getProperty(eq(BootstrapProperties.BootstrapPropertyKey.HASHICORP_VAULT_SENSITIVE_PROPERTY_PROVIDER_CONF))).thenReturn(Optional.empty());
        final HashiCorpVaultTransitSensitivePropertyProvider provider = new HashiCorpVaultTransitSensitivePropertyProvider(bootstrapProperties);

        final String identifierKey = provider.getIdentifierKey();

        assertEquals(IDENTIFIER_KEY, identifierKey);
    }

    @Test
    public void testIsSupportedPropertiesNotFound() {
        when(bootstrapProperties.getProperty(eq(BootstrapProperties.BootstrapPropertyKey.HASHICORP_VAULT_SENSITIVE_PROPERTY_PROVIDER_CONF))).thenReturn(Optional.empty());
        final HashiCorpVaultTransitSensitivePropertyProvider provider = new HashiCorpVaultTransitSensitivePropertyProvider(bootstrapProperties);

        assertFalse(provider.isSupported());
    }
}
