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

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.vault.hashicorp.HashiCorpVaultCommunicationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestHashiCorpVaultParameterValueProvider {

    private static final String PATH = "kv";
    private static final String CONFIG_FILE = "bootstrap-hashicorp-vault.conf";

    @Mock
    private HashiCorpVaultCommunicationService vaultCommunicationService;

    private HashiCorpVaultParameterValueProvider parameterProvider;

    @Before
    public void init() {
        final HashiCorpVaultParameterValueProvider realParameterProvider = new HashiCorpVaultParameterValueProvider();
        parameterProvider = Mockito.spy(realParameterProvider);

        doAnswer(args -> {
            parameterProvider.setPath(PATH);
            parameterProvider.setVaultCommunicationService(vaultCommunicationService);
            return null;
        }).when(parameterProvider).configure(CONFIG_FILE);
        parameterProvider.init(new ParameterValueProviderInitializationContext() {
            @Override
            public String getIdentifier() {
                return null;
            }

            @Override
            public PropertyValue getProperty(PropertyDescriptor descriptor) {
                return new StandardPropertyValue(CONFIG_FILE, null, null);
            }

            @Override
            public Map<String, String> getAllProperties() {
                return null;
            }
        });
    }

    @Test
    public void testGetParameterValue() {
        final String value = "value";
        when(vaultCommunicationService.readKeyValueSecret(PATH, "context/param")).thenReturn(Optional.of(value));

        assertEquals(value, parameterProvider.getParameterValue("context", "param"));
    }

    @Test
    public void testIsParameterDefined() {
        final String value = "value";
        when(vaultCommunicationService.readKeyValueSecret(PATH, "context/param")).thenReturn(Optional.of(value));

        assertTrue(parameterProvider.isParameterDefined("context", "param"));
    }
}
