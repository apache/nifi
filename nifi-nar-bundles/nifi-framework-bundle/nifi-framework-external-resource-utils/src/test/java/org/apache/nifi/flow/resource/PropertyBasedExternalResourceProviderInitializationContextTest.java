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
package org.apache.nifi.flow.resource;

import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
public class PropertyBasedExternalResourceProviderInitializationContextTest {
    private static final String PROVIDER_NAME = "external";
    private static final String PREFIX = "nifi.test.resources.external.provider." + PROVIDER_NAME + ".";

    @Mock
    private NiFiProperties properties;

    @Test
    public void testEmptyProperties() throws TlsException {
        final PropertyBasedExternalResourceProviderInitializationContext testSubject = getTestSubject();
        final Map<String, String> result = testSubject.getProperties();

        Mockito.verify(properties, Mockito.times(1)).getPropertiesWithPrefix(PREFIX);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGuardedPropertiesAreNotReturned() throws TlsException {
        final Map<String, String> availableProperties = new HashMap<>();
        availableProperties.put(PREFIX + "implementation", "value");
        Mockito.when(properties.getPropertiesWithPrefix(PREFIX)).thenReturn(availableProperties);

        final PropertyBasedExternalResourceProviderInitializationContext testSubject = getTestSubject();
        final Map<String, String> result = testSubject.getProperties();

        Mockito.verify(properties, Mockito.times(1)).getPropertiesWithPrefix(PREFIX);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testPropertiesWouldHaveEmptyKeyAreNotReturned() throws TlsException {
        final Map<String, String> availableProperties = new HashMap<>();
        availableProperties.put(PREFIX, "value");
        Mockito.when(properties.getPropertiesWithPrefix(PREFIX)).thenReturn(availableProperties);

        final PropertyBasedExternalResourceProviderInitializationContext testSubject = getTestSubject();
        final Map<String, String> result = testSubject.getProperties();

        Mockito.verify(properties, Mockito.times(1)).getPropertiesWithPrefix(PREFIX);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testPrefixIsRemoved() throws TlsException {
        final Map<String, String> availableProperties = new HashMap<>();
        availableProperties.put(PREFIX + "key1", "value1");
        availableProperties.put(PREFIX + "key2", "value2");
        Mockito.when(properties.getPropertiesWithPrefix(PREFIX)).thenReturn(availableProperties);

        final PropertyBasedExternalResourceProviderInitializationContext testSubject = getTestSubject();
        final Map<String, String> result = testSubject.getProperties();

        Mockito.verify(properties, Mockito.times(1)).getPropertiesWithPrefix(PREFIX);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.containsKey("key1"));
        Assertions.assertTrue(result.containsKey("key2"));
        Assertions.assertEquals("value1", result.get("key1"));
        Assertions.assertEquals("value2", result.get("key2"));
    }

    private PropertyBasedExternalResourceProviderInitializationContext getTestSubject() throws TlsException {
        return new PropertyBasedExternalResourceProviderInitializationContext(properties, PREFIX, (descriptor -> true));
    }
}