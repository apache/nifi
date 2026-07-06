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
package org.apache.nifi.registry.flow;

import org.apache.nifi.components.PropertyValue;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNifiRegistryFlowRegistryClient {

    @Test
    public void testGetProposedUri() throws Exception {
        final NifiRegistryFlowRegistryClient client = new NifiRegistryFlowRegistryClient();
        final FlowRegistryClientConfigurationContext context = mock(FlowRegistryClientConfigurationContext.class);
        final PropertyValue propertyValue = mock(PropertyValue.class);

        // Test with root context
        when(context.getProperty(NifiRegistryFlowRegistryClient.PROPERTY_URL)).thenReturn(propertyValue);
        when(propertyValue.evaluateAttributeExpressions()).thenReturn(propertyValue);
        when(propertyValue.getValue()).thenReturn("https://localhost:18443");

        // Access private method via reflection
        Method getProposedUri = NifiRegistryFlowRegistryClient.class.getDeclaredMethod("getProposedUri", FlowRegistryClientConfigurationContext.class);
        getProposedUri.setAccessible(true);
        String uri = (String) getProposedUri.invoke(client, context);

        assertEquals("https://localhost:18443", uri);

        // Test with custom context path
        when(propertyValue.getValue()).thenReturn("https://localhost:18443/my-registry");
        uri = (String) getProposedUri.invoke(client, context);

        // Before fix, this returns "https://localhost:18443" (stripped path)
        // We assert expected behavior (preserving path) to confirm failure
        assertEquals("https://localhost:18443/my-registry", uri);

        // Test with trailing slash
        when(propertyValue.getValue()).thenReturn("https://localhost:18443/my-registry/");
        uri = (String) getProposedUri.invoke(client, context);
        // URI.create(s).toString() normalizes? No, URI toString returns as is usually,
        // but the method constructs a new URI.
        // The current implementation reconstructs URI from scheme, host, port.
        // My proposed fix will append path.
    }
}
