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
package org.apache.nifi.security.encryption;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensitivePropertyContextFactoryTest {
    private static final String COMPONENT_ID = "0ce4d61b-e4f4-3f2a-b1d0-0d9f6b0e7a11";

    private static final String COMPONENT_TYPE = "org.apache.nifi.processors.standard.InvokeHTTP";

    private static final String PROPERTY_NAME = "Request Password";

    private static final String PARAMETER_CONTEXT_NAME = "Production";

    private static final String PARAMETER_NAME = "database.password";

    @Test
    void testForComponent() {
        final SensitivePropertyContext context = SensitivePropertyContextFactory.forComponent(COMPONENT_ID, COMPONENT_TYPE, PROPERTY_NAME);

        assertEquals(SensitivePropertyCategory.COMPONENT_PROPERTY, context.category());
        assertEquals(
                Map.of(
                        SensitivePropertyAttribute.COMPONENT_ID.getKey(), COMPONENT_ID,
                        SensitivePropertyAttribute.COMPONENT_TYPE.getKey(), COMPONENT_TYPE,
                        SensitivePropertyAttribute.PROPERTY_NAME.getKey(), PROPERTY_NAME
                ),
                context.attributes()
        );
    }

    /**
     * A call site that cannot resolve an attribute must produce the same context as the site that also cannot resolve
     * it, so null attributes are omitted rather than stored
     */
    @Test
    void testForComponentNullAttributesOmitted() {
        final SensitivePropertyContext context = SensitivePropertyContextFactory.forComponent(COMPONENT_ID, null, PROPERTY_NAME);

        assertEquals(
                Map.of(
                        SensitivePropertyAttribute.COMPONENT_ID.getKey(), COMPONENT_ID,
                        SensitivePropertyAttribute.PROPERTY_NAME.getKey(), PROPERTY_NAME
                ),
                context.attributes()
        );
    }

    @Test
    void testForRemoteProcessGroupProxyPassword() {
        final SensitivePropertyContext context = SensitivePropertyContextFactory.forRemoteProcessGroupProxyPassword(COMPONENT_ID);

        assertEquals(SensitivePropertyCategory.COMPONENT_PROPERTY, context.category());
        assertEquals(COMPONENT_ID, context.attributes().get(SensitivePropertyAttribute.COMPONENT_ID.getKey()));
        assertTrue(context.attributes().containsKey(SensitivePropertyAttribute.PROPERTY_NAME.getKey()));
        assertEquals(2, context.attributes().size());
    }

    @Test
    void testForParameter() {
        final SensitivePropertyContext context = SensitivePropertyContextFactory.forParameter(PARAMETER_CONTEXT_NAME, PARAMETER_NAME);

        assertEquals(SensitivePropertyCategory.PARAMETER, context.category());
        assertEquals(
                Map.of(
                        SensitivePropertyAttribute.PARAMETER_CONTEXT_NAME.getKey(), PARAMETER_CONTEXT_NAME,
                        SensitivePropertyAttribute.PARAMETER_NAME.getKey(), PARAMETER_NAME
                ),
                context.attributes()
        );
    }

    @Test
    void testForAuthorizationToken() {
        final SensitivePropertyContext context = SensitivePropertyContextFactory.forAuthorizationToken();

        assertEquals(SensitivePropertyCategory.AUTHORIZATION_TOKEN, context.category());
        assertEquals(Map.of(), context.attributes());
    }

    /**
     * Contexts built for the same value must be equal regardless of the call site, because a Provider may bind the
     * context to the encrypted value
     */
    @Test
    void testContextsEqualForSameValue() {
        assertEquals(
                SensitivePropertyContextFactory.forComponent(COMPONENT_ID, COMPONENT_TYPE, PROPERTY_NAME),
                SensitivePropertyContextFactory.forComponent(COMPONENT_ID, COMPONENT_TYPE, PROPERTY_NAME)
        );
        assertEquals(
                SensitivePropertyContextFactory.forParameter(PARAMETER_CONTEXT_NAME, PARAMETER_NAME),
                SensitivePropertyContextFactory.forParameter(PARAMETER_CONTEXT_NAME, PARAMETER_NAME)
        );
        assertEquals(SensitivePropertyContextFactory.forAuthorizationToken(), SensitivePropertyContextFactory.forAuthorizationToken());
    }
}
