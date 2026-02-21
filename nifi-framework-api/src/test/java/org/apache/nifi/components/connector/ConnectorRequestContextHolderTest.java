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
package org.apache.nifi.components.connector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConnectorRequestContextHolderTest {

    @AfterEach
    void clearContext() {
        ConnectorRequestContextHolder.clearContext();
    }

    @Test
    void testGetContextReturnsNullByDefault() {
        assertNull(ConnectorRequestContextHolder.getContext());
    }

    @Test
    void testSetAndGetContext() {
        final ConnectorRequestContext context = new StandardConnectorRequestContext(null, Map.of());
        ConnectorRequestContextHolder.setContext(context);

        assertEquals(context, ConnectorRequestContextHolder.getContext());
    }

    @Test
    void testClearContextRemovesContext() {
        final ConnectorRequestContext context = new StandardConnectorRequestContext(null, Map.of());
        ConnectorRequestContextHolder.setContext(context);
        ConnectorRequestContextHolder.clearContext();

        assertNull(ConnectorRequestContextHolder.getContext());
    }

    @Test
    void testContextIsThreadLocal() throws Exception {
        final ConnectorRequestContext context = new StandardConnectorRequestContext(null, Map.of());
        ConnectorRequestContextHolder.setContext(context);

        final ConnectorRequestContext[] otherThreadContext = new ConnectorRequestContext[1];
        final Thread thread = new Thread(() -> otherThreadContext[0] = ConnectorRequestContextHolder.getContext());
        thread.start();
        thread.join();

        assertNull(otherThreadContext[0]);
        assertEquals(context, ConnectorRequestContextHolder.getContext());
    }
}
