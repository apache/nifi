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
package org.apache.nifi.websocket;

import org.apache.nifi.processor.Processor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestWebSocketMessageRouters {

    @Test
    public void testRegisterProcessor() throws Exception {
        final String endpointId = "endpoint-id";
        final WebSocketMessageRouters routers = new WebSocketMessageRouters();
        assertThrows(WebSocketConfigurationException.class, () -> routers.getRouterOrFail(endpointId),
                "Should fail because no route exists with the endpointId.");

        final Processor processor1 = mock(Processor.class);
        when(processor1.getIdentifier()).thenReturn("processor-1");

        assertFalse(routers.isProcessorRegistered(endpointId, processor1));

        routers.registerProcessor(endpointId, processor1);
        assertNotNull(routers.getRouterOrFail(endpointId));

        assertTrue(routers.isProcessorRegistered(endpointId, processor1));

        routers.deregisterProcessor(endpointId, processor1);

        assertFalse(routers.isProcessorRegistered(endpointId, processor1));
    }

}
