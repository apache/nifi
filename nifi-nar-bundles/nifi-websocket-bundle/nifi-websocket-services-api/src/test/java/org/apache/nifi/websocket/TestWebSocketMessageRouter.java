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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestWebSocketMessageRouter {

    @Test
    public void testRegisterProcessor() throws Exception {
        final WebSocketMessageRouter router = new WebSocketMessageRouter("endpoint-id");

        final Processor processor1 = mock(Processor.class);
        when(processor1.getIdentifier()).thenReturn("processor-1");

        final Processor processor2 = mock(Processor.class);
        when(processor1.getIdentifier()).thenReturn("processor-2");

        router.registerProcessor(processor1);
        assertThrows(WebSocketConfigurationException.class, () -> router.registerProcessor(processor2),
                "Should fail since a processor is already registered.");

        assertTrue(router.isProcessorRegistered(processor1));
        assertFalse(router.isProcessorRegistered(processor2));

        // It's safe to call deregister even if it's not registered.
        router.deregisterProcessor(processor2);
        router.deregisterProcessor(processor1);
        // It's safe to call deregister even if it's not registered.
        router.deregisterProcessor(processor2);

    }

    @Test
    public void testSendMessage() throws Exception {
        final WebSocketMessageRouter router = new WebSocketMessageRouter("endpoint-id");

        final Processor processor1 = mock(Processor.class);
        when(processor1.getIdentifier()).thenReturn("processor-1");

        final AbstractWebSocketSession session = mock(AbstractWebSocketSession.class);
        when(session.getSessionId()).thenReturn("session-1");
        doAnswer(invocation -> {
            assertEquals("message", invocation.getArgument(0));
            return null;
        }).when(session).sendString(anyString());

        router.registerProcessor(processor1);
        router.captureSession(session);

        router.sendMessage("session-1", sender -> sender.sendString("message"));
        assertThrows(IllegalStateException.class,
                () -> router.sendMessage("session-2", sender -> sender.sendString("message")),
                "Should fail because there's no session with id session-2.");
    }
}
