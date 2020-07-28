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

package org.apache.nifi.connectable;

import org.apache.nifi.controller.queue.FlowFileQueueFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestLocalPort {

    @Test
    public void testDefaultValues() {
        LocalPort port = getLocalInputPort();
        assertEquals(1, port.getMaxConcurrentTasks());
        assertEquals(10, port.getMaxIterations());
    }

    @Test
    public void testSetConcurrentTasks() {
        LocalPort port = getLocalInputPort(LocalPort.MAX_CONCURRENT_TASKS_PROP_NAME, "2");
        assertEquals(2, port.getMaxConcurrentTasks());
        assertEquals(10, port.getMaxIterations());
    }

    @Test
    public void testSetFlowFileLimit() {
        {
            LocalPort port = getLocalInputPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "100000");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(100, port.getMaxIterations());
        }
        {
            LocalPort port = getLocalInputPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "100001");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(101, port.getMaxIterations());
        }
        {
            LocalPort port = getLocalInputPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "99999");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(100, port.getMaxIterations());
        }
        {
            LocalPort port = getLocalInputPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "0");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(1, port.getMaxIterations());
        }
        {
            LocalPort port = getLocalInputPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "1");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(1, port.getMaxIterations());
        }
    }

    private LocalPort getLocalInputPort() {
        return getLocalPort(ConnectableType.INPUT_PORT, Collections.emptyMap());
    }

    private LocalPort getLocalInputPort(String name, String value) {
        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(name, value);
        return getLocalPort(ConnectableType.INPUT_PORT, additionalProperties);
    }

    private LocalPort getLocalPort(ConnectableType type, Map<String, String> additionalProperties) {
        NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);
        return new LocalPort("1", "test", type, null, niFiProperties);
    }

    private LocalPort getLocalOutputPort() {
        return getLocalPort(ConnectableType.OUTPUT_PORT, Collections.emptyMap());
    }

    @Test
    public void testInvalidLocalInputPort() {
        final LocalPort port = getLocalInputPort();
        assertFalse(port.isValid());
    }

    @Test
    public void testValidLocalInputPort() {
        final LocalPort port = getLocalInputPort();

        // Add an incoming relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(mock(Connectable.class))
            .destination(port)
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        // Add an outgoing relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(port)
            .destination(mock(Connectable.class))
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        assertTrue(port.isValid());
    }

    @Test
    public void testInvalidLocalOutputPort() {
        final LocalPort port = getLocalOutputPort();

        assertFalse(port.isValid());
    }

    @Test
    public void testValidLocalOutputPort() {
        final LocalPort port = getLocalOutputPort();

        // Add an incoming relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(mock(Connectable.class))
            .destination(port)
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        // Add an outgoing relationship.
        port.addConnection(new StandardConnection.Builder(null)
            .source(port)
            .destination(mock(Connectable.class))
            .relationships(Collections.singleton(Relationship.ANONYMOUS))
            .flowFileQueueFactory(mock(FlowFileQueueFactory.class))
            .build());

        assertTrue(port.isValid());
    }

}
