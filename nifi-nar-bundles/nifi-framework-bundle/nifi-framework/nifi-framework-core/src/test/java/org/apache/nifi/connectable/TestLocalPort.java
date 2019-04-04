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

import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestLocalPort {

    @Test
    public void testDefaultValues() {
        LocalPort port = getLocalPort("", "");
        assertEquals(1, port.getMaxConcurrentTasks());
        assertEquals(10, port.maxIterations);
    }

    @Test
    public void testSetConcurrentTasks() {
        LocalPort port = getLocalPort(LocalPort.MAX_CONCURRENT_TASKS_PROP_NAME, "2");
        assertEquals(2, port.getMaxConcurrentTasks());
        assertEquals(10, port.maxIterations);
    }

    @Test
    public void testSetFlowFileLimit() {
        {
            LocalPort port = getLocalPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "100000");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(100, port.maxIterations);
        }
        {
            LocalPort port = getLocalPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "100001");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(101, port.maxIterations);
        }
        {
            LocalPort port = getLocalPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "99999");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(100, port.maxIterations);
        }
        {
            LocalPort port = getLocalPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "0");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(1, port.maxIterations);
        }
        {
            LocalPort port = getLocalPort(LocalPort.MAX_TRANSFERRED_FLOWFILES_PROP_NAME, "1");
            assertEquals(1, port.getMaxConcurrentTasks());
            assertEquals(1, port.maxIterations);
        }
    }

    private LocalPort getLocalPort(String name, String value) {
        HashMap<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(name, value);
        NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, additionalProperties);
        return new LocalPort("1", "test", ConnectableType.INPUT_PORT, null, niFiProperties);
    }
}