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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MockStateManagerProviderTest {

    @Test
    void testGetStateManagerReturnsSameInstance() {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager1 = provider.getStateManager("component1", false);
        final StateManager stateManager2 = provider.getStateManager("component1", false);

        assertNotNull(stateManager1);
        assertSame(stateManager1, stateManager2);
    }

    @Test
    void testGetStateManagerReturnsDifferentInstancesForDifferentComponents() {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager1 = provider.getStateManager("component1", false);
        final StateManager stateManager2 = provider.getStateManager("component2", false);

        assertNotNull(stateManager1);
        assertNotNull(stateManager2);
        assertNotSame(stateManager1, stateManager2);
    }

    @Test
    void testOnComponentRemoved() {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager1 = provider.getStateManager("component1", false);
        assertNotNull(stateManager1);

        provider.onComponentRemoved("component1");

        final StateManager stateManager2 = provider.getStateManager("component1", false);
        assertNotNull(stateManager2);
        assertNotSame(stateManager1, stateManager2);
    }

    @Test
    void testShutdown() {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        provider.getStateManager("component1", false);
        provider.getStateManager("component2", false);

        provider.shutdown();

        final StateManager stateManager = provider.getStateManager("component1", false);
        assertNotNull(stateManager);
    }

    @Test
    void testClusterProviderEnabled() {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        assertFalse(provider.isClusterProviderEnabled());

        provider.enableClusterProvider();
        assertTrue(provider.isClusterProviderEnabled());

        provider.disableClusterProvider();
        assertFalse(provider.isClusterProviderEnabled());
    }

    @Test
    void testStateManagerSetAndGetLocalState() throws IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);

        final Map<String, String> state = new HashMap<>();
        state.put("key1", "value1");
        state.put("key2", "value2");

        stateManager.setState(state, Scope.LOCAL);

        final StateMap stateMap = stateManager.getState(Scope.LOCAL);
        assertNotNull(stateMap);
        assertTrue(stateMap.getStateVersion().isPresent());
        assertEquals("value1", stateMap.get("key1"));
        assertEquals("value2", stateMap.get("key2"));
        assertEquals(2, stateMap.toMap().size());
    }

    @Test
    void testStateManagerSetAndGetClusterState() throws IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);

        final Map<String, String> state = new HashMap<>();
        state.put("key1", "value1");
        state.put("key2", "value2");

        stateManager.setState(state, Scope.CLUSTER);

        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        assertNotNull(stateMap);
        assertTrue(stateMap.getStateVersion().isPresent());
        assertEquals("value1", stateMap.get("key1"));
        assertEquals("value2", stateMap.get("key2"));
    }

    @Test
    void testStateManagerReplace() throws IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);

        final Map<String, String> initialState = new HashMap<>();
        initialState.put("key1", "value1");
        stateManager.setState(initialState, Scope.LOCAL);

        final StateMap oldStateMap = stateManager.getState(Scope.LOCAL);

        final Map<String, String> newState = new HashMap<>();
        newState.put("key1", "value2");

        final boolean replaced = stateManager.replace(oldStateMap, newState, Scope.LOCAL);
        assertTrue(replaced);

        final StateMap updatedStateMap = stateManager.getState(Scope.LOCAL);
        assertEquals("value2", updatedStateMap.get("key1"));

        final boolean replacedAgain = stateManager.replace(oldStateMap, initialState, Scope.LOCAL);
        assertFalse(replacedAgain);
    }

    @Test
    void testStateManagerClear() throws IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);

        final Map<String, String> state = new HashMap<>();
        state.put("key1", "value1");
        stateManager.setState(state, Scope.LOCAL);

        stateManager.clear(Scope.LOCAL);

        final StateMap stateMap = stateManager.getState(Scope.LOCAL);
        assertNotNull(stateMap);
        assertTrue(stateMap.toMap().isEmpty());
    }

    @Test
    void testStateKeyDropSupported() {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager1 = provider.getStateManager("component1", false);
        assertFalse(stateManager1.isStateKeyDropSupported());

        final StateManager stateManager2 = provider.getStateManager("component2", true);
        assertTrue(stateManager2.isStateKeyDropSupported());
    }

    @Test
    void testThreadSafety() throws InterruptedException, IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);
        final int threadCount = 10;
        final int operationsPerThread = 100;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(threadCount);
        final AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        final Map<String, String> state = new HashMap<>();
                        state.put("thread", String.valueOf(threadId));
                        state.put("operation", String.valueOf(j));
                        stateManager.setState(state, Scope.LOCAL);

                        final StateMap stateMap = stateManager.getState(Scope.LOCAL);
                        if (stateMap != null && stateMap.toMap().size() == 2) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (final Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        completionLatch.await(30, TimeUnit.SECONDS);
        executorService.shutdown();

        assertEquals(threadCount * operationsPerThread, successCount.get());
        final StateMap finalState = stateManager.getState(Scope.LOCAL);
        assertNotNull(finalState);
        assertEquals(2, finalState.toMap().size());
    }

    @Test
    void testEmptyStateMap() throws IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);

        final StateMap stateMap = stateManager.getState(Scope.LOCAL);
        assertNotNull(stateMap);
        assertFalse(stateMap.getStateVersion().isPresent());
        assertTrue(stateMap.toMap().isEmpty());
    }

    @Test
    void testVersionIncrementing() throws IOException {
        final MockStateManagerProvider provider = new MockStateManagerProvider();
        final StateManager stateManager = provider.getStateManager("component1", false);

        final Map<String, String> state1 = new HashMap<>();
        state1.put("key", "value1");
        stateManager.setState(state1, Scope.LOCAL);
        final StateMap stateMap1 = stateManager.getState(Scope.LOCAL);

        final Map<String, String> state2 = new HashMap<>();
        state2.put("key", "value2");
        stateManager.setState(state2, Scope.LOCAL);
        final StateMap stateMap2 = stateManager.getState(Scope.LOCAL);

        assertTrue(stateMap1.getStateVersion().isPresent());
        assertTrue(stateMap2.getStateVersion().isPresent());

        final long version1 = Long.parseLong(stateMap1.getStateVersion().get());
        final long version2 = Long.parseLong(stateMap2.getStateVersion().get());
        assertTrue(version2 > version1);
    }
}

