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

package org.apache.nifi.state;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.junit.Assert;


public class MockStateManager implements StateManager {
    private final AtomicInteger versionIndex = new AtomicInteger(0);

    private StateMap localStateMap = new MockStateMap(null, -1L);
    private StateMap clusterStateMap = new MockStateMap(null, -1L);

    @Override
    public synchronized void setState(final Map<String, String> state, final Scope scope) {
        final StateMap stateMap = new MockStateMap(state, versionIndex.incrementAndGet());

        if (scope == Scope.CLUSTER) {
            clusterStateMap = stateMap;
        } else {
            localStateMap = stateMap;
        }
    }

    @Override
    public synchronized StateMap getState(final Scope scope) {
        if (scope == Scope.CLUSTER) {
            return clusterStateMap;
        } else {
            return localStateMap;
        }
    }

    @Override
    public synchronized boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) {
        if (scope == Scope.CLUSTER) {
            if (oldValue == clusterStateMap) {
                clusterStateMap = new MockStateMap(newValue, versionIndex.incrementAndGet());
                return true;
            }

            return false;
        } else {
            if (oldValue == localStateMap) {
                localStateMap = new MockStateMap(newValue, versionIndex.incrementAndGet());
                return true;
            }

            return false;
        }
    }

    @Override
    public synchronized void clear(final Scope scope) {
        setState(Collections.<String, String> emptyMap(), scope);
    }


    private String getValue(final String key, final Scope scope) {
        final StateMap stateMap = getState(scope);
        return stateMap.get(key);
    }

    //
    // assertion methods to make unit testing easier
    //
    /**
     * Ensures that the state with the given key and scope is set to the given value, or else the test will fail
     *
     * @param key the state key
     * @param value the expected value
     * @param scope the scope
     */
    public void assertStateEquals(final String key, final String value, final Scope scope) {
        Assert.assertEquals(value, getValue(key, scope));
    }

    /**
     * Ensures that the state is equal to the given values
     *
     * @param stateValues the values expected
     * @param scope the scope to compare the stateValues against
     */
    public void assertStateEquals(final Map<String, String> stateValues, final Scope scope) {
        final StateMap stateMap = getState(scope);
        Assert.assertEquals(stateValues, stateMap.toMap());
    }

    /**
     * Ensures that the state is not equal to the given values
     *
     * @param stateValues the unexpected values
     * @param scope the scope to compare the stateValues against
     */
    public void assertStateNotEquals(final Map<String, String> stateValues, final Scope scope) {
        final StateMap stateMap = getState(scope);
        Assert.assertNotSame(stateValues, stateMap.toMap());
    }

    /**
     * Ensures that the state with the given key and scope is not set to the given value, or else the test will fail
     *
     * @param key the state key
     * @param value the unexpected value
     * @param scope the scope
     */
    public void assertStateNotEquals(final String key, final String value, final Scope scope) {
        Assert.assertNotEquals(value, getValue(key, scope));
    }

    /**
     * Ensures that some value is set for the given key and scope, or else the test will fail
     *
     * @param key the state key
     * @param scope the scope
     */
    public void assertStateSet(final String key, final Scope scope) {
        Assert.assertNotNull("Expected state to be set for key " + key + " and scope " + scope + ", but it was not set", getValue(key, scope));
    }

    /**
     * Ensures that no value is set for the given key and scope, or else the test will fail
     *
     * @param key the state key
     * @param scope the scope
     */
    public void assertStateNotSet(final String key, final Scope scope) {
        Assert.assertNull("Expected state not to be set for key " + key + " and scope " + scope + ", but it was set", getValue(key, scope));
    }

    /**
     * Ensures that the state was set for the given scope, regardless of what the value was.
     *
     * @param scope the scope
     */
    public void assertStateSet(final Scope scope) {
        final StateMap stateMap = (scope == Scope.CLUSTER) ? clusterStateMap : localStateMap;
        Assert.assertEquals("Expected state to be set for Scope " + scope + ", but it was not set", -1L, stateMap.getVersion());
    }

    /**
     * Ensures that the state was not set for the given scope
     * 
     * @param scope the scope
     */
    public void assertStateNotSet(final Scope scope) {
        final StateMap stateMap = (scope == Scope.CLUSTER) ? clusterStateMap : localStateMap;
        Assert.assertNotSame("Expected state not to be set for Scope " + scope + ", but it was set", -1L, stateMap.getVersion());
    }
}
