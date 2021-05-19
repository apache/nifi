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

package org.apache.nifi.components.state;

import org.apache.nifi.controller.state.manager.StandardStateManagerProvider;

import java.util.Map;

public class StatelessStateManagerProvider extends StandardStateManagerProvider {
    public StatelessStateManagerProvider() {
        super(new HashMapStateProvider(), new HashMapStateProvider());
    }

    public Map<String, StateMap> getAllComponentStates(final Scope scope) {
        final HashMapStateProvider stateProvider = getProvider(scope);
        return stateProvider.getAllComponentsState();
    }

    private HashMapStateProvider getProvider(final Scope scope) {
        if (scope == Scope.LOCAL) {
            return (HashMapStateProvider) getLocalStateProvider();
        }

        return (HashMapStateProvider) getClusterStateProvider();
    }

    public void updateComponentsStates(final Map<String, StateMap> states, final Scope scope) {
        final HashMapStateProvider stateProvider = getProvider(scope);
        stateProvider.updateAllComponentsStates(states);
    }

    public void commitUpdates() {
        getProvider(Scope.LOCAL).commit();
        getProvider(Scope.CLUSTER).commit();
    }

    public void rollbackUpdates() {
        getProvider(Scope.LOCAL).rollback();
        getProvider(Scope.CLUSTER).rollback();
    }
}
