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
package org.apache.nifi.documentation.init;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;

import java.util.Map;

public class NopStateManager implements StateManager {
    @Override
    public void setState(final Map<String, String> state, final Scope scope) {
    }

    @Override
    public StateMap getState(final Scope scope) {
        return null;
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) {
        return false;
    }

    @Override
    public void clear(final Scope scope) {
    }
}
