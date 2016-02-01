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

package org.apache.nifi.controller.state.config;

import org.apache.nifi.components.state.Scope;

import java.util.HashMap;
import java.util.Map;

public class StateProviderConfiguration {
    private final String id;
    private final Scope scope;
    private final String className;
    private final Map<String, String> properties;

    public StateProviderConfiguration(final String id, final String className, final Scope scope, final Map<String, String> properties) {
        this.id = id;
        this.className = className;
        this.scope = scope;
        this.properties = new HashMap<>(properties);
    }

    public String getId() {
        return id;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Scope getScope() {
        return scope;
    }
}
