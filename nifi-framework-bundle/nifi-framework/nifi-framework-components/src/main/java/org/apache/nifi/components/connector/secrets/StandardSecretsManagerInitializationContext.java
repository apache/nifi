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

package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.controller.flow.FlowManager;

import java.util.Collections;
import java.util.Map;

public class StandardSecretsManagerInitializationContext implements SecretsManagerInitializationContext {
    private final FlowManager flowManager;
    private final Map<String, String> properties;

    public StandardSecretsManagerInitializationContext(final FlowManager flowManager) {
        this(flowManager, Collections.emptyMap());
    }

    public StandardSecretsManagerInitializationContext(final FlowManager flowManager, final Map<String, String> properties) {
        this.flowManager = flowManager;
        this.properties = properties == null ? Collections.emptyMap() : Map.copyOf(properties);
    }

    @Override
    public FlowManager getFlowManager() {
        return flowManager;
    }

    @Override
    public String getApplicationProperty(final String key) {
        return properties.get(key);
    }
}
