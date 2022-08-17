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
package org.apache.nifi.registry.provider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Standard configuration context to be passed to onConfigured method of Providers.
 */
public class StandardProviderConfigurationContext implements ProviderConfigurationContext {

    private final Map<String,String> properties;

    public StandardProviderConfigurationContext(final Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

}
