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
package org.apache.nifi.web.security.spring;

import java.util.Collections;
import java.util.Map;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;

/**
 *
 */
public class StandardLoginIdentityProviderConfigurationContext implements LoginIdentityProviderConfigurationContext {

    private final String identifier;
    private final Map<String, String> properties;

    public StandardLoginIdentityProviderConfigurationContext(String identifier, Map<String, String> properties) {
        this.identifier = identifier;
        this.properties = properties;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public String getProperty(String property) {
        return properties.get(property);
    }

}
