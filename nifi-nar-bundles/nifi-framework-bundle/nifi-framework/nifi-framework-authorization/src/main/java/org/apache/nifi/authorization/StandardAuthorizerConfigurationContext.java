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
package org.apache.nifi.authorization;

import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StandardAuthorizerConfigurationContext implements AuthorizerConfigurationContext {

    private final String identifier;
    private final Map<String, String> properties;

    public StandardAuthorizerConfigurationContext(String identifier, Map<String, String> properties) {
        this.identifier = identifier;
        this.properties = Collections.unmodifiableMap(new HashMap<String, String>(properties));
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public PropertyValue getProperty(String property) {
        return new StandardPropertyValue(properties.get(property), null);
    }

}
