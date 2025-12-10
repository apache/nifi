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

package org.apache.nifi.components.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StandardStepConfigurationContext implements StepConfigurationContext {
    private final String stepName;
    private final ConnectorConfigurationContext parentContext;

    public StandardStepConfigurationContext(final String stepName, final ConnectorConfigurationContext parentContext) {
        this.stepName = stepName;
        this.parentContext = parentContext;
    }

    @Override
    public ConnectorPropertyValue getProperty(final String propertyName) {
        return parentContext.getProperty(stepName, propertyName);
    }

    @Override
    public ConnectorPropertyValue getProperty(final ConnectorPropertyDescriptor propertyDescriptor) {
        return parentContext.getProperty(stepName, propertyDescriptor.getName());
    }

    @Override
    public StepConfigurationContext createWithOverrides(final Map<String, String> propertyValues) {
        final ConnectorConfigurationContext updatedContext = parentContext.createWithOverrides(stepName, propertyValues);
        return updatedContext.scopedToStep(stepName);
    }

    @Override
    public Map<String, ConnectorPropertyValue> getProperties() {
        final Map<String, ConnectorPropertyValue> properties = new HashMap<>();
        final Set<String> propertyNames = parentContext.getPropertyNames(stepName);
        for (final String propertyName : propertyNames) {
            properties.put(propertyName, getProperty(propertyName));
        }

        return properties;
    }
}
