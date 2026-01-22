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

import java.util.Map;

public interface MutableConnectorConfigurationContext extends ConnectorConfigurationContext {

    /**
     * Sets the properties for the given step to the provided properties. Any existing properties
     * for the step that are not included in the provided configuration will remain unchanged.
     *
     * @param stepName the name of the configuration step
     * @param configuration the configuration to set
     * @return the result of the configuration update
     */
    ConfigurationUpdateResult setProperties(String stepName, StepConfiguration configuration);

    /**
     * Replaces all of the properties for the given step with the provided properties. Any existing properties
     * for the step that are not included in the provided configuration will be removed.
     *
     * @param stepName the name of the configuration step
     * @param configuration the configuration to set
     * @return the result of the configuration update
     */
    ConfigurationUpdateResult replaceProperties(String stepName, StepConfiguration configuration);

    /**
     * Resolves all existing property values.
     */
    void resolvePropertyValues();

    /**
     * Converts this mutable configuration context to an immutable ConnectorConfiguration.
     * @return the ConnectorConfiguration
     */
    ConnectorConfiguration toConnectorConfiguration();

    @Override
    MutableConnectorConfigurationContext createWithOverrides(String stepName, Map<String, String> propertyValues);

    /**
     * Creates a clone of this MutableConnectorConfigurationContext.
     * @return the cloned context
     */
    @Override
    MutableConnectorConfigurationContext clone();
}
