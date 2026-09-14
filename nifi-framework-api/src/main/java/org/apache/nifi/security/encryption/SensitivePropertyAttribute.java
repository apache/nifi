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
package org.apache.nifi.security.encryption;

/**
 * Attribute describing the location of a sensitive value within the flow configuration. Each attribute has a stable key
 * suitable for supplying as authenticated additional data or audit information to an external key management service.
 * Key values are part of the extension contract and must not change once published.
 */
public enum SensitivePropertyAttribute {
    /**
     * Identifier of the component that owns the value
     */
    COMPONENT_ID("componentId"),

    /**
     * Type of the component that owns the value
     */
    COMPONENT_TYPE("componentType"),

    /**
     * Name of the property that holds the value
     */
    PROPERTY_NAME("propertyName"),

    /**
     * Name of the Parameter Context that contains the Parameter
     */
    PARAMETER_CONTEXT_NAME("parameterContextName"),

    /**
     * Name of the Parameter that holds the value
     */
    PARAMETER_NAME("parameterName");

    private final String key;

    SensitivePropertyAttribute(final String key) {
        this.key = key;
    }

    /**
     * Get the stable key for the attribute
     *
     * @return Attribute key
     */
    public String getKey() {
        return key;
    }
}
