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

public record PropertyGroupConfiguration(String groupName, Map<String, ConnectorValueReference> propertyValues) {

    /**
     * Retrieves the raw string value for the given property. This method only returns a value
     * for StringLiteralValue references; for other reference types, it returns null.
     *
     * @param propertyName the name of the property
     * @return the String value of the property, or null if no value is set or if the reference is not a StringLiteralValue
     */
    public String getPropertyValue(final String propertyName) {
        final ConnectorValueReference valueReference = propertyValues.get(propertyName);
        if (valueReference instanceof StringLiteralValue stringLiteral) {
            return stringLiteral.getValue();
        }
        return null;
    }

    /**
     * Creates a PropertyGroupConfiguration from raw String values. This is a convenience method that
     * converts String values to ConnectorValueReference objects with STRING_LITERAL type.
     *
     * @param groupName the name of the property group
     * @param stringPropertyValues the property values as simple Strings
     * @return a new PropertyGroupConfiguration with the values wrapped as ConnectorValueReference
     */
    public static PropertyGroupConfiguration fromStringValues(final String groupName, final Map<String, String> stringPropertyValues) {
        final Map<String, ConnectorValueReference> referenceValues = new java.util.HashMap<>();
        for (final Map.Entry<String, String> entry : stringPropertyValues.entrySet()) {
            final String value = entry.getValue();
            referenceValues.put(entry.getKey(), new StringLiteralValue(value));
        }
        return new PropertyGroupConfiguration(groupName, referenceValues);
    }
}
