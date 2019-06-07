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
package org.apache.nifi.properties.sensitive;

/**
 * Encapsulates a sensitive property name and protection scheme.
 */
public class SensitivePropertyValueDescriptor {
    private String propertyValue;
    private String protectionScheme;

    /**
     * Creates a descriptor from a property value.
     *
     * @param propertyValue property value.
     * @return SensitivePropertyValueDescriptor with given property value.
     */
    public static SensitivePropertyValueDescriptor fromValue(String propertyValue) {
        return new SensitivePropertyValueDescriptor(propertyValue, propertyValue);
    }

    /**
     * Creates a descriptor from a property value.
     *
     * @param propertyValue property value.
     * @param protectionScheme property protection or encryption scheme.
     * @return SensitivePropertyValueDescriptor with given property value.
     */
    public static SensitivePropertyValueDescriptor fromValueAndScheme(String propertyValue, String protectionScheme) {
        return new SensitivePropertyValueDescriptor(propertyValue, protectionScheme);
    }

    /**
     * @param propertyValue property value.
     * @param protectionScheme property protection or encryption scheme.
     */
    private SensitivePropertyValueDescriptor(String propertyValue, String protectionScheme) {
        this.propertyValue = propertyValue;
        this.protectionScheme = protectionScheme;
    }

    /**
     * Gets the property value.
     * @return property value.
     */
    public String getPropertyValue() {
        return propertyValue;
    }

    /**
     * Gets the property protection scheme.
     * @return property protection scheme.
     */
    public String getProtectionScheme() {
        return protectionScheme;
    }
}
