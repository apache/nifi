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
package org.apache.nifi.properties;

import java.util.Objects;

/**
 * A context for protected properties, encapsulating the context name and property name.
 */
public class ProtectedPropertyContext {
    private static final String DEFAULT_CONTEXT = "default";

    private final String propertyName;
    private final String contextName;

    /**
     * Creates a ProtectedPropertyContext for the given property name, with a specific context name, acting as
     * a namespace for the property.
     * @param propertyName The property name in this location
     * @param contextName A custom context name.  If null, the default context will be assigned.
     * @return A property context representing a property within a specific context
     */
    public static ProtectedPropertyContext contextFor(final String propertyName, final String contextName) {
        return new ProtectedPropertyContext(propertyName, contextName);
    }

    /**
     * Creates a ProtectedPropertyContext for the given property name, using the default context.
     * @param propertyName The property name in this location
     * @return A property context representing a property with the given name in the default context
     */
    public static ProtectedPropertyContext defaultContext(final String propertyName) {
        return new ProtectedPropertyContext(propertyName, DEFAULT_CONTEXT);
    }

    /**
     * Creates a property context with a property name and custom location.
     * @param propertyName The property name
     * @param contextName The context name.  If null, the default context will be assigned.
     */
    private ProtectedPropertyContext(final String propertyName, final String contextName) {
        this.propertyName = Objects.requireNonNull(propertyName);
        this.contextName = contextName == null ? DEFAULT_CONTEXT : contextName;
    }

    /**
     * Returns the context key, in the format [contextName]/[propertyName]
     * @return The context key
     */
    public String getContextKey() {
        return String.format("%s/%s", contextName, propertyName);
    }

    /**
     * Returns the property name
     * @return The property name
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * Returns the context name
     * @return The context name
     */
    public String getContextName() {
        return contextName;
    }
}
