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
package org.apache.nifi.configuration;

import java.util.Map;
import org.apache.nifi.controller.ConfigurationContext;

/**
 * Shared interface for various feature-specific configuration contexts which allows common code to
 * handle property retrieval without awareness of the specific implementation.
 * <p>
 * Note: This interface is <em>not</em> used as the basis for component-specific configuration contexts (see {@link ConfigurationContext}).
 */
public interface NonComponentConfigurationContext {
    /**
     * @return identifier for the caller entity
     */
    String getIdentifier();

    /**
     * Returns all properties the configuration context contains regardless
     * of whether a value has been set for them or not. If no value is present
     * then its value is {@code null}.
     *
     * @return Map of all properties
     */
    Map<String, String> getProperties();

    /**
     * Returns the value of the provided property. This method does not substitute default values, so the value returned will be {@code null} if not set.
     *
     * @param property the property to retrieve
     * @return the current property value (can be null)
     */
    String getProperty(String property);
}
