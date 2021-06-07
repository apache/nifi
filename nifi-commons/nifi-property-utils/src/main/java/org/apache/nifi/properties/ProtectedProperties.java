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

import java.util.List;
import java.util.Properties;

/**
 * Represents a protected set of ApplicationProperties, with methods regarding which sensitive properties
 * are protected.
 * @param <T> The ApplicationProperties type
 */
public interface ProtectedProperties<T extends ApplicationProperties> {

    /**
     * Additional sensitive properties keys
     * @return Additional sensitive properties keys
     */
    String getAdditionalSensitivePropertiesKeys();

    /**
     * Returns the name of the property that specifies the additional sensitive properties keys
     * @return Name of additional sensitive properties keys
     */
    String getAdditionalSensitivePropertiesKeysName();

    /**
     * Additional sensitive properties keys
     * @return Additional sensitive properties keys
     */
    List<String> getDefaultSensitiveProperties();

    /**
     * Returns the application properties.
     * @return The application properties
     */
    T getApplicationProperties();

    /**
     * Create a new ApplicationProperties object of the generic type.
     * @param rawProperties Plain old properties
     * @return The ApplicationProperties
     */
    T createApplicationProperties(Properties rawProperties);
}
