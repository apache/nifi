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

/**
 * Initialization context for a {@link ConnectorConfigurationProvider}.
 * Provides the configuration properties necessary for the provider to initialize itself,
 * such as database connection strings or other external store configuration.
 *
 * <p>Properties are extracted from NiFi properties with the prefix
 * {@code nifi.components.connectors.configuration.provider.} stripped. For example,
 * a NiFi property {@code nifi.components.connectors.configuration.provider.db.url=jdbc:...}
 * becomes {@code db.url=jdbc:...} in the returned map.</p>
 */
public interface ConnectorConfigurationProviderInitializationContext {

    /**
     * Returns the configuration properties for this provider.
     *
     * @return a map of property names to values
     */
    Map<String, String> getProperties();
}
