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

import java.util.Properties;
import java.util.Set;

/**
 * A base interface for configuration properties of an application (e.g. NiFi or NiFi Registry).
 */
public interface ApplicationProperties {

    /**
     * Retrieves the property value for the given property key.
     *
     * @param key the key of property value to lookup
     * @return value of property at given key or null if not found
     */
    String getProperty(String key);

    /**
     * Retrieves the property value for the given property key.
     *
     * @param key the key of property value to lookup
     * @param defaultValue The default value to use if the property does not exist
     * @return value of property at given key or null if not found
     */
    String getProperty(String key, String defaultValue);

    /**
     * Retrieves all known property keys.
     *
     * @return all known property keys
     */
    Set<String> getPropertyKeys();

    /**
     * Returns the number of properties.
     * @return The number of properties
     */
    int size();

    /**
     * Returns the application properties in a basic Properties object.
     * @return The basic Properties object
     */
    Properties toBasicProperties();

    // Following are specific properties expected for all applications

    /**
     * Keystore path for TLS configuration
     * @return The keystore path
     */
    String getKeyStorePath();

    /**
     * Keystore type for TLS configuration
     * @return The keystore type
     */
    String getKeyStoreType();

    /**
     * Keystore password for TLS configuration
     * @return The keystore password
     */
    String getKeyStorePassword();

    /**
     * Key password for TLS configuration
     * @return The key password
     */
    String getKeyPassword();

    /**
     * Truststore path for TLS configuration
     * @return The truststore path
     */
    String getTrustStorePath();

    /**
     * Truststore type for TLS configuration
     * @return The truststore type
     */
    String getTrustStoreType();

    /**
     * Truststore password for TLS configuration
     * @return The truststore password
     */
    String getTrustStorePassword();
}
