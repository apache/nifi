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

import java.util.Set;

/**
 * A base interface for providing a readable set of properties.
 */
public interface ReadableProperties {

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

}
