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
package org.apache.nifi.properties.configuration;

import org.apache.nifi.properties.BootstrapProperties;

import java.util.Optional;
import java.util.Properties;

/**
 * Client Provider responsible for reading Client Properties and instantiating client services
 *
 * @param <T> Client Type
 */
public interface ClientProvider<T> {
    /**
     * Get Client Properties from Bootstrap Properties
     *
     * @param bootstrapProperties Bootstrap Properties
     * @return Client Properties or empty when not configured
     */
    Optional<Properties> getClientProperties(BootstrapProperties bootstrapProperties);

    /**
     * Get Client using Client Properties
     *
     * @param properties Client Properties
     * @return Client or empty when not configured
     */
    Optional<T> getClient(Properties properties);
}
