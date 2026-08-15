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
package org.apache.nifi.cdc.mysql.processors.ssl;

import com.github.shyiko.mysql.binlog.network.SSLMode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Standard implementation of Connection Properties Provider
 */
public class StandardConnectionPropertiesProvider implements ConnectionPropertiesProvider {

    private final SSLMode sslMode;

    public StandardConnectionPropertiesProvider(final SSLMode sslMode) {
        this.sslMode = Objects.requireNonNull(sslMode, "SSL Mode required");
    }

    /**
     * Get Connection Properties based on the configured SSL Mode
     *
     * @return JDBC Connection Properties
     */
    @Override
    public Map<String, String> getConnectionProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();
        properties.put(SecurityProperty.SSL_MODE.getProperty(), sslMode.toString());
        return properties;
    }
}
